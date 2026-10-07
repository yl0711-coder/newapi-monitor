package monitor

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const upstreamRecoveryCooldown = 5 * time.Minute

type upstreamRecoveryResult struct {
	Status  string `json:"status"`
	Task    string `json:"task"`
	Message string `json:"message"`
	RetryAt int64  `json:"retry_at,omitempty"`
}

// Column names and predicates are internal constants, never request input.
// Mutations below touch only scheduling metadata, not credentials, money,
// success timestamps, watermarks or persisted pagination checkpoints.
type upstreamRecoveryTarget struct {
	model                                                               any
	where                                                               string
	args                                                                []any
	status, errorText                                                   string
	next, last                                                          int64
	failures                                                            int
	attemptColumn, nextColumn, errorColumn, failureColumn, statusColumn string
	secondNextColumn                                                    string
	slotID                                                              string
	parentUpdates                                                       map[string]any
}

func (t upstreamRecoveryTarget) failed() bool {
	return t.status == upstreamStatusReconnect || t.status == upstreamStatusError ||
		t.next == upstreamAccountIsolatedUntil || (t.statusColumn == "" && t.errorText != "")
}

func (m *Monitor) loadUpstreamRecoveryTarget(ctx context.Context, row ChannelUpstreamAccount, task string) (upstreamRecoveryTarget, error) {
	t := upstreamRecoveryTarget{model: &ChannelUpstreamAccount{}, where: "domain = ?", args: []any{row.Domain}, statusColumn: "status"}
	switch task {
	case "balance":
		t.status, t.errorText, t.next, t.last, t.failures = row.Status, row.LastError, row.NextSyncAt, row.LastAttemptAt, row.ConsecutiveFails
		t.attemptColumn, t.nextColumn, t.errorColumn, t.failureColumn = "last_attempt_at", "next_sync_at", "last_error", "consecutive_fails"
	case "usage":
		t.status, t.errorText, t.next, t.last, t.failures = row.UsageStatus, row.UsageLastError, row.UsageNextSyncAt, row.UsageLastAttemptAt, row.UsageConsecutiveFails
		t.attemptColumn, t.nextColumn, t.errorColumn, t.failureColumn, t.statusColumn = "usage_last_attempt_at", "usage_next_sync_at", "usage_last_error", "usage_consecutive_fails", "usage_status"
		if row.UsageBackfillNextSyncAt == upstreamAccountIsolatedUntil {
			t.secondNextColumn = "usage_backfill_next_sync_at"
		}
	case "usage_history":
		t.errorText, t.next, t.last, t.failures = row.UsageBackfillLastError, row.UsageBackfillNextSyncAt, row.UsageBackfillLastAttemptAt, row.UsageBackfillConsecutiveFails
		t.attemptColumn, t.nextColumn, t.errorColumn, t.failureColumn, t.statusColumn = "usage_backfill_last_attempt_at", "usage_backfill_next_sync_at", "usage_backfill_last_error", "usage_backfill_consecutive_fails", ""
	case "funds":
		var state UpstreamFundSyncState
		t.model, t.where, t.args = &UpstreamFundSyncState{}, "domain = ? AND account_epoch = ?", []any{row.Domain, newAPIUpstreamAccountEpoch(row)}
		if err := m.storeDB.WithContext(ctx).Where(t.where, t.args...).First(&state).Error; err != nil {
			return t, err
		}
		t.status, t.errorText, t.next, t.last, t.failures = state.Status, state.LastError, state.NextSyncAt, state.LastAttemptAt, state.ConsecutiveFails
		t.attemptColumn, t.nextColumn, t.errorColumn, t.failureColumn = "last_attempt_at", "next_sync_at", "last_error", "consecutive_fails"
	case "error_logs":
		var state UpstreamErrorLogSyncState
		t.model = &UpstreamErrorLogSyncState{}
		if err := m.storeDB.WithContext(ctx).Where(t.where, t.args...).First(&state).Error; err != nil {
			return t, err
		}
		t.status, t.errorText, t.next, t.last, t.failures = state.Status, state.LastError, state.NextSyncAt, state.LastAttemptAt, state.ConsecutiveFails
		t.attemptColumn, t.nextColumn, t.errorColumn, t.failureColumn = "last_attempt_at", "next_sync_at", "last_error", "consecutive_fails"
	case "pricing":
		var state ChannelUpstreamPricingSyncState
		t.model, t.where, t.args = &ChannelUpstreamPricingSyncState{}, "domain = ? AND account_epoch = ? AND semantics_version = ?", []any{row.Domain, newAPIUpstreamAccountEpoch(row), upstreamPricingSemanticsVersion}
		if err := m.storeDB.WithContext(ctx).Where(t.where, t.args...).First(&state).Error; err != nil {
			return t, err
		}
		t.status, t.errorText, t.next, t.last, t.failures = state.Status, state.LastError, state.TailNextSyncAt, state.LastAttemptAt, state.ConsecutiveFailures
		if !state.BackfillDone && state.BackfillNextSyncAt < t.next {
			t.next = state.BackfillNextSyncAt
		}
		t.attemptColumn, t.nextColumn, t.errorColumn, t.failureColumn, t.secondNextColumn = "last_attempt_at", "tail_next_sync_at", "last_error", "consecutive_failures", "backfill_next_sync_at"
	default:
		return t, errors.New("同步任务类型无效")
	}
	return t, nil
}

func upstreamRecoverySameConfig(a, b ChannelUpstreamAccount) bool {
	return a.Domain == b.Domain && newAPIUpstreamAccountEpoch(a) == newAPIUpstreamAccountEpoch(b) &&
		a.Credential == b.Credential && a.CredentialVersion == b.CredentialVersion &&
		a.Enabled == b.Enabled && a.UsageSyncEnabled == b.UsageSyncEnabled && a.UsageAdapter == b.UsageAdapter &&
		a.BalanceUnit == b.BalanceUnit && a.UnitAssumed == b.UnitAssumed
}

func (m *Monitor) updateUpstreamRecoveryTarget(ctx context.Context, observed ChannelUpstreamAccount, target upstreamRecoveryTarget, last int64, updates map[string]any) error {
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current ChannelUpstreamAccount
		if err := tx.First(&current, "domain = ?", observed.Domain).Error; err != nil {
			return err
		}
		if !upstreamRecoverySameConfig(observed, current) {
			return errors.New("账户配置已变化，请重新检测")
		}
		result := tx.Model(target.model).Where(target.where, target.args...).Where(target.attemptColumn+" = ?", last).UpdateColumns(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errors.New("同步状态已变化，请刷新后重试")
		}
		if _, resuming := updates[target.nextColumn]; resuming && len(target.parentUpdates) > 0 {
			return tx.Model(&ChannelUpstreamAccount{}).Where("domain = ?", observed.Domain).UpdateColumns(target.parentUpdates).Error
		}
		return nil
	})
}

func (m *Monitor) recoverUpstreamTask(ctx context.Context, domain, task string) (upstreamRecoveryResult, error) {
	result := upstreamRecoveryResult{Task: task}
	// Try-only also for manual recovery: duplicate clicks must not queue behind
	// a running sync and then launch more probes when the first one finishes.
	release, err := m.tryAcquireUpstreamAccountBackground(domain)
	if err != nil {
		return result, err
	}
	defer release()
	var row ChannelUpstreamAccount
	if err := m.storeDB.WithContext(ctx).First(&row, "domain = ?", domain).Error; err != nil {
		return result, err
	}
	if !upstreamRecoveryTaskAllowed(m.cfg, row, task) {
		return result, errors.New("该任务未启用、未授权或上游不支持；恢复不会修改功能开关")
	}
	target, err := m.loadUpstreamRecoveryTarget(ctx, row, task)
	if err != nil {
		return result, err
	}
	if row.Provider == upstreamProviderAICodeWith && task != "balance" {
		return m.recoverAICodeWithTask(ctx, row, task, target)
	}
	if !target.failed() {
		result.Status, result.Message = "not_needed", "该任务未处于失败暂停状态，无需恢复"
		return result, nil
	}
	if task == "usage_history" && (row.UsageStatus == upstreamStatusReconnect || row.UsageStatus == upstreamStatusError) {
		return upstreamRecoveryResult{Task: task, Status: "blocked", Message: "近期消费同步也有异常，请先选择消费同步进行检测恢复"}, nil
	}
	return m.probeAndResumeUpstreamTask(ctx, row, row, task, target)
}

func (m *Monitor) probeAndResumeUpstreamTask(ctx context.Context, observed, probeRow ChannelUpstreamAccount, task string, target upstreamRecoveryTarget) (upstreamRecoveryResult, error) {
	now := time.Now().Unix()
	result := upstreamRecoveryResult{Task: task}
	retryAt := target.last + int64(upstreamRecoveryCooldown/time.Second)
	if target.next > now && target.next < upstreamAccountIsolatedUntil && target.next > retryAt {
		retryAt = target.next
	}
	if retryAt > now {
		result.Status, result.RetryAt, result.Message = "waiting", retryAt, "任务仍在重试冷却期，本次未请求上游"
		return result, nil
	}
	// Reserve the cooldown before HTTP, so a failed probe or process restart
	// cannot make repeated manual actions an unbounded retry loop.
	if err := m.updateUpstreamRecoveryTarget(ctx, observed, target, target.last, map[string]any{target.attemptColumn: now}); err != nil {
		return result, err
	}
	if err := m.probeUpstreamRecovery(ctx, probeRow, task); err != nil {
		result.Status, result.RetryAt = "blocked", now+int64(upstreamRecoveryCooldown/time.Second)
		if at := upstreamRetryAt(err); at > result.RetryAt {
			result.RetryAt = at
		}
		check := diagnosticFailure("同步恢复", err)
		result.Message = check.Message + "；" + check.Action
		if errors.Is(err, errUpstreamProbeRefreshRequired) {
			result.Message = err.Error()
		}
		return result, nil
	}
	updates := map[string]any{target.nextColumn: now, target.errorColumn: ""}
	if target.statusColumn != "" {
		updates[target.statusColumn] = upstreamStatusPending
	}
	if target.secondNextColumn != "" {
		updates[target.secondNextColumn] = now
	}
	// Do not erase failure history on probe success. Only a real successful
	// synchronization resets failures; recovery itself publishes no new facts.
	if err := m.updateUpstreamRecoveryTarget(ctx, observed, target, now, updates); err != nil {
		return result, err
	}
	result.Status, result.Message = "queued", "接口检测通过，已恢复排期；等待实际同步入库，历史缺口仍需补齐"
	return result, nil
}

func (m *Monitor) recoverChannelUpstreamHandler(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
	var in struct {
		Domain string `json:"domain"`
		Task   string `json:"task"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "恢复请求无效"})
		return
	}
	in.Domain = strings.ToLower(strings.TrimSpace(in.Domain))
	if in.Domain == "" || len(in.Domain) > 253 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "主域名无效"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), diagnosticTimeout)
	defer cancel()
	result, err := m.recoverUpstreamTask(ctx, in.Domain, in.Task)
	if err != nil {
		message := "恢复失败，请检查任务开关和同步状态"
		if errors.Is(err, errUpstreamAccountBusy) {
			message = "该账户正在同步或检测，本次未重复执行"
		}
		if errors.Is(err, gorm.ErrRecordNotFound) {
			message = "未找到已配置账户或对应同步任务"
		}
		c.JSON(http.StatusConflict, gin.H{"error": message})
		return
	}
	c.JSON(http.StatusOK, result)
}
