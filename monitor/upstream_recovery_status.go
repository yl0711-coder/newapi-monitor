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

type upstreamRecoveryTaskView struct {
	Task          string `json:"task"`
	Label         string `json:"label"`
	State         string `json:"state"`
	Note          string `json:"note"`
	CanRecover    bool   `json:"can_recover"`
	LastSuccessAt int64  `json:"last_success_at"`
	RetryAt       int64  `json:"retry_at,omitempty"`
}

// This is a local, read-only projection, not another scheduler or probe. Keep
// eligibility identical to the POST endpoint; never infer it in the browser.
func (m *Monitor) upstreamRecoveryStatus(ctx context.Context, row ChannelUpstreamAccount, now int64) ([]upstreamRecoveryTaskView, error) {
	tasks := []upstreamRecoveryTaskView{
		{Task: "balance", Label: "余额"}, {Task: "usage", Label: "消费账单"},
		{Task: "usage_history", Label: "历史消费补采"}, {Task: "funds", Label: "充值及额度调整"},
		{Task: "pricing", Label: "计价明细"}, {Task: "error_logs", Label: "错误日志"},
	}
	for i := range tasks {
		v := &tasks[i]
		if !upstreamRecoveryTaskAllowed(m.cfg, row, v.Task) {
			v.State, v.Note = "disabled", "未启用、未授权或该上游不支持"
			if m.cfg.LocalSnapshotOnly {
				v.Note = "离线快照预览，不执行上游同步"
			}
			continue
		}
		t, err := m.loadUpstreamRecoveryTarget(ctx, row, v.Task)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			v.State, v.Note = "pending", "尚未建立同步记录"
			continue
		}
		if err != nil {
			return nil, err
		}
		// Published account timestamps are authoritative, not a single Key's
		// success. A multi-Key round may still be incomplete after that success.
		v.LastSuccessAt = t.success
		blocked := v.Task == "usage_history" && (row.UsageStatus == upstreamStatusReconnect || row.UsageStatus == upstreamStatusError)
		if row.Provider == upstreamProviderAICodeWith && (v.Task == "usage" || v.Task == "usage_history") {
			t, blocked, err = m.aicodeWithRecoveryStatusTarget(ctx, row, v.Task, t)
			if err != nil {
				return nil, err
			}
		}
		projectUpstreamRecoveryStatus(v, t, blocked, now)
	}
	return tasks, nil
}

func projectUpstreamRecoveryStatus(v *upstreamRecoveryTaskView, t upstreamRecoveryTarget, blocked bool, now int64) {
	v.State, v.Note = "pending", "等待后台同步；检测通过不代表数据已入库"
	if !t.failed() {
		if t.success > 0 && t.success >= t.last && (t.status == upstreamStatusOK || t.status == "") {
			v.State, v.Note = "synced", "最近一批已同步，不代表历史覆盖完整"
		}
		switch t.status {
		case "paging":
			v.State, v.Note = "collecting", "正在分页读取，尚未完成本批校验"
		case "backfilling":
			v.State, v.Note = "collecting", "历史仍在补采，不代表覆盖完整"
		case "pending_verification":
			v.State, v.Note = "verifying", "已完整读取，等待下一轮一致性复核"
		case "reconcile_blocked":
			v.State, v.Note = "mismatch", "数据已读取但对账未闭合，请查看数据同步状态；重复检测不能消除差异"
		case upstreamStatusUnsupported, upstreamStatusDisabled:
			v.State, v.Note = "disabled", "任务不可用，请检查同步配置或接口支持情况"
		}
		if v.Task == "usage_history" && t.done {
			v.State, v.Note = "complete", "历史任务已完成；最终覆盖范围以数据同步状态的校验为准"
		}
		return
	}
	if blocked {
		v.State, v.Note = "blocked", "请先检查恢复近期消费同步"
		return
	}
	v.RetryAt = t.last + int64(upstreamRecoveryCooldown/time.Second)
	if t.next > now && t.next < upstreamAccountIsolatedUntil && t.next > v.RetryAt {
		v.RetryAt = t.next
	}
	if v.RetryAt > now {
		v.State, v.Note = "cooling", "冷却中，不重复请求上游"
		if t.next == upstreamAccountIsolatedUntil {
			v.Note = "自动同步已暂停，检查恢复仍在冷却期"
		} else if t.next > now {
			v.Note = "已安排自动重试，等待冷却结束"
			if upstreamNeedsRecoveryProbe(upstreamStatusError, t.failures) {
				v.Note = "连续失败，等待低频检测；达到重试上限后将暂停"
			}
		}
		return
	}
	v.RetryAt = 0
	v.State, v.Note, v.CanRecover = "failed", "检查已保存配置；通过后恢复后台排期", true
	if t.status == upstreamStatusReconnect {
		v.Note = "授权可能需要更新，可先检查；失效时需重新保存并验证"
	} else if t.next == upstreamAccountIsolatedUntil {
		v.Note = "自动重试已暂停，检查通过后可恢复排期；已有账单和断点保留"
		if t.failures >= upstreamRecoveryFailureLimit {
			v.Note = "已达到自动重试上限，需检查恢复；不会无限请求上游"
		}
	} else {
		v.Note = "已到后台重试时间，等待调度；也可检查恢复，不代表同步已完成"
	}
}

// Match recoverAICodeWithTask's first failed current Key and cooldown. Never
// offer recovery for removed keys or claim the whole account has been repaired.
func (m *Monitor) aicodeWithRecoveryStatusTarget(ctx context.Context, row ChannelUpstreamAccount, task string, parent upstreamRecoveryTarget) (upstreamRecoveryTarget, bool, error) {
	var cred aiCodeWithCredential
	if err := m.openUpstreamCredential(row, &cred); err != nil {
		return parent, false, err
	}
	cred, err := normalizeAICodeWithCredential(cred)
	if err != nil {
		return parent, false, err
	}
	version, err := aiCodeWithCredentialSetVersion(cred)
	if err != nil {
		return parent, false, err
	}
	var states []AICodeWithKeySyncState
	if err := m.storeDB.WithContext(ctx).Where("domain = ? AND credential_set_version = ?", row.Domain, version).Order("last_attempt_at ASC, ordinal ASC").Find(&states).Error; err != nil {
		return parent, false, err
	}
	for _, state := range states {
		t := upstreamRecoveryTarget{status: state.Status, errorText: state.LastError, next: state.NextSyncAt, last: state.LastAttemptAt, statusColumn: "status", success: parent.success, failures: state.ConsecutiveFails}
		if task == "usage_history" {
			if state.BackfillNextSyncAt == upstreamAccountIsolatedUntil && (state.Status == upstreamStatusReconnect || state.NextSyncAt == upstreamAccountIsolatedUntil) {
				return upstreamRecoveryTarget{next: upstreamAccountIsolatedUntil}, true, nil
			}
			t.status, t.statusColumn, t.errorText, t.next = "", "", state.BackfillLastError, state.BackfillNextSyncAt
			t.failures = state.BackfillConsecutiveFails
		}
		if t.failed() {
			return t, false, nil
		}
	}
	// A stale parent error alone does not identify a recoverable Key.
	if parent.failed() {
		parent.status, parent.errorText, parent.next = upstreamStatusPending, "", 0
	}
	return parent, false, nil
}

func (m *Monitor) upstreamRecoveryStatusHandler(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	domain := strings.ToLower(strings.TrimSpace(c.Query("domain")))
	if domain == "" || len(domain) > 253 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "主域名无效"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	var row ChannelUpstreamAccount
	if err := m.storeDB.WithContext(ctx).First(&row, "domain = ?", domain).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "同步状态暂不可用，请稍后刷新"})
		return
	}
	tasks, err := m.upstreamRecoveryStatus(ctx, row, time.Now().Unix())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "同步状态暂不可用，请稍后刷新"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"domain": domain, "account_epoch": newAPIUpstreamAccountEpoch(row), "tasks": tasks})
}
