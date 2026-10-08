package monitor

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
)

// One click verifies at most one failed Key. All other keys, round IDs, staging
// rows and frozen record modes are retained. The normal publisher still requires
// the entire credential set to complete before replacing any published bill.
func (m *Monitor) recoverAICodeWithTask(ctx context.Context, row ChannelUpstreamAccount, task string, parent upstreamRecoveryTarget) (upstreamRecoveryResult, error) {
	result := upstreamRecoveryResult{Task: task, Status: "not_needed", Message: "没有需要恢复的失败 Key"}
	var cred aiCodeWithCredential
	if err := m.openUpstreamCredential(row, &cred); err != nil {
		return result, err
	}
	cred, err := normalizeAICodeWithCredential(cred)
	if err != nil {
		return result, err
	}
	version, err := aiCodeWithCredentialSetVersion(cred)
	if err != nil {
		return result, err
	}
	selected := -1
	target := parent
	if task == "pricing" {
		if !parent.failed() {
			return result, nil
		}
		var checkpoint AICodeWithPricingCheckpoint
		err := m.storeDB.WithContext(ctx).Where("domain = ? AND account_epoch = ? AND credential_set_version = ? AND semantics_version = ? AND next_credential < total_credentials", row.Domain, newAPIUpstreamAccountEpoch(row), version, upstreamPricingSemanticsVersion).Order("updated_at DESC").First(&checkpoint).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return result, err
		}
		selected = checkpoint.NextCredential // The first failed Key has no saved page yet.
		if selected < 0 || selected >= len(cred.Slots) {
			return result, errors.New("春秋计价断点与当前 Key 集合不一致")
		}
	} else {
		var states []AICodeWithKeySyncState
		if err := m.storeDB.WithContext(ctx).Where("domain = ? AND credential_set_version = ?", row.Domain, version).Order("last_attempt_at ASC, ordinal ASC").Find(&states).Error; err != nil {
			return result, err
		}
		for _, state := range states {
			t := upstreamRecoveryTarget{model: &AICodeWithKeySyncState{}, where: "domain = ? AND slot_id = ? AND credential_set_version = ?", args: []any{row.Domain, state.SlotID, version}, slotID: state.SlotID}
			t.status, t.errorText, t.next, t.last, t.failures = state.Status, state.LastError, state.NextSyncAt, state.LastAttemptAt, state.ConsecutiveFails
			t.attemptColumn, t.nextColumn, t.errorColumn, t.failureColumn, t.statusColumn = "last_attempt_at", "next_sync_at", "last_error", "consecutive_fails", "status"
			if state.BackfillNextSyncAt == upstreamAccountIsolatedUntil {
				t.secondNextColumn = "backfill_next_sync_at"
			}
			if task == "usage_history" {
				if state.BackfillNextSyncAt == upstreamAccountIsolatedUntil && (state.Status == upstreamStatusReconnect || state.NextSyncAt == upstreamAccountIsolatedUntil) {
					return upstreamRecoveryResult{Task: task, Status: "blocked", Message: "该 Key 的近期消费也已暂停，请先检测恢复消费同步"}, nil
				}
				t.status, t.errorText, t.next, t.failures = "", state.BackfillLastError, state.BackfillNextSyncAt, state.BackfillConsecutiveFails
				t.nextColumn, t.errorColumn, t.failureColumn, t.statusColumn = "backfill_next_sync_at", "backfill_last_error", "backfill_consecutive_fails", ""
				t.secondNextColumn = ""
			}
			if !t.failed() {
				continue
			}
			for i, slot := range cred.Slots {
				if slot.SlotID == state.SlotID {
					selected = i
					break
				}
			}
			if selected < 0 {
				return result, errors.New("失败 Key 不属于当前配置")
			}
			target = t
			break
		}
		if selected < 0 {
			return result, nil
		}
		now := time.Now().Unix()
		target.parentUpdates = map[string]any{parent.nextColumn: now, parent.errorColumn: ""}
		if parent.statusColumn != "" {
			target.parentUpdates[parent.statusColumn] = upstreamStatusPending
		}
		if parent.secondNextColumn != "" {
			target.parentUpdates[parent.secondNextColumn] = now
		}
	}
	probeRow := row
	if err := m.sealUpstreamAccountCredential(&probeRow, aiCodeWithCredential{Slots: []aiCodeWithKeyCredential{cred.Slots[selected]}}); err != nil {
		return result, err
	}
	result, err = m.probeAndResumeUpstreamTask(ctx, row, probeRow, task, target)
	if err == nil && result.Status == "queued" {
		result.Message = "已检测并恢复当前失败 Key 的任务；其他 Key 不受影响，完整账单仍需正常同步校验后发布"
	}
	return result, err
}
