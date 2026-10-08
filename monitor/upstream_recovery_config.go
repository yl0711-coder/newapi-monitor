package monitor

import "gorm.io/gorm"

// Called inside the account-save transaction, only after a replacement
// credential has been verified for the same identity. No facts, cursors,
// checkpoints, feature switches or old identity's states are reset here.
func reactivateUpstreamAuxiliaryTasks(tx *gorm.DB, row ChannelUpstreamAccount) error {
	if !row.Enabled || !row.UsageSyncEnabled {
		return nil
	}
	epoch := newAPIUpstreamAccountEpoch(row)
	if err := tx.Model(&UpstreamFundSyncState{}).
		Where("domain = ? AND account_epoch = ? AND status IN ?", row.Domain, epoch, []string{upstreamStatusError, upstreamStatusReconnect}).
		UpdateColumns(map[string]any{"status": upstreamStatusPending, "next_sync_at": 0, "consecutive_fails": 0, "last_error": "", "updated_at": row.UpdatedAt}).Error; err != nil {
		return err
	}
	return tx.Model(&ChannelUpstreamPricingSyncState{}).
		Where("domain = ? AND account_epoch = ? AND semantics_version = ? AND status IN ?", row.Domain, epoch, upstreamPricingSemanticsVersion, []string{upstreamStatusError, upstreamStatusReconnect}).
		UpdateColumns(map[string]any{"status": upstreamStatusPending, "tail_next_sync_at": 0, "backfill_next_sync_at": 0, "consecutive_failures": 0, "last_error": "", "updated_at": row.UpdatedAt}).Error
}
