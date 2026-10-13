package monitor

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

// Record only after the collector's final probe and per-hour conservation
// check. This is evidence of stable observations, NOT permission to attribute
// cost: independent account reconciliation and a time-bounded binding are
// still required. Never publish money or infer historical mappings here.
func (m *Monitor) recordSub2TokenObservation(ctx context.Context, account ChannelUpstreamAccount, day int64,
	before ChannelUpstreamTokenScanCheckpoint, legacy map[int64][]ChannelUpstreamPricingHourEvidence,
	now int64, key []byte, keyID string) (ChannelUpstreamTokenScanCheckpoint, error) {
	if m.cfg.LocalSnapshotOnly {
		return ChannelUpstreamTokenScanCheckpoint{}, errUpstreamTokenReadOnly
	}
	cp, err := decodeSub2TokenCheckpoint([]byte(before.Envelope), account, day, key, keyID)
	if err != nil {
		return ChannelUpstreamTokenScanCheckpoint{}, err
	}
	if now < day+86400 || before.Revision <= 0 {
		return ChannelUpstreamTokenScanCheckpoint{}, errors.New("令牌扫描窗口尚未闭合")
	}
	if err := reconcileSub2TokenShadow(cp, legacy); err != nil {
		return ChannelUpstreamTokenScanCheckpoint{}, err
	}
	// Includes token source, hour, pricing dimension and counters, not merely
	// the day's money total; fetch timestamps intentionally do not participate.
	hash := channelCostEvidenceContentHash(cp.Rows)
	if err := m.lockUpstreamTokenWrite(ctx); err != nil {
		return ChannelUpstreamTokenScanCheckpoint{}, err
	}
	defer m.channelCostBindingMu.Unlock()
	var saved ChannelUpstreamTokenScanCheckpoint
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var currentAccount ChannelUpstreamAccount
		if err := tx.Select("domain", "provider", "base_url", "user_id", "account").Where("domain = ?", account.Domain).Take(&currentAccount).Error; err != nil {
			return err
		}
		if newAPIUpstreamAccountEpoch(currentAccount) != newAPIUpstreamAccountEpoch(account) {
			return errSub2TokenCursorConflict
		}
		current, err := readSub2TokenStoredCursor(tx, account, day)
		if err != nil {
			return err
		}
		if current.Revision != before.Revision || current.Envelope != before.Envelope || current.SuccessRevision > current.Revision {
			return errSub2TokenCursorConflict
		}
		if current.SuccessRevision == current.Revision {
			if current.SuccessHash != hash || current.SuccessKeyID != keyID {
				return errSub2TokenCursorConflict
			}
			saved = current // same complete scan/restarted finish is idempotent
			return nil
		}
		saved = current
		saved.StableScans = 1
		if current.SuccessRevision > 0 && current.SuccessHash == hash && current.SuccessKeyID == keyID && current.StableScans >= 1 {
			saved.StableScans = 2 // saturated: bounded state, no unbounded counter
		}
		saved.SuccessRevision, saved.SuccessHash, saved.SuccessKeyID = current.Revision, hash, keyID
		saved.SuccessAt = now
		result := sub2TokenCursorScope(tx.Model(&ChannelUpstreamTokenScanCheckpoint{}), account, day).
			Where("revision = ? AND envelope = ? AND COALESCE(success_revision, 0) = ?", current.Revision, current.Envelope, current.SuccessRevision).
			// Observation time is separate from page progress. UpdateColumns
			// bypasses GORM's automatic UpdatedAt hook on the staging cursor.
			UpdateColumns(map[string]any{"success_revision": saved.SuccessRevision, "success_hash": hash,
				"success_key_id": keyID, "stable_scans": saved.StableScans, "success_at": now})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errSub2TokenCursorConflict
		}
		return nil
	})
	if err != nil {
		return ChannelUpstreamTokenScanCheckpoint{}, err
	}
	return saved, nil
}
