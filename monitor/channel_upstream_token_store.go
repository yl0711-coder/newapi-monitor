package monitor

// Independent staging only. Saving or resetting this cursor never publishes
// money, changes bindings, or touches the legacy pricing cursor. A reset keeps
// the row/revision as a tombstone so a delayed writer cannot resurrect it.

import (
	"bytes"
	"context"
	"errors"
	"math"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ChannelUpstreamTokenScanCheckpoint struct {
	Domain       string `gorm:"primaryKey;size:253;column:domain"`
	AccountEpoch string `gorm:"primaryKey;size:64;column:account_epoch"`
	DayTs        int64  `gorm:"primaryKey;column:day_ts"`
	Format       string `gorm:"primaryKey;size:40;column:format"`
	Revision     int64  `gorm:"column:revision"`
	Envelope     string `gorm:"type:text;column:envelope"`
	CreatedAt    int64  `gorm:"column:created_at"`
	UpdatedAt    int64  `gorm:"column:updated_at"`
	// Successful whole-day observations, not billing verification. Preserved
	// across staging resets; a page retry must never count as another scan.
	SuccessRevision int64  `gorm:"column:success_revision"`
	SuccessHash     string `gorm:"size:64;column:success_hash"`
	SuccessKeyID    string `gorm:"size:64;column:success_key_id"`
	StableScans     int    `gorm:"column:stable_scans"`
	SuccessAt       int64  `gorm:"column:success_at"`
}

func (ChannelUpstreamTokenScanCheckpoint) TableName() string {
	return "channel_upstream_token_scan_checkpoints"
}

var errSub2TokenCursorConflict = errors.New("令牌采集断点已变更，请重新加载后继续")

func sub2TokenCursorScope(db *gorm.DB, account ChannelUpstreamAccount, day int64) *gorm.DB {
	return db.Where("domain = ? AND account_epoch = ? AND day_ts = ? AND format = ?",
		account.Domain, newAPIUpstreamAccountEpoch(account), day, sub2TokenCheckpointFormat)
}

func readSub2TokenStoredCursor(db *gorm.DB, account ChannelUpstreamAccount, day int64) (ChannelUpstreamTokenScanCheckpoint, error) {
	var row ChannelUpstreamTokenScanCheckpoint
	err := sub2TokenCursorScope(db, account, day).Limit(1).Find(&row).Error
	return row, err
}

// Durable pre-invalidation before reusing a completed staging envelope. Keep
// its pages, but fence delayed finish/page writers and discard old verification.
// One atomic CAS: if this fails the caller must not start token observation.
func (m *Monitor) invalidateSub2TokenObservation(ctx context.Context, account ChannelUpstreamAccount, day int64, before ChannelUpstreamTokenScanCheckpoint, now int64) (ChannelUpstreamTokenScanCheckpoint, error) {
	if m.cfg.LocalSnapshotOnly {
		return ChannelUpstreamTokenScanCheckpoint{}, errUpstreamTokenReadOnly
	}
	if before.Domain != account.Domain || before.AccountEpoch != newAPIUpstreamAccountEpoch(account) || before.DayTs != day || before.Format != sub2TokenCheckpointFormat ||
		before.Revision <= 0 || before.Revision == math.MaxInt64 || now <= 0 {
		return ChannelUpstreamTokenScanCheckpoint{}, errSub2TokenCursorConflict
	}
	result := sub2TokenCursorScope(m.storeDB.WithContext(ctx).Model(&ChannelUpstreamTokenScanCheckpoint{}), account, day).
		Where("revision=? AND envelope=? AND COALESCE(success_revision,0)=?", before.Revision, before.Envelope, before.SuccessRevision).
		UpdateColumns(map[string]any{"revision": before.Revision + 1, "updated_at": now,
			"success_revision": 0, "success_hash": "", "success_key_id": "", "stable_scans": 0, "success_at": 0})
	if result.Error != nil {
		return ChannelUpstreamTokenScanCheckpoint{}, result.Error
	}
	if result.RowsAffected != 1 {
		return ChannelUpstreamTokenScanCheckpoint{}, errSub2TokenCursorConflict
	}
	after := before
	after.Revision, after.UpdatedAt = before.Revision+1, now
	after.SuccessRevision, after.SuccessHash, after.SuccessKeyID, after.StableScans, after.SuccessAt = 0, "", "", 0, 0
	return after, nil
}

func (m *Monitor) loadSub2TokenScanCheckpoint(ctx context.Context, account ChannelUpstreamAccount, day int64, key []byte, keyID string) (ChannelUpstreamTokenScanCheckpoint, error) {
	if err := validateSub2TokenContext(account, day, key, keyID); err != nil {
		return ChannelUpstreamTokenScanCheckpoint{}, err
	}
	row, err := readSub2TokenStoredCursor(m.storeDB.WithContext(ctx), account, day)
	if err != nil {
		return row, err
	}
	if row.Envelope != "" {
		if _, err := decodeSub2TokenCheckpoint([]byte(row.Envelope), account, day, key, keyID); err != nil {
			return ChannelUpstreamTokenScanCheckpoint{}, err
		}
	}
	return row, nil
}

func (m *Monitor) saveSub2TokenScanPage(ctx context.Context, account ChannelUpstreamAccount, day int64, page sub2PricingPage, expectedRevision, now int64, key []byte, keyID string) (ChannelUpstreamTokenScanCheckpoint, error) {
	if m.cfg.LocalSnapshotOnly {
		return ChannelUpstreamTokenScanCheckpoint{}, errUpstreamTokenReadOnly
	}
	before, err := m.loadSub2TokenScanCheckpoint(ctx, account, day, key, keyID)
	if err != nil {
		return ChannelUpstreamTokenScanCheckpoint{}, err
	}
	if before.Revision != expectedRevision || expectedRevision < 0 {
		return ChannelUpstreamTokenScanCheckpoint{}, errSub2TokenCursorConflict
	}
	// Hashing/aggregation stays outside the SQLite write transaction.
	next, err := advanceSub2TokenCheckpoint([]byte(before.Envelope), account, page, day, now, key, keyID)
	if err != nil {
		return ChannelUpstreamTokenScanCheckpoint{}, err
	}
	return m.commitSub2TokenCursor(ctx, account, day, before, next, now, false)
}

// Reset is explicit and CAS-protected. Corrupt staging can be cleared, but no
// published cost/bill is deleted. There is intentionally no HTTP reset route.
func (m *Monitor) resetSub2TokenScanCheckpoint(ctx context.Context, account ChannelUpstreamAccount, day, expectedRevision, now int64, key []byte, keyID string) (ChannelUpstreamTokenScanCheckpoint, error) {
	if m.cfg.LocalSnapshotOnly {
		return ChannelUpstreamTokenScanCheckpoint{}, errUpstreamTokenReadOnly
	}
	if err := validateSub2TokenContext(account, day, key, keyID); err != nil {
		return ChannelUpstreamTokenScanCheckpoint{}, err
	}
	before, err := readSub2TokenStoredCursor(m.storeDB.WithContext(ctx), account, day)
	if err != nil {
		return ChannelUpstreamTokenScanCheckpoint{}, err
	}
	if before.Revision != expectedRevision || expectedRevision < 0 {
		return ChannelUpstreamTokenScanCheckpoint{}, errSub2TokenCursorConflict
	}
	return m.commitSub2TokenCursor(ctx, account, day, before, nil, now, true)
}

func (m *Monitor) commitSub2TokenCursor(ctx context.Context, account ChannelUpstreamAccount, day int64, before ChannelUpstreamTokenScanCheckpoint, next []byte, now int64, reset bool) (ChannelUpstreamTokenScanCheckpoint, error) {
	if m.cfg.LocalSnapshotOnly {
		return ChannelUpstreamTokenScanCheckpoint{}, errUpstreamTokenReadOnly
	}
	if now <= 0 || before.Revision < 0 || before.Revision == math.MaxInt64 || len(next) > upstreamPricingMaxCheckpointBytes {
		return ChannelUpstreamTokenScanCheckpoint{}, errors.New("令牌断点写入参数无效")
	}
	if err := m.lockUpstreamTokenWrite(ctx); err != nil {
		return ChannelUpstreamTokenScanCheckpoint{}, err
	}
	defer m.channelCostBindingMu.Unlock()
	var saved ChannelUpstreamTokenScanCheckpoint
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
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
		if current.Revision != before.Revision || current.Envelope != before.Envelope {
			return errSub2TokenCursorConflict
		}
		if !reset && bytes.Equal([]byte(current.Envelope), next) {
			saved = current // exact retry: no timestamp or revision churn
			return nil
		}
		saved = current
		saved.Domain, saved.AccountEpoch = account.Domain, newAPIUpstreamAccountEpoch(account)
		saved.DayTs, saved.Format = day, sub2TokenCheckpointFormat
		saved.Revision, saved.Envelope, saved.UpdatedAt = before.Revision+1, string(next), now
		if before.Revision == 0 {
			saved.CreatedAt = now
			result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&saved)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return errSub2TokenCursorConflict
			}
			return nil
		}
		result := sub2TokenCursorScope(tx.Model(&ChannelUpstreamTokenScanCheckpoint{}), account, day).
			Where("revision = ? AND envelope = ?", before.Revision, before.Envelope).
			Updates(map[string]any{"revision": saved.Revision, "envelope": saved.Envelope, "updated_at": now})
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
