package monitor

// This file preserves finalized upstream account data across identity changes.
// The live usage/error tables intentionally contain only the currently
// configured account for a domain. Before that namespace is reset, every row is
// copied into these append-only archives in the same SQLite transaction.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const upstreamArchiveReasonIdentityChange = "account_identity_changed"

// ChannelUpstreamUsageArchive preserves the exact legacy aggregate and its
// original provider/account epoch. It is never read into the current account's
// totals automatically; a later audit/export can select the old epoch without
// risking cross-account double counting.
type ChannelUpstreamUsageArchive struct {
	ID             uint64  `gorm:"primaryKey;autoIncrement"`
	ArchiveBatchID string  `gorm:"size:64;column:archive_batch_id;uniqueIndex:idx_upstream_usage_archive_batch_row,priority:1"`
	Domain         string  `gorm:"size:253;column:domain;index:idx_upstream_usage_archive_domain_hour,priority:1"`
	AccountEpoch   string  `gorm:"size:64;column:account_epoch;index"`
	ArchivedAt     int64   `gorm:"column:archived_at;index"`
	ArchiveReason  string  `gorm:"size:48;column:archive_reason"`
	HourTs         int64   `gorm:"column:hour_ts;uniqueIndex:idx_upstream_usage_archive_batch_row,priority:2;index:idx_upstream_usage_archive_domain_hour,priority:2"`
	BucketSeconds  int64   `gorm:"column:bucket_seconds"`
	Requests       int64   `gorm:"column:requests"`
	Tokens         int64   `gorm:"column:tokens"`
	Quota          float64 `gorm:"column:quota"`
	CostUSD        float64 `gorm:"column:cost_usd"`
	FetchedAt      int64   `gorm:"column:fetched_at"`
	Provider       string  `gorm:"size:24;column:provider;uniqueIndex:idx_upstream_usage_archive_batch_row,priority:3"`
}

func (*ChannelUpstreamUsageArchive) BeforeUpdate(_ *gorm.DB) error {
	return errors.New("channel_upstream_usage_archives is append-only")
}

func (*ChannelUpstreamUsageArchive) BeforeDelete(_ *gorm.DB) error {
	return errors.New("channel_upstream_usage_archives is append-only")
}

// ChannelUpstreamErrorLogArchive stores the complete previous evidence row as
// JSON plus a content hash. PayloadJSON contains no credential; it is the same
// already-persisted diagnostic evidence from ChannelUpstreamErrorLog.
type ChannelUpstreamErrorLogArchive struct {
	ID              uint64 `gorm:"primaryKey;autoIncrement"`
	ArchiveBatchID  string `gorm:"size:64;column:archive_batch_id;uniqueIndex:idx_upstream_error_archive_batch_row,priority:1"`
	Domain          string `gorm:"size:253;column:domain;index:idx_upstream_error_archive_domain_created,priority:1"`
	AccountEpoch    string `gorm:"size:64;column:account_epoch;index"`
	EventKey        string `gorm:"size:64;column:event_key;uniqueIndex:idx_upstream_error_archive_batch_row,priority:2"`
	SourceCreatedAt int64  `gorm:"column:source_created_at;index:idx_upstream_error_archive_domain_created,priority:2"`
	PayloadJSON     string `gorm:"type:text;column:payload_json"`
	PayloadHash     string `gorm:"size:64;column:payload_hash"`
	ArchivedAt      int64  `gorm:"column:archived_at;index"`
	ArchiveReason   string `gorm:"size:48;column:archive_reason"`
}

func (*ChannelUpstreamErrorLogArchive) BeforeUpdate(_ *gorm.DB) error {
	return errors.New("channel_upstream_error_log_archives is append-only")
}

func (*ChannelUpstreamErrorLogArchive) BeforeDelete(_ *gorm.DB) error {
	return errors.New("channel_upstream_error_log_archives is append-only")
}

// migrateUpstreamIdentityArchives enforces archive immutability below the ORM
// layer as well. Raw SQL, a future maintenance path, or an accidental
// Unscoped delete must not be able to rewrite finalized account evidence.
func migrateUpstreamIdentityArchives(db *gorm.DB) error {
	if db == nil {
		return errors.New("上游历史归档迁移缺少数据库连接")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		for _, statement := range []string{
			`CREATE TRIGGER IF NOT EXISTS channel_upstream_usage_archives_reject_update
			 BEFORE UPDATE ON channel_upstream_usage_archives
			 BEGIN SELECT RAISE(ABORT, 'channel_upstream_usage_archives is append-only'); END`,
			`CREATE TRIGGER IF NOT EXISTS channel_upstream_usage_archives_reject_delete
			 BEFORE DELETE ON channel_upstream_usage_archives
			 BEGIN SELECT RAISE(ABORT, 'channel_upstream_usage_archives is append-only'); END`,
			`CREATE TRIGGER IF NOT EXISTS channel_upstream_error_log_archives_reject_update
			 BEFORE UPDATE ON channel_upstream_error_log_archives
			 BEGIN SELECT RAISE(ABORT, 'channel_upstream_error_log_archives is append-only'); END`,
			`CREATE TRIGGER IF NOT EXISTS channel_upstream_error_log_archives_reject_delete
			 BEFORE DELETE ON channel_upstream_error_log_archives
			 BEGIN SELECT RAISE(ABORT, 'channel_upstream_error_log_archives is append-only'); END`,
		} {
			if err := tx.Exec(statement).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func upstreamIdentityArchiveBatchID(previous, next ChannelUpstreamAccount, archivedAt int64) string {
	payload := strings.Join([]string{
		previous.Domain, newAPIUpstreamAccountEpoch(previous), newAPIUpstreamAccountEpoch(next), strconv.FormatInt(archivedAt, 10),
	}, "\x00")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func archiveUpstreamIdentityDataTx(tx *gorm.DB, next ChannelUpstreamAccount, archivedAt int64) error {
	if tx == nil {
		return errors.New("归档上游历史数据缺少数据库事务")
	}
	var previous ChannelUpstreamAccount
	err := tx.First(&previous, "domain = ?", next.Domain).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if archivedAt <= 0 {
		return errors.New("归档上游历史数据缺少有效时间")
	}
	epoch := newAPIUpstreamAccountEpoch(previous)
	batchID := upstreamIdentityArchiveBatchID(previous, next, archivedAt)

	var usage []ChannelUpstreamUsageHour
	if err := tx.Where("domain = ?", previous.Domain).Order("hour_ts ASC").Find(&usage).Error; err != nil {
		return fmt.Errorf("读取待归档上游消费: %w", err)
	}
	usageArchive := make([]ChannelUpstreamUsageArchive, 0, len(usage))
	for _, row := range usage {
		usageArchive = append(usageArchive, ChannelUpstreamUsageArchive{
			ArchiveBatchID: batchID, Domain: row.Domain, AccountEpoch: epoch,
			ArchivedAt: archivedAt, ArchiveReason: upstreamArchiveReasonIdentityChange,
			HourTs: row.HourTs, BucketSeconds: row.BucketSeconds, Requests: row.Requests,
			Tokens: row.Tokens, Quota: row.Quota, CostUSD: row.CostUSD,
			FetchedAt: row.FetchedAt, Provider: row.Provider,
		})
	}
	if len(usageArchive) > 0 {
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(usageArchive, 200).Error; err != nil {
			return fmt.Errorf("归档上游消费: %w", err)
		}
	}

	var errorLogs []ChannelUpstreamErrorLog
	if err := tx.Where("domain = ?", previous.Domain).Order("created_at ASC,event_key ASC").Find(&errorLogs).Error; err != nil {
		return fmt.Errorf("读取待归档上游错误证据: %w", err)
	}
	errorArchive := make([]ChannelUpstreamErrorLogArchive, 0, len(errorLogs))
	for _, row := range errorLogs {
		payload, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("序列化上游错误证据: %w", err)
		}
		digest := sha256.Sum256(payload)
		errorArchive = append(errorArchive, ChannelUpstreamErrorLogArchive{
			ArchiveBatchID: batchID, Domain: row.Domain, AccountEpoch: epoch,
			EventKey: row.EventKey, SourceCreatedAt: row.CreatedAt,
			PayloadJSON: string(payload), PayloadHash: hex.EncodeToString(digest[:]),
			ArchivedAt: archivedAt, ArchiveReason: upstreamArchiveReasonIdentityChange,
		})
	}
	if len(errorArchive) > 0 {
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(errorArchive, 100).Error; err != nil {
			return fmt.Errorf("归档上游错误证据: %w", err)
		}
	}
	return nil
}
