package monitor

import (
	"fmt"
	"gorm.io/gorm"
)

// Changing the account's current conversion must not reprice an already
// published hour (or the hours covered by an older daily bill).
func preserveAICodeWithRecordUnits(tx *gorm.DB, domain string, round AICodeWithUsageRound, buckets map[int64]ChannelUpstreamUsageHour) error {
	var previous []ChannelUpstreamUsageHour
	if err := tx.Where("domain = ? AND hour_ts >= ? AND hour_ts < ?", domain, round.WindowFrom, round.WindowTo).Order("hour_ts ASC").Find(&previous).Error; err != nil {
		return err
	}
	var lastEnd int64
	for _, old := range previous {
		seconds := old.BucketSeconds
		if seconds <= 0 {
			seconds = 3600
		}
		if lastEnd > old.HourTs {
			return fmt.Errorf("AICodeWith 旧账单换算证据重叠，拒绝发布")
		}
		lastEnd = old.HourTs + seconds
		if !validUpstreamEconomicUnit(old.UnitPerUSD) {
			continue
		}
		for ts := old.HourTs; ts < lastEnd && ts < round.WindowTo; ts += 3600 {
			if bucket, ok := buckets[ts]; ok {
				bucket.UnitPerUSD = old.UnitPerUSD
				bucket.CostUSD = bucket.Quota / old.UnitPerUSD
				buckets[ts] = bucket
			}
		}
	}
	return nil
}

// Preserve the previous representation before replacing it atomically. The
// append-only archive is not part of live sums; rollback can never double count
// an old daily bill and the new hourly bills for the same day.
func archiveAICodeWithRepresentationChange(tx *gorm.DB, row ChannelUpstreamAccount, round AICodeWithUsageRound, now int64) error {
	var previous []ChannelUpstreamUsageHour
	if err := tx.Where("domain = ? AND hour_ts >= ? AND hour_ts < ?", row.Domain, round.WindowFrom, round.WindowTo).Find(&previous).Error; err != nil {
		return err
	}
	for _, old := range previous {
		if (old.SourceKind == upstreamUsageAdapterAICodeWithRecord) == round.RecordMode {
			continue
		}
		archive := ChannelUpstreamUsageArchive{SourceKind: old.SourceKind, Provisional: old.Provisional, SourceCostUnits: old.SourceCostUnits, UnitPerUSD: old.UnitPerUSD, ArchiveBatchID: round.RoundID, Domain: row.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(row), ArchivedAt: now, ArchiveReason: "aicodewith_bill_granularity", HourTs: old.HourTs, BucketSeconds: old.BucketSeconds, Requests: old.Requests, Tokens: old.Tokens, Quota: old.Quota, CostUSD: old.CostUSD, FetchedAt: old.FetchedAt, Provider: old.Provider}
		if err := tx.Create(&archive).Error; err != nil {
			return err
		}
	}
	return nil
}
