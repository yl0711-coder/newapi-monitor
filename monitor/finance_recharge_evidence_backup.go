//go:build unix

package monitor

import (
	"context"
	"errors"
	"math"
	"sort"
	"time"
)

const (
	financeRechargeEvidenceDomainLimit = 8
	financeRechargeEvidenceRowLimit    = 5000
)

// InspectFinanceRechargeEvidenceBackup is deliberately offline-only. It reads
// selected account identities (never credentials), existing fund metadata and
// original bills from one closed backup. No migrations, source clients, tasks,
// report publication, repair authorization or SQLite writes are constructed.
func InspectFinanceRechargeEvidenceBackup(parent context.Context, path, fromDate, toDate string, domains []string) (FinanceRechargeEvidenceInspection, error) {
	var empty FinanceRechargeEvidenceInspection
	if len(domains) == 0 || len(domains) > financeRechargeEvidenceDomainLimit {
		return empty, errors.New("evidence review requires 1 to 8 explicit domains")
	}
	selected := make(map[string]bool)
	for _, domain := range domains {
		if domain == "" || len(domain) > 253 || normalizeChannelBaseDomain(domain) != domain || selected[domain] {
			return empty, errors.New("evidence domains must be distinct canonical primary domains")
		}
		selected[domain] = true
	}
	from, err := financeStartHour(fromDate)
	if err != nil {
		return empty, err
	}
	to, err := financeStartHour(toDate)
	now := time.Now().Unix()
	if err != nil || from < 0 || to <= from || to > now-now%3600 || to-from > financeReportMaxDays*86400 {
		return empty, errors.New("evidence review requires a bounded closed date range")
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	backup, err := openGiftRelevantBackup(path)
	if err != nil {
		return empty, err
	}
	defer backup.close()
	before, err := financeRechargeBackupHash(ctx, backup)
	if err != nil {
		return empty, err
	}
	var directory []ChannelUpstreamAccount
	if err := backup.db.WithContext(ctx).Select("domain", "provider", "base_url", "account", "user_id", "usage_adapter", "usage_sync_enabled").
		Where("domain IN ?", domains).Order("domain").Find(&directory).Error; err != nil {
		return empty, err
	}
	if len(directory) != len(domains) {
		return empty, errors.New("selected evidence domain has no configured account")
	}
	accounts := make(map[string]ChannelUpstreamAccountView)
	for _, account := range directory {
		accounts[account.Domain] = ChannelUpstreamAccountView{Configured: true, Provider: account.Provider,
			UsageAdapter: account.UsageAdapter, UsageGranularity: upstreamUsageGranularity(account.Provider, account.UsageAdapter), UsageSyncEnabled: account.UsageSyncEnabled}
	}
	m := &Monitor{storeDB: backup.db}
	scope := stabilityScope{FromTs: from, ToTs: to}
	accounts, err = m.financeRelevantUpstreamAccounts(ctx, scope, accounts)
	if err != nil {
		return empty, err
	}
	var versionCount int64
	if err := backup.db.WithContext(ctx).Model(&ChannelFinanceVersion{}).Where("domain IN ?", domains).Count(&versionCount).Error; err != nil {
		return empty, err
	}
	if versionCount > financeRechargeEvidenceRowLimit {
		return empty, errors.New("evidence review version budget exceeded")
	}
	versions, err := m.loadChannelRechargeVersions(ctx, accounts, channelFinanceSnapshot{})
	if err != nil {
		return empty, err
	}
	var bills []ChannelUpstreamUsageHour
	if err := backup.db.WithContext(ctx).Where("domain IN ? AND hour_ts>=? AND hour_ts<?", domains, cstDayStart(from), to).
		Order("domain,hour_ts").Limit(financeRechargeInspectBucketLimit + 1).Find(&bills).Error; err != nil {
		return empty, err
	}
	if len(bills) > financeRechargeInspectBucketLimit {
		return empty, errors.New("evidence review bill budget exceeded; use a smaller range")
	}
	for _, row := range bills {
		seconds := row.BucketSeconds
		if seconds == 0 {
			seconds = 3600
		}
		if row.HourTs < 0 || (seconds != 3600 && seconds != 86400) || row.HourTs > math.MaxInt64-seconds {
			return empty, errors.New("evidence review found an invalid original bill interval")
		}
	}
	accepted := make(map[string][]ChannelUpstreamUsageHour)
	metrics, err := projectUpstreamUsageBuckets(bills, scope, now, accounts, versions, func(row ChannelUpstreamUsageHour) {
		accepted[row.Domain] = append(accepted[row.Domain], row)
	})
	if err != nil {
		return empty, err
	}
	out := FinanceRechargeEvidenceInspection{Mode: "offline_recharge_evidence_unconfirmed_preview_no_writes",
		SourceSnapshotSHA256: before, FromTs: from, ToTs: to, Domains: []FinanceRechargeEvidenceDomain{}}
	for _, account := range directory {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		epoch := newAPIUpstreamAccountEpoch(account)
		var funds []ChannelUpstreamFundEvent
		// Do not select free text, original JSON, request identifiers or credentials.
		if err := backup.db.WithContext(ctx).Select("occurred_at", "provider", "source_type", "kind", "direction", "confidence", "paid_amount", "paid_known",
			"upstream_amount", "upstream_amount_known", "parser_version", "raw_truncated", "reparse_error", "observed_count").
			Where("domain=? AND account_epoch=? AND occurred_at>=? AND occurred_at<?", account.Domain, epoch, from, to).
			Order("occurred_at,event_key").Limit(financeRechargeEvidenceRowLimit + 1).Find(&funds).Error; err != nil {
			return empty, err
		}
		if len(funds) > financeRechargeEvidenceRowLimit {
			return empty, errors.New("evidence review fund budget exceeded; use a smaller range")
		}
		metric := metrics[account.Domain]
		detail := FinanceRechargeEvidenceDomain{Domain: account.Domain, Provider: account.Provider,
			BillComplete: metric.Complete, BillStatus: metric.IntegrityStatus,
			Funds: reviewFinanceRechargeFunds(funds, account.Provider, scope), RecordedChanges: financeRechargeRecordedChanges(versions[account.Domain], to)}
		if err := backup.db.WithContext(ctx).Model(&ChannelUpstreamFundEvent{}).
			Where("domain=? AND account_epoch<>? AND occurred_at>=? AND occurred_at<?", account.Domain, epoch, from, to).
			Count(&detail.Funds.ExcludedAccountRows).Error; err != nil {
			return empty, err
		}
		var states []UpstreamFundSyncState
		if err := backup.db.WithContext(ctx).Select("status", "coverage_from", "tail_synced_until", "backfill_done", "history_scope").
			Where("domain=? AND account_epoch=?", account.Domain, epoch).Limit(1).Find(&states).Error; err != nil {
			return empty, err
		}
		if len(states) > 0 {
			state := states[0]
			detail.Funds.SyncStateKnown = true
			detail.Funds.CoverageFrom, detail.Funds.DataUntil = state.CoverageFrom, state.TailSyncedUntil
			detail.Funds.HistoryCoversQuery = state.Status == "ok" && state.BackfillDone && state.HistoryScope != "provider_recent" &&
				state.CoverageFrom > 0 && state.CoverageFrom <= from && state.TailSyncedUntil >= to
		}
		if metric.Available && metric.IntegrityStatus == upstreamUsageIntegrityComplete && !metric.Provisional && account.Provider != upstreamProviderOpenOx {
			ref, _ := financeRechargeReviewVersions(versions[account.Domain], to-1, to)
			detail.ReferencePreview, err = previewFinanceRechargeReference(ctx, accepted[account.Domain], versions[account.Domain], ref)
			if err != nil {
				return empty, err
			}
		}
		out.Domains = append(out.Domains, detail)
	}
	sort.Slice(out.Domains, func(i, j int) bool { return out.Domains[i].Domain < out.Domains[j].Domain })
	if err := backup.unchanged(); err != nil {
		return empty, err
	}
	after, err := financeRechargeBackupHash(ctx, backup)
	if err != nil || before != after {
		return empty, errors.New("closed snapshot changed during evidence review; discard preview")
	}
	if err := backup.unchanged(); err != nil {
		return empty, err
	}
	return out, ctx.Err()
}
