//go:build unix

package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"gorm.io/gorm"
)

type attributionLocalQuery struct {
	name string
	sql  string
	args []any
}

// Hash rows without exporting credentials or unrelated private snapshot data.
// These queries run only against the fresh, sealed-fixture acceptance copy.
func attributionQueryDigests(ctx context.Context, t *testing.T, db *gorm.DB, queries []attributionLocalQuery) map[string]string {
	t.Helper()
	digests := make(map[string]string, len(queries))
	for _, query := range queries {
		digests[query.name] = attributionQueryDigest(ctx, t, db, query)
	}
	return digests
}

func attributionQueryDigest(ctx context.Context, t *testing.T, db *gorm.DB, query attributionLocalQuery) string {
	t.Helper()
	rows, err := db.WithContext(ctx).Raw(query.sql, query.args...).Rows()
	if err != nil {
		t.Fatalf("protected query %s: %v", query.name, err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	encoder := json.NewEncoder(hash)
	if err := encoder.Encode(columns); err != nil {
		t.Fatal(err)
	}
	values, pointers := make([]any, len(columns)), make([]any, len(columns))
	for i := range values {
		pointers[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		if err := encoder.Encode(values); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func attributionProtectedQueries(t *testing.T, db *gorm.DB, account ChannelUpstreamAccount, hours []int64) []attributionLocalQuery {
	t.Helper()
	return attributionProtectedChannelQueries(t, db, account, hours, []int{0, attributionLocalChannel})
}

func attributionProtectedChannelQueries(t *testing.T, db *gorm.DB, account ChannelUpstreamAccount, hours []int64, channels []int) []attributionLocalQuery {
	t.Helper()
	var tables []string
	if err := db.Raw("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").Scan(&tables).Error; err != nil {
		t.Fatal(err)
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	keys := make([]string, 0, len(hours)*len(channels))
	for _, hour := range hours {
		for _, channel := range channels {
			keys = append(keys, economicsLogicalKey(account.Domain, epoch, hour, channel))
		}
	}
	queries := []attributionLocalQuery{{name: "schema", sql: "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"}}
	for _, table := range tables {
		query := attributionLocalQuery{name: table, sql: "SELECT * FROM " + attributionQuotedTable(table)}
		switch table {
		case "channel_cost_source_bindings", "channel_economics_dirty_hours":
			continue // Exact binding and queue contents have separate assertions.
		case "channel_economics_hour_publications", "channel_economics_hour_manifest_publications":
			var boundary int64
			if err := db.Raw("SELECT COALESCE(MAX(rowid),0) FROM " + attributionQuotedTable(table)).Scan(&boundary).Error; err != nil {
				t.Fatal(err)
			}
			// Retain EVERY old revision; only new, explicitly in-scope revisions
			// may be appended. New out-of-scope rows also fail this digest.
			query.sql += " WHERE NOT(rowid>? AND domain=? AND hour_ts IN ? AND semantics_version=?"
			query.args = []any{boundary, account.Domain, hours, channelEconomicsSemanticsVersion}
			if table == "channel_economics_hour_publications" {
				query.sql += " AND account_epoch=? AND local_channel_id IN ?)"
				query.args = append(query.args, epoch, channels)
			} else {
				query.sql += " AND authoritative_epoch=?)"
				query.args = append(query.args, epoch)
			}
		case "channel_economics_hour_current":
			query.sql += " WHERE logical_key NOT IN ?"
			query.args = []any{keys}
		case "channel_economics_hour_manifest_current":
			query.sql += " WHERE NOT(domain=? AND hour_ts IN ? AND semantics_version=?)"
			query.args = []any{account.Domain, hours, channelEconomicsSemanticsVersion}
		}
		query.sql += " ORDER BY rowid"
		queries = append(queries, query)
	}
	return queries
}

func attributionMutablePublicationQueries() []attributionLocalQuery {
	var queries []attributionLocalQuery
	for _, table := range []string{
		"channel_economics_hour_publications", "channel_economics_hour_current",
		"channel_economics_hour_manifest_publications", "channel_economics_hour_manifest_current",
		"channel_economics_global_hour_facts", "channel_cost_source_bindings",
	} {
		queries = append(queries, attributionLocalQuery{name: table, sql: "SELECT * FROM " + attributionQuotedTable(table) + " ORDER BY rowid"})
	}
	return queries
}

func attributionQuotedTable(table string) string {
	return `"` + strings.ReplaceAll(table, `"`, `""`) + `"`
}

func attributionRowCount(t *testing.T, db *gorm.DB, table string) int64 {
	t.Helper()
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM " + attributionQuotedTable(table)).Scan(&count).Error; err != nil {
		t.Fatal(err)
	}
	return count
}

func attributionEvidenceHours(t *testing.T, db *gorm.DB, account ChannelUpstreamAccount) []int64 {
	t.Helper()
	var hours []int64
	err := db.Raw(`SELECT DISTINCT e.hour_ts FROM channel_upstream_cost_hour_evidence e
		JOIN channel_upstream_cost_hour_states s ON s.domain=e.domain AND s.account_epoch=e.account_epoch
		AND s.hour_ts=e.hour_ts AND s.semantics_version=e.semantics_version
		WHERE e.domain=? AND e.account_epoch=? AND e.source_ref=? AND e.semantics_version=?
		AND s.status='verified' AND s.reconcile_status='matched' ORDER BY e.hour_ts`,
		account.Domain, newAPIUpstreamAccountEpoch(account), attributionLocalSource, channelCostEvidenceSemanticsVersion).Scan(&hours).Error
	if err != nil || len(hours) != 11 {
		t.Fatal("expected exactly eleven verified source hours", err)
	}
	return hours
}

type attributionLocalTotals struct {
	HourTs, LocalRequests, LocalConsumeQuota, LocalRefundRecords, LocalRefundQuota int64
	LocalNetQuota, RevenueMicroUSD, UpstreamRequests, UpstreamChargeUnits          int64
	UpstreamCostMicroUSD, CorrectedCostMicroUSD                                    int64
}

func attributionHourTotals(t *testing.T, db *gorm.DB, account ChannelUpstreamAccount, hours []int64) []attributionLocalTotals {
	t.Helper()
	var totals []attributionLocalTotals
	err := currentEconomicsPublicationQuery(db).Select(`p.hour_ts,
		SUM(p.local_requests) local_requests, SUM(p.local_consume_quota) local_consume_quota,
		SUM(p.local_refund_records) local_refund_records, SUM(p.local_refund_quota) local_refund_quota,
		SUM(p.local_net_quota) local_net_quota, SUM(p.revenue_micro_usd) revenue_micro_usd,
		SUM(p.upstream_requests) upstream_requests, SUM(p.upstream_charge_units) upstream_charge_units,
		SUM(p.upstream_cost_micro_usd) upstream_cost_micro_usd,
		SUM(CASE WHEN p.corrected_cost_known THEN p.corrected_cost_micro_usd ELSE 0 END) corrected_cost_micro_usd`).
		Where("p.domain=? AND p.account_epoch=? AND p.hour_ts IN ? AND p.semantics_version=?", account.Domain, newAPIUpstreamAccountEpoch(account), hours, channelEconomicsSemanticsVersion).
		Group("p.hour_ts").Order("p.hour_ts").Scan(&totals).Error
	if err != nil || len(totals) != len(hours) {
		t.Fatal("missing authoritative totals", err)
	}
	return totals
}

func attributionTargetRows(t *testing.T, db *gorm.DB, account ChannelUpstreamAccount, hours []int64) []ChannelEconomicsHourPublication {
	t.Helper()
	var rows []ChannelEconomicsHourPublication
	err := currentEconomicsPublicationQuery(db).Select("p.*").
		Where("p.domain=? AND p.account_epoch=? AND p.hour_ts IN ? AND p.local_channel_id=? AND p.semantics_version=?",
			account.Domain, newAPIUpstreamAccountEpoch(account), hours, attributionLocalChannel, channelEconomicsSemanticsVersion).
		Order("p.hour_ts").Scan(&rows).Error
	if err != nil || len(rows) != len(hours) {
		t.Fatal("missing candidate publication rows", err)
	}
	return rows
}

func verifyAttributionLocalTarget(t *testing.T, db *gorm.DB, account ChannelUpstreamAccount, hours []int64, before []ChannelEconomicsHourPublication) []ChannelEconomicsHourPublication {
	t.Helper()
	after := attributionTargetRows(t, db, account, hours)
	var requests, charges, rawCost, revenue int64
	for i, row := range after {
		old := before[i]
		if row.HourTs != old.HourTs || row.Revision != old.Revision+1 || row.SupersedesPublicationID != old.PublicationID || row.SourceHash == old.SourceHash {
			t.Fatal("candidate did not append exactly one traceable revision")
		}
		if !reflect.DeepEqual(attributionLocalRevenue(row), attributionLocalRevenue(old)) || row.FinanceVersion != old.FinanceVersion {
			t.Fatal("attribution unexpectedly changed revenue or recharge version")
		}
		var evidence struct{ Requests, ChargeUnits int64 }
		err := db.Model(&ChannelUpstreamCostHourEvidence{}).Select("SUM(requests) requests, SUM(charge_units) charge_units").
			Where("domain=? AND account_epoch=? AND source_ref=? AND hour_ts=? AND semantics_version=?",
				account.Domain, newAPIUpstreamAccountEpoch(account), attributionLocalSource, row.HourTs, channelCostEvidenceSemanticsVersion).Scan(&evidence).Error
		if err != nil {
			t.Fatal(err)
		}
		// Independent expectations for this pinned 500,000-unit, 1:1 scenario;
		// do not use the production conversion function to verify itself.
		if row.UpstreamRequests != evidence.Requests || row.UpstreamChargeUnits != evidence.ChargeUnits || row.ChargeUnitsPerUSD != "500000" ||
			row.UpstreamCostMicroUSD != evidence.ChargeUnits*2 || row.CorrectedCostMicroUSD != row.UpstreamCostMicroUSD || !row.CorrectedCostKnown {
			t.Fatal("candidate cost differs from its exact source evidence")
		}
		if !row.ProfitKnown || row.CoverageStatus != "verified_complete" || row.ProfitMicroUSD != row.RevenueMicroUSD-row.CorrectedCostMicroUSD {
			t.Fatal("candidate row did not close under the explicitly hypothetical binding")
		}
		requests += row.UpstreamRequests
		charges += row.UpstreamChargeUnits
		rawCost += row.UpstreamCostMicroUSD
		revenue += row.RevenueMicroUSD
	}
	if requests != 43 || charges != 1736780 || rawCost != 3473560 || revenue != 1869478 {
		t.Fatal("candidate control totals differ from the sealed evidence")
	}
	verifyAttributionLocalRemainingGaps(t, db, account, hours)
	return after
}

func attributionLocalRevenue(row ChannelEconomicsHourPublication) []any {
	return []any{row.LocalRequests, row.LocalConsumeQuota, row.LocalRefundRecords, row.LocalRefundQuota,
		row.LocalNetQuota, row.RevenueMicroUSD, row.LocalFactStatus, row.UnallocatedRefundRecords, row.UnallocatedRefundQuota}
}

func verifyAttributionLocalRemainingGaps(t *testing.T, db *gorm.DB, account ChannelUpstreamAccount, hours []int64) {
	t.Helper()
	var manifests []ChannelEconomicsHourManifestPublication
	err := db.Table("channel_economics_hour_manifest_current c").Select("p.*").
		Joins("JOIN channel_economics_hour_manifest_publications p ON p.manifest_id=c.manifest_id").
		Where("c.domain=? AND c.hour_ts IN ? AND c.semantics_version=?", account.Domain, hours, channelEconomicsSemanticsVersion).
		Order("c.hour_ts").Scan(&manifests).Error
	if err != nil || len(manifests) != len(hours) {
		t.Fatal("missing domain manifests", err)
	}
	var complete int
	for _, manifest := range manifests {
		var otherSources int64
		if err := db.Model(&ChannelUpstreamCostHourEvidence{}).
			Where("domain=? AND account_epoch=? AND hour_ts=? AND semantics_version=? AND source_ref<>?",
				account.Domain, newAPIUpstreamAccountEpoch(account), manifest.HourTs, channelCostEvidenceSemanticsVersion, attributionLocalSource).
			Count(&otherSources).Error; err != nil {
			t.Fatal(err)
		}
		if otherSources > 0 {
			if manifest.ProfitKnown || manifest.CoverageStatus != "unallocated_cost" {
				t.Fatal("remaining unallocated sources were incorrectly marked complete")
			}
		} else {
			if !manifest.ProfitKnown || manifest.CoverageStatus != "verified_complete" {
				t.Fatal("sole-source hour did not close after its only candidate was attributed")
			}
			complete++
		}
	}
	if complete != 1 {
		t.Fatal("pinned scenario must close one domain hour and retain ten incomplete hours", complete)
	}
}

func attributionLocalPairingRows(ctx context.Context, t *testing.T, db *gorm.DB, hours []int64) []financePairingHourView {
	t.Helper()
	rows, err := loadFinancePairingHours(ctx, db, stabilityScope{FromTs: hours[0], ToTs: hours[len(hours)-1] + 3600})
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func verifyAttributionLocalPairingChange(t *testing.T, before, after []financePairingHourView) {
	t.Helper()
	expected := append([]financePairingHourView(nil), before...)
	var found bool
	for i := range expected {
		if expected[i].Domain == attributionLocalDomain {
			expected[i].PairedActivityHours++
			expected[i].UnallocatedCostHours--
			found = true
		}
	}
	if !found || !reflect.DeepEqual(expected, after) {
		t.Fatal("finance report must count only one newly paired domain hour; all other diagnosis counts stay unchanged")
	}
}
