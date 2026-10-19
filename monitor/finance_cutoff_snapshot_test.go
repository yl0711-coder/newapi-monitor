//go:build unix

package monitor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// Explicit opt-in, closed LOCAL snapshots only. No initializer, migrations,
// network clients or background collectors are started. All database handles
// are immutable/read-only; report caches (if needed) belong to t.TempDir.
func TestFinanceCutoffClosedSnapshotAcceptance(t *testing.T) {
	mainPath, factsPath := os.Getenv("MONITOR_FINANCE_CUTOFF_MAIN"), os.Getenv("MONITOR_FINANCE_CUTOFF_FACTS")
	if mainPath == "" || factsPath == "" {
		t.Skip("requires two explicitly selected closed local snapshots")
	}
	paths := []string{mainPath, factsPath}
	hashes := make([]string, len(paths))
	for i, path := range paths {
		hashes[i] = giftLocalFileHash(t, path)
	}
	t.Cleanup(func() {
		for i, path := range paths {
			if giftLocalFileHash(t, path) != hashes[i] {
				t.Errorf("original snapshot changed: %d", i)
			}
		}
	})
	mainDB, closeMain, err := giftLocalReadonlyDatabase(mainPath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeMain()
	factsDB, closeFacts, err := giftLocalReadonlyDatabase(factsPath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFacts()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var state FinanceInternalAccountFactState
	if err := factsDB.WithContext(ctx).First(&state, financeInternalFactStateID).Error; err != nil {
		t.Fatal(err)
	}
	var last int64
	if err := mainDB.WithContext(ctx).Raw("SELECT MAX(hour_ts) FROM stability_hour_ingest_states WHERE status='complete'").Scan(&last).Error; err != nil {
		t.Fatal(err)
	}
	end := min(last+3600, state.NextHourTs)
	if end <= state.StartHourTs+86400 {
		t.Fatal("snapshot lacks a closed day")
	}
	m := &Monitor{storeDB: mainDB, usageFactsDB: factsDB, cfg: Settings{
		FinanceEnabled: true, FinanceStartDate: "2026-05-01", LocalSnapshotOnly: true,
		UsageFactsHistorySourceEpoch: state.SourceEpoch, StorePath: filepath.Join(t.TempDir(), "cache-only.db"),
	}}
	policies, err := loadChannelBusinessGroupPolicies(ctx, mainDB)
	if err != nil {
		t.Fatal(err)
	}
	var excluded []string
	for group, included := range policies {
		if !included {
			excluded = append(excluded, group)
		}
	}
	loc := time.FixedZone("CST", 8*3600)
	now := time.Unix(end+3600+8*60, 0).In(loc)
	startOfMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, loc)
	previousMonth := startOfMonth.AddDate(0, -1, 0)
	closedDay := time.Unix(cstDayStart(end)-86400, 0).In(loc)
	cases := []struct{ name, query string }{
		{"historical_month", "?from=" + previousMonth.Format("2006-01-02") + "&to=" + startOfMonth.AddDate(0, 0, -1).Format("2006-01-02")},
		{"historical_day", "?from=" + closedDay.Format("2006-01-02") + "&to=" + closedDay.Format("2006-01-02")},
		{"snapshot_current_day", "?from=" + time.Unix(end-1, 0).In(loc).Format("2006-01-02")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			from, to, err := parseFinanceRange(financeRangeContext(tc.query), now.UTC(), m.cfg.FinanceStartDate)
			if err != nil || to.Unix() > end {
				t.Fatalf("invalid snapshot range: %v %v", to, err)
			}
			scope := stabilityScope{FromTs: from.Unix(), ToTs: to.Unix()}
			internal, err := m.loadFinanceInternalEvidence(ctx, scope, policies, false)
			if err != nil || !internal.Complete {
				t.Fatalf("internal-account baseline incomplete: %+v %v", internal.SyncState, err)
			}
			got, coverage, domains, err := m.loadFinanceUserFacts(ctx, scope, now.Unix(), internal, policies)
			if err != nil || !coverage.Complete || coverage.ToTs != scope.ToTs || coverage.LatestHourPending || got.UserConsumption == nil {
				t.Fatalf("closed real range not published: %+v %v", coverage, err)
			}
			// Independent quota totals: do not reuse domain attribution or the
			// production aggregation helper to manufacture the expected amount.
			sum := func(table, expression string, classified bool) int64 {
				t.Helper()
				q := mainDB.WithContext(ctx)
				if !classified {
					q = factsDB.WithContext(ctx)
				}
				q = q.Table(table).Where("hour_ts>=? AND hour_ts<?", scope.FromTs, scope.ToTs)
				if classified {
					q = q.Where("traffic_class_version IN ?", accountingTrafficVersions())
				}
				if len(excluded) > 0 {
					q = q.Where("COALESCE(grp,'') NOT IN ?", excluded)
				}
				var n int64
				if err := q.Select("COALESCE(SUM(" + expression + "),0)").Scan(&n).Error; err != nil {
					t.Fatal(err)
				}
				return n
			}
			quota := sum("stability_hour_samples", "quota-refund_quota", true) - sum("finance_internal_account_hour_facts", "consume_quota-refund_quota", false)
			if quotaPerUSD != 500000 || got.UserConsumption.MicroUSD != strconv.FormatInt(quota*2, 10) {
				t.Fatal("real amount differs from independently summed quota")
			}
			var domainQuota int64
			for _, d := range domains {
				domainQuota += d.ConsumeQuota - d.RefundQuota
			}
			if domainQuota != quota {
				t.Fatal("domain amounts do not reconcile")
			}
			var split int64
			for cursor := scope.FromTs; cursor < scope.ToTs; {
				next := min(cstDayStart(cursor)+86400, scope.ToTs)
				partScope := stabilityScope{FromTs: cursor, ToTs: next}
				partInternal, err := financeConfiguredInternalSubrange(internal, partScope)
				if err != nil {
					t.Fatal(err)
				}
				part, cov, _, err := m.loadFinanceUserFacts(ctx, partScope, now.Unix(), partInternal, policies)
				if err != nil || !cov.Complete || part.UserConsumption == nil {
					t.Fatalf("daily range incomplete: %v", err)
				}
				n, err := strconv.ParseInt(part.UserConsumption.MicroUSD, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				split += n
				cursor = next
			}
			if fmt.Sprint(split) != got.UserConsumption.MicroUSD {
				t.Fatal("day/month amounts do not reconcile")
			}
			dailyScope := financeClosedNaturalDayScope(scope, now.Unix())
			if dailyScope.ToTs > scope.ToTs || dailyScope.ToTs%86400 != 16*3600 {
				t.Fatal("daily-only cost included an unfinished Beijing day")
			}
			t.Logf("range=%s..%s hours=%d domains=%d net_micro_usd=%s daily_cost_through=%s independent_SQL=equal daily_sum=equal", from.In(loc).Format(time.DateTime), to.In(loc).Format(time.DateTime), coverage.ExpectedHours, len(domains), got.UserConsumption.MicroUSD, time.Unix(dailyScope.ToTs, 0).In(loc).Format(time.DateTime))
		})
	}
	t.Logf("sealed snapshots main_sha256=%s facts_sha256=%s; original files unchanged", hashes[0], hashes[1])
}
