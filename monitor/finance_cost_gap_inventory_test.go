//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"
)

type financeGapAuditInterval struct {
	From int64 `json:"from_ts"`
	To   int64 `json:"to_ts_exclusive"`
}

// Test-only interval arithmetic. A valid zero bill covers time, but absent
// rows do not. Daily buckets remain indivisible; this never invents money.
func financeGapAuditMissing(from, to int64, covered []financeGapAuditInterval) []financeGapAuditInterval {
	covered = append([]financeGapAuditInterval(nil), covered...)
	sort.Slice(covered, func(i, j int) bool { return covered[i].From < covered[j].From })
	var gaps []financeGapAuditInterval
	cursor := from
	for _, interval := range covered {
		if interval.To <= cursor || interval.From >= to {
			continue
		}
		if interval.From > cursor {
			gaps = append(gaps, financeGapAuditInterval{cursor, interval.From})
		}
		cursor = max(cursor, min(to, interval.To))
	}
	if cursor < to {
		gaps = append(gaps, financeGapAuditInterval{cursor, to})
	}
	return gaps
}

func TestFinanceGapAuditMissingIntervals(t *testing.T) {
	for _, tc := range []struct {
		name          string
		covered, want []financeGapAuditInterval
	}{
		{"absent", nil, []financeGapAuditInterval{{10, 100}}},
		{"zero_bill_still_covers", []financeGapAuditInterval{{10, 100}}, nil},
		{"holes", []financeGapAuditInterval{{40, 50}, {20, 30}, {50, 90}}, []financeGapAuditInterval{{10, 20}, {30, 40}, {90, 100}}},
		{"partial_tail", []financeGapAuditInterval{{10, 43}}, []financeGapAuditInterval{{43, 100}}},
		{"outside_and_overlap", []financeGapAuditInterval{{0, 20}, {15, 30}, {80, 120}, {100, 130}}, []financeGapAuditInterval{{30, 80}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := financeGapAuditMissing(10, 100, tc.covered); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

// Opt-in inventory of a closed LOCAL fixture. No client, worker, migration,
// authorization, binding, or repair is created. All credentials are excluded
// from SELECTs. This is evidence for planning, not a production import plan.
func TestFinanceCostGapSealedInventory(t *testing.T) {
	path := os.Getenv("MONITOR_FINANCE_GAP_AUDIT_DB")
	if path == "" {
		t.Skip("requires explicit sealed local snapshot")
	}
	const expectedHash = "28e3460df585bbde44071013ec13e8fa696e0aa94d39c334e477d85ec723a20e"
	if giftLocalFileHash(t, path) != expectedHash {
		t.Fatal("unexpected sealed snapshot")
	}
	t.Cleanup(func() {
		if giftLocalFileHash(t, path) != expectedHash {
			t.Error("input changed")
		}
	})
	backup, err := openGiftRelevantBackup(path)
	if err != nil {
		t.Fatal(err)
	}
	defer backup.close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	from, err := financeStartHour("2026-05-01")
	if err != nil {
		t.Fatal(err)
	}
	to, err := financeStartHour("2026-10-09")
	if err != nil {
		t.Fatal(err)
	}
	scope := stabilityScope{FromTs: from, ToTs: to}
	var directory []struct {
		Domain, Provider, UsageAdapter string
		UsageSyncEnabled               bool
	}
	if err := backup.db.WithContext(ctx).Raw(`SELECT domain,provider,COALESCE(usage_adapter,'') usage_adapter,usage_sync_enabled FROM channel_upstream_accounts ORDER BY domain`).Scan(&directory).Error; err != nil {
		t.Fatal(err)
	}
	accounts := map[string]ChannelUpstreamAccountView{}
	for _, a := range directory {
		accounts[a.Domain] = ChannelUpstreamAccountView{Configured: true, Provider: a.Provider, UsageAdapter: a.UsageAdapter, UsageSyncEnabled: a.UsageSyncEnabled}
	}
	m := &Monitor{storeDB: backup.db}
	accounts, err = m.financeRelevantUpstreamAccounts(ctx, scope, accounts)
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := m.loadFinanceBillInputs(ctx, scope, accounts, channelFinanceSnapshot{})
	if err != nil {
		t.Fatal(err)
	}
	if len(inputs.rows) > financeRechargeInspectBucketLimit {
		t.Fatal("bucket budget exceeded")
	}
	covered := map[string][]financeGapAuditInterval{}
	metrics, err := projectUpstreamUsageBuckets(inputs.rows, scope, to, accounts, inputs.versions, func(row ChannelUpstreamUsageHour) {
		seconds := row.BucketSeconds
		if seconds <= 0 {
			seconds = 3600
		}
		covered[row.Domain] = append(covered[row.Domain], financeGapAuditInterval{row.HourTs, row.HourTs + seconds})
	})
	if err != nil {
		t.Fatal(err)
	}
	type domainGap struct {
		Domain                  string                    `json:"domain"`
		Provider                string                    `json:"provider"`
		ExpectedHours           int64                     `json:"expected_hours"`
		CompletedHours          int64                     `json:"completed_hours"`
		MissingHours            int64                     `json:"missing_hours"`
		MissingSeconds          int64                     `json:"missing_seconds"`
		Intervals               []financeGapAuditInterval `json:"missing_intervals"`
		ArchivedOverlappingRows int64                     `json:"archived_overlapping_rows"`
		Position                string                    `json:"position"`
		SyncState               string                    `json:"snapshot_sync_state"`
		BackfillDone            bool                      `json:"configured_backfill_done"`
	}
	var gaps []domainGap
	var expected, completed, missing int64
	for domain, metric := range metrics {
		if metric.IntegrityStatus != "" && metric.IntegrityStatus != upstreamUsageIntegrityComplete {
			t.Fatalf("%s invalid bill: %s; cannot classify as absent", domain, metric.IntegrityStatus)
		}
		if metric.Provisional {
			t.Fatalf("%s provisional bill requires separate classification", domain)
		}
		account := accounts[domain]
		intervals := financeGapAuditMissing(max(from, account.FinanceRequiredFrom), to, covered[domain])
		var seconds int64
		for _, interval := range intervals {
			seconds += interval.To - interval.From
		}
		// Coverage exposes floor(recorded seconds / 3600), so an unfinished
		// tail counts as one missing whole-hour equivalent. Preserve exact
		// seconds here rather than inventing a whole missing log hour.
		missingHours := (seconds + 3599) / 3600
		if missingHours != metric.ExpectedHours-metric.CompletedHours {
			t.Fatalf("%s inventory differs from report coverage: gap_seconds=%d expected_hours=%d completed_hours=%d from=%d to=%d intervals=%v", domain, seconds, metric.ExpectedHours, metric.CompletedHours, account.FinanceRequiredFrom, to, intervals)
		}
		expected += metric.ExpectedHours
		completed += metric.CompletedHours
		missing += missingHours
		if seconds == 0 {
			continue
		}
		entry := domainGap{Domain: domain, Provider: account.Provider, ExpectedHours: metric.ExpectedHours, CompletedHours: metric.CompletedHours, MissingHours: missingHours, MissingSeconds: seconds, Intervals: intervals}
		entry.Position = "internal_or_multiple_gaps"
		if len(intervals) == 1 && intervals[0].From == max(from, account.FinanceRequiredFrom) && len(covered[domain]) > 0 {
			entry.Position = "before_first_recorded_bill"
		} else if len(intervals) == 1 && intervals[0].To == to && len(covered[domain]) > 0 {
			entry.Position = "after_last_recorded_bill"
		}
		var sync struct {
			UsageStatus       string
			UsageBackfillDone bool
		}
		if err := backup.db.WithContext(ctx).Raw("SELECT usage_status,usage_backfill_done FROM channel_upstream_accounts WHERE domain=?", domain).Scan(&sync).Error; err != nil {
			t.Fatal(err)
		}
		entry.SyncState, entry.BackfillDone = sync.UsageStatus, sync.UsageBackfillDone
		for _, interval := range intervals {
			var count int64
			if err := backup.db.WithContext(ctx).Model(&ChannelUpstreamUsageArchive{}).Where("domain=? AND hour_ts<? AND hour_ts+CASE WHEN bucket_seconds>0 THEN bucket_seconds ELSE 3600 END>?", domain, interval.To, interval.From).Count(&count).Error; err != nil {
				t.Fatal(err)
			}
			entry.ArchivedOverlappingRows += count
		}
		gaps = append(gaps, entry)
	}
	sort.Slice(gaps, func(i, j int) bool { return gaps[i].Domain < gaps[j].Domain })
	if len(accounts) != 39 || expected != 46594 || completed != 45756 || missing != 838 || len(gaps) != 5 {
		t.Fatalf("pinned report control totals differ: %d %d %d %d %d", len(accounts), expected, completed, missing, len(gaps))
	}
	if err := backup.unchanged(); err != nil {
		t.Fatal(err)
	}
	result := struct {
		Mode               string                  `json:"mode"`
		SourceSHA256       string                  `json:"source_sha256"`
		Scope              financeGapAuditInterval `json:"scope"`
		Domains            []domainGap             `json:"domains"`
		MissingHours       int64                   `json:"missing_hours"`
		ProductionAccessed bool                    `json:"production_accessed"`
		ImportAuthorized   bool                    `json:"import_authorized"`
	}{Mode: "local_readonly_gap_inventory", SourceSHA256: expectedHash, Scope: financeGapAuditInterval{from, to}, Domains: gaps, MissingHours: missing}
	if out := os.Getenv("MONITOR_FINANCE_GAP_AUDIT_RESULT"); out != "" {
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := giftLocalWriteNew(out, data); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("PASS: %d configured domains, %d/%d covered account-hours, %d gaps across %d domains; source unchanged", len(accounts), completed, expected, missing, len(gaps))
}
