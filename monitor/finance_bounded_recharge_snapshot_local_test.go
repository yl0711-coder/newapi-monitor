//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"gorm.io/gorm"
)

// Evidence may have deliberately stopped at the reviewed period. Later
// publications are legitimate, but must be preserved as the actual current
// anchor, not replaced by a reduced historical fixture.
func boundedRechargeHistoryMatches(recorded, reviewed []channelRechargeVersion, end int64) bool {
	wanted := make(map[int64]channelRechargeVersion, len(reviewed))
	for _, term := range reviewed {
		if _, duplicate := wanted[term.Version]; duplicate {
			return false
		}
		wanted[term.Version] = term
	}
	seen := make(map[int64]bool, len(recorded))
	matched := 0
	for _, term := range recorded {
		if seen[term.Version] {
			return false
		}
		seen[term.Version] = true
		if expected, exists := wanted[term.Version]; exists {
			if term != expected {
				return false
			}
			matched++
		} else if term.EffectiveAt < end {
			return false
		}
	}
	return len(reviewed) > 0 && matched == len(reviewed)
}

func TestFinanceBoundedRechargeHistoryScope(t *testing.T) {
	base := []channelRechargeVersion{{Version: 1, EffectiveAt: 10, Paid: 1, Credit: 10, Valid: true}}
	for _, tc := range []struct {
		name string
		rows []channelRechargeVersion
		want bool
	}{
		{"exact", base, true},
		{"later_current_terms", append(append([]channelRechargeVersion(nil), base...), channelRechargeVersion{Version: 2, EffectiveAt: 30, Paid: 1, Credit: 7, Valid: true}), true},
		{"end_excluded", append(append([]channelRechargeVersion(nil), base...), channelRechargeVersion{Version: 2, EffectiveAt: 20, Paid: 1, Credit: 7, Valid: true}), true},
		{"unreviewed_inside", append(append([]channelRechargeVersion(nil), base...), channelRechargeVersion{Version: 2, EffectiveAt: 19, Paid: 1, Credit: 7, Valid: true}), false},
		{"changed_reviewed", []channelRechargeVersion{{Version: 1, EffectiveAt: 10, Paid: 2, Credit: 10, Valid: true}}, false},
		{"missing_reviewed", nil, false},
		{"duplicate", append(append([]channelRechargeVersion(nil), base...), base...), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := boundedRechargeHistoryMatches(tc.rows, base, 20); got != tc.want {
				t.Fatalf("got %t want %t", got, tc.want)
			}
		})
	}
}

// Join the prior independently verified batches and the newly confirmed three
// apipros hours on one temporary copy. This is still bill-layer acceptance only.
func TestFinanceBoundedRechargeCombinedBills(t *testing.T) {
	manifestPath := os.Getenv("MONITOR_RECHARGE_REUSE_MANIFEST")
	fixturePath := os.Getenv("MONITOR_FINANCE_BOUNDED_RECHARGE_FIXTURE")
	if manifestPath == "" || fixturePath == "" {
		t.Skip("requires reviewed historical batches and bounded evidence")
	}
	boundedRechargeCombinedBills(t, manifestPath, fixturePath)
}

func boundedRechargeCombinedBills(t *testing.T, manifestPath, fixturePath string) (string, []FinanceRechargeRehearsalRequest) {
	t.Helper()
	raw, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var fixture financeBoundedRechargeFixture
	if err := json.Unmarshal(raw, &fixture); err != nil || !fixture.NotAnImportPlan {
		t.Fatal("invalid reviewed fixture", err)
	}
	source, requests := rechargeReuseCurrentSnapshot(t, manifestPath)
	original := source
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	before, err := InspectFinanceRechargeBackup(ctx, source, "2026-05-01", "2026-10-01", FinanceRechargeInspectionOptions{})
	if err != nil || len(before.Domains) != 1 || before.Domains[0].Domain != "apipros.cc" {
		t.Fatal("unexpected remaining historical ratio scope", err)
	}
	root, added := t.TempDir(), 0
	var receipts []FinanceRechargeRehearsalResult
	var delta int64
	for _, domain := range fixture.Domains {
		if domain.Domain != "apipros.cc" {
			continue // blackaicoding is already in the prior reviewed batches.
		}
		if len(domain.Corrections) != 3 {
			t.Fatal("expected exactly three owner-confirmed apipros hours")
		}
		for _, correction := range domain.Corrections {
			if correction.Domain != domain.Domain || correction.ToTs-correction.FromTs != 3600 || correction.FromTs < domain.From || correction.ToTs > domain.To || correction.Paid != 1 || correction.Credit != 1 {
				t.Fatal("correction escaped confirmed scope")
			}
			out := filepath.Join(root, fmt.Sprintf("apipros-%d", len(receipts)))
			request := FinanceRechargeRehearsalRequest{SourceSHA256: giftLocalFileHash(t, source), FromDate: "2026-09-01", ToDate: "2026-10-01", Corrections: []FinanceRechargeCorrection{correction}}
			result, err := RehearseFinanceRechargeCorrections(ctx, source, out, request)
			if err != nil || result.Publishable || len(result.Domains) != 1 {
				t.Fatal("unsafe or failed combined local rehearsal", err)
			}
			if err := addEconomicsInt64(&delta, result.Domains[0].Total.KnownAfterMicro-result.Domains[0].Total.KnownBeforeMicro); err != nil {
				t.Fatal(err)
			}
			added += result.AddedVersions
			requests = append(requests, request)
			receipts = append(receipts, result)
			source = filepath.Join(out, "monitor-rehearsal.db")
		}
	}
	if len(receipts) != 3 || delta != 298907 {
		t.Fatal("combined correction amount or count differs", delta, len(receipts))
	}
	after, err := InspectFinanceRechargeBackup(ctx, source, "2026-05-01", "2026-10-01", FinanceRechargeInspectionOptions{})
	if err != nil || len(after.Domains) != 0 || !reflect.DeepEqual(before.BillCoverage, after.BillCoverage) || !reflect.DeepEqual(before.Skipped, after.Skipped) {
		t.Fatal("historical terms remain missing or raw coverage changed", err)
	}
	db, closeDB, err := giftLocalReadonlyDatabase(source)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	rechargeReuseUnchangedTables(t, db, original, added)
	if output := os.Getenv("MONITOR_FINANCE_COMBINED_BILLS_RESULT"); output != "" {
		data, err := json.MarshalIndent(struct {
			Mode          string                           `json:"mode"`
			Delta         int64                            `json:"additional_corrected_cost_micro"`
			Receipts      []FinanceRechargeRehearsalResult `json:"additional_receipts"`
			Before, After FinanceRechargeGapInspection
		}{"local_combined_bill_layer_not_publishable", delta, receipts, before, after}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := giftLocalWriteNew(output, data); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("combined bill-layer PASS: prior six batches plus 3 apipros hours; remaining ratio-gap domains=0; additional delta_micro=%d; raw bill gaps preserved; no profit claim", delta)
	return source, requests
}

// Verify the independently usable bill layer while the paired facts backup is
// still being transferred. This deliberately makes no revenue/profit claim.
func TestFinanceBoundedRechargeSealedBills(t *testing.T) {
	manifestPath := os.Getenv("MONITOR_FINANCE_SEALED_BACKUP_MANIFEST")
	fixturePath := os.Getenv("MONITOR_FINANCE_BOUNDED_RECHARGE_FIXTURE")
	if manifestPath == "" || fixturePath == "" {
		t.Skip("requires explicit sealed backup and reviewed evidence")
	}
	var manifest struct{ Main struct{ File, SHA256 string } }
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &manifest); err != nil || manifest.Main.File == "" || filepath.Base(manifest.Main.File) != manifest.Main.File {
		t.Fatal("invalid sealed backup manifest", err)
	}
	source := filepath.Join(filepath.Dir(manifestPath), manifest.Main.File)
	if giftLocalFileHash(t, source) != manifest.Main.SHA256 {
		t.Fatal("backup identity mismatch")
	}
	original := source
	t.Cleanup(func() {
		if giftLocalFileHash(t, original) != manifest.Main.SHA256 {
			t.Error("original backup changed")
		}
	})
	raw, err = os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var fixture financeBoundedRechargeFixture
	if err := json.Unmarshal(raw, &fixture); err != nil || !fixture.NotAnImportPlan || len(fixture.Domains) != 2 {
		t.Fatal("requires bounded reviewed two-account fixture", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	before, err := InspectFinanceRechargeBackup(ctx, source, "2026-09-01", "2026-10-01", FinanceRechargeInspectionOptions{})
	if err != nil || len(before.Domains) != 2 {
		t.Fatal("baseline gap inventory changed", err)
	}
	sourceHash, root, added := manifest.Main.SHA256, t.TempDir(), 0
	var receipts []FinanceRechargeRehearsalResult
	var delta int64
	for _, domain := range fixture.Domains {
		if len(domain.Corrections) == 0 || len(domain.Corrections) > 4 {
			t.Fatal("unbounded corrections")
		}
		for _, correction := range domain.Corrections {
			if correction.Domain != domain.Domain || correction.FromTs < domain.From || correction.ToTs > domain.To {
				t.Fatal("correction escaped scope")
			}
			output := filepath.Join(root, fmt.Sprintf("step-%02d", len(receipts)))
			request := FinanceRechargeRehearsalRequest{SourceSHA256: sourceHash, FromDate: "2026-09-01", ToDate: "2026-10-01", Corrections: []FinanceRechargeCorrection{correction}}
			result, err := RehearseFinanceRechargeCorrections(ctx, source, output, request)
			if err != nil || result.Publishable || len(result.Domains) != 1 {
				t.Fatal("unsafe or failed local rehearsal", err)
			}
			v := result.Domains[0]
			for _, periods := range [][]FinanceRechargeRehearsalAmounts{v.Days, v.Months} {
				var sum FinanceRechargeRehearsalAmounts
				for _, p := range periods {
					if err := addRechargeRehearsalAmounts(&sum, p); err != nil {
						t.Fatal(err)
					}
				}
				sum.Period = "total"
				if sum != v.Total {
					t.Fatal("day/month correction totals differ")
				}
			}
			if err := addEconomicsInt64(&delta, v.Total.KnownAfterMicro-v.Total.KnownBeforeMicro); err != nil {
				t.Fatal(err)
			}
			added += result.AddedVersions
			receipts = append(receipts, result)
			source, sourceHash = filepath.Join(output, "monitor-rehearsal.db"), result.OutputSHA256
		}
	}
	after, err := InspectFinanceRechargeBackup(ctx, source, "2026-09-01", "2026-10-01", FinanceRechargeInspectionOptions{})
	if err != nil || len(after.Domains) != 0 || !reflect.DeepEqual(before.BillCoverage, after.BillCoverage) || !reflect.DeepEqual(before.Skipped, after.Skipped) {
		t.Fatal("bill gap closure changed raw coverage or left ratio gaps", err)
	}
	if delta != 101887868 {
		t.Fatal("unexpected correction delta", delta)
	}
	db, closeDB, err := giftLocalReadonlyDatabase(source)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	rechargeReuseUnchangedTables(t, db, original, added)
	if output := os.Getenv("MONITOR_FINANCE_SEALED_BILLS_RESULT"); output != "" {
		data, err := json.MarshalIndent(struct {
			Mode          string                           `json:"mode"`
			Delta         int64                            `json:"corrected_cost_delta_micro"`
			Receipts      []FinanceRechargeRehearsalResult `json:"receipts"`
			Before, After FinanceRechargeGapInspection
		}{"local_sealed_bills_only_not_publishable", delta, receipts, before, after}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := giftLocalWriteNew(output, data); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("bill-only PASS: gap domains 2 -> 0; corrected delta_micro=%d; appended=%d; daily/monthly conservation; raw tables/current settings unchanged; no revenue/profit claim", delta, added)
}

// This is an opt-in local acceptance, not a production importer. The real,
// full finance snapshots are loaded from a hash-verified sealed backup. The
// reduced fixture supplies reviewed intervals and an independent amount oracle.
func TestFinanceBoundedRechargeSealedMonth(t *testing.T) {
	manifestPath := os.Getenv("MONITOR_FINANCE_SEALED_BACKUP_MANIFEST")
	fixturePath := os.Getenv("MONITOR_FINANCE_BOUNDED_RECHARGE_FIXTURE")
	if manifestPath == "" || fixturePath == "" {
		t.Skip("requires explicit sealed backup manifest and reviewed local evidence")
	}
	var manifest struct {
		Version     int
		Main, Facts struct {
			File, SHA256 string
			Bytes        int64
		}
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &manifest); err != nil || manifest.Version != 1 {
		t.Fatal("invalid backup manifest", err)
	}
	paths := make([]string, 0, 2)
	for _, member := range []struct {
		File, SHA256 string
		Bytes        int64
	}{manifest.Main, manifest.Facts} {
		if member.File == "" || filepath.Base(member.File) != member.File || len(member.SHA256) != 64 || member.Bytes <= 0 {
			t.Fatal("unsafe backup member")
		}
		path := filepath.Join(filepath.Dir(manifestPath), member.File)
		stat, err := os.Lstat(path)
		if err != nil || !stat.Mode().IsRegular() || stat.Size() != member.Bytes || giftLocalFileHash(t, path) != member.SHA256 {
			t.Fatal("sealed backup identity mismatch", member.File, err)
		}
		paths = append(paths, path)
	}
	identities := map[string]string{paths[0]: manifest.Main.SHA256, paths[1]: manifest.Facts.SHA256,
		manifestPath: giftLocalFileHash(t, manifestPath), fixturePath: giftLocalFileHash(t, fixturePath)}
	t.Cleanup(func() {
		for path, hash := range identities {
			if giftLocalFileHash(t, path) != hash {
				t.Error("input evidence mutated", filepath.Base(path))
			}
		}
	})
	raw, err = os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var fixture financeBoundedRechargeFixture
	if err := json.Unmarshal(raw, &fixture); err != nil || !fixture.NotAnImportPlan || len(fixture.Domains) == 0 || len(fixture.Domains) > 2 {
		t.Fatal("invalid bounded fixture", err)
	}
	mainDB, closeMain, err := giftLocalReadonlyDatabase(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	defer closeMain()
	factsDB, closeFacts, err := giftLocalReadonlyDatabase(paths[1])
	if err != nil {
		t.Fatal(err)
	}
	defer closeFacts()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	fromDate, toDate := "2026-09-01", "2026-10-01"
	from, _ := financeStartHour(fromDate)
	to, _ := financeStartHour(toDate)
	var expectedDelta int64
	var corrections []FinanceRechargeCorrection
	for _, domain := range fixture.Domains {
		if domain.From < from || domain.To > to || len(domain.Rows) > 800 || len(domain.Corrections) == 0 || len(domain.Corrections) > 4 {
			t.Fatal("fixture exceeds monthly acceptance scope")
		}
		var full []ChannelFinanceVersion
		if err := mainDB.WithContext(ctx).Where("domain=?", domain.Domain).Order("version").Limit(201).Find(&full).Error; err != nil {
			t.Fatal(err)
		}
		if !boundedRechargeHistoryMatches(correctionTerms(t, full), correctionTerms(t, domain.Versions), domain.To) {
			t.Fatal("reduced evidence no longer matches full backup version history", domain.Domain)
		}
		// Verify the raw bill oracle independently of the correction planner.
		for _, expected := range domain.Rows {
			var actual ChannelUpstreamUsageHour
			if err := mainDB.WithContext(ctx).Where("domain=? AND hour_ts=?", domain.Domain, expected.HourTs).First(&actual).Error; err != nil {
				t.Fatal(err)
			}
			actual.FetchedAt, expected.FetchedAt = 0, 0
			if !reflect.DeepEqual(actual, expected) {
				t.Fatal("raw bill differs from reviewed evidence", domain.Domain, expected.HourTs)
			}
		}
		accounts := map[string]ChannelUpstreamAccountView{domain.Domain: {Configured: true, UsageSyncEnabled: true,
			Provider: upstreamProviderSub2API, UsageGranularity: "hour", FinanceRequiredFrom: domain.From}}
		_, money, err := projectFinanceBillWindow(domain.Rows, stabilityScope{FromTs: domain.From, ToTs: domain.To}, fixture.ObservedAt, accounts,
			map[string][]channelRechargeVersion{domain.Domain: correctionTerms(t, full)})
		if err != nil {
			t.Fatal(err)
		}
		beforeAmount := int64(0)
		if money[domain.Domain].RechargeCorrected.MicroUSD != "" {
			var ok bool
			beforeAmount, ok = financeMoneyInt64(money[domain.Domain].RechargeCorrected)
			if !ok {
				t.Fatal("invalid baseline corrected amount")
			}
		}
		want, ok := financeMoneyInt64(channelEconomicsMoneyView{MicroUSD: domain.ExpectedAfterMicro})
		if !ok || want < beforeAmount {
			t.Fatal("unexpected oracle delta")
		}
		if err := addEconomicsInt64(&expectedDelta, want-beforeAmount); err != nil {
			t.Fatal(err)
		}
		for _, c := range domain.Corrections {
			if c.Domain != domain.Domain || c.FromTs < domain.From || c.ToTs > domain.To {
				t.Fatal("correction escaped reviewed window")
			}
			corrections = append(corrections, c)
		}
	}
	newMonitor := func(db *gorm.DB) *Monitor {
		m := &Monitor{storeDB: db, usageFactsDB: factsDB, backgroundCtx: ctx, cfg: Settings{
			FinanceEnabled: true, FinanceStartDate: "2026-05-01", LocalSnapshotOnly: true,
			ChannelEconomicsReportEnabled: true, UsageFactsHistorySourceEpoch: "newapi-hotlogs-complete-20260817-v1",
			StorePath: filepath.Join(t.TempDir(), "cache-only.db"),
		}}
		t.Cleanup(func() { m.financeGiftEvidenceGuard.close() })
		return m
	}
	before := giftReuseReadReport(t, newMonitor(mainDB), fromDate, "2026-09-30")
	root, source, sourceHash := t.TempDir(), paths[0], manifest.Main.SHA256
	var receipts []FinanceRechargeRehearsalResult
	added := 0
	for i, correction := range corrections {
		request := FinanceRechargeRehearsalRequest{SourceSHA256: sourceHash, FromDate: fromDate, ToDate: toDate,
			Corrections: []FinanceRechargeCorrection{correction}}
		out := filepath.Join(root, fmt.Sprintf("step-%02d", i))
		result, err := RehearseFinanceRechargeCorrections(ctx, source, out, request)
		if err != nil || result.Publishable {
			t.Fatal("local correction failed or became publishable", err)
		}
		receipts = append(receipts, result)
		added += result.AddedVersions
		source, sourceHash = filepath.Join(out, "monitor-rehearsal.db"), result.OutputSHA256
	}
	afterDB, closeAfter, err := giftLocalReadonlyDatabase(source)
	if err != nil {
		t.Fatal(err)
	}
	defer closeAfter()
	rechargeReuseUnchangedTables(t, afterDB, paths[0], added)
	for _, c := range corrections {
		var history []ChannelFinanceVersion
		if err := afterDB.WithContext(ctx).Where("domain=?", c.Domain).Find(&history).Error; err != nil {
			t.Fatal(err)
		}
		more, err := buildFinanceRechargeCorrection(history, c, fixture.ObservedAt)
		if err != nil || len(more) != 0 {
			t.Fatal("combined replay is not idempotent", err)
		}
	}
	after := giftReuseReadReport(t, newMonitor(afterDB), fromDate, "2026-09-30")
	if before.From != from || before.To != to || after.From != from || after.To != to {
		t.Fatal("not a complete September report")
	}
	if !reflect.DeepEqual(before.Statement.UserConsumption, after.Statement.UserConsumption) ||
		!reflect.DeepEqual(before.Statement.OperatingRevenue, after.Statement.OperatingRevenue) ||
		!reflect.DeepEqual(before.GiftCoverage, after.GiftCoverage) ||
		!reflect.DeepEqual(before.Statement.KnownUpstreamBilledCost, after.Statement.KnownUpstreamBilledCost) ||
		!reflect.DeepEqual(before.Statement.AWSInfrastructureCost, after.Statement.AWSInfrastructureCost) {
		t.Fatal("recharge correction changed unrelated monetary evidence")
	}
	b, bok := financeMoneyInt64(before.Statement.KnownRawCorrectedUpstreamCost)
	a, aok := financeMoneyInt64(after.Statement.KnownRawCorrectedUpstreamCost)
	if !bok || !aok || a-b != expectedDelta {
		t.Fatalf("monthly corrected cost delta got %d want %d", a-b, expectedDelta)
	}
	bc, ac := before.UpstreamCoverage, after.UpstreamCoverage
	if bc.RelevantDomains != ac.RelevantDomains || bc.CompleteDomains != ac.CompleteDomains ||
		bc.ExpectedDomainHours != ac.ExpectedDomainHours || bc.CompletedDomainHours != ac.CompletedDomainHours ||
		bc.UnconfiguredDomains != ac.UnconfiguredDomains || bc.UnconfiguredUserRequests != ac.UnconfiguredUserRequests ||
		bc.UnconfiguredUserConsumption != ac.UnconfiguredUserConsumption ||
		ac.CorrectedCompleteDomains-bc.CorrectedCompleteDomains != len(fixture.Domains) {
		t.Fatal("unexpected coverage change", bc, ac)
	}
	if ac.UnconfiguredDomains > 0 && after.Statement.RawCorrectedUpstreamCost != nil {
		t.Fatal("unconfigured source cost became a fully known total")
	}
	beforeDays := map[string]financeDailyView{}
	for _, day := range before.Days {
		beforeDays[day.Date] = day
	}
	var daily, monthly int64
	for _, day := range after.Days {
		old, found := beforeDays[day.Date]
		if !found || !reflect.DeepEqual(old.Statement.UserConsumption, day.Statement.UserConsumption) ||
			!reflect.DeepEqual(old.Statement.OperatingRevenue, day.Statement.OperatingRevenue) {
			t.Fatal("daily revenue or day scope changed", day.Date)
		}
		touched := false
		for _, c := range corrections {
			touched = touched || (c.FromTs < day.To && c.ToTs > day.From)
		}
		if !touched && old.CostReconciliation.KnownTotal != day.CostReconciliation.KnownTotal {
			t.Fatal("unrelated day cost changed", day.Date)
		}
		if day.CostReconciliation.Status == "source_mismatch" {
			t.Fatal("daily cost source mismatch", day.Date)
		}
		if _, err := financeAddMoney(&daily, day.CostReconciliation.KnownTotal); err != nil {
			t.Fatal(err)
		}
	}
	for _, period := range after.Periods {
		if _, err := financeAddMoney(&monthly, period.Statement.KnownRawCorrectedUpstreamCost); err != nil {
			t.Fatal(err)
		}
	}
	if daily != a || monthly != a {
		t.Fatal("daily/monthly/total cost mismatch", daily, monthly, a)
	}
	restarted := giftReuseReadReport(t, newMonitor(afterDB), fromDate, "2026-09-30")
	if !reflect.DeepEqual(after.Statement, restarted.Statement) || !reflect.DeepEqual(after.Days, restarted.Days) || !reflect.DeepEqual(after.CostDetails, restarted.CostDetails) {
		t.Fatal("cold/warm/restart report differs")
	}
	if after.Statement.AWSInfrastructureCost == nil && after.Statement.OperatingProfit != nil {
		t.Fatal("missing AWS turned into exact profit")
	}
	if output := os.Getenv("MONITOR_FINANCE_SEALED_MONTH_RESULT"); output != "" {
		data, err := json.MarshalIndent(struct {
			Mode          string                           `json:"mode"`
			Delta         int64                            `json:"corrected_cost_delta_micro"`
			Receipts      []FinanceRechargeRehearsalResult `json:"receipts"`
			Before, After financeOperatingReport
		}{"local_sealed_month_rehearsal_not_publishable", expectedDelta, receipts, before, after}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := giftLocalWriteNew(output, data); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("sealed month PASS: appended=%d corrected_delta_micro=%d raw bills/revenue/gifts unchanged; daily/monthly totals agree; replay=no-op; warm/restart agree; profit_complete=%t", added, expectedDelta, after.Statement.OperatingProfit != nil)
}
