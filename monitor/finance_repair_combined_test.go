//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"gorm.io/gorm"
)

// Reuse the independently guarded acceptance steps; do not add a second gift
// import protocol or bypass finite recharge/ledger safety limits.
func TestFinanceRepairCombinedCurrentSnapshot(t *testing.T) {
	manifest := os.Getenv("MONITOR_RECHARGE_REUSE_MANIFEST")
	if manifest == "" || os.Getenv("MONITOR_GIFT_RETAINED_MANIFEST") == "" {
		t.Skip("requires explicit private recharge and retained gift inventories")
	}
	var corrected string
	var requests []FinanceRechargeRehearsalRequest
	if fixture := os.Getenv("MONITOR_FINANCE_BOUNDED_RECHARGE_FIXTURE"); fixture != "" {
		corrected, requests = boundedRechargeCombinedBills(t, manifest, fixture)
	} else {
		corrected, requests = rechargeReuseCurrentSnapshot(t, manifest)
	}
	gift := giftReuseRetainedSeries(t, corrected)
	// Keep the preceding step's checkpoint closed and immutable. Only this new
	// continuation copy gets ledger revisions; repaired facts remain the same.
	path := filepath.Join(t.TempDir(), "combined.db")
	if _, err := giftLocalCopyBackup(corrected, path); err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := giftLocalDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	from, to := os.Getenv("MONITOR_GIFT_REUSE_FROM"), os.Getenv("MONITOR_GIFT_REUSE_TO")
	m := &Monitor{storeDB: db, usageFactsDB: gift.usageFactsDB, backgroundCtx: ctx, cfg: Settings{
		FinanceEnabled: true, FinanceStartDate: from, LocalSnapshotOnly: true,
		ChannelEconomicsReportEnabled: true, UsageFactsHistorySourceEpoch: gift.cfg.UsageFactsHistorySourceEpoch,
	}}
	defer m.financeGiftEvidenceGuard.close()
	defer func() {
		cancel()
		deadline := time.Now().Add(5 * time.Second)
		for m.financeReportRefreshRunning.Load() && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if m.financeReportRefreshRunning.Load() {
			t.Error("combined report refresh did not stop")
		}
	}()
	before := giftReuseReadReport(t, m, from, to) // Warm the real HTTP report cache.
	facts := giftMixedMonetarySnapshot(t, m.usageFactsDB)
	headsBefore := combinedManifestHeads(t, db)
	var windows []FinanceRechargeCorrection
	for _, request := range requests {
		for _, correction := range request.Corrections {
			for start := correction.FromTs; start < correction.ToTs; {
				window := correction
				window.FromTs = start
				window.ToTs = min(correction.ToTs, start-start%3600+financeRechargeRebuildHourLimit*3600)
				windows = append(windows, window)
				start = window.ToTs
			}
		}
	}
	if len(windows) > 100 {
		t.Fatal("combined rehearsal window budget exceeded")
	}
	var results []FinanceRechargeEconomicsRebuild
	for _, window := range windows {
		var result []FinanceRechargeEconomicsRebuild
		if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var err error
			result, err = rebuildFinanceRechargeEconomics(ctx, tx, []FinanceRechargeCorrection{window}, time.Now().Unix())
			return err
		}); err != nil {
			t.Fatalf("bounded rebuild %s: %v", window.Domain, err)
		}
		results = append(results, result...)
	}
	headsAfter := combinedManifestHeads(t, db)
	for _, old := range headsBefore {
		inScope := false
		for _, window := range windows {
			inScope = inScope || (old.Domain == window.Domain && old.HourTs >= window.FromTs-window.FromTs%3600 && old.HourTs < window.ToTs)
		}
		if inScope {
			continue
		}
		found := false
		for _, next := range headsAfter {
			if reflect.DeepEqual(old, next) {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("unrelated ledger head changed", old.Domain, old.HourTs)
		}
	}
	for _, window := range windows {
		result, err := rebuildFinanceRechargeEconomics(ctx, db, []FinanceRechargeCorrection{window}, time.Now().Unix())
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range result {
			if row.RebuiltHours != 0 {
				t.Fatal("replay republished an existing cost", row.Domain)
			}
		}
	}
	if !reflect.DeepEqual(headsAfter, combinedManifestHeads(t, db)) {
		t.Fatal("replay changed ledger heads")
	}
	after := giftReuseReadReport(t, m, from, to)
	assertGiftReuseBreakdown(t, after.Statement, after.Periods, after.Days)
	if !after.GiftCoverage.Complete || !reflect.DeepEqual(before.GiftCoverage, after.GiftCoverage) ||
		!reflect.DeepEqual(before.Statement.UserConsumption, after.Statement.UserConsumption) ||
		!reflect.DeepEqual(before.Statement.OperatingRevenue, after.Statement.OperatingRevenue) ||
		!reflect.DeepEqual(before.Statement.KnownUpstreamBilledCost, after.Statement.KnownUpstreamBilledCost) ||
		facts != giftMixedMonetarySnapshot(t, m.usageFactsDB) {
		t.Fatal("ledger rebuild changed user/gift/original bill evidence")
	}
	var monthlyCost, dailyCost int64
	var expectedHours, completedHours int64
	for _, period := range after.Periods {
		expectedHours += period.UpstreamCoverage.ExpectedDomainHours
		completedHours += period.UpstreamCoverage.CompletedDomainHours
		if _, err := financeAddMoney(&monthlyCost, period.Statement.KnownRawCorrectedUpstreamCost); err != nil {
			t.Fatal(err)
		}
	}
	if expectedHours != after.UpstreamCoverage.ExpectedDomainHours || completedHours != after.UpstreamCoverage.CompletedDomainHours {
		t.Fatal("monthly bill coverage disagrees with total")
	}
	for _, day := range after.Days {
		if day.CostReconciliation.Status == "source_mismatch" {
			t.Fatal("daily cost source mismatch", day.Date)
		}
		if _, err := financeAddMoney(&dailyCost, day.CostReconciliation.KnownTotal); err != nil {
			t.Fatal(err)
		}
	}
	if economicsMoney(monthlyCost) != after.Statement.KnownRawCorrectedUpstreamCost || dailyCost != monthlyCost {
		t.Fatal("day/month corrected costs do not reconcile")
	}
	if after.Statement.AWSInfrastructureCost == nil && after.Statement.OperatingProfit != nil {
		t.Fatal("missing AWS produced complete profit")
	}
	// A fresh monitor must agree with the refreshed in-memory cache.
	fresh := &Monitor{storeDB: db, usageFactsDB: gift.usageFactsDB, cfg: m.cfg, backgroundCtx: ctx}
	defer fresh.financeGiftEvidenceGuard.close()
	restarted := giftReuseReadReport(t, fresh, from, to)
	if !reflect.DeepEqual(after.Statement, restarted.Statement) || !reflect.DeepEqual(after.Periods, restarted.Periods) ||
		!reflect.DeepEqual(after.Days, restarted.Days) || !reflect.DeepEqual(after.CostDetails, restarted.CostDetails) {
		t.Fatal("cached report disagrees with restart")
	}
	var check string
	if err := db.Raw("PRAGMA quick_check").Scan(&check).Error; err != nil || check != "ok" {
		t.Fatal("combined database integrity", check, err)
	}
	var rebuilt, unchanged int64
	for _, result := range results {
		rebuilt += result.RebuiltHours
		unchanged += result.UnchangedHours
	}
	if output := os.Getenv("MONITOR_FINANCE_COMBINED_RESULT"); output != "" {
		raw, err := json.Marshal(struct {
			Results []FinanceRechargeEconomicsRebuild `json:"rebuild"`
			Before  financeOperatingReport            `json:"before_rebuild"`
			After   financeOperatingReport            `json:"after_rebuild"`
		}{results, before, after})
		if err != nil {
			t.Fatal(err)
		}
		if err := giftLocalWriteNew(output, raw); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("combined PASS: windows=%d rebuilt=%d unchanged=%d; gift_complete=%t months=%d days=%d; cache/restart/replay matched; profit_complete=%t", len(windows), rebuilt, unchanged, after.GiftCoverage.Complete, len(after.Periods), len(after.Days), after.Statement.OperatingProfit != nil)
}

func combinedManifestHeads(t *testing.T, db *gorm.DB) []ChannelEconomicsHourManifestCurrent {
	t.Helper()
	var rows []ChannelEconomicsHourManifestCurrent
	if err := db.Order("domain,hour_ts,semantics_version").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}
