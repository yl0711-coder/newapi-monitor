//go:build unix

package monitor

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// Enlarge the existing hand-calculated report fixture with zero-quota usage.
// All known dollar expectations remain unchanged, while the repaired hour
// now exercises the independent 4833-row path end-to-end through report HTTP.
func TestFinanceGiftLargeLocalReportRefresh(t *testing.T) {
	m, evidence, targets, hour := giftReportLocalFixture(t)
	ctx, db := context.Background(), m.usageFactsStore()
	prior, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", hour, 7)
	if err != nil {
		t.Fatal(err)
	}
	large := evidence[0]
	for i := 0; i < 4830; i++ {
		event := FinanceGiftBoundaryEvent{SourceLogID: int64(1000 + i), HourTs: hour, UserID: 7, EventAt: hour + 50 + int64(i/3), Kind: "usage"}
		event.EvidenceHash = financeGiftBoundaryEventHash(event)
		prior.Events = append(prior.Events, event)
		row := evidence[0].Rows[0]
		quota, group := int64(0), "business"
		row.ID, row.CreatedAt, row.Type, row.Quota, row.Group = event.SourceLogID, event.EventAt, 2, &quota, &group
		large.Rows = append(large.Rows, row)
	}
	var facts []FinanceUserHourFact
	if err := db.Where("hour_ts=?", hour).Find(&facts).Error; err != nil {
		t.Fatal(err)
	}
	for i := range facts {
		if facts[i].UserID == 7 {
			facts[i].Requests += 4830
		}
	}
	if _, err := replaceFinanceUserHourFacts(ctx, db, hour, "v1", facts, hour+10800); err != nil {
		t.Fatal(err)
	}
	if _, err := replaceFinanceGiftBoundaryUserHour(ctx, db, hour, 7, hour+10800, "v1", prior.Events); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(t.TempDir(), "backup.db")
	if err := db.Exec("VACUUM INTO ?", backup).Error; err != nil {
		t.Fatal(err)
	}
	plan, readDigest, err := PrepareFinanceGiftLargeHourPlan(ctx, backup, "v1", hour, 7)
	if err != nil {
		t.Fatal(err)
	}
	readPath, evidencePath := filepath.Join(t.TempDir(), "read.json"), filepath.Join(t.TempDir(), "evidence.json")
	giftLargeWriteTestJSON(t, readPath, plan)
	giftLargeWriteTestJSON(t, evidencePath, financeGiftLargeEvidence{Version: 1, PlanSHA256: readDigest, SourceEpoch: "v1", LocalContentHash: plan.LocalContentHash, financeGiftLocalEvidence: large})
	dir := filepath.Join(t.TempDir(), "job")
	_, digest, err := PrepareFinanceGiftLargeLocalJob(ctx, backup, readPath, evidencePath, dir, readDigest)
	if err != nil {
		t.Fatal(err)
	}
	copyDB, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeDB)
	m.usageFactsDB = copyDB
	money := giftMixedMonetarySnapshot(t, copyDB)
	before := giftReadReportHTTP(t, m)
	giftAssertReportAmounts(t, before.Report, 8)
	if hit := giftReadReportHTTP(t, m); hit.Cache != "hit" {
		t.Fatal("initial report not cached")
	}
	base := giftPeriodCacheSnapshot(m)
	result, err := RunFinanceGiftLargeLocalJob(ctx, dir, digest)
	if err != nil || len(result.Entries) != 1 || result.Entries[0].RowsUpdated != 4833 {
		t.Fatal("large repair failed", result, err)
	}
	stale := giftReadReportHTTP(t, m)
	if stale.Cache != "stale-refreshing" || !reflect.DeepEqual(stale.Report, before.Report) {
		t.Fatal("did not preserve old report during refresh")
	}
	updated := giftWaitReportRefresh(t, m, 5)
	giftAssertReportAmounts(t, updated.Report, 5)
	if !reflect.DeepEqual(base, giftPeriodCacheSnapshot(m)) {
		t.Fatal("large repair rebuilt unaffected base month")
	}
	// Finish other small targets using the unchanged original repair path.
	source, err := giftLocalSource(ctx, evidence[1:])
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	for _, target := range targets[1:] {
		if _, err := repairFinanceGiftBoundaryScope(ctx, copyDB, source, target.SourceEpoch, target.HourTs, target.UserID, time.Now().Unix()); err != nil {
			t.Fatal(err)
		}
	}
	complete := giftWaitReportRefresh(t, m, 0)
	giftAssertReportAmounts(t, complete.Report, 0)
	if giftMixedMonetarySnapshot(t, copyDB) != money {
		t.Fatal("large/small repairs or report refresh altered monetary facts")
	}
}
