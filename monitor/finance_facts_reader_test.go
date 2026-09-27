package monitor

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// Exercise the complete gift reader, including the data-version cache guard,
// while the facts writer holds unpublished changes. Use real WAL files and a
// one-connection writer pool; a plain Count test cannot cover this boundary.
func TestFinanceGiftReaderDuringPublicationAndRollback(t *testing.T) {
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{
		StorePath: filepath.Join(dir, "main.db"), UsageFactsStorePath: filepath.Join(dir, "facts.db"),
		FinanceEnabled: true, FinanceFactsReadIsolationEnabled: true, LocalSnapshotOnly: true,
	}}
	if err := m.openStore(m.cfg.StorePath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	const hour = int64(3600)
	publishFinanceGiftTestHour(t, m, hour, 7, "epoch", 500_000, 2)
	grant := FinanceCreditEvent{SourceLogID: 1, EventAt: hour + 10, TargetUserID: 7, UserCreatedAt: 1,
		Action: "quota_add", Unit: "USD_quota", ValueMicro: 100_000_000, NetChangeMicro: 100_000_000,
		Cohort: "trial_candidate_within_24h", EligibleTrial: true}
	grant.EvidenceHash = financeCreditEventHash(grant)
	if _, err := replaceFinanceCreditHour(context.Background(), m.usageFactsStore(), hour, 20_000, "epoch",
		financeCreditHourFetch{Events: []FinanceCreditEvent{grant}, SourceRows: 1}); err != nil {
		t.Fatal(err)
	}
	load := func(want int64) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		result, err := m.loadFinanceGiftAllocation(ctx, hour, hour, hour+3600)
		if err != nil || !result.Coverage.Complete || result.Allocation.PeriodGiftConsumptionMicroUSD != want {
			t.Fatalf("unpublished/stale gift amount: got=%d complete=%v err=%v", result.Allocation.PeriodGiftConsumptionMicroUSD, result.Coverage.Complete, err)
		}
	}
	load(1_000_000) // Warm evidence cache before taking the writer connection.
	pool, err := m.usageFactsStore().DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(1)
	tx := m.usageFactsStore().Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	// Atomically publish different usage and its matching evidence inside an
	// outer transaction. The independent reader must still see the old state.
	publishFinanceGiftTestHour(t, &Monitor{usageFactsDB: tx}, hour, 7, "epoch", 1_000_000, 2)
	load(1_000_000)
	blocked, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	var count int64
	err = m.usageFactsStore().WithContext(blocked).Model(&FinanceUserHourState{}).Count(&count).Error
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fixture did not hold writer connection: %v", err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	load(2_000_000) // Committed correction must invalidate the cached old amount.
	load(2_000_000)
	tx = m.usageFactsStore().Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	if err := tx.Model(&FinanceGiftBoundaryEvent{}).Where("hour_ts=?", hour).Update("quota", 99).Error; err != nil {
		t.Fatal(err)
	}
	load(2_000_000) // Uncommitted broken evidence must not leak into the report.
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	load(2_000_000)
}

func TestFinanceFactsReaderContinuesDuringWriterTransaction(t *testing.T) {
	dir := t.TempDir()
	m := &Monitor{cfg: Settings{
		StorePath:                        filepath.Join(dir, "main.db"),
		UsageFactsStorePath:              filepath.Join(dir, "facts.db"),
		FinanceEnabled:                   true,
		FinanceFactsReadIsolationEnabled: true,
		LocalSnapshotOnly:                true,
	}}
	if err := m.openStore(m.cfg.StorePath); err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.usageFactsDB.Create(&FinanceUserHourState{HourTs: 3600, Status: "complete"}).Error; err != nil {
		t.Fatal(err)
	}
	reader := m.financeFactsReadStore()
	if reader == nil || reader == m.usageFactsDB || m.financeFactsReadDB.Load() == nil {
		t.Fatal("finance read was not isolated from the one-connection facts writer")
	}
	readPool, err := reader.DB()
	if err != nil || readPool.Stats().MaxOpenConnections != 1 {
		t.Fatalf("read-only pool is not bounded: err=%v stats=%+v", err, readPool.Stats())
	}
	if err := reader.Exec("INSERT INTO finance_user_hour_states(hour_ts,status) VALUES(?,?)", 7200, "complete").Error; err == nil {
		t.Fatal("finance read handle accepted a write")
	}
	tx := m.usageFactsDB.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	if err := tx.Create(&FinanceUserHourState{HourTs: 7200, Status: "complete"}).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var count int64
	if err := reader.WithContext(ctx).Model(&FinanceUserHourState{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("read-only WAL snapshot blocked by writer or saw uncommitted data: count=%d err=%v", count, err)
	}
	blockedCtx, blockedCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer blockedCancel()
	if err := m.usageFactsDB.WithContext(blockedCtx).Model(&FinanceUserHourState{}).Count(&count).Error; err == nil {
		t.Fatal("test setup did not occupy the single facts writer connection")
	}
}

func TestFinanceFactsReaderOptInAndSharedStoreFallback(t *testing.T) {
	m := newStabilityTestMonitor(t)
	if got := m.financeFactsReadStore(); got != m.usageFactsStore() {
		t.Fatal("disabled isolation opened a new connection")
	}
	m.cfg.FinanceFactsReadIsolationEnabled = true
	if got := m.financeFactsReadStore(); got != m.usageFactsStore() {
		t.Fatal("shared legacy main/facts store unexpectedly opened a second connection")
	}
}
