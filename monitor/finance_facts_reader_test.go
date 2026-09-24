package monitor

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

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
