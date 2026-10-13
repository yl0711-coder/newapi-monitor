package monitor

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func sub2TokenStoreFixture(t *testing.T) (*Monitor, ChannelUpstreamAccount, []sub2PricingUsageItem, int64, []byte) {
	t.Helper()
	m, account := tokenConfigFixture(t)
	if err := m.storeDB.AutoMigrate(&ChannelUpstreamTokenScanCheckpoint{}, &ChannelUpstreamPricingPageCheckpoint{}); err != nil {
		t.Fatal(err)
	}
	_, items, day, key := sub2TokenFixture(t)
	return m, account, items, day, key
}

func TestSub2TokenStoreResumeAndRetryLeavesLegacyUntouched(t *testing.T) {
	m, account, items, day, key := sub2TokenStoreFixture(t)
	ctx := context.Background()
	legacy := ChannelUpstreamPricingPageCheckpoint{Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account),
		HourTs: day, Provider: account.Provider, SemanticsVersion: upstreamPricingSemanticsVersion,
		WindowSeconds: 86400, NextPage: 2, Total: 3, SourceRows: 2, AggregatesJSON: "legacy-sentinel"}
	if err := m.storeDB.Create(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	first, err := m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[:2], 1), 0, day+86400, key, "v1")
	if err != nil || first.Revision != 1 {
		t.Fatal("first save", err)
	}
	// Discard all Monitor-local state and use the persisted revision/envelope.
	m = &Monitor{storeDB: m.storeDB}
	loaded, err := m.loadSub2TokenScanCheckpoint(ctx, account, day, key, "v1")
	if err != nil || !reflect.DeepEqual(first, loaded) {
		t.Fatal("reload", err)
	}
	retry, err := m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[:2], 1), 1, day+86401, key, "v1")
	if err != nil || retry != first {
		t.Fatal("retry changed cursor", err)
	}
	second, err := m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[2:], 2), 1, day+86402, key, "v1")
	if err != nil || second.Revision != 2 {
		t.Fatal("resume", err)
	}
	cp, err := decodeSub2TokenCheckpoint([]byte(second.Envelope), account, day, key, "v1")
	if err != nil || cp.NextPage != 3 || len(cp.SeenRows) != 3 {
		t.Fatal("complete cursor", err)
	}
	var unchanged ChannelUpstreamPricingPageCheckpoint
	if err := m.storeDB.First(&unchanged).Error; err != nil || unchanged != legacy {
		t.Fatal("legacy cursor changed", err)
	}
	for _, model := range []any{&ChannelUpstreamCostHourEvidence{}, &ChannelUpstreamCostHourState{}, &ChannelCostSourceBinding{},
		&ChannelCostDirtyHour{}, &ChannelEconomicsDirtyHour{}, &ChannelEconomicsHourPublication{}, &ChannelFinanceVersion{}} {
		var count int64
		if err := m.storeDB.Model(model).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("staging modified %T: %d %v", model, count, err)
		}
	}
}

func TestSub2TokenStoreResetTombstoneRejectsDelayedWriter(t *testing.T) {
	m, account, items, day, key := sub2TokenStoreFixture(t)
	ctx := context.Background()
	// Reset even a nonexistent cursor: cancellation must fence off a producer
	// that has already read revision zero but has not saved its first page.
	reset, err := m.resetSub2TokenScanCheckpoint(ctx, account, day, 0, day+86400, key, "v1")
	if err != nil || reset.Revision != 1 || reset.Envelope != "" {
		t.Fatal("empty tombstone", err)
	}
	if _, err := m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[:2], 1), 0, day+86401, key, "v1"); !errors.Is(err, errSub2TokenCursorConflict) {
		t.Fatal("stale empty writer accepted", err)
	}
	first, err := m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[:2], 1), 1, day+86402, key, "v1")
	if err != nil || first.Revision != 2 {
		t.Fatal(err)
	}
	reset, err = m.resetSub2TokenScanCheckpoint(ctx, account, day, 2, day+86403, key, "v1")
	if err != nil || reset.Revision != 3 || reset.Envelope != "" {
		t.Fatal(err)
	}
	if _, err := m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[2:], 2), 2, day+86404, key, "v1"); !errors.Is(err, errSub2TokenCursorConflict) {
		t.Fatal("stale pre-reset writer accepted", err)
	}
	if _, err := m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[2:], 2), 3, day+86404, key, "v1"); err == nil {
		t.Fatal("reset allowed skipping first page")
	}
	if _, err := m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[:2], 1), 3, day+86405, key, "v1"); err != nil {
		t.Fatal(err)
	}
}

func TestSub2TokenStoreWriteFailureRollsBackAndCorruptReset(t *testing.T) {
	m, account, items, day, key := sub2TokenStoreFixture(t)
	ctx := context.Background()
	first, err := m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[:2], 1), 0, day+86400, key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Exec(`CREATE TRIGGER fail_token_cursor BEFORE UPDATE ON channel_upstream_token_scan_checkpoints BEGIN SELECT RAISE(ABORT, 'synthetic cursor failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[2:], 2), 1, day+86401, key, "v1"); err == nil {
		t.Fatal("expected write failure")
	}
	if _, err := m.resetSub2TokenScanCheckpoint(ctx, account, day, 1, day+86401, key, "v1"); err == nil {
		t.Fatal("expected reset failure")
	}
	after, err := m.loadSub2TokenScanCheckpoint(ctx, account, day, key, "v1")
	if err != nil || after != first {
		t.Fatal("failed save/reset changed cursor", err)
	}
	if err := m.storeDB.Exec(`DROP TRIGGER fail_token_cursor`).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&ChannelUpstreamTokenScanCheckpoint{}).Where("domain = ?", account.Domain).Update("envelope", "corrupt").Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.loadSub2TokenScanCheckpoint(ctx, account, day, key, "v1"); err == nil {
		t.Fatal("corrupt cursor resumed")
	}
	reset, err := m.resetSub2TokenScanCheckpoint(ctx, account, day, 1, day+86402, key, "v1")
	if err != nil || reset.Envelope != "" || reset.Revision != 2 {
		t.Fatal("explicit corrupt reset failed", err)
	}
}

func TestSub2TokenStoreConcurrentMonitorsAndAccountChange(t *testing.T) {
	m, account, items, day, key := sub2TokenStoreFixture(t)
	sqlDB, err := m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	ctx := context.Background()
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			other := &Monitor{storeDB: m.storeDB} // deliberately different mutexes
			_, err := other.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[:2], 1), 0, day+86400, key, "v1")
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	passed, conflicts := 0, 0
	for err := range results {
		if err == nil {
			passed++
		} else if errors.Is(err, errSub2TokenCursorConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if passed != 1 || conflicts != 1 {
		t.Fatalf("lost CAS: passed=%d conflicts=%d", passed, conflicts)
	}
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", account.Domain).Update("user_id", account.UserID+1).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[2:], 2), 1, day+86401, key, "v1"); !errors.Is(err, errSub2TokenCursorConflict) {
		t.Fatal("stale account wrote cursor", err)
	}
	if _, err := m.resetSub2TokenScanCheckpoint(ctx, account, day, 1, day+86401, key, "v1"); !errors.Is(err, errSub2TokenCursorConflict) {
		t.Fatal("stale account reset cursor", err)
	}
}

func TestSub2TokenStoreReadOnlyAndCancelledDoNotWrite(t *testing.T) {
	m, account, items, day, key := sub2TokenStoreFixture(t)
	ctx := context.Background()
	m.cfg.LocalSnapshotOnly = true
	if _, err := m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[:2], 1), 0, day+86400, key, "v1"); !errors.Is(err, errUpstreamTokenReadOnly) {
		t.Fatal(err)
	}
	if _, err := m.resetSub2TokenScanCheckpoint(ctx, account, day, 0, day+86400, key, "v1"); !errors.Is(err, errUpstreamTokenReadOnly) {
		t.Fatal(err)
	}
	m.cfg.LocalSnapshotOnly = false
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := m.saveSub2TokenScanPage(cancelled, account, day, sub2TokenPage(items[:2], 1), 0, day+86400, key, "v1"); err == nil {
		t.Fatal("cancelled save succeeded")
	}
	var count int64
	if err := m.storeDB.Model(&ChannelUpstreamTokenScanCheckpoint{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("rejected operation wrote data", err)
	}
}

func TestSub2TokenStoreSQLiteCloseReopen(t *testing.T) {
	account, items, day, key := sub2TokenFixture(t)
	path := filepath.Join(t.TempDir(), "token-cursor.db")
	open := func() *gorm.DB {
		db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.AutoMigrate(&ChannelUpstreamAccount{}, &ChannelUpstreamTokenScanCheckpoint{}); err != nil {
			t.Fatal(err)
		}
		return db
	}
	db := open()
	if err := db.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db}
	if _, err := m.saveSub2TokenScanPage(context.Background(), account, day, sub2TokenPage(items[:2], 1), 0, day+86400, key, "v1"); err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	if err := sqlDB.Close(); err != nil {
		t.Fatal(err)
	}
	db = open()
	sqlDB, _ = db.DB()
	defer sqlDB.Close()
	m = &Monitor{storeDB: db}
	row, err := m.saveSub2TokenScanPage(context.Background(), account, day, sub2TokenPage(items[2:], 2), 1, day+86401, key, "v1")
	if err != nil || row.Revision != 2 {
		t.Fatal("SQLite reopen lost cursor", err)
	}
	cp, err := decodeSub2TokenCheckpoint([]byte(row.Envelope), account, day, key, "v1")
	if err != nil || len(cp.SeenRows) != 3 {
		t.Fatal(err)
	}
	var charge int64
	for _, r := range cp.Rows {
		charge += r.ChargeUnits
	}
	if charge != 481479 {
		t.Fatal("SQLite restart changed money")
	}
	var integrity string
	if err := db.Raw("PRAGMA quick_check").Scan(&integrity).Error; err != nil || integrity != "ok" {
		t.Fatal("SQLite integrity", err)
	}
}

func TestSub2TokenStoreIndependentConnectionsNeverOverwrite(t *testing.T) {
	account, items, day, key := sub2TokenFixture(t)
	path := filepath.Join(t.TempDir(), "competing.db")
	open := func() *gorm.DB {
		db, err := gorm.Open(sqlite.Open(path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(250)"), &gorm.Config{})
		if err != nil {
			t.Fatal(err)
		}
		sqlDB, err := db.DB()
		if err != nil {
			t.Fatal(err)
		}
		sqlDB.SetMaxOpenConns(1)
		t.Cleanup(func() { _ = sqlDB.Close() })
		return db
	}
	firstDB := open()
	if err := firstDB.AutoMigrate(&ChannelUpstreamAccount{}, &ChannelUpstreamTokenScanCheckpoint{}); err != nil {
		t.Fatal(err)
	}
	if err := firstDB.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	monitors := []*Monitor{{storeDB: firstDB}, {storeDB: open()}}
	// Both writers explicitly prepare from the same revision before either is
	// released. They have separate SQL pools, connections, and Go mutexes.
	encoded, err := advanceSub2TokenCheckpoint(nil, account, sub2TokenPage(items[:2], 1), day, day+86400, key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, m := range monitors {
		go func(m *Monitor) {
			<-start
			_, err := m.commitSub2TokenCursor(context.Background(), account, day, ChannelUpstreamTokenScanCheckpoint{}, encoded, day+86400, false)
			results <- err
		}(m)
	}
	close(start)
	success := 0
	for i := 0; i < 2; i++ {
		err := <-results
		if err == nil {
			success++
		} else if !errors.Is(err, errSub2TokenCursorConflict) && !isUpstreamUsageLocalStoreBusy(err) {
			t.Fatal(err)
		}
	}
	if success != 1 {
		t.Fatalf("expected exactly one committed revision: %d", success)
	}
	current, err := monitors[0].loadSub2TokenScanCheckpoint(context.Background(), account, day, key, "v1")
	if err != nil || current.Revision != 1 || current.Envelope != string(encoded) {
		t.Fatal("concurrent commit damaged cursor", err)
	}
	// A loser retries by reloading, never by overriding the winner's cursor.
	again, err := monitors[1].saveSub2TokenScanPage(context.Background(), account, day, sub2TokenPage(items[:2], 1), current.Revision, day+86401, key, "v1")
	if err != nil || again != current {
		t.Fatal("retry did not converge idempotently", err)
	}
}
