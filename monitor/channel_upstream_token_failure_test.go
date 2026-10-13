package monitor

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func tokenResumeCheckpoint(t *testing.T, f tokenReconcileFixture) ChannelUpstreamPricingPageCheckpoint {
	t.Helper()
	cp, err := decodeSub2TokenCheckpoint([]byte(f.cursor.Envelope), f.account, f.day, f.key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	return ChannelUpstreamPricingPageCheckpoint{NextPage: cp.NextPage, Total: cp.Total, SourceRows: int64(len(cp.SeenRows))}
}

func TestSub2TokenFailurePreinvalidatedAcrossSQLiteLockAndRestart(t *testing.T) {
	f, in := tokenCostStoreFixture(t, -1)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "durable-token-failure.db")
	// Clone only this synthetic fixture, never a live or user database.
	if err := f.m.storeDB.Exec("VACUUM INTO ?", path).Error; err != nil {
		t.Fatal(err)
	}
	open := func() *gorm.DB {
		db, err := gorm.Open(sqlite.Open(path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(50)"), &gorm.Config{})
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
	f.m = &Monitor{storeDB: open(), cfg: f.m.cfg}
	shadow := f.m.beginSub2TokenShadow(ctx, f.account, f.day, f.day+86401, tokenResumeCheckpoint(t, f), true)
	if shadow == nil || shadow.failed || shadow.cursor.Revision != f.cursor.Revision+1 || shadow.cursor.SuccessRevision != 0 || shadow.cursor.StableScans != 0 {
		t.Fatalf("did not invalidate before resume: %+v", shadow)
	}
	// Another connection holds the SQLite writer lock after the durable
	// transition. Failure must not depend on a second successful DB write.
	lockDB, _ := open().DB()
	conn, err := lockDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	shadow.fail("synthetic_failure_with_writer_locked")
	if _, err = conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	oldDB, _ := f.m.storeDB.DB()
	if err := oldDB.Close(); err != nil {
		t.Fatal(err)
	}
	f.m = &Monitor{storeDB: open(), cfg: f.m.cfg}
	view, err := f.m.loadUpstreamTokenReconciliation(ctx, f.account.Domain, 185, f.day, f.day+86402)
	if err != nil || view.Status == "evidence_ready" || view.StableScans >= 2 {
		t.Fatalf("restart resurrected readiness: %+v %v", view, err)
	}
	if _, err := f.m.commitSub2TokenCostCandidates(ctx, in, f.day+86402); !errors.Is(err, errSub2TokenCursorConflict) {
		t.Fatal("old proof accepted after restart", err)
	}
	legacy := map[int64][]ChannelUpstreamPricingHourEvidence{}
	for h := f.day; h < f.day+86400; h += 3600 {
		legacy[h] = nil
	}
	for _, r := range f.rows {
		legacy[r.HourTs] = append(legacy[r.HourTs], r)
	}
	current, err := readSub2TokenStoredCursor(f.m.storeDB, f.account, f.day)
	if err != nil {
		t.Fatal(err)
	}
	first, err := f.m.recordSub2TokenObservation(ctx, f.account, f.day, current, legacy, f.day+86403, f.key, "v1")
	if err != nil || first.StableScans != 1 {
		t.Fatal("recovery did not restart verification", err)
	}
	if retry, err := f.m.recordSub2TokenObservation(ctx, f.account, f.day, first, legacy, f.day+86403, f.key, "v1"); err != nil || retry != first {
		t.Fatal("retry counted as independent scan", err)
	}
	current, err = f.m.resetSub2TokenScanCheckpoint(ctx, f.account, f.day, first.Revision, f.day+86404, f.key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	_, items, _, _ := sub2TokenFixture(t)
	for _, page := range []sub2PricingPage{sub2TokenPage(items[:2], 1), sub2TokenPage(items[2:], 2)} {
		current, err = f.m.saveSub2TokenScanPage(ctx, f.account, f.day, page, current.Revision, f.day+86404, f.key, "v1")
		if err != nil {
			t.Fatal(err)
		}
	}
	current, err = f.m.recordSub2TokenObservation(ctx, f.account, f.day, current, legacy, f.day+86404, f.key, "v1")
	if err != nil || current.StableScans != 2 {
		t.Fatal("second complete scan did not restore readiness", err)
	}
	in.CursorRevision, in.SuccessHash = current.Revision, current.SuccessHash
	if result, err := f.m.commitSub2TokenCostCandidates(ctx, in, f.day+86405); err != nil || result.UpdatedHours != 24 {
		t.Fatalf("recovery commit %+v %v", result, err)
	}
	var total int64
	if err := f.m.storeDB.Model(&ChannelUpstreamCostHourEvidence{}).Select("COALESCE(SUM(charge_units),0)").Scan(&total).Error; err != nil || total != 481479 {
		t.Fatal("recovery changed account cost", total, err)
	}
}

func TestSub2TokenFailureCanceledParentStillRevokesAndFencesLateFinish(t *testing.T) {
	f := newTokenReconcileFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := &sub2TokenShadow{m: f.m, ctx: ctx, account: f.account, day: f.day, now: f.day + 86401, cursor: f.cursor}
	s.fail("canceled_parent")
	current, err := readSub2TokenStoredCursor(f.m.storeDB, f.account, f.day)
	if err != nil || current.Revision != f.cursor.Revision+1 || current.SuccessRevision != 0 || current.StableScans != 0 {
		t.Fatalf("revocation lost on cancellation: %+v %v", current, err)
	}
	legacy := map[int64][]ChannelUpstreamPricingHourEvidence{}
	for h := f.day; h < f.day+86400; h += 3600 {
		legacy[h] = nil
	}
	for _, r := range f.rows {
		legacy[r.HourTs] = append(legacy[r.HourTs], r)
	}
	if _, err := f.m.recordSub2TokenObservation(context.Background(), f.account, f.day, f.cursor, legacy, f.day+86402, f.key, "v1"); !errors.Is(err, errSub2TokenCursorConflict) {
		t.Fatal("delayed finish restored failed scan", err)
	}
	// A delayed failure for an old revision must not revoke a newer success.
	newer, err := f.m.recordSub2TokenObservation(context.Background(), f.account, f.day, current, legacy, f.day+86403, f.key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	s.failed = false
	s.fail("late_old_failure")
	got, err := readSub2TokenStoredCursor(f.m.storeDB, f.account, f.day)
	if err != nil || got != newer {
		t.Fatal("old failure revoked newer result", err)
	}
}

func TestSub2TokenFailureCannotStartWhenInvalidationIsBlocked(t *testing.T) {
	f := newSub2ShadowHTTPFixture(t)
	f.configure(t, "575", 1)
	f.m.cfg.ChannelCostClosureEnabled = true
	rows, done, err := f.fetch(10)
	if err != nil || !done {
		t.Fatal(err)
	}
	key := []byte(f.m.cfg.ChannelCostHMACKey)
	before, err := f.m.loadSub2TokenScanCheckpoint(context.Background(), f.account, f.day, key, f.m.cfg.ChannelCostHMACKeyID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.m.storeDB.Exec(`CREATE TRIGGER reject_token_invalidation BEFORE UPDATE ON channel_upstream_token_scan_checkpoints BEGIN SELECT RAISE(ABORT,'synthetic unavailable writer'); END`).Error; err != nil {
		t.Fatal(err)
	}
	calls := f.requests.Load()
	afterRows, done, err := f.fetch(10)
	if err != nil || !done || !reflect.DeepEqual(rows, afterRows) || f.requests.Load() != calls+1 {
		t.Fatal("failed pre-invalidation affected legacy or added requests", err)
	}
	after, err := readSub2TokenStoredCursor(f.m.storeDB, f.account, f.day)
	if err != nil || after != before {
		t.Fatal("failed start partially changed checkpoint", err)
	}
	// The last stored result remains historical evidence; no new success,
	// publication or binding may be produced from this failed-start attempt.
	for _, model := range []any{&ChannelUpstreamCostHourEvidence{}, &ChannelCostSourceBinding{}, &ChannelEconomicsHourPublication{}} {
		var n int64
		if err := f.m.storeDB.Model(model).Count(&n).Error; err != nil || n != 0 {
			t.Fatal("failed start published", err)
		}
	}
}

func TestSub2TokenFailureCorruptResumeRevokesBeforeDecode(t *testing.T) {
	f, _ := tokenCostStoreFixture(t, -1)
	if err := f.m.storeDB.Model(&ChannelUpstreamTokenScanCheckpoint{}).Where("domain=?", f.account.Domain).UpdateColumn("envelope", "corrupt").Error; err != nil {
		t.Fatal(err)
	}
	s := f.m.beginSub2TokenShadow(context.Background(), f.account, f.day, f.day+86401, tokenResumeCheckpoint(t, f), true)
	if s == nil || !s.failed {
		t.Fatal("corrupt cursor accepted")
	}
	got, err := readSub2TokenStoredCursor(f.m.storeDB, f.account, f.day)
	if err != nil || got.SuccessRevision != 0 || got.StableScans != 0 || got.Revision <= f.cursor.Revision {
		t.Fatal("corrupt completion kept success marker", err)
	}
}
