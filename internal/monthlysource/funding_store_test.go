package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func fundingTestStore(t *testing.T) (*sql.DB, string) {
	t.Helper()
	db, path := walletTestStore(t)
	if err := InitFundingPageStore(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return db, path
}

func TestFundingStoreDurableResumeReplayAndIndependentRescan(t *testing.T) {
	db, path := fundingTestStore(t)
	ctx := context.Background()
	p := fundingPageFixture(t, 201)
	if _, err := LoadLatestFundingPage(ctx, db, p.Scan.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("absent scan should not become empty complete range", err)
	}
	id, err := SaveFundingPage(ctx, db, p)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		got, err := SaveFundingPage(ctx, db, p)
		if err != nil || got != id {
			t.Fatal("retry duplicated page", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(1000)")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.SetMaxOpenConns(1)
	loaded, err := LoadLatestFundingPage(ctx, reopened, p.Scan.ID)
	if err != nil || !reflect.DeepEqual(loaded, p) {
		t.Fatal("restart lost durable cursor", err)
	}
	next := fundingNextFixture(t, loaded)
	if _, err := SaveFundingPage(ctx, reopened, next); err != nil {
		t.Fatal(err)
	}
	last, err := LoadLatestFundingPage(ctx, reopened, p.Scan.ID)
	if err != nil || !last.EndReached || last.Next.ID != 201 {
		t.Fatal("next page missing", err)
	}
	if got, err := SaveFundingPage(ctx, reopened, p); err != nil || got != id {
		t.Fatal("old completed page retry failed", err)
	}
	lastAgain, err := LoadLatestFundingPage(ctx, reopened, p.Scan.ID)
	if err != nil || !reflect.DeepEqual(lastAgain, last) {
		t.Fatal("old page retry rewound head", err)
	}
	var pages, records int
	if err := reopened.QueryRow("SELECT COUNT(*) FROM portal_monthly_funding_pages").Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if err := reopened.QueryRow("SELECT COUNT(*) FROM portal_monthly_funding_records").Scan(&records); err != nil {
		t.Fatal(err)
	}
	if pages != 2 || records != 201 {
		t.Fatal("duplicate records persisted")
	}
	// Fresh traversal can retain corrected evidence; the prior scan is immutable.
	fresh := p
	fresh.Scan.ID = strings.Repeat("b", 32)
	fresh.Rows = append([]FundingLog(nil), p.Rows...)
	fresh.Rows[0].Content = "changed observation"
	if _, err := SaveFundingPage(ctx, reopened, fresh); err != nil {
		t.Fatal(err)
	}
	old, err := LoadLatestFundingPage(ctx, reopened, p.Scan.ID)
	if err != nil || !reflect.DeepEqual(old, last) {
		t.Fatal("fresh scan overwrote earlier observation", err)
	}
	for _, query := range []string{
		"UPDATE portal_monthly_funding_pages SET payload='{}'",
		"DELETE FROM portal_monthly_funding_pages",
		"UPDATE portal_monthly_funding_records SET row_digest='changed'",
		"DELETE FROM portal_monthly_funding_records",
	} {
		if _, err := reopened.Exec(query); err == nil {
			t.Fatal("append-only guard missing")
		}
	}
}

func TestFundingStoreRejectsSkippedForkedOrChangedContinuation(t *testing.T) {
	for name, change := range map[string]func(*FundingPage){
		"scope":             func(p *FundingPage) { p.Scan.UserIDs = []int64{201} },
		"source":            func(p *FundingPage) { p.Scan.SourceIdentity = strings.Repeat("f", 64) },
		"window":            func(p *FundingPage) { p.Scan.To-- },
		"fence":             func(p *FundingPage) { p.FenceID++ },
		"skip":              func(p *FundingPage) { p.After.ID-- },
		"wrong predecessor": func(p *FundingPage) { p.PreviousID = strings.Repeat("f", 64) },
		"backward capture":  func(p *FundingPage) { p.StartedAtMS -= 100; p.CompletedAtMS = p.StartedAtMS },
		"duplicate ID moved later": func(p *FundingPage) {
			p.Rows[0].ID = 1
			p.Rows[0].CreatedAt++
			p.Next = FundingCursor{At: p.Rows[0].CreatedAt, ID: 1}
		},
	} {
		t.Run(name, func(t *testing.T) {
			db, _ := fundingTestStore(t)
			ctx := context.Background()
			first := fundingPageFixture(t, 201)
			if _, err := SaveFundingPage(ctx, db, first); err != nil {
				t.Fatal(err)
			}
			next := fundingNextFixture(t, first)
			change(&next)
			if _, err := SaveFundingPage(ctx, db, next); err == nil {
				t.Fatal("invalid continuation persisted")
			}
			got, err := LoadLatestFundingPage(ctx, db, first.Scan.ID)
			if err != nil || !reflect.DeepEqual(got, first) {
				t.Fatal("failed continuation moved checkpoint", err)
			}
			var records int
			if err := db.QueryRow("SELECT COUNT(*) FROM portal_monthly_funding_records").Scan(&records); err != nil || records != 200 {
				t.Fatal("partial funding records survived", err)
			}
		})
	}
	db, _ := fundingTestStore(t)
	ctx := context.Background()
	first := fundingPageFixture(t, 201)
	next := fundingNextFixture(t, first)
	if _, err := SaveFundingPage(ctx, db, next); err == nil {
		t.Fatal("unsaved predecessor accepted")
	}
	if _, err := SaveFundingPage(ctx, db, first); err != nil {
		t.Fatal(err)
	}
	fork := first
	fork.StartedAtMS++
	fork.CompletedAtMS++
	if _, err := SaveFundingPage(ctx, db, fork); err == nil {
		t.Fatal("forked first page accepted")
	}
	if _, err := SaveFundingPage(ctx, db, next); err != nil {
		t.Fatal(err)
	}
	afterEnd := fundingNextFixture(t, next)
	if _, err := SaveFundingPage(ctx, db, afterEnd); err == nil {
		t.Fatal("terminal traversal extended")
	}
}

func TestFundingStoreAtomicFailureConcurrentReplayAndLockRecovery(t *testing.T) {
	db, path := fundingTestStore(t)
	ctx := context.Background()
	first := fundingPageFixture(t, 201)
	if _, err := SaveFundingPage(ctx, db, first); err != nil {
		t.Fatal(err)
	}
	next := fundingNextFixture(t, first)
	if _, err := db.Exec("CREATE TRIGGER fail_funding_test BEFORE INSERT ON portal_monthly_funding_records WHEN NEW.log_id=201 BEGIN SELECT RAISE(ABORT,'synthetic write failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := SaveFundingPage(ctx, db, next); err == nil {
		t.Fatal("injected write failure accepted")
	}
	got, err := LoadLatestFundingPage(ctx, db, first.Scan.ID)
	if err != nil || !reflect.DeepEqual(got, first) {
		t.Fatal("half save advanced checkpoint", err)
	}
	if _, err := db.Exec("DROP TRIGGER fail_funding_test"); err != nil {
		t.Fatal(err)
	}
	blocker, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(50)")
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	blocker.SetMaxOpenConns(1)
	if _, err := blocker.Exec("BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	_, saveErr := SaveFundingPage(short, db, next)
	cancel()
	if _, err := blocker.Exec("ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if saveErr == nil {
		t.Fatal("write bypassed SQLite lock")
	}
	if _, err := SaveFundingPage(ctx, db, next); err != nil {
		t.Fatal("lock recovery failed", err)
	}
	var wg sync.WaitGroup
	failures := make(chan error, 12)
	for range 12 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := SaveFundingPage(ctx, db, next); failures <- err }()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM portal_monthly_funding_pages").Scan(&count); err != nil || count != 2 {
		t.Fatal("concurrent retries duplicated pages", err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM portal_monthly_wallet_observations").Scan(&count); err != nil || count != 0 {
		t.Fatal("unrelated wallet store changed", err)
	}
}

func TestFundingStoreCorruptHeadIsNotSilentlySkipped(t *testing.T) {
	db, _ := fundingTestStore(t)
	ctx := context.Background()
	first := fundingPageFixture(t, 201)
	if _, err := SaveFundingPage(ctx, db, first); err != nil {
		t.Fatal(err)
	}
	next := fundingNextFixture(t, first)
	id, err := SaveFundingPage(ctx, db, next)
	if err != nil {
		t.Fatal(err)
	}
	// Deliberate corruption of this synthetic test DB only, bypassing its guard.
	if _, err := db.Exec("DROP TRIGGER monthly_funding_pages_no_update"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE portal_monthly_funding_pages SET payload=? WHERE id=?", strings.Repeat("x", 2*fundingPageBytesLimit+1), id); err != nil {
		t.Fatal(err)
	}
	if got, err := LoadLatestFundingPage(ctx, db, first.Scan.ID); err == nil || got.Version != 0 {
		t.Fatal("corrupt head silently fell back to older cursor")
	}
	if _, err := SaveFundingPage(ctx, db, next); err == nil {
		t.Fatal("corrupt persisted page replay accepted")
	}
}
