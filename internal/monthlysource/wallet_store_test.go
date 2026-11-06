package monthlysource

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
)

func walletTestStore(t *testing.T) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "observations.db")
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(1000)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if err := InitWalletObservationStore(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return db, path
}

func TestWalletStoreAppendOnlyIdempotentAndReopen(t *testing.T) {
	db, path := walletTestStore(t)
	ctx := context.Background()
	original := walletFixture()
	if err := InitWalletObservationStore(ctx, db); err != nil {
		t.Fatal("idempotent setup failed", err)
	}
	id, err := SaveWalletObservation(ctx, db, original)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		again, err := SaveWalletObservation(ctx, db, original)
		if err != nil || again != id {
			t.Fatal("save not idempotent", err)
		}
	}
	changed := walletFixture()
	changed.StartedAtMS += 1000
	changed.CompletedAtMS += 1000
	*changed.Users[0].BalanceQuota = 800
	otherID, err := SaveWalletObservation(ctx, db, changed)
	if err != nil || otherID == id {
		t.Fatal("new observation overwrote old", err)
	}
	for _, query := range []string{"UPDATE portal_monthly_wallet_observations SET payload='{}' WHERE id=?", "DELETE FROM portal_monthly_wallet_observations WHERE id=?"} {
		if _, err := db.ExecContext(ctx, query, id); err == nil {
			t.Fatal("append-only guard absent")
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for recordID, want := range map[string]WalletObservation{id: original, otherID: changed} {
		got, err := LoadWalletObservation(ctx, reopened, recordID)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("durable observation changed", err)
		}
	}
	if _, err := SaveWalletObservation(ctx, reopened, original); err == nil {
		t.Fatal("readonly receiver unexpectedly accepted a write")
	}
}

func TestWalletStoreFailureKeepsPriorCaptureAndConcurrentRetryIsUnique(t *testing.T) {
	db, _ := walletTestStore(t)
	ctx := context.Background()
	original := walletFixture()
	id, err := SaveWalletObservation(ctx, db, original)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER wallet_fixture_fail BEFORE INSERT ON portal_monthly_wallet_observations BEGIN SELECT RAISE(ABORT,'synthetic disk failure'); END`); err != nil {
		t.Fatal(err)
	}
	changed := walletFixture()
	changed.CompletedAtMS++
	if _, err := SaveWalletObservation(ctx, db, changed); err == nil {
		t.Fatal("failed local write reported success")
	}
	if _, err := db.Exec("DROP TRIGGER wallet_fixture_fail"); err != nil {
		t.Fatal(err)
	}
	got, err := LoadWalletObservation(ctx, db, id)
	if err != nil || !reflect.DeepEqual(got, original) {
		t.Fatal("failed write damaged previous capture", err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := SaveWalletObservation(ctx, db, changed); errs <- err }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM portal_monthly_wallet_observations").Scan(&count); err != nil || count != 2 {
		t.Fatal("duplicate captures after retry", count, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := SaveWalletObservation(canceled, db, changed); err == nil {
		t.Fatal("canceled write accepted")
	}
}

func TestWalletStoreRejectsCorruptOrAmbiguousCapture(t *testing.T) {
	for name, change := range map[string]func(*string, *int64, *[]byte){
		"wrong source": func(source *string, _ *int64, _ *[]byte) { *source = strings.Repeat("b", 64) },
		"wrong time":   func(_ *string, at *int64, _ *[]byte) { *at++ },
		"missing exists": func(_ *string, _ *int64, raw *[]byte) {
			*raw = []byte(strings.Replace(string(*raw), `"exists":false,`, "", 1))
		},
		"duplicate fields": func(_ *string, _ *int64, raw *[]byte) {
			*raw = []byte(strings.Replace(string(*raw), `"version":1`, `"version":1,"version":1`, 1))
		},
		"unknown field": func(_ *string, _ *int64, raw *[]byte) {
			*raw = []byte(strings.Replace(string(*raw), `"version":1`, `"version":1,"month":"2026-09"`, 1))
		},
		"null to zero": func(_ *string, _ *int64, raw *[]byte) {
			*raw = []byte(strings.Replace(string(*raw), `"balance_quota":null`, `"balance_quota":0`, 1))
		},
	} {
		t.Run(name, func(t *testing.T) {
			db, _ := walletTestStore(t)
			o := walletFixture()
			raw, err := json.Marshal(o)
			if err != nil {
				t.Fatal(err)
			}
			source, at := o.SourceIdentity, o.StartedAtMS
			change(&source, &at, &raw)
			id := digest(raw)
			if _, err := db.Exec(`INSERT INTO portal_monthly_wallet_observations VALUES(?,?,?,?,?)`, id, source, at, o.CompletedAtMS, raw); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadWalletObservation(context.Background(), db, id); err == nil {
				t.Fatal("invalid capture accepted despite matching checksum")
			}
			if name == "wrong source" || name == "wrong time" {
				if _, err := SaveWalletObservation(context.Background(), db, o); err == nil {
					t.Fatal("conflicting capture overwritten or accepted")
				}
			}
		})
	}
}

func TestWalletStoreIsExplicitAndDoesNotModifyOtherTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE usage_user_snapshots(user_id INTEGER PRIMARY KEY,balance_quota INTEGER); INSERT INTO usage_user_snapshots VALUES(1,123456789)"); err != nil {
		t.Fatal(err)
	}
	if _, err := SaveWalletObservation(context.Background(), db, walletFixture()); err == nil {
		t.Fatal("save implicitly initialized database")
	}
	if err := InitWalletObservationStore(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if _, err := SaveWalletObservation(context.Background(), db, walletFixture()); err != nil {
		t.Fatal(err)
	}
	var balance int64
	if err := db.QueryRow("SELECT balance_quota FROM usage_user_snapshots WHERE user_id=1").Scan(&balance); err != nil || balance != 123456789 {
		t.Fatal("other table changed", err)
	}
	var tables int
	if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&tables); err != nil || tables != 2 {
		t.Fatal("unexpected schema changes", tables, err)
	}
}

func TestWalletStoreMaximumBatchIsBounded(t *testing.T) {
	db, path := walletTestStore(t)
	o := walletFixture()
	o.Users = nil
	for i := int64(1); i <= WalletBatchLimit; i++ {
		o.Users = append(o.Users, WalletUser{UserID: i, Exists: true, BalanceQuota: walletPointer(i * 999999), UsedQuota: walletPointer(i * 888888)})
	}
	raw, err := walletObservationBytes(o)
	if err != nil || len(raw) > 65536 {
		t.Fatal("maximum batch exceeds storage cap", err)
	}
	started := time.Now()
	for i := range 20 {
		o.StartedAtMS += 10000
		o.CompletedAtMS += 10000
		id, err := SaveWalletObservation(context.Background(), db, o)
		if err != nil {
			t.Fatal(err)
		}
		got, err := LoadWalletObservation(context.Background(), db, id)
		if err != nil || len(got.Users) != 200 {
			t.Fatal(fmt.Sprintf("capture %d incomplete", i), err)
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("20 captures x 200 accounts: payload=%d bytes, DB=%d bytes, elapsed=%s", len(raw), info.Size(), time.Since(started))
}

func TestWalletStoreLockFailureDoesNotLoseHistory(t *testing.T) {
	db, path := walletTestStore(t)
	ctx := context.Background()
	id, err := SaveWalletObservation(ctx, db, walletFixture())
	if err != nil {
		t.Fatal(err)
	}
	locker, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Close()
	conn, err := locker.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = conn.ExecContext(ctx, "ROLLBACK") }()
	changed := walletFixture()
	changed.CompletedAtMS++
	if _, err := SaveWalletObservation(ctx, db, changed); err == nil {
		t.Fatal("locked write claimed success")
	}
	if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	got, err := LoadWalletObservation(ctx, db, id)
	if err != nil || !reflect.DeepEqual(got, walletFixture()) {
		t.Fatal("lock failure damaged history", err)
	}
	if _, err := SaveWalletObservation(ctx, db, changed); err != nil {
		t.Fatal("retry after unlock failed", err)
	}
}
