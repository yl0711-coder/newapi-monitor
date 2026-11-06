package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func registeredWalletStoreObservation() RegisteredWalletObservation {
	old := walletFixture()
	return RegisteredWalletObservation{Version: 1, Basis: registeredWalletObservationBasis, SourceIdentity: old.SourceIdentity, StartedAtMS: old.StartedAtMS, CompletedAtMS: old.CompletedAtMS, Users: []RegisteredWalletUser{
		{UserID: 1, RegisteredAt: old.StartedAtMS/1000 - 86400, BalanceQuota: walletPointer(-1), UsedQuota: walletPointer(0)},
		{UserID: 2, RegisteredAt: old.StartedAtMS/1000 - 3600, BalanceQuota: walletPointer(9007199254740993), UsedQuota: walletPointer(math.MaxInt64)},
	}}
}

func registeredWalletTestStore(t *testing.T) (*sql.DB, string) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "registered-source.sqlite")
	db, err := sql.Open("sqlite", file+"?_pragma=busy_timeout(1000)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if err := InitRegisteredWalletObservationStore(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	return db, file
}

func registeredWalletStoreCount(t *testing.T, db *sql.DB, count int) {
	t.Helper()
	var got int
	if err := db.QueryRow("SELECT COUNT(*) FROM portal_monthly_registered_wallet_observations").Scan(&got); err != nil || got != count {
		t.Fatal("original record count changed", got, count, err)
	}
}

func TestRegisteredWalletStoreExplicitSetupAndLegacyIsolation(t *testing.T) {
	db, _ := walletTestStore(t)
	o, old := registeredWalletStoreObservation(), walletFixture()
	oldID, err := SaveWalletObservation(t.Context(), db, old)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE usage_user_snapshots(user_id INTEGER PRIMARY KEY,balance_quota INTEGER); INSERT INTO usage_user_snapshots VALUES(1,123456789)"); err != nil {
		t.Fatal(err)
	}
	if id, err := SaveRegisteredWalletObservation(t.Context(), db, o); err == nil || id != "" {
		t.Fatal("save implicitly initialized new storage")
	}
	for range 2 {
		if err := InitRegisteredWalletObservationStore(t.Context(), db); err != nil {
			t.Fatal("explicit repeated initialization failed", err)
		}
	}
	id, err := SaveRegisteredWalletObservation(t.Context(), db, o)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := LoadWalletObservation(t.Context(), db, oldID); err != nil || !reflect.DeepEqual(got, old) {
		t.Fatal("legacy observation was changed", err)
	}
	if _, err := LoadWalletObservation(t.Context(), db, id); err == nil {
		t.Fatal("legacy reader accepted enriched metadata as old evidence")
	}
	if got, err := LoadRegisteredWalletObservation(t.Context(), db, oldID, o.SourceIdentity, []int64{1, 2}); err == nil || got.Version != 0 {
		t.Fatal("new reader upgraded unrelated legacy evidence")
	}
	var balance, tables int64
	if err := db.QueryRow("SELECT balance_quota FROM usage_user_snapshots WHERE user_id=1").Scan(&balance); err != nil || balance != 123456789 {
		t.Fatal("business snapshot changed", err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&tables); err != nil || tables != 3 {
		t.Fatal("unexpected other schema changes", tables, err)
	}
}

func TestRegisteredWalletStoreAppendOnlyIdempotentReopenAndExactBytes(t *testing.T) {
	db, file := registeredWalletTestStore(t)
	o := registeredWalletStoreObservation()
	original, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	id, err := SaveRegisteredWalletObservation(t.Context(), db, o)
	if err != nil || id != digest(original) {
		t.Fatal("content address differs from complete original", err)
	}
	for range 3 {
		if again, err := SaveRegisteredWalletObservation(t.Context(), db, o); err != nil || again != id {
			t.Fatal("same source was not idempotent", err)
		}
	}
	changed := registeredWalletStoreObservation()
	changed.StartedAtMS += 1000
	changed.CompletedAtMS += 1000
	*changed.Users[0].BalanceQuota = 123456789
	other, err := SaveRegisteredWalletObservation(t.Context(), db, changed)
	if err != nil || other == id {
		t.Fatal("later source overwrote old source", err)
	}
	registeredWalletStoreCount(t, db, 2)
	var stored []byte
	if err := db.QueryRow("SELECT payload FROM portal_monthly_registered_wallet_observations WHERE id=?", id).Scan(&stored); err != nil || !bytes.Equal(original, stored) {
		t.Fatal("original bytes were not retained", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := sql.Open("sqlite", "file:"+file+"?mode=ro&_pragma=query_only(1)")
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	for key, want := range map[string]RegisteredWalletObservation{id: o, other: changed} {
		ids := []int64{2, 1}
		got, err := LoadRegisteredWalletObservation(t.Context(), ro, key, o.SourceIdentity, ids)
		if err != nil || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(ids, []int64{2, 1}) {
			t.Fatal("reopen/read-only changed original values or expected roster", err)
		}
	}
	if _, err := SaveRegisteredWalletObservation(t.Context(), ro, o); err == nil {
		t.Fatal("readonly receiver accepted a write")
	}
}

func TestRegisteredWalletStoreGuardsUpdateDeleteReplaceAndUpsert(t *testing.T) {
	for _, recursive := range []string{"OFF", "ON"} {
		t.Run(recursive, func(t *testing.T) {
			db, _ := registeredWalletTestStore(t)
			o := registeredWalletStoreObservation()
			id, err := SaveRegisteredWalletObservation(t.Context(), db, o)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("PRAGMA recursive_triggers=" + recursive); err != nil {
				t.Fatal(err)
			}
			for _, query := range []string{"UPDATE portal_monthly_registered_wallet_observations SET payload=x'7b7d' WHERE id=?", "DELETE FROM portal_monthly_registered_wallet_observations WHERE id=?"} {
				if _, err := db.Exec(query, id); err == nil {
					t.Fatal("original record was mutable")
				}
			}
			changed := registeredWalletStoreObservation()
			*changed.Users[0].BalanceQuota++
			raw, err := json.Marshal(changed)
			if err != nil {
				t.Fatal(err)
			}
			for _, query := range []string{
				"INSERT OR REPLACE INTO portal_monthly_registered_wallet_observations VALUES(?,?,?,?,?)",
				"INSERT INTO portal_monthly_registered_wallet_observations VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
			} {
				if _, err := db.Exec(query, id, o.SourceIdentity, o.StartedAtMS, o.CompletedAtMS, raw); err == nil {
					t.Fatal("replace/upsert bypassed original-record protection")
				}
			}
			got, err := LoadRegisteredWalletObservation(t.Context(), db, id, o.SourceIdentity, []int64{1, 2})
			if err != nil || !reflect.DeepEqual(got, o) {
				t.Fatal("failed mutation damaged original", err)
			}
			registeredWalletStoreCount(t, db, 1)
		})
	}
}

func TestRegisteredWalletStoreRejectsDamagedNoncanonicalOrUnknownSources(t *testing.T) {
	for _, name := range []string{"digest", "source_column", "start_column", "end_column", "duplicate_key", "unknown_key", "missing_balance", "null_balance", "float_balance", "missing_registration", "boundary_claim", "trailing_space"} {
		t.Run(name, func(t *testing.T) {
			db, _ := registeredWalletTestStore(t)
			o := registeredWalletStoreObservation()
			raw, err := json.Marshal(o)
			if err != nil {
				t.Fatal(err)
			}
			source, started, completed := o.SourceIdentity, o.StartedAtMS, o.CompletedAtMS
			switch name {
			case "source_column":
				source = strings.Repeat("b", 64)
			case "start_column":
				started++
			case "end_column":
				completed++
			case "duplicate_key":
				raw = bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
			case "unknown_key":
				raw = bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"opening_balance":0`), 1)
			case "missing_balance":
				raw = bytes.Replace(raw, []byte(`"balance_quota":-1,`), nil, 1)
			case "null_balance":
				raw = bytes.Replace(raw, []byte(`"balance_quota":-1`), []byte(`"balance_quota":null`), 1)
			case "float_balance":
				raw = bytes.Replace(raw, []byte(`"balance_quota":-1`), []byte(`"balance_quota":-1.0`), 1)
			case "missing_registration":
				o.Users[0].RegisteredAt = 0
				raw, err = json.Marshal(o)
				if err != nil {
					t.Fatal(err)
				}
			case "boundary_claim":
				raw = bytes.Replace(raw, []byte(registeredWalletObservationBasis), []byte("historical_month_boundary"), 1)
			case "trailing_space":
				raw = append(raw, ' ')
			}
			id := digest(raw)
			if name == "digest" {
				id = strings.Repeat("b", 64)
			}
			if _, err := db.Exec("INSERT INTO portal_monthly_registered_wallet_observations VALUES(?,?,?,?,?)", id, source, started, completed, raw); err != nil {
				t.Fatal(err)
			}
			got, err := LoadRegisteredWalletObservation(t.Context(), db, id, o.SourceIdentity, []int64{1, 2})
			if err == nil || got.Version != 0 || got.Users != nil {
				t.Fatal("invalid or noncanonical original became usable source")
			}
			if name == "source_column" || name == "start_column" || name == "end_column" {
				if id, err := SaveRegisteredWalletObservation(t.Context(), db, o); err == nil || id != "" {
					t.Fatal("conflicting prior row was silently repaired or overwritten")
				}
			}
			registeredWalletStoreCount(t, db, 1)
		})
	}
}

func TestRegisteredWalletStoreLoadRequiresExactSourceAndFullRoster(t *testing.T) {
	db, _ := registeredWalletTestStore(t)
	o := registeredWalletStoreObservation()
	id, err := SaveRegisteredWalletObservation(t.Context(), db, o)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"different_source", "subset", "superset", "foreign", "duplicate", "invalid_id", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			key, source, ids, ctx := id, o.SourceIdentity, []int64{1, 2}, t.Context()
			switch name {
			case "different_source":
				source = strings.Repeat("b", 64)
			case "subset":
				ids = []int64{1}
			case "superset":
				ids = []int64{1, 2, 3}
			case "foreign":
				ids = []int64{3, 4}
			case "duplicate":
				ids = []int64{1, 1}
			case "invalid_id":
				key = "invalid"
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			got, err := LoadRegisteredWalletObservation(ctx, db, key, source, ids)
			if err == nil || got.Version != 0 || got.Users != nil {
				t.Fatal("unrelated, partial or cancelled source returned")
			}
		})
	}
	registeredWalletStoreCount(t, db, 1)
}

func TestRegisteredWalletStoreFailedWriteRecoveryAndConcurrentRetry(t *testing.T) {
	db, _ := registeredWalletTestStore(t)
	o := registeredWalletStoreObservation()
	id, err := SaveRegisteredWalletObservation(t.Context(), db, o)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER registered_fixture_fail BEFORE INSERT ON portal_monthly_registered_wallet_observations BEGIN SELECT RAISE(ABORT,'synthetic local write failure'); END`); err != nil {
		t.Fatal(err)
	}
	changed := registeredWalletStoreObservation()
	changed.CompletedAtMS++
	if id, err := SaveRegisteredWalletObservation(t.Context(), db, changed); err == nil || id != "" {
		t.Fatal("failed write returned a committed source")
	}
	registeredWalletStoreCount(t, db, 1)
	if _, err := db.Exec("DROP TRIGGER registered_fixture_fail"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	ids, errs := make(chan string, 16), make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := SaveRegisteredWalletObservation(t.Context(), db, changed)
			ids <- id
			errs <- err
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal("idempotent retry failed", err)
		}
	}
	var first string
	for key := range ids {
		if key == "" || key == id || first != "" && first != key {
			t.Fatal("retry created a new observation or changed prior ID")
		}
		first = key
	}
	registeredWalletStoreCount(t, db, 2)
	got, err := LoadRegisteredWalletObservation(t.Context(), db, id, o.SourceIdentity, []int64{1, 2})
	if err != nil || !reflect.DeepEqual(got, o) {
		t.Fatal("failure/retry changed prior source", err)
	}
}

func TestRegisteredWalletStoreFullBatchHasNoAliasingOrPrecisionLoss(t *testing.T) {
	db, _ := registeredWalletTestStore(t)
	o := registeredWalletStoreObservation()
	o.Users = nil
	ids := make([]int64, WalletBatchLimit)
	for i := range ids {
		id := int64(i + 1)
		ids[i] = id
		o.Users = append(o.Users, RegisteredWalletUser{UserID: id, RegisteredAt: o.StartedAtMS/1000 - 3600, BalanceQuota: walletPointer(9007199254740993 + id), UsedQuota: walletPointer(math.MaxInt64 - id)})
	}
	id, err := SaveRegisteredWalletObservation(t.Context(), db, o)
	if err != nil {
		t.Fatal(err)
	}
	got, err := LoadRegisteredWalletObservation(t.Context(), db, id, o.SourceIdentity, ids)
	if err != nil || !reflect.DeepEqual(got, o) {
		t.Fatal("full batch lost identity or exact integers", err)
	}
	*got.Users[0].BalanceQuota = 0
	again, err := LoadRegisteredWalletObservation(t.Context(), db, id, o.SourceIdentity, ids)
	if err != nil || !reflect.DeepEqual(again, o) {
		t.Fatal("caller mutation changed immutable source", err)
	}
}

func TestRegisteredWalletStoreOversizedSourceRejectedBeforeReturningPayload(t *testing.T) {
	db, _ := registeredWalletTestStore(t)
	o := registeredWalletStoreObservation()
	raw := bytes.Repeat([]byte(" "), registeredWalletPayloadLimit+1)
	id := digest(raw)
	if _, err := db.Exec("INSERT INTO portal_monthly_registered_wallet_observations VALUES(?,?,?,?,?)", id, o.SourceIdentity, o.StartedAtMS, o.CompletedAtMS, raw); err == nil {
		t.Fatal("oversized source entered guarded storage")
	}
	// A damaged/foreign schema without CHECK must still be bounded on reads.
	foreign, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "untrusted.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	if _, err := foreign.Exec("CREATE TABLE portal_monthly_registered_wallet_observations(id TEXT,source_identity TEXT,started_at_ms INTEGER,completed_at_ms INTEGER,payload BLOB)"); err != nil {
		t.Fatal(err)
	}
	if _, err := foreign.Exec("INSERT INTO portal_monthly_registered_wallet_observations VALUES(?,?,?,?,?)", id, o.SourceIdentity, o.StartedAtMS, o.CompletedAtMS, raw); err != nil {
		t.Fatal(err)
	}
	if got, err := LoadRegisteredWalletObservation(t.Context(), foreign, id, o.SourceIdentity, []int64{1, 2}); err == nil || got.Users != nil {
		t.Fatal("oversized damaged payload returned a source")
	}
	registeredWalletStoreCount(t, db, 0)
	registeredWalletStoreCount(t, foreign, 1)
}

func TestRegisteredWalletStoreInvalidHandlesAndCancellationDoNotWrite(t *testing.T) {
	db, file := registeredWalletTestStore(t)
	o := registeredWalletStoreObservation()
	for _, name := range []string{"nil_context", "nil_receiver", "cancelled", "invalid_source"} {
		t.Run(name, func(t *testing.T) {
			ctx, target, input := t.Context(), db, registeredWalletStoreObservation()
			switch name {
			case "nil_context":
				ctx = nil //nolint:staticcheck // Intentionally reject invalid context without I/O.
			case "nil_receiver":
				target = nil
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "invalid_source":
				input.Users[0].BalanceQuota = nil
			}
			if id, err := SaveRegisteredWalletObservation(ctx, target, input); err == nil || id != "" {
				t.Fatal("invalid save succeeded")
			}
			if name != "invalid_source" {
				if err := InitRegisteredWalletObservationStore(ctx, target); err == nil {
					t.Fatal("invalid setup succeeded")
				}
				if got, err := LoadRegisteredWalletObservation(ctx, target, strings.Repeat("b", 64), o.SourceIdentity, []int64{1, 2}); err == nil || got.Users != nil {
					t.Fatal("invalid read returned source")
				}
			}
		})
	}
	registeredWalletStoreCount(t, db, 0)
	info, err := os.Stat(file)
	if err != nil || !info.Mode().IsRegular() {
		t.Fatal("receiver unexpectedly removed", err)
	}
}

func TestRegisteredWalletStoreBusyReceiverRecoversWithoutSourceRecapture(t *testing.T) {
	at := time.Now().Add(-time.Hour).Unix()
	source, state, pin := walletTestSource(t, [][]driver.Value{{int64(1), at, int64(-1), int64(0)}})
	captured, err := CaptureRegisteredWallet(t.Context(), source, pin, []int64{1})
	if err != nil {
		t.Fatal(err)
	}
	db, file := registeredWalletTestStore(t)
	prior := registeredWalletStoreObservation()
	priorID, err := SaveRegisteredWalletObservation(t.Context(), db, prior)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE fixture_lock(value INTEGER)"); err != nil {
		t.Fatal(err)
	}
	other, err := sql.Open("sqlite", file)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	tx, err := other.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec("INSERT INTO fixture_lock VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if id, err := SaveRegisteredWalletObservation(ctx, db, captured); err == nil || id != "" {
		t.Fatal("busy/cancelled receiver returned a committed source")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	registeredWalletStoreCount(t, db, 1)
	if id, err := SaveRegisteredWalletObservation(t.Context(), db, captured); err != nil || id == "" {
		t.Fatal("same retained capture could not recover after receiver lock", err)
	}
	state.mu.Lock()
	queries := len(state.queries)
	state.mu.Unlock()
	if queries != 2 {
		t.Fatal("receiver retry recaptured or wrote source", queries)
	}
	got, err := LoadRegisteredWalletObservation(t.Context(), db, priorID, prior.SourceIdentity, []int64{1, 2})
	if err != nil || !reflect.DeepEqual(got, prior) {
		t.Fatal("busy/retry damaged prior original", err)
	}
	registeredWalletStoreCount(t, db, 2)
}
