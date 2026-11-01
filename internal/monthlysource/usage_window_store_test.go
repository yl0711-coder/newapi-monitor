package monthlysource

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

func usageTestStore(t *testing.T) (*sql.DB, string) {
	t.Helper()
	db, path := walletTestStore(t)
	if err := InitUsageWindowStore(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return db, path
}

func TestUsageWindowStoreAppendOnlyIdempotentAndReopen(t *testing.T) {
	db, path := usageTestStore(t)
	ctx, o := context.Background(), usageFixture(t)
	if err := InitUsageWindowStore(ctx, db); err != nil {
		t.Fatal(err)
	}
	id, err := SaveUsageWindowObservation(ctx, db, o)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if again, err := SaveUsageWindowObservation(ctx, db, o); err != nil || again != id {
			t.Fatal("identical capture not idempotent", err)
		}
	}
	changed := o
	changed.StartedAtMS += 1000
	changed.CompletedAtMS += 1000
	newID, err := SaveUsageWindowObservation(ctx, db, changed)
	if err != nil || newID == id {
		t.Fatal("later capture overwrote history", err)
	}
	for _, q := range []string{"UPDATE portal_monthly_usage_observations SET payload='{}' WHERE id=?", "DELETE FROM portal_monthly_usage_observations WHERE id=?"} {
		if _, err := db.ExecContext(ctx, q, id); err == nil {
			t.Fatal("append-only restriction absent")
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]UsageWindowObservation{id: o, newID: changed} {
		got, err := LoadUsageWindowObservation(ctx, reader, key)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("reopened observation changed", err)
		}
	}
	if _, err := SaveUsageWindowObservation(ctx, reader, o); err == nil {
		t.Fatal("read-only receiver accepted writes")
	}
	after, err := os.ReadFile(path)
	if err != nil || digest(after) != digest(before) {
		t.Fatal("read-only verification modified evidence", err)
	}
}

func TestUsageWindowStoreRejectsDamageAndNonCanonicalPayload(t *testing.T) {
	for _, scenario := range []string{"digest", "source", "user", "from", "to", "started", "completed", "unknown_field", "duplicate_key", "noncanonical", "invalid_semantics", "oversized"} {
		t.Run(scenario, func(t *testing.T) {
			db, _ := usageTestStore(t)
			ctx, o := context.Background(), usageFixture(t)
			id, err := SaveUsageWindowObservation(ctx, db, o)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`DROP TRIGGER monthly_usage_no_update`); err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(o)
			q, args := "", []any{}
			switch scenario {
			case "digest":
				q, args = "UPDATE portal_monthly_usage_observations SET payload='{}' WHERE id=?", []any{id}
			case "source":
				q, args = "UPDATE portal_monthly_usage_observations SET source_identity=? WHERE id=?", []any{strings.Repeat("b", 64), id}
			case "user", "from", "to", "started", "completed":
				column := map[string]string{"user": "user_id", "from": "window_from", "to": "window_to", "started": "started_at_ms", "completed": "completed_at_ms"}[scenario]
				q, args = "UPDATE portal_monthly_usage_observations SET "+column+"="+column+"+1 WHERE id=?", []any{id}
			case "unknown_field":
				raw = []byte(strings.TrimSuffix(string(raw), "}") + `,"complete":true}`)
			case "duplicate_key":
				raw = []byte(strings.Replace(string(raw), `"version":1`, `"version":1,"version":1`, 1))
			case "noncanonical":
				raw = append([]byte(" "), raw...)
			case "invalid_semantics":
				o.Basis = "exact_month_boundary"
				raw, _ = json.Marshal(o)
			case "oversized":
				raw = append(raw, []byte(strings.Repeat(" ", usageWindowBytesLimit))...)
				if _, err := db.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
					t.Fatal(err)
				}
			}
			if q == "" {
				// Rehash to ensure canonical/semantic validation is not merely a
				// digest comparison. All damage is in a new synthetic receiver.
				newID := digest(raw)
				q, args = "UPDATE portal_monthly_usage_observations SET id=?,payload=? WHERE id=?", []any{newID, raw, id}
				id = newID
			}
			if _, err := db.ExecContext(ctx, q, args...); err != nil {
				t.Fatal(err)
			}
			if got, err := LoadUsageWindowObservation(ctx, db, id); err == nil || got.Version != 0 {
				t.Fatal("damaged observation accepted", err)
			}
		})
	}
}

func TestUsageWindowStoreNoImplicitInitAndWriteFailurePreservesHistory(t *testing.T) {
	db, _ := walletTestStore(t)
	ctx, o := context.Background(), usageFixture(t)
	if _, err := SaveUsageWindowObservation(ctx, db, o); err == nil {
		t.Fatal("save silently initialized missing schema")
	}
	if _, err := LoadUsageWindowObservation(ctx, db, strings.Repeat("a", 64)); err == nil {
		t.Fatal("read silently initialized missing schema")
	}
	if err := InitUsageWindowStore(ctx, db); err != nil {
		t.Fatal(err)
	}
	id, err := SaveUsageWindowObservation(ctx, db, o)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER usage_fixture_fail BEFORE INSERT ON portal_monthly_usage_observations BEGIN SELECT RAISE(ABORT,'synthetic disk failure'); END`); err != nil {
		t.Fatal(err)
	}
	changed := o
	changed.CompletedAtMS++
	if _, err := SaveUsageWindowObservation(ctx, db, changed); err == nil {
		t.Fatal("failed write reported success")
	}
	got, err := LoadUsageWindowObservation(ctx, db, id)
	if err != nil || !reflect.DeepEqual(got, o) {
		t.Fatal("failed write damaged prior observation", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := SaveUsageWindowObservation(cancelled, db, changed); err == nil {
		t.Fatal("cancel ignored")
	}
	for _, badID := range []string{"", "../outside", strings.Repeat("A", 64)} {
		if _, err := LoadUsageWindowObservation(ctx, db, badID); err == nil {
			t.Fatal("invalid ID accepted")
		}
	}
	if err := InitUsageWindowStore(nil, db); err == nil { //nolint:staticcheck // Intentionally tests rejection of an invalid nil context.
		t.Fatal("nil context accepted")
	}
	if _, err := SaveUsageWindowObservation(ctx, nil, o); err == nil {
		t.Fatal("nil receiver accepted")
	}
	if _, err := LoadUsageWindowObservation(nil, db, id); err == nil { //nolint:staticcheck // Intentionally tests rejection of an invalid nil context.
		t.Fatal("nil context accepted")
	}
}
