package monthlysource

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/glebarez/go-sqlite"
)

func snapshotSelectionDB(t *testing.T, schema, inserts string) (*sql.DB, string, string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "cold.sqlite")
	db, err := sql.Open("sqlite", file)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{schema, inserts} {
		if statement != "" {
			if _, err := db.Exec(statement); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	u := url.URL{Scheme: "file", Path: file}
	u.RawQuery = url.Values{"mode": {"ro"}, "immutable": {"1"}, "_pragma": {"query_only(1)"}}.Encode()
	ro, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	ro.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = ro.Close() })
	return ro, file, hex.EncodeToString(sum[:])
}

const snapshotSelectionSchema = `CREATE TABLE usage_user_snapshots(user_id INTEGER,balance_quota INTEGER,used_quota INTEGER,"exists" numeric,captured_at INTEGER)`

func TestRetainedSnapshotSelectionExactIntegersNullsAndReadOnly(t *testing.T) {
	db, file, pin := snapshotSelectionDB(t, snapshotSelectionSchema, `INSERT INTO usage_user_snapshots VALUES(10,9007199254740993,123,1,1787249527),(20,NULL,NULL,0,NULL),(30,-3500,0,1,1787249527)`)
	before, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	s, err := ReadRetainedSnapshotSelection(t.Context(), db, pin)
	if err != nil || s.Version != 1 || s.Mode != RetainedSnapshotSelectionMode || len(s.Rows) != 3 || s.OriginSHA256 != pin {
		t.Fatal("exact retained rows rejected", err)
	}
	if s.Rows[0].BalanceQuota == nil || *s.Rows[0].BalanceQuota != 9007199254740993 || s.Rows[1].BalanceQuota != nil || s.Rows[1].UsedQuota != nil || s.Rows[1].CapturedAt != nil || *s.Rows[2].BalanceQuota != -3500 {
		t.Fatal("integer money rounded, null zeroed or negative balance changed")
	}
	body, err := json.Marshal(s)
	if err != nil || !bytes.Contains(body, []byte(`9007199254740993`)) || !bytes.Contains(body, []byte(`"balance_quota":null`)) {
		t.Fatal("JSON lost original precision or unknown values", err)
	}
	var decoded RetainedSnapshotSelection
	if json.Unmarshal(body, &decoded) != nil || *decoded.Rows[0].BalanceQuota != 9007199254740993 {
		t.Fatal("typed source round-trip changed money")
	}
	*s.Rows[0].BalanceQuota = 0
	again, err := ReadRetainedSnapshotSelection(t.Context(), db, pin)
	if err != nil || *again.Rows[0].BalanceQuota != 9007199254740993 {
		t.Fatal("caller modified cached/original snapshot rows", err)
	}
	if _, err := db.Exec("UPDATE usage_user_snapshots SET balance_quota=0"); err == nil {
		t.Fatal("source handle is writable")
	}
	after, err := os.ReadFile(file)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("original cold source changed", err)
	}
}

func TestRetainedSnapshotSelectionRejectsAmbiguousOrUnsealedSources(t *testing.T) {
	for _, scenario := range []string{"wrong-digest", "wal", "empty-wal", "shm", "journal", "float-money", "blob-money", "overflow-money", "float-used", "float-time", "null-user", "zero-user", "negative-user", "duplicate-user", "missing-column", "view", "bad-existence", "writable", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			schema, inserts := snapshotSelectionSchema, `INSERT INTO usage_user_snapshots VALUES(1,12,0,1,1787249527)`
			switch scenario {
			case "float-money":
				inserts = `INSERT INTO usage_user_snapshots VALUES(1,1.25,0,1,1787249527)`
			case "blob-money":
				inserts = `INSERT INTO usage_user_snapshots VALUES(1,x'3132',0,1,1787249527)`
			case "overflow-money":
				inserts = `INSERT INTO usage_user_snapshots VALUES(1,9223372036854775808,0,1,1787249527)`
			case "float-used":
				inserts = `INSERT INTO usage_user_snapshots VALUES(1,12,1.25,1,1787249527)`
			case "float-time":
				inserts = `INSERT INTO usage_user_snapshots VALUES(1,12,0,1,1787249527.5)`
			case "null-user":
				inserts = `INSERT INTO usage_user_snapshots VALUES(NULL,12,0,1,1787249527)`
			case "zero-user":
				inserts = `INSERT INTO usage_user_snapshots VALUES(0,12,0,1,1787249527)`
			case "negative-user":
				inserts = `INSERT INTO usage_user_snapshots VALUES(-1,12,0,1,1787249527)`
			case "duplicate-user":
				inserts += `,(1,13,0,1,1787249527)`
			case "missing-column":
				schema, inserts = `CREATE TABLE usage_user_snapshots(user_id INTEGER)`, ""
			case "view":
				schema, inserts = `CREATE VIEW usage_user_snapshots AS SELECT 1 AS user_id,1 AS balance_quota,0 AS used_quota,1 AS "exists",1787249527 AS captured_at`, ""
			case "bad-existence":
				inserts = `INSERT INTO usage_user_snapshots VALUES(1,12,0,7,1787249527)`
			}
			db, file, pin := snapshotSelectionDB(t, schema, inserts)
			ctx := context.Background()
			switch scenario {
			case "wrong-digest":
				pin = strings.Repeat("0", 64)
			case "wal", "shm", "journal":
				if err := os.WriteFile(file+"-"+scenario, []byte("retained-do-not-delete"), 0600); err != nil {
					t.Fatal(err)
				}
			case "empty-wal":
				if err := os.WriteFile(file+"-wal", nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "writable":
				if _, err := db.Exec("PRAGMA query_only=0"); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			out, err := ReadRetainedSnapshotSelection(ctx, db, pin)
			if err == nil || out.Version != 0 || out.Rows != nil {
				t.Fatal("invalid source returned a partial selection")
			}
			if scenario == "wal" || scenario == "shm" || scenario == "journal" {
				if _, err := os.Stat(file + "-" + scenario); err != nil {
					t.Fatal("source sidecar was repaired/deleted", err)
				}
			}
		})
	}
}

func TestRetainedSnapshotSelectionBoundedAndEmpty(t *testing.T) {
	for _, count := range []int{0, retainedSnapshotRowsLimit, retainedSnapshotRowsLimit + 1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			insert := ""
			if count > 0 {
				insert = fmt.Sprintf(`WITH RECURSIVE ids(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM ids WHERE n<%d) INSERT INTO usage_user_snapshots SELECT n,12,0,1,1787249527 FROM ids`, count)
			}
			db, _, pin := snapshotSelectionDB(t, snapshotSelectionSchema, insert)
			out, err := ReadRetainedSnapshotSelection(t.Context(), db, pin)
			if count > retainedSnapshotRowsLimit {
				if err == nil || out.Rows != nil || out.Version != 0 {
					t.Fatal("oversized source returned partial data")
				}
				return
			}
			if err != nil || len(out.Rows) != count || out.Rows == nil {
				t.Fatal("empty/bounded selection changed", err)
			}
		})
	}
}

func TestRetainedSnapshotSelectionMissingContextAndUnpinned(t *testing.T) {
	db, _, pin := snapshotSelectionDB(t, snapshotSelectionSchema, "")
	for _, invalid := range []string{"", "not-a-digest", strings.Repeat("G", 64)} {
		if _, err := ReadRetainedSnapshotSelection(t.Context(), db, invalid); err == nil {
			t.Fatal("unpinned source accepted")
		}
	}
	if _, err := ReadRetainedSnapshotSelection(nil, db, pin); err == nil { //nolint:staticcheck // Intentionally tests rejection of an invalid nil context.
		t.Fatal("nil context accepted")
	}
	if _, err := ReadRetainedSnapshotSelection(t.Context(), nil, pin); err == nil {
		t.Fatal("nil database accepted")
	}
}
