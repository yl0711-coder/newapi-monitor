//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Include audit, inputs, lock, and the DB in the comparison. A status query
// must neither rewrite existing files nor leave new SQLite sidecar files.
func giftLocalJobFileHashes(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	result := map[string]string{}
	for _, entry := range entries {
		result[entry.Name()] = giftLocalFileHash(t, filepath.Join(dir, entry.Name()))
	}
	return result
}

func giftAssertLocalProgress(t *testing.T, dir, hash string, targets, complete, rows, verified int) {
	t.Helper()
	before := giftLocalJobFileHashes(t, dir)
	got, err := InspectFinanceGiftLocalJob(context.Background(), dir, hash)
	status := "pending"
	if complete == targets {
		status = "complete"
	} else if verified > 0 {
		status = "partial"
	}
	if err != nil || got.Mode != "offline_plan_only" || got.Status != status || got.Targets != targets ||
		got.CompletedTargets != complete || got.RemainingTargets != targets-complete ||
		got.Rows != rows || got.VerifiedRows != verified || got.RemainingRows != rows-verified || len(got.Entries) != targets {
		t.Fatalf("progress=%+v error=%v", got, err)
	}
	var entryRows, entryVerified, entryComplete int
	for _, entry := range got.Entries {
		entryRows += entry.Rows
		entryVerified += entry.VerifiedRows
		if entry.RemainingRows != entry.Rows-entry.VerifiedRows {
			t.Fatal("entry counts do not reconcile")
		}
		if entry.Status == "complete" {
			entryComplete++
			if entry.RemainingRows != 0 {
				t.Fatal("unfinished entry published as complete")
			}
		}
	}
	if entryRows != rows || entryVerified != verified || entryComplete != complete {
		t.Fatal("entries and totals differ")
	}
	if !reflect.DeepEqual(before, giftLocalJobFileHashes(t, dir)) {
		t.Fatal("readonly status changed job files")
	}
}

func TestFinanceGiftScopeLocalStatusGuards(t *testing.T) {
	for _, mode := range []string{"confirmation", "plan", "evidence", "audit_partial", "pending_wal", "pending_journal", "database_hash", "locked", "cancelled", "db_link"} {
		t.Run(mode, func(t *testing.T) {
			backup, evidence, dir := giftLocalFixture(t)
			_, hash, err := PrepareFinanceGiftLocalJob(context.Background(), backup, dir, []string{evidence})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			write := func(name string, data []byte) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "confirmation":
				hash = "wrong"
			case "plan":
				write("plan.json", []byte(`{}`))
			case "evidence":
				write(giftLocalEvidenceName(0), []byte(`{}`))
			case "audit_partial":
				write("audit.jsonl", []byte(`{"status":`))
			case "pending_wal":
				write("usage-facts.db-wal", []byte("pending"))
			case "pending_journal":
				write("usage-facts.db-journal", []byte("pending"))
			case "database_hash":
				db, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
				if err != nil {
					t.Fatal(err)
				}
				err = db.Exec("UPDATE finance_gift_boundary_events SET quota=quota+1 WHERE user_id=7").Error
				closeDB()
				if err != nil {
					t.Fatal(err)
				}
			case "locked":
				lock, err := giftLocalLock(dir)
				if err != nil {
					t.Fatal(err)
				}
				defer lock.Close()
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "db_link":
				path := filepath.Join(dir, "usage-facts.db")
				if err := os.Rename(path, path+".saved"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(path+".saved", path); err != nil {
					t.Fatal(err)
				}
			}
			before := giftLocalJobFileHashes(t, dir)
			got, err := InspectFinanceGiftLocalJob(ctx, dir, hash)
			if err == nil || got.Status != "unverified" || got.Targets != 0 || len(got.Entries) != 0 {
				t.Fatalf("untrusted progress published: %+v error=%v", got, err)
			}
			candidates, err := SuggestFinanceGiftLocalCandidates(ctx, dir, hash)
			if err == nil || candidates.Status != "unverified" || len(candidates.Entries) != 0 {
				t.Fatalf("untrusted candidates published: %+v error=%v", candidates, err)
			}
			if !reflect.DeepEqual(before, giftLocalJobFileHashes(t, dir)) {
				t.Fatal("rejected status changed job files")
			}
		})
	}
}

func TestFinanceGiftScopeLocalStatusDoesNotTrustAuditCounts(t *testing.T) {
	backup, evidence, dir := giftLocalFixture(t)
	plan, hash, err := PrepareFinanceGiftLocalJob(context.Background(), backup, dir, []string{evidence})
	if err != nil {
		t.Fatal(err)
	}
	entry := financeGiftScopeBatchEntry{financeGiftScopeTarget: plan.Targets[0], Status: "repaired", RowsChecked: 3, RowsUpdated: 3}
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "audit.jsonl"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	// Even a syntactically valid successful audit cannot make unknown DB
	// facts complete. Progress is derived from verified source/fact matching.
	giftAssertLocalProgress(t, dir, hash, 1, 0, 3, 0)
}

func TestFinanceGiftScopeLocalStatusDatabaseRejectsWrites(t *testing.T) {
	backup, evidence, dir := giftLocalFixture(t)
	_, _, err := PrepareFinanceGiftLocalJob(context.Background(), backup, dir, []string{evidence})
	if err != nil {
		t.Fatal(err)
	}
	before := giftLocalJobFileHashes(t, dir)
	db, closeDB, err := giftLocalReadonlyDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Exec("UPDATE finance_gift_boundary_events SET quota=quota+1").Error
	closeDB()
	if err == nil || !reflect.DeepEqual(before, giftLocalJobFileHashes(t, dir)) {
		t.Fatal("status DB accepted a write or changed files")
	}
}
