package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func emptyCapturedCopy(t *testing.T) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "selected.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db, path
}

func TestCapturedObservationCopyExactIsolatedAndReopen(t *testing.T) {
	source, path, key, scope := capturedSelectionFixture(t, false)
	// Unrelated private tables and jobs must never be carried into a bill's
	// sealed source. A whole-database VACUUM/copy would incorrectly include it.
	if _, err := source.Exec(`CREATE TABLE synthetic_other_customer(secret TEXT); INSERT INTO synthetic_other_customer VALUES('do-not-export')`); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want, err := BuildCapturedMonthSelection(t.Context(), source, key, scope)
	if err != nil {
		t.Fatal(err)
	}
	destination, output := emptyCapturedCopy(t)
	got, err := CopyCapturedMonthObservations(t.Context(), source, destination, key, scope)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("changed original evidence", err)
	}
	for table, count := range map[string]int{"portal_monthly_funding_pages": 60, "portal_monthly_usage_observations": 60, "portal_monthly_wallet_observations": 2} {
		var n int
		if err := destination.QueryRow(`SELECT count(*) FROM ` + table).Scan(&n); err != nil || n != count {
			t.Fatal(table, n, err)
		}
	}
	var n int
	if err := destination.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN ('synthetic_other_customer','portal_monthly_funding_capture_plans','portal_monthly_token_capture_slots','portal_monthly_wallet_capture_slots')`).Scan(&n); err != nil || n != 0 {
		t.Fatal("exported unrelated/control data", n, err)
	}
	if err := destination.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := sql.Open("sqlite", "file:"+output+"?mode=ro&immutable=1&_pragma=query_only(1)")
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	ro.SetMaxOpenConns(1)
	if err := VerifyCapturedObservationSelection(t.Context(), ro, got.Selection); err != nil {
		t.Fatal("closed copy invalid", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("source modified", err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(output + suffix); !os.IsNotExist(err) {
			t.Fatal("copy unsealed", suffix, err)
		}
	}
	if _, err := CopyCapturedMonthObservations(t.Context(), source, source, key, scope); err == nil {
		t.Fatal("source overwritten")
	}
	alias, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer alias.Close()
	if _, err := CopyCapturedMonthObservations(t.Context(), source, alias, key, scope); err == nil {
		t.Fatal("source alias overwritten")
	}
}

func TestCapturedObservationCopyIncompleteAndNonemptyNeverSuccess(t *testing.T) {
	for _, scenario := range []string{"changed", "cancel", "nonempty", "readonly", "damaged_copy"} {
		t.Run(scenario, func(t *testing.T) {
			source, _, key, scope := capturedSelectionFixture(t, scenario == "changed")
			destination, _ := emptyCapturedCopy(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if scenario == "cancel" {
				cancel()
			}
			if scenario == "nonempty" {
				if _, err := destination.Exec(`CREATE TABLE unrelated(x)`); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "readonly" {
				if _, err := destination.Exec(`PRAGMA query_only=ON`); err != nil {
					t.Fatal(err)
				}
			}
			got, err := CopyCapturedMonthObservations(ctx, source, destination, key, scope)
			if scenario != "damaged_copy" {
				if err == nil || len(got.Selection) != 0 || len(got.Tokens) != 0 {
					t.Fatal("partial copy accepted", err)
				}
				if scenario == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := destination.Exec(`DROP TRIGGER monthly_usage_no_delete; DELETE FROM portal_monthly_usage_observations WHERE id=(SELECT min(id) FROM portal_monthly_usage_observations)`); err != nil {
				t.Fatal(err)
			}
			if err := VerifyCapturedObservationSelection(ctx, destination, got.Selection); err == nil {
				t.Fatal("lost original accepted")
			}
		})
	}
}

func TestCapturedFundingChainCopyPreservesPaginationAndDedup(t *testing.T) {
	upstream, driver, scan := fundingTestSource(t, 201)
	source, _ := fundingTestStore(t)
	first, err := CaptureFundingPage(t.Context(), upstream, scan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SaveFundingPage(t.Context(), source, first); err != nil {
		t.Fatal(err)
	}
	driver.s.mu.Lock()
	driver.s.data = driver.s.data[200:]
	driver.s.mu.Unlock()
	second, err := CaptureFundingPage(t.Context(), upstream, scan, &first)
	if err != nil {
		t.Fatal(err)
	}
	head, err := SaveFundingPage(t.Context(), source, second)
	if err != nil {
		t.Fatal(err)
	}
	destination, _ := fundingTestStore(t)
	if err := copyCapturedFundingChain(t.Context(), source, destination, scan.ID, head); err != nil {
		t.Fatal(err)
	}
	got, err := LoadLatestFundingPage(t.Context(), destination, scan.ID)
	if err != nil || !reflect.DeepEqual(got, second) {
		t.Fatal("pagination altered", err)
	}
	var n int
	if err := destination.QueryRow(`SELECT count(*) FROM portal_monthly_funding_records`).Scan(&n); err != nil || n != 201 {
		t.Fatal(n, err)
	}
}

func TestCapturedObservationCopyLowerPhysicalBudget(t *testing.T) {
	source, _, key, scope := capturedSelectionFixture(t, false)
	for _, limit := range []int64{4095, (64 << 20) + 1, 4096, 1 << 20} {
		destination, path := emptyCapturedCopy(t)
		got, err := CopyCapturedMonthObservationsBounded(t.Context(), source, destination, key, scope, limit)
		if limit != 1<<20 {
			if !errors.Is(err, ErrTokenMonthCaptureBudget) || len(got.Selection) != 0 {
				t.Fatal("physical limit not enforced/classified", limit, err)
			}
			continue
		}
		if err != nil || len(got.Selection) == 0 {
			t.Fatal(err)
		}
		var pageSize, pages int64
		if err := destination.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
			t.Fatal(err)
		}
		if err := destination.QueryRow(`PRAGMA max_page_count`).Scan(&pages); err != nil {
			t.Fatal(err)
		}
		if pageSize != 4096 || pages != limit/4096 {
			t.Fatal("database reservation widened", pageSize, pages)
		}
		if err := destination.Close(); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil || info.Size() > limit {
			t.Fatal("output exceeded allocation", err)
		}
	}
}
