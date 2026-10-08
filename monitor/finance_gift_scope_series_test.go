//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestFinanceGiftSeriesContinuousPauseResume(t *testing.T) {
	plan, path, digest := giftSeriesFixture(t)
	original := giftLocalFileHash(t, filepath.Join(plan.PriorDir, "usage-facts.db"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waits := 0
	paused, err := runFinanceGiftLocalSeries(ctx, path, digest, func(ctx context.Context, _ time.Duration) error {
		waits++
		if waits == 2 {
			cancel()
		}
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) || paused.Status != "paused" || paused.Updated != 1 || paused.Completed != 0 {
		t.Fatalf("pause=%+v %v", paused, err)
	}
	if _, err = os.Stat(plan.Steps[1].NextDir); !os.IsNotExist(err) {
		t.Fatal("later batch started after pause")
	}
	for attempt := 0; attempt < 2; attempt++ {
		result, err := runFinanceGiftLocalSeries(context.Background(), path, digest, func(ctx context.Context, _ time.Duration) error { return ctx.Err() })
		want := 19
		if attempt == 1 {
			want = 0
		}
		if err != nil || result.Status != "complete" || result.Completed != 2 || result.Updated != want {
			t.Fatalf("result=%+v %v", result, err)
		}
	}
	if giftLocalFileHash(t, filepath.Join(plan.PriorDir, "usage-facts.db")) != original {
		t.Fatal("predecessor changed")
	}
	db, closeDB, err := giftLocalReadonlyDatabase(filepath.Join(plan.Steps[1].NextDir, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	var unknown int64
	if err = db.Model(&FinanceGiftBoundaryEvent{}).Where("group_known=0").Count(&unknown).Error; err != nil || unknown != 0 {
		t.Fatal("chain lost an earlier repair", unknown, err)
	}
	var money int64
	if err = db.Model(&FinanceGiftBoundaryEvent{}).Select("SUM(quota)").Scan(&money).Error; err != nil || money != 231 {
		t.Fatal("series changed amounts", money, err)
	}
}

func TestFinanceGiftSeriesBudgetsAndLocks(t *testing.T) {
	for _, scenario := range []string{"rows", "batches", "seconds", "duplicate", "confirmation", "lock", "timeout"} {
		t.Run(scenario, func(t *testing.T) {
			plan, path, digest := giftSeriesFixture(t)
			switch scenario {
			case "rows":
				plan.MaxRows = 19
			case "batches":
				plan.MaxBatches = 1
			case "seconds":
				plan.RuntimeSeconds = 601
			case "duplicate":
				plan.Steps[1] = plan.Steps[0]
			case "timeout":
				plan.RuntimeSeconds = 1
			case "confirmation":
				digest = "wrong"
			}
			if scenario != "confirmation" && scenario != "lock" {
				data, _ := json.Marshal(plan)
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				digest = giftLocalDigest(data)
			}
			if scenario == "lock" {
				f, err := os.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
					t.Fatal(err)
				}
			}
			result, err := runFinanceGiftLocalSeries(context.Background(), path, digest, func(ctx context.Context, _ time.Duration) error {
				if scenario == "timeout" {
					<-ctx.Done()
				}
				return ctx.Err()
			})
			if err == nil {
				t.Fatal("unsafe series accepted")
			}
			if scenario == "timeout" {
				if result.Status != "paused" || result.Updated != 0 {
					t.Fatal("deadline not respected", result)
				}
			} else if _, err = os.Stat(plan.Steps[0].NextDir); !os.IsNotExist(err) {
				t.Fatal("rejected manifest created a child")
			}
		})
	}
}

func TestFinanceGiftSeriesStopsOnSecondEvidenceFailure(t *testing.T) {
	plan, path, digest := giftSeriesFixture(t)
	if err := os.WriteFile(filepath.Join(plan.Steps[1].ExportDir, giftLocalEvidenceName(0)), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	result, err := runFinanceGiftLocalSeries(context.Background(), path, digest, func(ctx context.Context, _ time.Duration) error { return ctx.Err() })
	if err == nil || result.Status != "failed" || result.Completed != 1 || result.Updated != 10 {
		t.Fatalf("did not stop at bad batch: %+v %v", result, err)
	}
	if _, err = os.Stat(plan.Steps[1].NextDir); !os.IsNotExist(err) {
		t.Fatal("bad evidence created next job")
	}
}

func TestFinanceGiftSeriesCheckDoesNotCreateJobs(t *testing.T) {
	plan, path, digest := giftSeriesFixture(t)
	original := giftLocalFileHash(t, filepath.Join(plan.PriorDir, "usage-facts.db"))
	preview, err := InspectFinanceGiftLocalSeries(path)
	if err != nil || preview.Confirmation != digest || preview.Rows != 20 || preview.Batches != 2 {
		t.Fatalf("preview=%+v %v", preview, err)
	}
	for _, step := range plan.Steps {
		if _, err := os.Stat(step.NextDir); !os.IsNotExist(err) {
			t.Fatal("check created a child")
		}
	}
	if giftLocalFileHash(t, filepath.Join(plan.PriorDir, "usage-facts.db")) != original {
		t.Fatal("check changed database")
	}
}

func TestFinanceGiftSeriesRevalidatesCompletedChain(t *testing.T) {
	for _, scenario := range []string{"parent", "marker", "child-evidence"} {
		t.Run(scenario, func(t *testing.T) {
			plan, path, digest := giftSeriesFixture(t)
			noWait := func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
			if _, err := runFinanceGiftLocalSeries(context.Background(), path, digest, noWait); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "parent":
				db, closeDB, err := giftLocalDatabase(filepath.Join(plan.PriorDir, "usage-facts.db"))
				if err != nil {
					t.Fatal(err)
				}
				err = db.Exec("PRAGMA user_version=123").Error
				closeDB()
				if err != nil {
					t.Fatal(err)
				}
			case "marker":
				giftLargeWriteTestJSON(t, filepath.Join(plan.PriorDir, "continuation-complete.json"), map[string]any{"version": 1, "next_plan_sha256": "incorrect"})
			case "child-evidence":
				if err := os.WriteFile(filepath.Join(plan.Steps[0].NextDir, giftLocalEvidenceName(0)), []byte("corrupt"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			result, err := runFinanceGiftLocalSeries(context.Background(), path, digest, noWait)
			if err == nil || result.Completed != 0 || result.Updated != 0 {
				t.Fatalf("trusted changed chain: %+v %v", result, err)
			}
		})
	}
}
