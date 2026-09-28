//go:build unix

package monitor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func giftHandoffRunFixture(t *testing.T) (string, string, string) {
	t.Helper()
	series, path, digest := giftSeriesFixture(t)
	ctx := context.Background()
	if _, err := runFinanceGiftLocalSeries(ctx, path, digest, func(ctx context.Context, _ time.Duration) error { return ctx.Err() }); err != nil {
		t.Fatal(err)
	}
	source := series.Steps[0].NextDir
	var plan FinanceGiftLocalPlan
	data, err := giftLocalReadJSON(filepath.Join(source, "plan.json"), &plan)
	if err != nil {
		t.Fatal(err)
	}
	job := filepath.Join(t.TempDir(), "handoff")
	receiver := filepath.Join(series.PriorDir, "usage-facts.db")
	_, confirmation, err := PrepareFinanceGiftLocalHandoff(ctx, source, giftLocalDigest(data), receiver, job)
	if err != nil {
		t.Fatal(err)
	}
	return job, confirmation, receiver
}

func TestFinanceGiftHandoffRunPauseResumeRepeat(t *testing.T) {
	job, digest, receiver := giftHandoffRunFixture(t)
	before := giftLocalFileHash(t, receiver)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	paused, err := runFinanceGiftLocalHandoff(ctx, job, digest, func(ctx context.Context, _ time.Duration) error {
		calls++
		if calls == 2 {
			cancel()
		}
		return ctx.Err()
	}, nil)
	if !errors.Is(err, context.Canceled) || paused.Status != "paused" || paused.Remaining != 9 || paused.Entries[0].RowsUpdated != 1 {
		t.Fatalf("pause %+v %v", paused, err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		var waits int
		result, err := runFinanceGiftLocalHandoff(context.Background(), job, digest, func(ctx context.Context, _ time.Duration) error { waits++; return ctx.Err() }, nil)
		total := 0
		for _, entry := range result.Entries {
			total += entry.RowsUpdated
		}
		want := 9
		if attempt == 1 {
			want = 0
		}
		if err != nil || result.Status != "complete" || result.Remaining != 0 || total != want {
			t.Fatalf("resume %+v %v", result, err)
		}
		if attempt == 1 && waits != 1 {
			t.Fatal("completed retry should only incur startup cooldown", waits)
		}
	}
	if giftLocalFileHash(t, receiver) != before {
		t.Fatal("original receiver changed")
	}
}

func TestFinanceGiftHandoffRunAuditFailureAfterCommit(t *testing.T) {
	job, digest, _ := giftHandoffRunFixture(t)
	noWait := func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	failed, err := runFinanceGiftLocalHandoff(context.Background(), job, digest, noWait, func(financeGiftScopeBatchEntry) error { return errors.New("injected audit failure") })
	if err == nil || failed.Status != "audit_failed" || len(failed.Entries) != 1 || failed.Entries[0].RowsUpdated != 1 {
		t.Fatalf("audit %+v %v", failed, err)
	}
	resumed, err := runFinanceGiftLocalHandoff(context.Background(), job, digest, noWait, nil)
	if err != nil || resumed.Status != "complete" || resumed.Entries[0].RowsUpdated != 0 {
		t.Fatalf("retry duplicated committed work %+v %v", resumed, err)
	}
	count := 0
	for _, entry := range resumed.Entries {
		count += entry.RowsUpdated
	}
	if count != 9 {
		t.Fatal("remaining updates", count)
	}
}

func TestFinanceGiftHandoffRunRejectsUnsafeInputs(t *testing.T) {
	for _, scenario := range []string{"confirmation", "receipt", "evidence", "partial-audit", "lock", "receiver-conflict"} {
		t.Run(scenario, func(t *testing.T) {
			job, digest, _ := giftHandoffRunFixture(t)
			switch scenario {
			case "confirmation":
				digest = "wrong"
			case "receipt":
				if err := os.Remove(filepath.Join(job, financeGiftHandoffReceiptName)); err != nil {
					t.Fatal(err)
				}
			case "evidence":
				if err := os.WriteFile(filepath.Join(job, giftLocalEvidenceName(0)), []byte("bad"), 0600); err != nil {
					t.Fatal(err)
				}
			case "partial-audit":
				if err := os.WriteFile(filepath.Join(job, "audit.jsonl"), []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			case "lock":
				lock, err := giftLocalLock(job)
				if err != nil {
					t.Fatal(err)
				}
				defer lock.Close()
			case "receiver-conflict":
				_, plan, _, err := loadFinanceGiftHandoffJob(job, digest)
				if err != nil {
					t.Fatal(err)
				}
				target := plan.Targets[len(plan.Targets)-1]
				db, closeDB, err := giftLocalDatabase(filepath.Join(job, "usage-facts.db"))
				if err != nil {
					t.Fatal(err)
				}
				err = db.Model(&FinanceUserHourFact{}).Where("hour_ts=? AND user_id=?", target.HourTs, target.UserID).Update("consume_quota", -1).Error
				closeDB()
				if err != nil {
					t.Fatal(err)
				}
			}
			before := giftLocalFileHash(t, filepath.Join(job, "usage-facts.db"))
			result, err := runFinanceGiftLocalHandoff(context.Background(), job, digest, func(ctx context.Context, _ time.Duration) error { return ctx.Err() }, nil)
			if err == nil {
				t.Fatal("unsafe job accepted", result)
			}
			if giftLocalFileHash(t, filepath.Join(job, "usage-facts.db")) != before {
				t.Fatal("rejected job wrote data")
			}
		})
	}
}
