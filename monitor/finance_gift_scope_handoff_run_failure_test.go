//go:build unix

package monitor

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestFinanceGiftHandoffRunWriteFailureStopsAndResumes(t *testing.T) {
	job, digest, _ := giftHandoffRunFixture(t)
	_, plan, _, err := loadFinanceGiftHandoffJob(job, digest)
	if err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := giftLocalDatabase(filepath.Join(job, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Exec(fmt.Sprintf(`CREATE TRIGGER reject_handoff_run BEFORE INSERT ON finance_gift_boundary_events WHEN NEW.hour_ts=%d AND NEW.group_known=1 BEGIN SELECT RAISE(ABORT,'injected'); END`, plan.Targets[1].HourTs)).Error
	closeDB()
	if err != nil {
		t.Fatal(err)
	}
	noWait := func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	failed, err := runFinanceGiftLocalHandoff(context.Background(), job, digest, noWait, nil)
	if err == nil || failed.Status != "failed" || failed.Remaining != 9 || len(failed.Entries) != 2 || failed.Entries[0].RowsUpdated != 1 || failed.Entries[1].RowsUpdated != 0 {
		t.Fatalf("failure %+v %v", failed, err)
	}
	db, closeDB, err = giftLocalDatabase(filepath.Join(job, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range plan.Targets[1:] {
		proof, err := loadFinanceGiftScopeSnapshot(context.Background(), db, target.SourceEpoch, target.HourTs, target.UserID)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range proof.Events {
			if e.GroupKnown {
				t.Fatal("failure wrote later target")
			}
		}
	}
	err = db.Exec("DROP TRIGGER reject_handoff_run").Error
	closeDB()
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := runFinanceGiftLocalHandoff(context.Background(), job, digest, noWait, nil)
	if err != nil || resumed.Status != "complete" || resumed.Entries[0].RowsUpdated != 0 {
		t.Fatalf("resume %+v %v", resumed, err)
	}
}

func TestFinanceGiftHandoffRunRechecksAfterCooldown(t *testing.T) {
	job, digest, _ := giftHandoffRunFixture(t)
	_, plan, _, err := loadFinanceGiftHandoffJob(job, digest)
	if err != nil {
		t.Fatal(err)
	}
	waits := 0
	failed, err := runFinanceGiftLocalHandoff(context.Background(), job, digest, func(ctx context.Context, _ time.Duration) error {
		waits++
		if waits == 2 {
			db, closeDB, err := giftLocalDatabase(filepath.Join(job, "usage-facts.db"))
			if err != nil {
				t.Fatal(err)
			}
			err = db.Model(&FinanceUserHourFact{}).Where("hour_ts=? AND user_id=?", plan.Targets[1].HourTs, plan.Targets[1].UserID).Update("consume_quota", -1).Error
			closeDB()
			if err != nil {
				t.Fatal(err)
			}
		}
		return ctx.Err()
	}, nil)
	if err == nil || failed.Status != "failed" || len(failed.Entries) != 2 || failed.Entries[1].RowsUpdated != 0 {
		t.Fatalf("stale validation used %+v %v", failed, err)
	}
}
