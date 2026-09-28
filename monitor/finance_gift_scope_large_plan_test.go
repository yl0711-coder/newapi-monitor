//go:build unix

package monitor

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
)

func TestFinanceGiftLargeHourPlanReadOnlyAndPinned(t *testing.T) {
	backup, _ := giftLargeLocalFixture(t, 4833)
	before := giftLocalFileHash(t, backup)
	db, closeDB, err := giftLocalReadonlyDatabase(backup)
	if err != nil {
		t.Fatal(err)
	}
	var state FinanceGiftBoundaryState
	if err := db.First(&state).Error; err != nil {
		t.Fatal(err)
	}
	closeDB()
	plan, digest, err := PrepareFinanceGiftLargeHourPlan(context.Background(), backup, "v1", state.HourTs, state.UserID)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Rows) != 4833 || plan.LocalContentHash != state.ContentHash {
		t.Fatal("lost pinned ledger")
	}
	for i, row := range plan.Rows {
		if row.ID != int64(i+1) || row.Quota != int64(i+1) || row.Group != nil {
			t.Fatal("identity or unknown group changed")
		}
	}
	confirmed, err := financegiftexport.LargeHourConfirmation(plan)
	if err != nil || confirmed != digest {
		t.Fatal("confirmation mismatch", err)
	}
	second, otherDigest, err := PrepareFinanceGiftLargeHourPlan(context.Background(), backup, "v1", state.HourTs, state.UserID)
	if err != nil || otherDigest != digest || !reflect.DeepEqual(plan, second) || giftLocalFileHash(t, backup) != before {
		t.Fatal("plan changed backup or is not deterministic", err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(backup + suffix); !os.IsNotExist(err) {
			t.Fatal("readonly plan created a sidecar")
		}
	}
	// The normal path must still stop at this hour. A large plan is not a
	// hidden approval to widen the existing source exporter or local importer.
	inspection, err := InspectFinanceGiftScopeBackup(context.Background(), backup, "v1")
	if err != nil || inspection.Candidates.Status != "blocked" || inspection.ReadPlan != nil {
		t.Fatal("ordinary budget widened", err)
	}
}

func TestFinanceGiftLargeHourPlanRejectsUnsafeBackup(t *testing.T) {
	for _, scenario := range []string{"small", "oversize", "corrupt", "epoch", "cancel", "wal", "symlink"} {
		t.Run(scenario, func(t *testing.T) {
			count := 3001
			if scenario == "small" {
				count = 3000
			}
			if scenario == "oversize" {
				count = 5001
			}
			backup, _ := giftLargeLocalFixture(t, count)
			db, closeDB, err := giftLocalDatabase(backup)
			if err != nil {
				t.Fatal(err)
			}
			var state FinanceGiftBoundaryState
			if err := db.First(&state).Error; err != nil {
				t.Fatal(err)
			}
			if scenario == "corrupt" {
				if err := db.Exec("UPDATE finance_gift_boundary_events SET quota=quota+1 WHERE source_log_id=1").Error; err != nil {
					t.Fatal(err)
				}
			}
			closeDB()
			epoch := "v1"
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch scenario {
			case "epoch":
				epoch = "wrong"
			case "cancel":
				cancel()
			case "wal":
				if err := os.WriteFile(backup+"-wal", []byte("pending"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				link := filepath.Join(t.TempDir(), "linked.db")
				if err := os.Symlink(backup, link); err != nil {
					t.Fatal(err)
				}
				backup = link
			}
			if _, digest, err := PrepareFinanceGiftLargeHourPlan(ctx, backup, epoch, state.HourTs, state.UserID); err == nil || digest != "" {
				t.Fatal("unsafe plan accepted")
			}
		})
	}
}

// Opt-in: plan only from a previously downloaded backup; no source connection.
func TestFinanceGiftLargeHourPlanLocalSnapshot(t *testing.T) {
	backup := os.Getenv("MONITOR_GIFT_INSPECT_SNAPSHOT")
	if backup == "" {
		t.Skip("requires a closed private local backup")
	}
	epoch := os.Getenv("MONITOR_GIFT_INSPECT_EPOCH")
	if epoch == "" {
		t.Fatal("explicit source epoch required")
	}
	before := giftLocalFileHash(t, backup)
	db, closeDB, err := giftLocalReadonlyDatabase(backup)
	if err != nil {
		t.Fatal(err)
	}
	var states []FinanceGiftBoundaryState
	err = db.Where(`source_epoch=? AND rows>? AND EXISTS (
SELECT 1 FROM finance_gift_boundary_events e WHERE
e.source_epoch=finance_gift_boundary_states.source_epoch AND
e.hour_ts=finance_gift_boundary_states.hour_ts AND e.user_id=finance_gift_boundary_states.user_id
AND COALESCE(e.group_known,0)=0)`, epoch, financeGiftLocalMaxRows).Order("hour_ts,user_id").Limit(20).Find(&states).Error
	closeDB()
	if err != nil {
		t.Fatal(err)
	}
	if len(states) == 0 || len(states) == 20 {
		t.Fatal("no oversized states or inventory exceeds this acceptance bound")
	}
	total := 0
	for _, state := range states {
		plan, digest, err := PrepareFinanceGiftLargeHourPlan(context.Background(), backup, epoch, state.HourTs, state.UserID)
		if err != nil || digest == "" || len(plan.Rows) != int(state.Rows) {
			t.Fatal("cannot pin large-hour ledger", err)
		}
		total += len(plan.Rows)
	}
	if giftLocalFileHash(t, backup) != before {
		t.Fatal("original backup changed")
	}
	t.Logf("readonly plans verified: %d oversized user-hours, %d pinned records; original backup unchanged; no source reads", len(states), total)
}
