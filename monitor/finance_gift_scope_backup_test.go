//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
)

// Opt-in: readonly inventory of an already downloaded, closed backup. No source
// credentials, connection or repair worker are involved.
func TestFinanceGiftBackupInspectionLocalSnapshot(t *testing.T) {
	backup := os.Getenv("MONITOR_GIFT_INSPECT_SNAPSHOT")
	if backup == "" {
		t.Skip("requires a closed private local backup")
	}
	epoch := os.Getenv("MONITOR_GIFT_INSPECT_EPOCH")
	before := giftLocalFileHash(t, backup)
	result, err := InspectFinanceGiftScopeBackup(context.Background(), backup, epoch)
	if err != nil || result.Mode == "" || result.SourceEpoch != epoch {
		t.Fatalf("inspection failed: %+v %v", result, err)
	}
	if before != giftLocalFileHash(t, backup) {
		t.Fatal("readonly inspection changed original backup")
	}
	if result.ReadPlan != nil {
		digest, err := financegiftexport.BatchConfirmation(*result.ReadPlan)
		if err != nil || digest != result.Confirmation {
			t.Fatal("invalid handoff", err)
		}
	}
	if output := os.Getenv("MONITOR_GIFT_INSPECT_OUTPUT"); output != "" {
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := giftLocalWriteNew(output, data); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("raw unknown rows=%d nonzero=%d user-hours=%d oversize=%d; bounded plan targets=%d rows=%d; backup SHA256=%s unchanged",
		result.UnknownRows, result.MonetaryRows, result.UserHours, result.OversizeHours, len(result.Candidates.Entries), result.Candidates.Rows, before)
}

func TestFinanceGiftBackupInspectionReadOnlyAndBounded(t *testing.T) {
	backup, _ := giftLargeLocalFixture(t, 11, 1500, 1501, 3001)
	before := giftLocalFileHash(t, backup)
	first, err := InspectFinanceGiftScopeBackup(context.Background(), backup, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if first.Mode != "offline_backup_inspection_no_source_access" || first.SourceEpoch != "v1" {
		t.Fatalf("inspection lost its source or safety boundary: %+v", first)
	}
	if first.UnknownRows != 6013 || first.MonetaryRows != 6013 || first.UserHours != 4 || first.OversizeHours != 1 || first.MissingStates != 0 {
		t.Fatalf("wrong raw gap inventory: %+v", first)
	}
	if first.Candidates.Rows != 1511 || len(first.Candidates.Entries) != 2 || first.Candidates.StopReason != "row_budget" || first.ReadPlan == nil {
		t.Fatalf("wrong bounded first plan: %+v", first)
	}
	digest, err := financegiftexport.BatchConfirmation(*first.ReadPlan)
	if err != nil || first.Confirmation != digest {
		t.Fatal("invalid export handoff", err)
	}
	second, err := InspectFinanceGiftScopeBackup(context.Background(), backup, "v1")
	if err != nil || !reflect.DeepEqual(first, second) || before != giftLocalFileHash(t, backup) {
		t.Fatal("inspection mutated backup or is not deterministic", err)
	}
	for _, suffix := range []string{"-wal", "-journal", "-shm"} {
		if _, err := os.Stat(backup + suffix); !os.IsNotExist(err) {
			t.Fatal("inspection created SQLite sidecar", suffix, err)
		}
	}
}

func TestFinanceGiftBackupInspectionBlocksUnrepairableAndRejectsUnsafeInputs(t *testing.T) {
	for _, scenario := range []string{"oversize", "orphan", "corrupt", "wrong_epoch", "blank_epoch", "wal", "symlink", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			count := 2
			if scenario == "oversize" {
				count = 3001
			}
			backup, _ := giftLargeLocalFixture(t, count)
			epoch := "v1"
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch scenario {
			case "wrong_epoch":
				epoch = "not-present"
			case "blank_epoch":
				epoch = " "
			case "cancel":
				cancel()
			case "wal":
				if err := os.WriteFile(backup+"-wal", []byte("pending"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				link := filepath.Join(t.TempDir(), "backup.db")
				if err := os.Symlink(backup, link); err != nil {
					t.Fatal(err)
				}
				backup = link
			case "orphan", "corrupt":
				db, closeDB, err := giftLocalDatabase(backup)
				if err != nil {
					t.Fatal(err)
				}
				query := "UPDATE finance_gift_boundary_events SET hour_ts=hour_ts+3600"
				if scenario == "corrupt" {
					query = "UPDATE finance_gift_boundary_events SET quota=quota+1"
				}
				err = db.Exec(query).Error
				closeDB()
				if err != nil {
					t.Fatal(err)
				}
			}
			result, err := InspectFinanceGiftScopeBackup(ctx, backup, epoch)
			if scenario == "oversize" || scenario == "orphan" {
				if err != nil || result.Candidates.Status != "blocked" || result.ReadPlan != nil || result.Confirmation != "" {
					t.Fatalf("unsafe read plan issued: %+v %v", result, err)
				}
			} else if err == nil || result.ReadPlan != nil {
				t.Fatalf("unsafe input accepted: %+v %v", result, err)
			}
		})
	}
}

func TestFinanceGiftBackupInspectionSeparatesZeroQuotaAndKnownRows(t *testing.T) {
	backup, _ := giftLargeLocalFixture(t, 3)
	db, closeDB, err := giftLocalDatabase(backup)
	if err != nil {
		t.Fatal(err)
	}
	var events []FinanceGiftBoundaryEvent
	if err := db.Order("source_log_id").Find(&events).Error; err != nil {
		t.Fatal(err)
	}
	events[0].GroupKnown = true
	events[1].Quota = 0
	for i := range events {
		events[i].EvidenceHash = financeGiftBoundaryEventHash(events[i])
	}
	// Reconcile the fixture's monetary aggregate before publishing its new proof.
	if err := db.Model(&FinanceUserHourFact{}).Where("user_id=7").Update("consume_quota", 3).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := replaceFinanceGiftBoundaryUserHour(context.Background(), db, events[0].HourTs, 7, events[0].HourTs+7200, "v1", events); err != nil {
		t.Fatal(err)
	}
	closeDB()
	result, err := InspectFinanceGiftScopeBackup(context.Background(), backup, "v1")
	if err != nil || result.UnknownRows != 2 || result.MonetaryRows != 1 || result.Candidates.Rows != 3 {
		t.Fatalf("raw unknowns differ from monetary unknowns; export must keep whole hour: %+v %v", result, err)
	}
}
