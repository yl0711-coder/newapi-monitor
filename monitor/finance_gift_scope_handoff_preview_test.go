//go:build unix

package monitor

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func giftHandoffPreviewFixture(t *testing.T) (string, string, string, FinanceGiftLocalPlan) {
	t.Helper()
	backup, evidence, job := giftLocalFixture(t)
	plan, digest, err := PrepareFinanceGiftLocalJob(context.Background(), backup, job, []string{evidence})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runFinanceGiftLocalJob(context.Background(), job, digest, func(ctx context.Context, _ time.Duration) error { return ctx.Err() }); err != nil {
		t.Fatal(err)
	}
	return job, digest, backup, plan
}

func TestFinanceGiftHandoffPreviewReadyAndMatchedReadonly(t *testing.T) {
	job, digest, backup, _ := giftHandoffPreviewFixture(t)
	for _, path := range []string{backup, filepath.Join(job, "usage-facts.db")} {
		before := giftLocalFileHash(t, path)
		audit := giftLocalFileHash(t, filepath.Join(job, "audit.jsonl"))
		first, err := PreviewFinanceGiftLocalHandoff(context.Background(), job, digest, path)
		if err != nil {
			t.Fatal(err)
		}
		if path == backup {
			if first.Status != "ready" || first.ReadyTargets != 1 || first.RowsToUpdate != 3 {
				t.Fatalf("preview %+v", first)
			}
		} else if first.Status != "already_matched" || first.MatchedTargets != 1 || first.RowsToUpdate != 0 {
			t.Fatalf("matched %+v", first)
		}
		second, err := PreviewFinanceGiftLocalHandoff(context.Background(), job, digest, path)
		if err != nil || !reflect.DeepEqual(first, second) {
			t.Fatal("unstable preview", err)
		}
		if giftLocalFileHash(t, path) != before || giftLocalFileHash(t, filepath.Join(job, "audit.jsonl")) != audit {
			t.Fatal("preview wrote data")
		}
		for _, suffix := range []string{"-wal", "-shm", "-journal"} {
			if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
				t.Fatal("preview created SQLite sidecar", suffix, err)
			}
		}
	}
}

func TestFinanceGiftHandoffPreviewBlocksReceiverProblems(t *testing.T) {
	for _, scenario := range []string{"missing-proof", "aggregate", "hash", "conflict"} {
		t.Run(scenario, func(t *testing.T) {
			job, digest, backup, plan := giftHandoffPreviewFixture(t)
			db, closeDB, err := giftLocalDatabase(backup)
			if err != nil {
				t.Fatal(err)
			}
			target := plan.Targets[0]
			switch scenario {
			case "missing-proof":
				err = db.Where("user_id=?", target.UserID).Delete(&FinanceGiftBoundaryState{}).Error
			case "aggregate":
				err = db.Model(&FinanceUserHourFact{}).Where("user_id=?", target.UserID).Update("consume_quota", 1).Error
			case "hash":
				err = db.Model(&FinanceGiftBoundaryEvent{}).Where("user_id=?", target.UserID).Update("evidence_hash", "wrong").Error
			case "conflict":
				var current financeGiftScopeSnapshot
				current, err = loadFinanceGiftScopeSnapshot(context.Background(), db, target.SourceEpoch, target.HourTs, target.UserID)
				if err == nil {
					current.Events[0].GroupKnown, current.Events[0].Group = true, "different"
					current.Events[0].EvidenceHash = financeGiftBoundaryEventHash(current.Events[0])
					_, err = replaceFinanceGiftBoundaryUserHour(context.Background(), db, target.HourTs, target.UserID, target.HourTs+8000, target.SourceEpoch, current.Events)
				}
			}
			closeDB()
			if err != nil {
				t.Fatal(err)
			}
			before := giftLocalFileHash(t, backup)
			report, err := PreviewFinanceGiftLocalHandoff(context.Background(), job, digest, backup)
			want := map[string]string{"missing-proof": "receiver_proof_missing", "aggregate": "receiver_aggregate_mismatch", "hash": "receiver_unverifiable", "conflict": "evidence_conflict"}[scenario]
			if err != nil || report.Status != "blocked" || report.BlockedTargets != 1 || report.RowsToUpdate != 0 || report.Entries[0].Reason != want {
				t.Fatalf("blocked %+v %v", report, err)
			}
			if giftLocalFileHash(t, backup) != before {
				t.Fatal("blocked preview wrote receiver")
			}
		})
	}
}

func TestFinanceGiftHandoffPreviewRejectsInvalidSourceAndPendingJournal(t *testing.T) {
	for _, scenario := range []string{"digest", "evidence", "incomplete", "journal", "cancel", "locked"} {
		t.Run(scenario, func(t *testing.T) {
			job, digest, backup, _ := giftHandoffPreviewFixture(t)
			ctx := context.Background()
			switch scenario {
			case "digest":
				digest = "wrong"
			case "evidence":
				if err := os.WriteFile(filepath.Join(job, giftLocalEvidenceName(0)), []byte("bad"), 0600); err != nil {
					t.Fatal(err)
				}
			case "incomplete":
				newPath := filepath.Join(t.TempDir(), "job")
				_, newDigest, err := PrepareFinanceGiftLocalJob(ctx, backup, newPath, []string{filepath.Join(job, giftLocalEvidenceName(0))})
				if err != nil {
					t.Fatal(err)
				}
				job, digest = newPath, newDigest
			case "journal":
				if err := os.WriteFile(backup+"-wal", []byte("pending"), 0600); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "locked":
				lock, err := giftLocalLock(job)
				if err != nil {
					t.Fatal(err)
				}
				defer lock.Close()
			}
			report, err := PreviewFinanceGiftLocalHandoff(ctx, job, digest, backup)
			if err == nil || len(report.Entries) != 0 || report.RowsToUpdate != 0 {
				t.Fatalf("unsafe preview %+v %v", report, err)
			}
		})
	}
}
