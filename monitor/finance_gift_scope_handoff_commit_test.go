//go:build unix

package monitor

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func handoffNoWait(ctx context.Context, _ time.Duration) error { return ctx.Err() }

func handoffRepairAudit(t *testing.T, job string) []financeGiftScopeBatchEntry {
	t.Helper()
	f, err := os.Open(filepath.Join(job, "audit.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var result []financeGiftScopeBatchEntry
	s := bufio.NewScanner(f)
	for s.Scan() {
		var entry financeGiftScopeBatchEntry
		if err := giftLocalDecodeJSON(s.Bytes(), &entry); err != nil {
			t.Fatal(err)
		}
		if entry.Status == "repaired" {
			result = append(result, entry)
		}
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestFinanceGiftHandoffCommitSurvivesProcessExit(t *testing.T) {
	if job := os.Getenv("MONITOR_HANDOFF_CRASH_TEST_JOB"); job != "" {
		_, err := runFinanceGiftLocalHandoff(context.Background(), job, os.Getenv("MONITOR_HANDOFF_CRASH_TEST_DIGEST"), handoffNoWait, func(financeGiftScopeBatchEntry) error {
			os.Exit(73) // Actual process exit after commit, before external audit.
			return nil
		})
		t.Fatalf("child did not reach crash point: %v", err)
	}
	job, digest, receiver := giftHandoffRunFixture(t)
	original := giftLocalFileHash(t, receiver)
	cmd := exec.Command(os.Args[0], "-test.run=^TestFinanceGiftHandoffCommitSurvivesProcessExit$")
	cmd.Env = append(os.Environ(), "MONITOR_HANDOFF_CRASH_TEST_JOB="+job, "MONITOR_HANDOFF_CRASH_TEST_DIGEST="+digest)
	err := cmd.Run()
	var exited *exec.ExitError
	if !errors.As(err, &exited) || exited.ExitCode() != 73 {
		t.Fatalf("child result: %v", err)
	}
	if len(handoffRepairAudit(t, job)) != 0 {
		t.Fatal("unexpected pre-crash external audit")
	}
	db, closeDB, err := giftLocalDatabase(filepath.Join(job, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	var commit financeGiftHandoffCommit
	err = db.Where("job = ?", digest).First(&commit).Error
	closeDB()
	if err != nil {
		t.Fatal(err)
	}
	var expected financeGiftScopeBatchEntry
	if err := giftLocalDecodeJSON([]byte(commit.Entry), &expected); err != nil {
		t.Fatal(err)
	}
	// A second external audit failure must prevent any later repair.
	before := giftLocalFileHash(t, filepath.Join(job, "usage-facts.db"))
	failed, err := runFinanceGiftLocalHandoff(context.Background(), job, digest, handoffNoWait, func(financeGiftScopeBatchEntry) error { return errors.New("audit still unavailable") })
	if err == nil || failed.Status != "audit_failed" || before != giftLocalFileHash(t, filepath.Join(job, "usage-facts.db")) {
		t.Fatal("recovery failure advanced facts", failed, err)
	}
	result, err := runFinanceGiftLocalHandoff(context.Background(), job, digest, handoffNoWait, nil)
	if err != nil || result.Status != "complete" || result.Entries[0].RowsUpdated != 0 {
		t.Fatal(result, err)
	}
	entries := handoffRepairAudit(t, job)
	if len(entries) != 10 || entries[0] != expected {
		t.Fatal("original committed audit not restored exactly", entries)
	}
	before = giftLocalFileHash(t, filepath.Join(job, "usage-facts.db"))
	if _, err := runFinanceGiftLocalHandoff(context.Background(), job, digest, handoffNoWait, nil); err != nil {
		t.Fatal(err)
	}
	if len(handoffRepairAudit(t, job)) != 10 || before != giftLocalFileHash(t, filepath.Join(job, "usage-facts.db")) || original != giftLocalFileHash(t, receiver) {
		t.Fatal("repeat changed facts, original, or duplicated repair audit")
	}
}

func TestFinanceGiftHandoffCommitFailureRollsBackFacts(t *testing.T) {
	job, digest, _ := giftHandoffRunFixture(t)
	db, closeDB, err := giftLocalDatabase(filepath.Join(job, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Exec(`CREATE TRIGGER reject_commit BEFORE INSERT ON local_gift_handoff_commits BEGIN SELECT RAISE(ABORT,'injected commit failure'); END`).Error
	closeDB()
	if err != nil {
		t.Fatal(err)
	}
	before := giftLocalFileHash(t, filepath.Join(job, "usage-facts.db"))
	result, err := runFinanceGiftLocalHandoff(context.Background(), job, digest, handoffNoWait, nil)
	if err == nil || result.Status != "failed" || len(result.Entries) != 1 || result.Entries[0].RowsUpdated != 0 || before != giftLocalFileHash(t, filepath.Join(job, "usage-facts.db")) {
		t.Fatal("commit failure did not roll back", result, err)
	}
}

func TestFinanceGiftHandoffCommitAuditWriteUncertain(t *testing.T) {
	job, digest, _ := giftHandoffRunFixture(t)
	result, err := runFinanceGiftLocalHandoff(context.Background(), job, digest, handoffNoWait, func(entry financeGiftScopeBatchEntry) error {
		f, err := os.OpenFile(filepath.Join(job, "audit.jsonl"), os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := json.NewEncoder(f).Encode(entry); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
		return errors.New("injected uncertain fsync result after full line persisted")
	})
	if err == nil || result.Status != "audit_failed" {
		t.Fatal(result, err)
	}
	if _, err := runFinanceGiftLocalHandoff(context.Background(), job, digest, handoffNoWait, nil); err != nil {
		t.Fatal(err)
	}
	if len(handoffRepairAudit(t, job)) != 10 {
		t.Fatal("uncertain append duplicated repair audit")
	}
}

func TestFinanceGiftHandoffCommitRejectsTampering(t *testing.T) {
	for _, scenario := range []string{"commit-body", "commit-index", "commit-deleted", "audit-body", "audit-duplicate", "ledger-missing", "legacy"} {
		t.Run(scenario, func(t *testing.T) {
			job, digest, _ := giftHandoffRunFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			waits := 0
			_, err := runFinanceGiftLocalHandoff(ctx, job, digest, func(ctx context.Context, _ time.Duration) error {
				waits++
				if waits == 2 {
					cancel()
				}
				return ctx.Err()
			}, nil)
			cancel()
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			db, closeDB, err := giftLocalDatabase(filepath.Join(job, "usage-facts.db"))
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "commit-body":
				err = db.Exec(`UPDATE local_gift_handoff_commits SET entry='{}'`).Error
			case "commit-index":
				err = db.Exec(`UPDATE local_gift_handoff_commits SET "index"=99`).Error
			case "commit-deleted":
				err = db.Exec(`DELETE FROM local_gift_handoff_commits`).Error
			case "ledger-missing":
				err = db.Exec(`DROP TABLE local_gift_handoff_commits`).Error
			case "audit-body", "audit-duplicate":
				entries := handoffRepairAudit(t, job)
				entry := entries[0]
				if scenario == "audit-body" {
					entry.FinishedAt++
				}
				f, openErr := os.OpenFile(filepath.Join(job, "audit.jsonl"), os.O_APPEND|os.O_WRONLY, 0)
				if openErr != nil {
					t.Fatal(openErr)
				}
				err = json.NewEncoder(f).Encode(entry)
				f.Close()
			case "legacy":
				path := filepath.Join(job, financeGiftHandoffReceiptName)
				var receipt FinanceGiftHandoffReceipt
				if _, err := giftLocalReadJSON(path, &receipt); err != nil {
					t.Fatal(err)
				}
				receipt.Version = 1
				data, _ := json.Marshal(receipt)
				err = os.WriteFile(path, data, 0600)
				digest = giftLocalDigest(data)
			}
			closeDB()
			if err != nil {
				t.Fatal(err)
			}
			before := giftLocalFileHash(t, filepath.Join(job, "usage-facts.db"))
			auditBefore := giftLocalFileHash(t, filepath.Join(job, "audit.jsonl"))
			if _, err := runFinanceGiftLocalHandoff(context.Background(), job, digest, handoffNoWait, nil); err == nil {
				t.Fatal("tampered job accepted")
			}
			if before != giftLocalFileHash(t, filepath.Join(job, "usage-facts.db")) || auditBefore != giftLocalFileHash(t, filepath.Join(job, "audit.jsonl")) {
				t.Fatal("rejection modified job")
			}
		})
	}
}
