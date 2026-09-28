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

func giftLargeImportFixture(t *testing.T, count int) (string, string, string, financegiftexport.LargeHourPlan, string) {
	t.Helper()
	backup, paths := giftLargeLocalFixture(t, count, 2)
	var original financeGiftLocalEvidence
	if _, err := giftLocalReadJSON(paths[0], &original); err != nil {
		t.Fatal(err)
	}
	plan, digest, err := PrepareFinanceGiftLargeHourPlan(context.Background(), backup, "v1", original.Hour, original.UserID)
	if err != nil {
		t.Fatal(err)
	}
	evidence := financeGiftLargeEvidence{Version: 1, PlanSHA256: digest, SourceEpoch: plan.SourceEpoch, LocalContentHash: plan.LocalContentHash, financeGiftLocalEvidence: original}
	readPath, evidencePath := filepath.Join(t.TempDir(), "read.json"), filepath.Join(t.TempDir(), "evidence.json")
	giftLargeWriteTestJSON(t, readPath, plan)
	giftLargeWriteTestJSON(t, evidencePath, evidence)
	return backup, readPath, evidencePath, plan, digest
}

func giftLargeWriteTestJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestFinanceGiftLargeLocalCompleteRetryAndCache(t *testing.T) {
	ctx := context.Background()
	backup, readPath, evidencePath, plan, readDigest := giftLargeImportFixture(t, 4833)
	originalHash := giftLocalFileHash(t, backup)
	dir := filepath.Join(t.TempDir(), "job")
	_, digest, err := PrepareFinanceGiftLargeLocalJob(ctx, backup, readPath, evidencePath, dir, readDigest)
	if err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	cursor := FinanceFactSyncState{ID: financeFactSyncStateID, SourceEpoch: "v1", StartHourTs: plan.HourTs, NextHourTs: plan.HourTs + 7200, Status: "backfilling"}
	if err := db.Create(&cursor).Error; err != nil {
		t.Fatal(err)
	}
	moneyBefore := giftMixedMonetarySnapshot(t, db)
	otherBefore, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", plan.HourTs+3600, 8)
	if err != nil {
		t.Fatal(err)
	}
	m := newFinanceReportTestMonitor(t, "large-local.example")
	m.usageFactsDB = db
	fullBefore, err := m.financeReportSourceFingerprint(ctx, plan.HourTs+3600, plan.HourTs+7200)
	if err != nil {
		t.Fatal(err)
	}
	baseBefore, err := m.financeReportPeriodSourceFingerprint(ctx, plan.HourTs+3600, plan.HourTs+7200)
	if err != nil {
		t.Fatal(err)
	}
	var completed financeGiftScopeSnapshot
	for attempt := 0; attempt < 2; attempt++ {
		result, err := RunFinanceGiftLargeLocalJob(ctx, dir, digest)
		want := 4833
		if attempt == 1 {
			want = 0
		}
		if err != nil || result.Status != "complete" || result.Remaining != 0 || len(result.Entries) != 1 || result.Entries[0].RowsUpdated != want {
			t.Fatalf("attempt %d: %+v err=%v", attempt, result, err)
		}
		after, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", plan.HourTs, plan.UserID)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range after.Events {
			if !event.GroupKnown {
				t.Fatal("partial hour published")
			}
		}
		if attempt == 0 {
			completed = after
		} else if !reflect.DeepEqual(completed, after) {
			t.Fatal("retry changed timestamps or ledger")
		}
		if giftMixedMonetarySnapshot(t, db) != moneyBefore || giftLocalFileHash(t, backup) != originalHash {
			t.Fatal("money, cursor or original backup changed")
		}
	}
	otherAfter, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", plan.HourTs+3600, 8)
	if err != nil || !reflect.DeepEqual(otherBefore, otherAfter) {
		t.Fatal("unrelated user changed", err)
	}
	fullAfter, err := m.financeReportSourceFingerprint(ctx, plan.HourTs+3600, plan.HourTs+7200)
	if err != nil || fullBefore == fullAfter {
		t.Fatal("gift-dependent report cache did not invalidate", err)
	}
	baseAfter, err := m.financeReportPeriodSourceFingerprint(ctx, plan.HourTs+3600, plan.HourTs+7200)
	if err != nil || baseBefore != baseAfter {
		t.Fatal("unrelated monthly base cache invalidated", err)
	}
	var integrity string
	if err := db.Raw("PRAGMA quick_check").Scan(&integrity).Error; err != nil || integrity != "ok" {
		t.Fatal("database integrity", integrity, err)
	}
	if _, err := RunFinanceGiftLocalJob(ctx, dir, digest); err == nil {
		t.Fatal("ordinary runner accepted large job")
	}
}

func TestFinanceGiftLargeLocalRejectsInvalidEvidence(t *testing.T) {
	backup, readPath, evidencePath, _, digest := giftLargeImportFixture(t, 3001)
	originalHash := giftLocalFileHash(t, backup)
	var valid financeGiftLargeEvidence
	if _, err := giftLocalReadJSON(evidencePath, &valid); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"missing", "duplicate", "quota", "null_quota", "null_group", "epoch", "proof", "confirmation"} {
		t.Run(scenario, func(t *testing.T) {
			// Decode independently so pointer mutations cannot leak across cases.
			var e financeGiftLargeEvidence
			data, _ := json.Marshal(valid)
			if err := json.Unmarshal(data, &e); err != nil {
				t.Fatal(err)
			}
			confirmation := digest
			switch scenario {
			case "missing":
				e.Rows = e.Rows[:len(e.Rows)-1]
			case "duplicate":
				e.Rows[501] = e.Rows[500]
			case "quota":
				*e.Rows[500].Quota++
			case "null_quota":
				e.Rows[500].Quota = nil
			case "null_group":
				e.Rows[500].Group = nil
			case "epoch":
				e.SourceEpoch = "other"
			case "proof":
				e.LocalContentHash = "other"
			case "confirmation":
				confirmation = "wrong"
			}
			path, dir := filepath.Join(t.TempDir(), "bad.json"), filepath.Join(t.TempDir(), "job")
			giftLargeWriteTestJSON(t, path, e)
			if _, _, err := PrepareFinanceGiftLargeLocalJob(context.Background(), backup, readPath, path, dir, confirmation); err == nil {
				t.Fatal("invalid evidence accepted")
			}
			if _, err := os.Stat(filepath.Join(dir, "large-plan.json")); !os.IsNotExist(err) {
				t.Fatal("failed preparation became executable")
			}
			if giftLocalFileHash(t, backup) != originalHash {
				t.Fatal("failed preparation modified backup")
			}
		})
	}
}

func TestFinanceGiftLargeLocalRollbackAndStaleLedger(t *testing.T) {
	for _, scenario := range []string{"write_failure", "ledger_change", "fact_change", "evidence_change", "partial_audit", "lock", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			backup, readPath, evidencePath, plan, readDigest := giftLargeImportFixture(t, 3001)
			dir := filepath.Join(t.TempDir(), "job")
			_, digest, err := PrepareFinanceGiftLargeLocalJob(ctx, backup, readPath, evidencePath, dir, readDigest)
			if err != nil {
				t.Fatal(err)
			}
			db, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer closeDB()
			switch scenario {
			case "write_failure":
				// Fail after multiple 200-row insert chunks, not only at row one.
				err = db.Exec(`CREATE TRIGGER fail_large BEFORE INSERT ON finance_gift_boundary_events WHEN NEW.group_known=1 AND NEW.source_log_id=1001 BEGIN SELECT RAISE(ABORT,'synthetic write failure'); END`).Error
			case "ledger_change":
				prior, loadErr := loadFinanceGiftScopeSnapshot(ctx, db, "v1", plan.HourTs, plan.UserID)
				if loadErr != nil {
					t.Fatal(loadErr)
				}
				// Even a partial change agreeing with the fetched group requires
				// a new plan; only exact old or fully completed hashes are valid.
				prior.Events[0].Group, prior.Events[0].GroupKnown = "test", true
				prior.Events[0].EvidenceHash = financeGiftBoundaryEventHash(prior.Events[0])
				_, err = replaceFinanceGiftBoundaryUserHour(ctx, db, plan.HourTs, plan.UserID, prior.State.UpdatedAt+1, "v1", prior.Events)
			case "fact_change":
				err = db.Exec("UPDATE finance_user_hour_facts SET consume_quota=consume_quota+1 WHERE user_id=7").Error
			case "evidence_change":
				err = os.WriteFile(filepath.Join(dir, "evidence.json"), []byte("{}"), 0600)
			case "partial_audit":
				err = os.WriteFile(filepath.Join(dir, "audit.jsonl"), []byte("{"), 0600)
			case "lock":
				lock, lockErr := giftLocalLock(dir)
				if lockErr != nil {
					t.Fatal(lockErr)
				}
				defer lock.Close()
			case "cancel":
				cancel()
			}
			if err != nil {
				t.Fatal(err)
			}
			before, err := loadFinanceGiftScopeSnapshot(context.Background(), db, "v1", plan.HourTs, plan.UserID)
			if err != nil {
				t.Fatal(err)
			}
			money := giftMixedMonetarySnapshot(t, db)
			if scenario != "write_failure" {
				status, statusErr := InspectFinanceGiftLargeLocalJob(ctx, dir, digest)
				if statusErr == nil || status.Status != "unverified" {
					t.Fatal("unsafe status published progress", status, statusErr)
				}
			}
			result, err := RunFinanceGiftLargeLocalJob(ctx, dir, digest)
			if err == nil || result.Status == "complete" || result.Remaining != 1 {
				t.Fatalf("unsafe run: %+v %v", result, err)
			}
			after, err := loadFinanceGiftScopeSnapshot(context.Background(), db, "v1", plan.HourTs, plan.UserID)
			if err != nil || !reflect.DeepEqual(before, after) || giftMixedMonetarySnapshot(t, db) != money {
				t.Fatal("failed repair left partial changes", err)
			}
		})
	}
}

func TestFinanceGiftLargeLocalRetryAfterCommitWithoutAudit(t *testing.T) {
	backup, readPath, evidencePath, _, readDigest := giftLargeImportFixture(t, 3001)
	dir := filepath.Join(t.TempDir(), "job")
	_, digest, err := PrepareFinanceGiftLargeLocalJob(context.Background(), backup, readPath, evidencePath, dir, readDigest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RunFinanceGiftLargeLocalJob(context.Background(), dir, digest); err != nil {
		t.Fatal(err)
	}
	// Simulate a completed database commit whose audit append never happened.
	if err := os.WriteFile(filepath.Join(dir, "audit.jsonl"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	before := giftLocalFileHash(t, filepath.Join(dir, "usage-facts.db"))
	result, err := RunFinanceGiftLargeLocalJob(context.Background(), dir, digest)
	if err != nil || result.Status != "complete" || len(result.Entries) != 1 || result.Entries[0].RowsUpdated != 0 || before != giftLocalFileHash(t, filepath.Join(dir, "usage-facts.db")) {
		t.Fatal("post-commit retry duplicated publication", result, err)
	}
	db, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("UPDATE finance_user_hour_facts SET consume_quota=consume_quota+1 WHERE user_id=7").Error; err != nil {
		t.Fatal(err)
	}
	closeDB()
	if _, err := RunFinanceGiftLargeLocalJob(context.Background(), dir, digest); err == nil {
		t.Fatal("completed retry hid changed financial facts")
	}
}
