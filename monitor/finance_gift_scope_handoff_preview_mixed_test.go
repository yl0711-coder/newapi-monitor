//go:build unix

package monitor

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestFinanceGiftHandoffPreviewMixedDoesNotClaimWholeBatchReady(t *testing.T) {
	series, path, digest := giftSeriesFixture(t)
	ctx := context.Background()
	if _, err := runFinanceGiftLocalSeries(ctx, path, digest, func(ctx context.Context, _ time.Duration) error { return ctx.Err() }); err != nil {
		t.Fatal(err)
	}
	job := series.Steps[0].NextDir
	var plan FinanceGiftLocalPlan
	bytes, err := giftLocalReadJSON(filepath.Join(job, "plan.json"), &plan)
	if err != nil {
		t.Fatal(err)
	}
	confirmation := giftLocalDigest(bytes)
	source, closeSource, err := giftLocalReadonlyDatabase(filepath.Join(job, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSource()
	receiver := filepath.Join(t.TempDir(), "receiver.db")
	if _, err := giftLocalCopyBackup(filepath.Join(series.PriorDir, "usage-facts.db"), receiver); err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := giftLocalDatabase(receiver)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		target := plan.Targets[i]
		proof, err := loadFinanceGiftScopeSnapshot(ctx, source, target.SourceEpoch, target.HourTs, target.UserID)
		if err != nil {
			closeDB()
			t.Fatal(err)
		}
		switch i {
		case 0:
			_, err = applyFinanceGiftScopeHandoff(ctx, db, target, proof.Events, proof.State.ContentHash, time.Now().Unix())
		case 1:
			err = db.Where("source_epoch=? AND hour_ts=? AND user_id=?", target.SourceEpoch, target.HourTs, target.UserID).Delete(&FinanceGiftBoundaryState{}).Error
		case 2:
			proof.Events[0].Group = "conflicting"
			proof.Events[0].EvidenceHash = financeGiftBoundaryEventHash(proof.Events[0])
			_, err = replaceFinanceGiftBoundaryUserHour(ctx, db, target.HourTs, target.UserID, time.Now().Unix(), target.SourceEpoch, proof.Events)
		}
		if err != nil {
			closeDB()
			t.Fatal(err)
		}
	}
	closeDB()
	before := giftLocalFileHash(t, receiver)
	report, err := PreviewFinanceGiftLocalHandoff(ctx, job, confirmation, receiver)
	if err != nil || report.Status != "blocked" || report.ReadyTargets != 7 || report.MatchedTargets != 1 || report.BlockedTargets != 2 || report.RowsToUpdate != 7 {
		t.Fatalf("mixed %+v %v", report, err)
	}
	if report.Entries[1].Reason != "receiver_proof_missing" || report.Entries[2].Reason != "evidence_conflict" {
		t.Fatal("reasons lost", report.Entries)
	}
	if giftLocalFileHash(t, receiver) != before {
		t.Fatal("mixed preview wrote receiver")
	}
}

func TestFinanceGiftHandoffPreviewCannotAuthorizeStaleReceiver(t *testing.T) {
	job, digest, backup, plan := giftHandoffPreviewFixture(t)
	ctx := context.Background()
	report, err := PreviewFinanceGiftLocalHandoff(ctx, job, digest, backup)
	if err != nil || report.Status != "ready" {
		t.Fatal("initial preview", err)
	}
	target := plan.Targets[0]
	source, closeSource, err := giftLocalReadonlyDatabase(filepath.Join(job, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSource()
	evidence, err := loadFinanceGiftScopeSnapshot(ctx, source, target.SourceEpoch, target.HourTs, target.UserID)
	if err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := giftLocalDatabase(backup)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	current, err := loadFinanceGiftScopeSnapshot(ctx, db, target.SourceEpoch, target.HourTs, target.UserID)
	if err != nil {
		t.Fatal(err)
	}
	current.Events[0].Quota++
	current.Events[1].Quota--
	for i := range current.Events {
		current.Events[i].EvidenceHash = financeGiftBoundaryEventHash(current.Events[i])
	}
	if _, err := replaceFinanceGiftBoundaryUserHour(ctx, db, target.HourTs, target.UserID, time.Now().Unix(), target.SourceEpoch, current.Events); err != nil {
		t.Fatal(err)
	}
	result, err := applyFinanceGiftScopeHandoff(ctx, db, target, evidence.Events, evidence.State.ContentHash, time.Now().Unix())
	if err == nil || result.RowsUpdated != 0 {
		t.Fatal("old preview authorized changed receiver", result, err)
	}
}
