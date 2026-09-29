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

func TestFinanceGiftAuthorizedLocalRealSnapshot(t *testing.T) {
	testFinanceGiftAuthorizedRealSnapshot(t, false)
}

func TestFinanceGiftLiveRealSnapshot(t *testing.T) {
	testFinanceGiftAuthorizedRealSnapshot(t, true)
}

func testFinanceGiftAuthorizedRealSnapshot(t *testing.T, live bool) {
	t.Helper()
	job, digest, backup := os.Getenv("MONITOR_GIFT_HTTP_SOURCE_JOB"), os.Getenv("MONITOR_GIFT_HTTP_SOURCE_SHA256"), os.Getenv("MONITOR_GIFT_HTTP_RECEIVER")
	if job == "" || digest == "" || backup == "" {
		t.Skip("requires private closed local source job and receiver snapshot")
	}
	var plan FinanceGiftLocalPlan
	if _, err := giftLocalReadJSON(filepath.Join(job, "plan.json"), &plan); err != nil || len(plan.Targets) < 3 {
		t.Fatal("need a real multi-target source", err)
	}
	paths := []string{filepath.Join(job, "usage-facts.db"), filepath.Join(job, "audit.jsonl"), backup}
	hashes := make([]string, len(paths))
	for i, path := range paths {
		hashes[i] = giftLocalFileHash(t, path)
	}
	m, r, _ := giftPreviewHTTPMonitor(t, job, digest, backup, plan.Targets[0].SourceEpoch)
	attachGiftAuthorizationStore(t, m)
	m.cfg.LocalSnapshotOnly = !live
	run := m.runFinanceGiftAuthorizedLocal
	if live {
		m.cfg.FinanceGiftHandoffLiveExecutionEnabled = true
		m.cfg.ProdDSN, m.cfg.NewAPIBaseURL = "must-not-connect", "https://invalid.example"
		run = m.runFinanceGiftAuthorizedLive
	} else {
		if err := m.usageFactsDB.AutoMigrate(&financeGiftHandoffCommit{}); err != nil {
			t.Fatal(err)
		}
	}
	body := `{"confirmation":"` + digest + `","request_id":"` + giftAuthorizationNonce + `"}`
	a := giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL, body, "local-root", roleRoot))
	before := make(map[financeGiftScopeTarget]financeGiftScopeSnapshot)
	for _, target := range plan.Targets {
		proof, err := loadFinanceGiftScopeSnapshot(context.Background(), m.usageFactsDB, target.SourceEpoch, target.HourTs, target.UserID)
		if err != nil {
			t.Fatal(err)
		}
		before[target] = proof
	}
	money := giftMixedMonetarySnapshot(t, m.usageFactsDB)
	result, err := run(context.Background(), a.TaskID, handoffNoWait)
	wantRows := 0
	for _, target := range a.Authorization.Targets {
		wantRows += target.RowsToUpdate
	}
	if err != nil || result.Status != "complete" || result.CommittedTargets != 2 || result.RowsUpdated != wantRows {
		t.Fatal(result, err)
	}
	progressStart := time.Now()
	if live {
		progress, err := readFinanceGiftCommitProgress(context.Background(), m.financeFactsReadStore(), a.TaskID, a.Authorization)
		if err != nil || progress.CommittedTargets != result.CommittedTargets || progress.RowsUpdated != result.RowsUpdated {
			t.Fatal("durable receipt progress differs from execution", progress, err)
		}
	} else {
		progress := giftGetProgress(t, m, r, a.TaskID, 200)
		if progress.Progress == nil || progress.Progress.CommittedTargets != result.CommittedTargets || progress.Progress.RowsUpdated != result.RowsUpdated {
			t.Fatal("HTTP receipt progress differs from execution", progress)
		}
	}
	t.Logf("live=%t durable receipt query: %s (no source hashing)", live, time.Since(progressStart))
	for _, target := range plan.Targets {
		approved := false
		for _, scope := range a.Authorization.Targets {
			if target == scope.financeGiftScopeTarget {
				approved = true
			}
		}
		if approved {
			continue
		}
		after, err := loadFinanceGiftScopeSnapshot(context.Background(), m.usageFactsDB, target.SourceEpoch, target.HourTs, target.UserID)
		if err != nil || !reflect.DeepEqual(before[target], after) {
			t.Fatal("unapproved real target changed", err)
		}
	}
	repeat, err := run(context.Background(), a.TaskID, handoffNoWait)
	if err != nil || repeat != result || money != giftMixedMonetarySnapshot(t, m.usageFactsDB) {
		t.Fatal("replay or monetary invariant failed", repeat, err)
	}
	for i, path := range paths {
		if hashes[i] != giftLocalFileHash(t, path) {
			t.Fatal("original source or backup changed")
		}
	}
	t.Logf("authorized %d/%d user-hours: committed %d rows; unauthorized targets, original inputs and monetary facts unchanged", result.CommittedTargets, len(plan.Targets), result.RowsUpdated)
}
