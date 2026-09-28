//go:build unix

package monitor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFinanceGiftTaskRealHTTPExecution(t *testing.T) {
	job, digest, backup := os.Getenv("MONITOR_GIFT_HTTP_SOURCE_JOB"), os.Getenv("MONITOR_GIFT_HTTP_SOURCE_SHA256"), os.Getenv("MONITOR_GIFT_HTTP_RECEIVER")
	if job == "" || digest == "" || backup == "" {
		t.Skip("requires closed private local snapshots")
	}
	var plan FinanceGiftLocalPlan
	if _, err := giftLocalReadJSON(filepath.Join(job, "plan.json"), &plan); err != nil {
		t.Fatal(err)
	}
	m, r, _ := giftPreviewHTTPMonitor(t, job, digest, backup, plan.Targets[0].SourceEpoch)
	attachGiftAuthorizationStore(t, m)
	m.cfg.LocalSnapshotOnly = true
	m.cfg.FinanceGiftHandoffLocalExecutionEnabled = true
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if !m.financeGiftTask.shutdown(ctx) {
			t.Error("worker still active")
		}
	})
	if m.usageFactsDB.Migrator().HasTable(&financeGiftHandoffCommit{}) {
		t.Fatal("real receiver fixture unexpectedly has a commit table")
	}
	money := giftMixedMonetarySnapshot(t, m.usageFactsDB)
	beforeSource := giftLocalFileHash(t, filepath.Join(job, "usage-facts.db"))
	beforeBackup := giftLocalFileHash(t, backup)
	body := `{"confirmation":"` + digest + `","request_id":"` + giftAuthorizationNonce + `"}`
	a := giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL, body, "local-root", roleRoot))
	v := giftHTTPControl(t, m, r)
	v.RequestID = strings.Repeat("a", 32)
	started := time.Now()
	w := giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL+"/"+a.TaskID+"/start", giftTaskBody(v), "local-root", roleRoot)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	m.financeGiftTask.mu.Lock()
	done := m.financeGiftTask.done
	m.financeGiftTask.mu.Unlock()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("bounded real task did not finish")
	}
	view := giftHTTPControl(t, m, r)
	if view.Status != "complete" {
		t.Fatal(view)
	}
	progress := giftGetProgress(t, m, r, a.TaskID, 200)
	wantRows := 0
	for _, target := range a.Authorization.Targets {
		wantRows += target.RowsToUpdate
	}
	if progress.Progress.CommittedTargets != 2 || progress.Progress.RowsUpdated != wantRows {
		t.Fatal(progress)
	}
	if money != giftMixedMonetarySnapshot(t, m.usageFactsDB) || beforeSource != giftLocalFileHash(t, filepath.Join(job, "usage-facts.db")) || beforeBackup != giftLocalFileHash(t, backup) {
		t.Fatal("money or original input changed")
	}
	t.Logf("real HTTP task completed %d authorized user-hours / %d rows in %s, including normal cooldowns; original inputs and money unchanged", progress.Progress.CommittedTargets, progress.Progress.RowsUpdated, time.Since(started))
}
