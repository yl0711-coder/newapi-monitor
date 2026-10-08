//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestFinanceGiftHTTPPreviewRealSnapshot(t *testing.T) {
	job, digest, backup := os.Getenv("MONITOR_GIFT_HTTP_SOURCE_JOB"), os.Getenv("MONITOR_GIFT_HTTP_SOURCE_SHA256"), os.Getenv("MONITOR_GIFT_HTTP_RECEIVER")
	if job == "" || digest == "" || backup == "" {
		t.Skip("requires private closed local source job and receiver snapshot")
	}
	var plan FinanceGiftLocalPlan
	if _, err := giftLocalReadJSON(filepath.Join(job, "plan.json"), &plan); err != nil || len(plan.Targets) == 0 {
		t.Fatal("read private plan", err)
	}
	paths := []string{filepath.Join(job, "usage-facts.db"), filepath.Join(job, "audit.jsonl"), backup}
	hashes := make([]string, len(paths))
	for i, path := range paths {
		hashes[i] = giftLocalFileHash(t, path)
	}
	m, r, body := giftPreviewHTTPMonitor(t, job, digest, backup, plan.Targets[0].SourceEpoch)
	localHash := giftLocalFileHash(t, m.cfg.UsageFactsStorePath)
	var before, after []FinanceGiftBoundaryState
	if err := m.usageFactsDB.Order("source_epoch,hour_ts,user_id").Find(&before).Error; err != nil {
		t.Fatal(err)
	}
	w := giftPreviewRequest(m, r, giftPreviewURL, body, roleRoot)
	var result struct {
		Execution bool                      `json:"execution_enabled"`
		Preview   FinanceGiftHandoffPreview `json:"preview"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	expected, err := PreviewFinanceGiftLocalHandoff(context.Background(), job, digest, backup)
	if err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || result.Execution || result.Preview.Status != expected.Status || result.Preview.RowsToUpdate != expected.RowsToUpdate || result.Preview.ReadyTargets != expected.ReadyTargets || result.Preview.ReceiverSHA256 != "" {
		t.Fatal("HTTP differs from closed-snapshot preview", w.Code, result, expected)
	}
	if err := m.usageFactsDB.Order("source_epoch,hour_ts,user_id").Find(&after).Error; err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(before)
	b, _ := json.Marshal(after)
	if string(a) != string(b) || localHash != giftLocalFileHash(t, m.cfg.UsageFactsStorePath) {
		t.Fatal("preview changed receiver proofs or facts")
	}
	for i, path := range paths {
		if hashes[i] != giftLocalFileHash(t, path) {
			t.Fatal("original input changed")
		}
	}
	if m.usageFactsDB.Migrator().HasTable(&financeGiftHandoffCommit{}) {
		t.Fatal("preview created execution ledger")
	}
	if err := m.financeFactsReadStore().Exec("DELETE FROM finance_gift_boundary_states").Error; err == nil {
		t.Fatal("preview handle is writable")
	}
	t.Logf("HTTP 200 root-only preview: targets=%d ready=%d rows=%d execution=false; original and receiver hashes unchanged", len(result.Preview.Entries), result.Preview.ReadyTargets, result.Preview.RowsToUpdate)
}
