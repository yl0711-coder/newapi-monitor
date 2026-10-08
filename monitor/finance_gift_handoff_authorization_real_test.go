//go:build unix

package monitor

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFinanceGiftAuthorizationRealSnapshot(t *testing.T) {
	job, digest, backup := os.Getenv("MONITOR_GIFT_HTTP_SOURCE_JOB"), os.Getenv("MONITOR_GIFT_HTTP_SOURCE_SHA256"), os.Getenv("MONITOR_GIFT_HTTP_RECEIVER")
	if job == "" || digest == "" || backup == "" {
		t.Skip("requires private closed local source job and receiver snapshot")
	}
	var plan FinanceGiftLocalPlan
	if _, err := giftLocalReadJSON(filepath.Join(job, "plan.json"), &plan); err != nil || len(plan.Targets) < 2 {
		t.Fatal("need multi-target real source", err)
	}
	paths := []string{filepath.Join(job, "usage-facts.db"), filepath.Join(job, "audit.jsonl"), backup}
	hashes := make([]string, len(paths))
	for i, path := range paths {
		hashes[i] = giftLocalFileHash(t, path)
	}
	m, r, _ := giftPreviewHTTPMonitor(t, job, digest, backup, plan.Targets[0].SourceEpoch)
	attachGiftAuthorizationStore(t, m)
	before := giftLocalFileHash(t, m.cfg.UsageFactsStorePath)
	body := `{"confirmation":"` + digest + `","request_id":"` + giftAuthorizationNonce + `"}`
	result := giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL, body, "local-acceptance-root", roleRoot))
	if result.Status != "awaiting_execution" || len(result.Authorization.Targets) != 2 || result.Execution {
		t.Fatal(result)
	}
	rows := 0
	for _, target := range result.Authorization.Targets {
		rows += target.RowsToUpdate
	}
	status := giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(m, r, "GET", giftAuthorizationURL+"/"+result.TaskID, "", "local-acceptance-root", roleRoot))
	if status.TaskID != result.TaskID || status.Authorization.ExpiresAt != result.Authorization.ExpiresAt {
		t.Fatal("status mismatch")
	}
	if before != giftLocalFileHash(t, m.cfg.UsageFactsStorePath) {
		t.Fatal("authorization modified receiver facts")
	}
	for i, path := range paths {
		if hashes[i] != giftLocalFileHash(t, path) {
			t.Fatal("authorization modified original inputs")
		}
	}
	t.Logf("confirmed %d user-hours / %d rows from %d planned targets; execution disabled, facts/source unchanged", len(result.Authorization.Targets), rows, len(plan.Targets))
}
