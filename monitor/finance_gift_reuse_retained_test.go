//go:build unix

package monitor

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Retaining original evidence does not require retaining every intermediate
// database. Rebuild a NEW offline job against a closed current snapshot, then
// exercise the same bounded authorization path used by the live handler.
// No original file is modified and no production endpoint is contacted.
func TestFinanceGiftReuseRetainedEvidence(t *testing.T) {
	oldJob := os.Getenv("MONITOR_GIFT_RETAINED_JOB")
	digest := os.Getenv("MONITOR_GIFT_RETAINED_PLAN_SHA256")
	backup := os.Getenv("MONITOR_GIFT_HTTP_RECEIVER")
	if oldJob == "" || digest == "" || backup == "" {
		t.Skip("requires retained evidence, reviewed plan hash and closed receiver backup")
	}
	// Closed retained inputs may be mounted read-only. Only newly prepared
	// jobs acquire writable lock files; all source bytes are hash-validated.
	plan, _, err := giftLocalLoadConfirmedInputs(oldJob, digest)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Targets) < 3 {
		t.Fatal("requires at least three targets to test the two-target authorization boundary")
	}
	if err := giftLocalValidateAudit(filepath.Join(oldJob, "audit.jsonl"), plan); err != nil {
		t.Fatal(err)
	}
	evidence := make([]string, len(plan.Targets))
	inputs := []string{backup, filepath.Join(oldJob, "plan.json"), filepath.Join(oldJob, "audit.jsonl")}
	for i := range evidence {
		evidence[i] = filepath.Join(oldJob, giftLocalEvidenceName(i))
		inputs = append(inputs, evidence[i])
	}
	hashes := make([]string, len(inputs))
	for i, path := range inputs {
		hashes[i] = giftLocalFileHash(t, path)
	}
	t.Cleanup(func() {
		for i, path := range inputs {
			if giftLocalFileHash(t, path) != hashes[i] {
				t.Errorf("retained input changed: index %d", i)
			}
		}
	})
	job, rebuiltDigest := completedGiftAuthorizationSource(t, backup, evidence)
	rebuilt, _, err := giftLocalLoadConfirmedInputs(job, rebuiltDigest)
	if err != nil {
		t.Fatal(err)
	}
	if rebuilt.BackupSHA256 != hashes[0] || !reflect.DeepEqual(plan.Targets, rebuilt.Targets) ||
		!reflect.DeepEqual(plan.EvidenceSHA256, rebuilt.EvidenceSHA256) || !reflect.DeepEqual(plan.Rows, rebuilt.Rows) {
		t.Fatal("rebuilt job changed evidence, scope or receiver binding")
	}
	preview, err := PreviewFinanceGiftLocalHandoff(context.Background(), job, rebuiltDigest, backup)
	if err != nil || preview.BlockedTargets != 0 || preview.ReadyTargets < 3 {
		t.Fatalf("rebuilt handoff not ready: %+v, %v", preview, err)
	}
	t.Setenv("MONITOR_GIFT_HTTP_SOURCE_JOB", job)
	t.Setenv("MONITOR_GIFT_HTTP_SOURCE_SHA256", rebuiltDigest)
	// Despite the name, this helper uses only t.TempDir receivers, an in-process
	// HTTP router, and deliberately invalid external connection settings.
	testFinanceGiftAuthorizedRealSnapshot(t, true)
	t.Logf("retained evidence verified: targets=%d ready=%d rows=%d; rebuilt source and receiver are temporary copies", len(plan.Targets), preview.ReadyTargets, preview.RowsToUpdate)
}
