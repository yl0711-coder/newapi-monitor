//go:build unix

package monitor

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
)

// Opt-in compiled-command acceptance. Inputs are synthetic only. An optional
// independently exported MySQL artifact must match this exact synthetic plan.
func TestFinanceGiftLargeCLIWorkflow(t *testing.T) {
	binary := os.Getenv("MONITOR_GIFT_LOCAL_CLI")
	if binary == "" {
		t.Skip("requires explicitly built local CLI")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("absolute CLI path required")
	}
	backup, _, evidence, plan, _ := giftLargeImportFixture(t, 4833)
	original := giftLocalFileHash(t, backup)
	readPath := filepath.Join(t.TempDir(), "read-plan.json")
	invoke := func(args ...string) []byte {
		t.Helper()
		data, err := exec.Command(binary, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("offline command failed: %v: %s", err, data)
		}
		if !json.Valid(data) {
			t.Fatalf("command output is not one JSON document: %s", data)
		}
		return data
	}
	var readResult struct {
		Digest string `json:"confirm_read_plan_sha256"`
		Rows   int    `json:"rows"`
	}
	data := invoke("-action", "large-read-plan", "-backup", backup, "-source-epoch", "v1", "-user-id", "7", "-hour-ts", strconv.FormatInt(plan.HourTs, 10), "-output", readPath)
	if err := json.Unmarshal(data, &readResult); err != nil || readResult.Digest == "" || readResult.Rows != 4833 {
		t.Fatal("wrong read plan result", err)
	}
	if fixtureDir := os.Getenv("MONITOR_GIFT_CLI_FIXTURE_DIR"); fixtureDir != "" {
		if !filepath.IsAbs(fixtureDir) {
			t.Fatal("absolute new private fixture directory required")
		}
		if err := os.Mkdir(fixtureDir, 0700); err != nil {
			t.Fatal(err)
		}
		if _, err := financegiftexport.WriteLargeHourPlan(filepath.Join(fixtureDir, "read-plan.json"), plan); err != nil {
			t.Fatal(err)
		}
		t.Logf("synthetic source fixture: hour=%d, user=7, rows=4833, digest=%s", plan.HourTs, readResult.Digest)
	}
	if exported := os.Getenv("MONITOR_GIFT_CLI_EXPORTED_EVIDENCE"); exported != "" {
		evidence = exported
	}
	dir := filepath.Join(t.TempDir(), "job")
	var prepared struct {
		Digest string `json:"confirm_plan_sha256"`
	}
	data = invoke("-action", "large-plan", "-backup", backup, "-read-plan-file", readPath, "-confirm-read-plan", readResult.Digest, "-evidence", evidence, "-job-dir", dir)
	if err := json.Unmarshal(data, &prepared); err != nil || prepared.Digest == "" {
		t.Fatal("wrong prepared result", err)
	}
	status := func(want string, remaining int) {
		t.Helper()
		dbHash, auditHash := giftLocalFileHash(t, filepath.Join(dir, "usage-facts.db")), giftLocalFileHash(t, filepath.Join(dir, "audit.jsonl"))
		var got FinanceGiftLocalProgress
		if err := json.Unmarshal(invoke("-action", "large-status", "-job-dir", dir, "-confirm-plan", prepared.Digest), &got); err != nil {
			t.Fatal(err)
		}
		if got.Mode != "offline_large_hour_only" || got.Status != want || got.Rows != 4833 || got.RemainingRows != remaining {
			t.Fatalf("status=%+v", got)
		}
		if giftLocalFileHash(t, filepath.Join(dir, "usage-facts.db")) != dbHash || giftLocalFileHash(t, filepath.Join(dir, "audit.jsonl")) != auditHash {
			t.Fatal("status wrote to job")
		}
	}
	status("pending", 4833)
	for attempt := 0; attempt < 2; attempt++ {
		var result financeGiftScopeBatchResult
		if err := json.Unmarshal(invoke("-action", "large-run", "-job-dir", dir, "-confirm-plan", prepared.Digest), &result); err != nil {
			t.Fatal(err)
		}
		want := 4833
		if attempt > 0 {
			want = 0
		}
		if result.Status != "complete" || len(result.Entries) != 1 || result.Entries[0].RowsUpdated != want {
			t.Fatalf("run=%+v", result)
		}
		status("complete", 0)
	}
	for _, action := range []string{"large-run", "large-status"} {
		if data, err := exec.Command(binary, "-action", action, "-job-dir", dir, "-confirm-plan", "wrong").CombinedOutput(); err == nil {
			t.Fatalf("bad confirmation accepted: %s", data)
		}
	}
	if giftLocalFileHash(t, backup) != original {
		t.Fatal("CLI changed original backup")
	}
	t.Log("compiled offline CLI: plan -> prepare -> pending status -> 4833-row repair -> readonly complete status -> 0-row retry; original backup unchanged")
}
