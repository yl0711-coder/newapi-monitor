//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
)

func giftContinuationFixture(t *testing.T) (string, string, string, string, financegiftexport.BatchPlan) {
	t.Helper()
	backup, original, _ := giftMixedLocalFixture(t)
	priorDir, priorHash := giftCandidateJob(t, backup, original[:1])
	if result, err := runFinanceGiftLocalJob(context.Background(), priorDir, priorHash,
		func(ctx context.Context, _ time.Duration) error { return ctx.Err() }); err != nil || result.Status != "complete" {
		t.Fatalf("prior job incomplete: %+v %v", result, err)
	}
	readPlan, readHash, err := PrepareFinanceGiftReadPlan(context.Background(), priorDir, priorHash)
	if err != nil || len(readPlan.Targets) != 3 {
		t.Fatalf("next read plan: %+v %v", readPlan, err)
	}
	exportDir := filepath.Join(t.TempDir(), "export")
	if err := os.Mkdir(exportDir, 0700); err != nil {
		t.Fatal(err)
	}
	writeJSON := func(name string, value any) {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := giftLocalWriteNew(filepath.Join(exportDir, name), data); err != nil {
			t.Fatal(err)
		}
	}
	writeJSON("plan.json", readPlan)
	result := financegiftexport.BatchResult{Status: "complete"}
	var manifest []string
	for i, target := range readPlan.Targets {
		var matching []byte
		for _, originalPath := range original {
			var evidence financeGiftLocalEvidence
			data, err := giftLocalReadJSON(originalPath, &evidence)
			if err != nil {
				t.Fatal(err)
			}
			if evidence.UserID == target.UserID && evidence.Hour == target.HourTs {
				matching = data
				break
			}
		}
		if matching == nil {
			t.Fatal("missing original evidence for synthetic handoff")
		}
		name := giftLocalEvidenceName(i)
		path := filepath.Join(exportDir, name)
		if err := giftLocalWriteNew(path, matching); err != nil {
			t.Fatal(err)
		}
		result.Entries = append(result.Entries, financegiftexport.BatchEntry{BatchTarget: target, File: name, SHA256: giftLocalDigest(matching)})
		manifest = append(manifest, path)
	}
	writeJSON("result.json", result)
	writeJSON("evidence-manifest.json", manifest)
	return priorDir, priorHash, exportDir, readHash, readPlan
}

func TestFinanceGiftLocalContinuationRequiresVerifiedHandoff(t *testing.T) {
	priorDir, priorHash, exportDir, readHash, readPlan := giftContinuationFixture(t)
	before := giftLocalJobFileHashes(t, priorDir)
	nextDir := filepath.Join(t.TempDir(), "next")
	plan, nextHash, err := PrepareFinanceGiftLocalContinuation(context.Background(), priorDir, priorHash, exportDir, readHash, nextDir)
	if err != nil || len(plan.Targets) != len(readPlan.Targets) || nextHash == "" {
		t.Fatalf("valid handoff rejected: %+v %q %v", plan, nextHash, err)
	}
	for i, target := range readPlan.Targets {
		if plan.Targets[i].UserID != target.UserID || plan.Targets[i].HourTs != target.HourTs {
			t.Fatal("handoff changed confirmed target order")
		}
	}
	after := giftLocalJobFileHashes(t, priorDir)
	if after["continuation-intent.json"] == "" || after["continuation-complete.json"] == "" {
		t.Fatal("handoff lacks durable lineage markers")
	}
	delete(after, "continuation-intent.json")
	delete(after, "continuation-complete.json")
	if !reflect.DeepEqual(before, after) {
		t.Fatal("handoff modified previous facts, evidence or audit")
	}
	status, err := InspectFinanceGiftLocalJob(context.Background(), nextDir, nextHash)
	if err != nil || status.Status != "pending" || status.RemainingTargets != len(readPlan.Targets) {
		t.Fatalf("handoff silently repaired next job: %+v %v", status, err)
	}
	result, err := runFinanceGiftLocalJob(context.Background(), nextDir, nextHash,
		func(ctx context.Context, _ time.Duration) error { return ctx.Err() })
	if err != nil || result.Status != "complete" || result.Remaining != 0 {
		t.Fatalf("new offline job could not complete: %+v %v", result, err)
	}
	if _, _, err := PrepareFinanceGiftLocalContinuation(context.Background(), priorDir, priorHash, exportDir, readHash, filepath.Join(t.TempDir(), "duplicate")); err == nil {
		t.Fatal("previous offline job forked into a second child")
	}
}

func TestFinanceGiftLocalContinuationRejectsChangedEvidence(t *testing.T) {
	priorDir, priorHash, exportDir, readHash, _ := giftContinuationFixture(t)
	path := filepath.Join(exportDir, giftLocalEvidenceName(0))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, ' '), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := PrepareFinanceGiftLocalContinuation(context.Background(), priorDir, priorHash, exportDir, readHash, filepath.Join(t.TempDir(), "next")); err == nil {
		t.Fatal("changed source export accepted")
	}
}

func TestFinanceGiftLocalContinuationRejectsWrongReadPlan(t *testing.T) {
	priorDir, priorHash, exportDir, _, _ := giftContinuationFixture(t)
	if _, _, err := PrepareFinanceGiftLocalContinuation(context.Background(), priorDir, priorHash, exportDir, "wrong", filepath.Join(t.TempDir(), "next")); err == nil {
		t.Fatal("unconfirmed source read plan accepted")
	}
}

func TestFinanceGiftLocalContinuationRejectsIncompleteExport(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*testing.T, string)
	}{
		{"missing_manifest", func(t *testing.T, dir string) {
			if err := os.Remove(filepath.Join(dir, "evidence-manifest.json")); err != nil {
				t.Fatal(err)
			}
		}},
		{"failed_result", func(t *testing.T, dir string) {
			path := filepath.Join(dir, "result.json")
			var result financegiftexport.BatchResult
			if _, err := giftLocalReadJSON(path, &result); err != nil {
				t.Fatal(err)
			}
			result.Status = "failed"
			data, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"wrong_manifest_path", func(t *testing.T, dir string) {
			path := filepath.Join(dir, "evidence-manifest.json")
			var manifest []string
			if _, err := giftLocalReadJSON(path, &manifest); err != nil {
				t.Fatal(err)
			}
			manifest[0] = filepath.Join(dir, "other.json")
			data, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			priorDir, priorHash, exportDir, readHash, _ := giftContinuationFixture(t)
			tc.change(t, exportDir)
			nextDir := filepath.Join(t.TempDir(), "next")
			if _, _, err := PrepareFinanceGiftLocalContinuation(context.Background(), priorDir, priorHash, exportDir, readHash, nextDir); err == nil {
				t.Fatal("incomplete export accepted")
			}
			if _, err := os.Stat(nextDir); !os.IsNotExist(err) {
				t.Fatal("failed handoff created a runnable job", err)
			}
			if _, err := os.Stat(filepath.Join(priorDir, "continuation-intent.json")); !os.IsNotExist(err) {
				t.Fatal("failed export validation marked the previous job as continued", err)
			}
		})
	}
}
