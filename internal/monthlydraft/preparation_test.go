package monthlydraft

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func preparationFixture(t *testing.T) (generationFixture, DeploymentConfig) {
	t.Helper()
	f := selectionFixture(t)
	_, ro, path := usageAuthorityFixture(t)
	owner, _, err := LoadUsageGenerationOwner(context.Background(), ro, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	if err := os.Chmod(f.receiverPath, 0600); err != nil {
		t.Fatal(err)
	}
	return f, DeploymentConfig{Rollout: GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: "2026-09"}}}, ReceiverPath: f.receiverPath, FundingPath: f.path, AuthorityPath: path, EvidenceRoot: f.root}
}

func preparationCounts(t *testing.T, f generationFixture, inputs, selections int) {
	t.Helper()
	for _, item := range []struct {
		table string
		want  int
	}{
		{"portal_monthly_generation_inputs", inputs}, {"portal_monthly_generation_selections", selections}, {"portal_monthly_draft_tasks", 0},
	} {
		var got int
		if err := f.receiver.QueryRow("SELECT COUNT(*) FROM " + item.table).Scan(&got); err != nil || got != item.want {
			t.Fatalf("%s = %d, want %d: %v", item.table, got, item.want, err)
		}
	}
}

func TestPreparationInspectApplyIdempotent(t *testing.T) {
	f, cfg := preparationFixture(t)
	ctx := context.Background()
	files := []string{cfg.ReceiverPath, cfg.FundingPath, cfg.AuthorityPath}
	hashes := make(map[string]string)
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		hashes[path] = generationHash(raw)
	}
	plan := mustGenerationPlan(t, f)
	preview, err := PrepareGenerationInput(ctx, cfg, plan, f.raw, "", false)
	if err != nil || preview.Applied || preview.Changed || preview.SelectionHead != "" {
		t.Fatal(preview, err)
	}
	preparationCounts(t, f, 0, 0)
	if _, err := os.Stat(cfg.ReceiverPath + ".runtime.lock"); !os.IsNotExist(err) {
		t.Fatal("inspection created lock", err)
	}
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil || generationHash(raw) != hashes[path] {
			t.Fatal("inspection changed file", path, err)
		}
	}
	applied, err := PrepareGenerationInput(ctx, cfg, plan, f.raw, "", true)
	if err != nil || !applied.Applied || !applied.Changed || applied.SelectionHead == "" || applied.PlanSHA256 != preview.PlanSHA256 {
		t.Fatal(applied, err)
	}
	preparationCounts(t, f, 1, 1)
	again, err := PrepareGenerationInput(ctx, cfg, plan, f.raw, applied.SelectionHead, true)
	if err != nil || !again.Applied || again.Changed || again.SelectionHead != applied.SelectionHead {
		t.Fatal(again, err)
	}
	preparationCounts(t, f, 1, 1)
	if _, err := PrepareGenerationInput(ctx, cfg, plan, f.raw, "", true); !errors.Is(err, ErrGenerationSelectionConflict) {
		t.Fatal("stale head accepted", err)
	}
	for _, path := range files[1:] {
		raw, err := os.ReadFile(path)
		if err != nil || generationHash(raw) != hashes[path] {
			t.Fatal("apply changed source", path, err)
		}
	}
}

func TestPreparationRejectChangedInputAndBusy(t *testing.T) {
	f, cfg := preparationFixture(t)
	ctx := context.Background()
	plan := mustGenerationPlan(t, f)
	for _, bad := range [][]byte{[]byte(`{}`), append([]byte(`{"unknown":1,`), plan[1:]...), append([]byte(`{"version":1,`), plan[1:]...)} {
		if _, err := PrepareGenerationInput(ctx, cfg, bad, f.raw, "", true); err == nil {
			t.Fatal("noncanonical plan accepted")
		}
	}
	c := cfg
	c.Rollout.Targets = []GenerationTarget{{GroupID: 2, Month: "2026-09"}}
	if _, err := PrepareGenerationInput(ctx, c, plan, f.raw, "", true); err == nil {
		t.Fatal("scope mismatch accepted")
	}
	if _, err := PrepareGenerationInput(ctx, cfg, plan, append([]byte(" "), f.raw...), "", true); err == nil {
		t.Fatal("preparation digest mismatch accepted")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := PrepareGenerationInput(canceled, cfg, plan, f.raw, "", true); err == nil {
		t.Fatal("canceled operation accepted")
	}
	d, err := OpenDeployment(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDeploymentTest(t, d) })
	if _, err := PrepareGenerationInput(ctx, cfg, plan, f.raw, "", true); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("writer lock bypassed", err)
	}
	if _, err := PrepareGenerationInput(ctx, cfg, plan, f.raw, "", false); err != nil {
		t.Fatal("read-only inspection unnecessarily blocked", err)
	}
	closeDeploymentTest(t, d)
	if err := os.WriteFile(f.root+"/independent-evidence.json", []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareGenerationInput(ctx, cfg, plan, f.raw, "", true); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("changed evidence accepted", err)
	}
	preparationCounts(t, f, 0, 0)
}

func TestPreparationFinalPermissionCheckRollsBack(t *testing.T) {
	f, _ := preparationFixture(t)
	called := false
	_, err := stageGenerationChecked(context.Background(), f.receiver, f.plan, f.raw, "", func(context.Context) error {
		called = true
		return ErrGenerationChanged
	})
	if !called || !errors.Is(err, ErrGenerationChanged) {
		t.Fatal(called, err)
	}
	preparationCounts(t, f, 0, 0)
}

func TestPreparationReadOnlyDeploymentCannotStart(t *testing.T) {
	_, cfg := preparationFixture(t)
	d, err := openDeployment(context.Background(), cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDeploymentTest(t, d)
	if err := d.Start(context.Background()); !errors.Is(err, ErrGenerationInput) {
		t.Fatal(err)
	}
	if _, err := d.SubmitMonthly(context.Background(), 1, 1, "2026-09"); !errors.Is(err, ErrGenerationInput) {
		t.Fatal(err)
	}
	if _, err := d.receiver.Exec("CREATE TABLE forbidden(id INTEGER)"); err == nil {
		t.Fatal("read-only receiver writable")
	}
}

func TestPreparationRejectPermissionsFundingAndMoney(t *testing.T) {
	for _, kind := range []string{"permission", "funding", "money"} {
		t.Run(kind, func(t *testing.T) {
			f, cfg := preparationFixture(t)
			switch kind {
			case "permission":
				db := generationDB(t, cfg.AuthorityPath, false)
				if _, err := db.Exec(`UPDATE customer_groups SET portal_auth_ver=2 WHERE id=1`); err != nil {
					t.Fatal(err)
				}
			case "funding":
				f.plan.Windows[0].NewerHeadID = generationHash([]byte("different retained source"))
			case "money":
				f.input.Accounts[0].Movements[0].DeltaQuota++
				f.raw = draftJSON(t, f.input)
				f.plan.PreparationSHA256 = generationHash(f.raw)
			}
			for _, apply := range []bool{false, true} {
				if _, err := PrepareGenerationInput(context.Background(), cfg, mustGenerationPlan(t, f), f.raw, "", apply); err == nil {
					t.Fatal("bad input accepted", kind, apply)
				}
				preparationCounts(t, f, 0, 0)
			}
		})
	}
}

// Optional real binary integration, explicitly supplied by the local test runner.
// All inputs/databases are newly created synthetic fixtures, never operator data.
func TestPreparationCLI(t *testing.T) {
	bin := os.Getenv("MONTHLY_STAGE_TEST_BINARY")
	if bin == "" {
		t.Skip("set MONTHLY_STAGE_TEST_BINARY to locally built command")
	}
	if !filepath.IsAbs(bin) {
		t.Fatal("test binary must be absolute")
	}
	f, cfg := preparationFixture(t)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	cfg.ReceiverPath = filepath.Join(dir, "new-receiver.sqlite")
	for _, apply := range []bool{false, true} {
		args := []string{"init", "-receiver", cfg.ReceiverPath}
		if apply {
			args = append(args, "-apply")
		}
		raw, err := exec.CommandContext(t.Context(), bin, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("init: %v %s", err, raw)
		}
		var result ReceiverFileResult
		if err := json.Unmarshal(raw, &result); err != nil || result.Applied != apply {
			t.Fatal(result, err)
		}
		if !apply {
			if _, err := os.Stat(cfg.ReceiverPath); !os.IsNotExist(err) {
				t.Fatal("preview created receiver", err)
			}
		}
	}
	f.receiver = generationDB(t, cfg.ReceiverPath, false)
	planPath, prepPath := filepath.Join(dir, "plan.json"), filepath.Join(dir, "preparation.json")
	if err := os.WriteFile(planPath, mustGenerationPlan(t, f), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(prepPath, f.raw, 0600); err != nil {
		t.Fatal(err)
	}
	base := []string{"-receiver", cfg.ReceiverPath, "-funding", cfg.FundingPath, "-authority", cfg.AuthorityPath,
		"-evidence-root", cfg.EvidenceRoot, "-group-id", "1", "-month", "2026-09", "-plan", planPath, "-preparation", prepPath}
	call := func(apply bool, head string) PreparationResult {
		t.Helper()
		args := append(append([]string(nil), base...), "-expected-head="+head)
		if apply {
			args = append(args, "-apply")
		}
		raw, err := exec.CommandContext(t.Context(), bin, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("command failed: %v %s", err, raw)
		}
		var result PreparationResult
		if err := json.Unmarshal(raw, &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	preview := call(false, "")
	if preview.Applied {
		t.Fatal(preview)
	}
	preparationCounts(t, f, 0, 0)
	applied := call(true, "")
	if !applied.Applied || !applied.Changed {
		t.Fatal(applied)
	}
	preparationCounts(t, f, 1, 1)
	if again := call(true, applied.SelectionHead); !again.Applied || again.Changed {
		t.Fatal(again)
	}
	preparationCounts(t, f, 1, 1)
	if _, err := exec.CommandContext(t.Context(), bin, append(base, "-apply")...).CombinedOutput(); err == nil {
		t.Fatal("missing explicit head accepted")
	}
}
