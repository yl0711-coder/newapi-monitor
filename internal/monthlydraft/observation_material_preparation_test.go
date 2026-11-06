package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func materialPreparationJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func observationMaterialPreparationFixture(t *testing.T) (generationFixture, DeploymentConfig, ObservationMaterialPreparationRequest, monthlysource.MonthlyObservationMaterial) {
	t.Helper()
	f, cfg, old := automaticPreparationFileFixture(t)
	addTokenArchivesFromObserved(t, &f)
	r, m := writeObservationMaterialPreparationFixture(t, f)
	r.Scope, r.ScopeSourceID, r.OutputRoot = old.Scope, old.ScopeSourceID, old.OutputRoot
	// A wrong default DB must not silently supply any selected observations.
	cfg.FundingPath = unrelatedObservationDefault(t, f.g)
	return f, cfg, r, m
}

func writeObservationMaterialPreparationFixture(t *testing.T, f generationFixture) (ObservationMaterialPreparationRequest, monthlysource.MonthlyObservationMaterial) {
	t.Helper()
	m := monthlysource.MonthlyObservationMaterial{Version: 1, Mode: monthlysource.MonthlyObservationMaterialMode, GroupID: f.plan.Owner.GroupID,
		FundingVersion: f.plan.Version, Funding: f.plan.Funding, FundingWindows: f.plan.Windows, UsageWindows: f.plan.UsageWindows,
		WalletIDs: f.plan.WalletObservations.IDs, Database: monthlysource.MonthlyObservationFile{Path: f.path, SHA256: f.sourceHash}}
	for _, token := range f.plan.TokenUsageSources {
		for _, s := range f.input.Sources {
			if s.ID == token.SourceID {
				m.Tokens = append(m.Tokens, monthlysource.MonthlyObservationTokenFile{UserID: token.UserID,
					File: monthlysource.MonthlyObservationFile{Path: s.Path, SHA256: s.SHA256}})
			}
		}
	}
	raw := materialPreparationJSON(t, m)
	path := filepath.Join(f.root, "materials.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	// Keep ONLY independently established history in the template. No manually
	// copied observation IDs, DB sources or token selections remain.
	input := f.input
	input.Sources = input.Sources[:1:1]
	return ObservationMaterialPreparationRequest{Material: monthlysource.MonthlyObservationFile{Path: path, SHA256: generationHash(raw)},
		Template: draftJSON(t, input)}, m
}

func TestObservationMaterialPreparationReadOnlyFilesAndExactMoney(t *testing.T) {
	f, cfg, r, m := observationMaterialPreparationFixture(t)
	ctx := context.Background()
	before := map[string]string{}
	for _, path := range []string{cfg.FundingPath, cfg.AuthorityPath, cfg.ReceiverPath, r.Material.Path, m.Database.Path,
		m.Tokens[0].File.Path, m.Tokens[1].File.Path, f.input.Sources[0].Path} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		before[path] = generationHash(raw)
	}
	preview, err := PrepareObservationMaterialFiles(ctx, cfg, r)
	if err != nil || preview.Complete || preview.Directory != "" {
		t.Fatal("material preflight failed", preview, err)
	}
	again, err := PrepareObservationMaterialFiles(ctx, cfg, r)
	if err != nil || !reflect.DeepEqual(preview, again) {
		t.Fatal("material inspection changed output identity", err)
	}
	entries, err := os.ReadDir(r.OutputRoot)
	if err != nil || len(entries) != 0 {
		t.Fatal("inspection wrote output", err)
	}
	preparationCounts(t, f, 0, 0)
	r.Write = true
	receipt, err := PrepareObservationMaterialFiles(ctx, cfg, r)
	if err != nil || !receipt.Complete || receipt.ManifestSHA256 != preview.ManifestSHA256 {
		t.Fatal("material could not enter existing prepared format", receipt, err)
	}
	read := func(name string) []byte {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(r.OutputRoot, name))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	plan, raw := read("plan.json"), read("input.json")
	input, err := monitor.DecodeMonthlyPreparationInput(raw)
	if err != nil {
		t.Fatal(err)
	}
	var original monitor.MonthlyPreparationInput
	if json.Unmarshal(r.Template, &original) != nil {
		t.Fatal("invalid fixture template")
	}
	for i, a := range input.Accounts {
		// Only the original existing normalizers may add observed movements,
		// usage, later wallet captures and per-token values. History is untouched.
		a.Spans, a.Movements, a.TokenMonths = original.Accounts[i].Spans, original.Accounts[i].Movements, original.Accounts[i].TokenMonths
		a.Snapshots = a.Snapshots[:len(original.Accounts[i].Snapshots)]
		if !reflect.DeepEqual(a, original.Accounts[i]) {
			t.Fatal("adapter rewrote independent money or history")
		}
	}
	found := false
	for _, source := range input.Sources {
		found = found || source.ID == observationMaterialSourceID && source.Path == r.Material.Path && source.SHA256 == r.Material.SHA256
	}
	if !found {
		t.Fatal("material handoff digest not bound to generated task")
	}
	for path, digest := range before {
		body, err := os.ReadFile(path)
		if err != nil || generationHash(body) != digest {
			t.Fatal("read-only preparation changed retained input or database", path, err)
		}
	}
	preparationCounts(t, f, 0, 0)
	if result, err := PrepareGenerationInput(ctx, cfg, plan, raw, "", false); err != nil || result.Applied {
		t.Fatal("material output rejected by existing inspector", err)
	}
	// Explicit local activation is test-only, never part of the adapter.
	if result, err := PrepareGenerationInput(ctx, cfg, plan, raw, "", true); err != nil || !result.Applied {
		t.Fatal("material output rejected by existing selection", err)
	}
	useWalletPreparation(t, &f, WalletPreparation{Plan: plan, Preparation: raw})
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(ctx, job.Request.Owner, job.ID)
	if err != nil || artifact.TokenSHA256 == "" {
		t.Fatal("material failed independent task audit", err)
	}
	assertGenerationAmounts(t, artifact, 1)
	cached, err := f.g.Run(ctx, job.Request.Owner, job.ID)
	if err != nil || !reflect.DeepEqual(artifact, cached) {
		t.Fatal("repeated reading changed per-account money", err)
	}
	if _, err := PrepareObservationMaterialFiles(ctx, cfg, r); err == nil {
		t.Fatal("existing output overwritten")
	}
}

func TestObservationMaterialPreparationRejectsAmbiguousInput(t *testing.T) {
	f, cfg, original, material := observationMaterialPreparationFixture(t)
	materialRaw := materialPreparationJSON(t, material)
	for _, kind := range []string{"hash", "mode", "version", "unknown", "duplicate", "case", "group", "month", "identity", "rules", "member", "token_missing", "token_duplicate", "token_user", "token_hash", "wallet_missing", "usage_missing", "funding_head", "database_hash", "source_collision", "path_collision", "scope_collision", "owner", "target", "history_gap", "refund", "money", "symlink", "parent_symlink", "hardlink", "public", "outside", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			r, c := original, cfg
			c.Rollout.Targets = append([]GenerationTarget{}, cfg.Rollout.Targets...)
			m := material
			m.Funding.UserIDs = append([]int64{}, material.Funding.UserIDs...)
			m.Tokens = append([]monthlysource.MonthlyObservationTokenFile{}, material.Tokens...)
			m.FundingWindows = append([]monthlysource.FundingMonthWindowVersion{}, material.FundingWindows...)
			var input monitor.MonthlyPreparationInput
			var scope RetainedAutomaticScope
			if json.Unmarshal(r.Template, &input) != nil || json.Unmarshal(r.Scope, &scope) != nil {
				t.Fatal("fixture decode failed")
			}
			ctx := context.Background()
			switch kind {
			case "mode":
				m.Mode = "approved"
			case "version":
				m.Version++
			case "group":
				m.GroupID++
			case "month":
				m.Funding.Month = "2026-08"
			case "identity":
				m.Funding.SourceIdentity = strings.Repeat("a", 64)
			case "rules":
				m.Funding.Rules.QuotaPerUnit = 1000000
			case "member":
				m.Funding.UserIDs[0] = 999
			case "token_missing":
				m.Tokens = m.Tokens[:1]
			case "token_duplicate":
				m.Tokens[1] = m.Tokens[0]
			case "token_user":
				m.Tokens[0].UserID = 999
			case "token_hash":
				m.Tokens[0].File.SHA256 = strings.Repeat("a", 64)
			case "wallet_missing":
				m.WalletIDs = m.WalletIDs[:1]
			case "usage_missing":
				m.UsageWindows = nil
			case "funding_head":
				m.FundingWindows[0].NewerHeadID = strings.Repeat("a", 64)
			case "database_hash":
				m.Database.SHA256 = strings.Repeat("a", 64)
			case "source_collision":
				input.Sources[0].ID = observationMaterialSourceID
			case "path_collision":
				input.Sources[0].Path = m.Database.Path
			case "scope_collision":
				r.ScopeSourceID = observationMaterialDatabaseID
			case "owner":
				scope.Owner.AuthVersion++
			case "target":
				c.Rollout.Targets[0].GroupID++
			case "history_gap":
				input.Accounts[0].Baseline.BalanceQuota++
			case "refund":
				input.Accounts[0].WalletMonths[0].RefundQuota--
			case "money":
				input.Accounts[0].Movements = []monitor.MonthlyPreparationMovement{{UserID: 201, ID: 999, At: m.FundingWindows[0].From + 1, DeltaQuota: 1, Source: input.Sources[0].ID + "#duplicate"}}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			raw := materialPreparationJSON(t, m)
			switch kind {
			case "unknown":
				raw = append(raw[:len(raw)-1], []byte(`,"approved":true}`)...)
			case "duplicate":
				raw = bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
			case "case":
				raw = bytes.Replace(raw, []byte(`"version":1`), []byte(`"VERSION":1`), 1)
			}
			if err := os.WriteFile(original.Material.Path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.WriteFile(original.Material.Path, materialRaw, 0600) })
			r.Material.SHA256 = generationHash(raw)
			if kind == "hash" {
				r.Material.SHA256 = strings.Repeat("a", 64)
			}
			if kind == "symlink" || kind == "parent_symlink" || kind == "hardlink" {
				alias := filepath.Join(f.root, "alias-"+kind)
				var err error
				if kind == "hardlink" {
					err = os.Link(original.Material.Path, alias)
				} else if kind == "parent_symlink" {
					err = os.Symlink(f.root, alias)
					r.Material.Path = filepath.Join(alias, "materials.json")
				} else {
					err = os.Symlink(original.Material.Path, alias)
				}
				if err != nil {
					t.Fatal(err)
				}
				defer os.Remove(alias)
				if kind != "parent_symlink" {
					r.Material.Path = alias
				}
			}
			if kind == "public" {
				if err := os.Chmod(original.Material.Path, 0644); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := os.Chmod(original.Material.Path, 0600); err != nil {
						t.Error(err)
					}
				}()
			}
			if kind == "outside" {
				r.Material.Path = filepath.Join(t.TempDir(), "missing.json")
			}
			r.Template, r.Scope, r.Write = draftJSON(t, input), materialPreparationJSON(t, scope), true
			out, err := PrepareObservationMaterialFiles(ctx, c, r)
			if err == nil || out.Complete || out.Directory != "" {
				t.Fatal("bad material produced prepared output", out, err)
			}
			entries, err := os.ReadDir(r.OutputRoot)
			if err != nil || len(entries) != 0 {
				t.Fatal("bad selection wrote output", err)
			}
			preparationCounts(t, f, 0, 0)
		})
	}
}

func TestObservationMaterialPreparationLateMutationDoesNotComplete(t *testing.T) {
	for _, kind := range []string{"material", "source_wal", "authority", "disk"} {
		t.Run(kind, func(t *testing.T) {
			f, cfg, r, m := observationMaterialPreparationFixture(t)
			r.Write = true
			writer := func(ctx context.Context, root *os.Root, name string, raw []byte) error {
				if kind == "disk" && name == "input.json" {
					return errors.New("synthetic disk failure")
				}
				if err := writeArchiveFile(ctx, root, name, raw); err != nil {
					return err
				}
				if name == "manifest.pending" {
					switch kind {
					case "material":
						return os.WriteFile(r.Material.Path, []byte("changed"), 0600)
					case "source_wal":
						return os.WriteFile(m.Database.Path+"-wal", []byte("late sidecar"), 0600)
					case "authority":
						auth := generationDB(t, cfg.AuthorityPath, false)
						_, err := auth.Exec(`UPDATE customer_groups SET portal_auth_ver=2 WHERE id=1`)
						return err
					}
				}
				return nil
			}
			out, err := prepareObservationMaterialFiles(context.Background(), cfg, r, writer)
			if err == nil || out.Complete || out.Directory != r.OutputRoot {
				t.Fatal("late failure obtained completion marker", out, err)
			}
			if _, err := os.Stat(filepath.Join(r.OutputRoot, "manifest.json")); !os.IsNotExist(err) {
				t.Fatal("failed material marked complete", err)
			}
			preparationCounts(t, f, 0, 0)
		})
	}
}

func TestObservationMaterialPreparationOptInBusyAndNoInitialization(t *testing.T) {
	f, cfg, r, _ := observationMaterialPreparationFixture(t)
	ctx := context.Background()
	for _, kind := range []string{"disabled", "unbounded", "nil_context", "template_limit", "scope_limit", "missing_receiver"} {
		c, request, parent := cfg, r, ctx
		switch kind {
		case "disabled":
			c.Rollout.Enabled = false
		case "unbounded":
			c.Rollout.Targets = nil
		case "nil_context":
			parent = nil
		case "template_limit":
			request.Template = make([]byte, monitor.MonthlyPreparationInputLimit+1)
		case "scope_limit":
			request.Scope = make([]byte, monitor.MonthlyPreparationInputLimit+1)
		case "missing_receiver":
			c.ReceiverPath = filepath.Join(f.root, "never-initialize.sqlite")
		}
		if out, err := PrepareObservationMaterialFiles(parent, c, request); err == nil || out.Complete {
			t.Fatal("unsafe preparation configuration accepted", kind, err)
		}
	}
	if _, err := os.Stat(filepath.Join(f.root, "never-initialize.sqlite")); !os.IsNotExist(err) {
		t.Fatal("preparation initialized missing receiver", err)
	}
	observationMaterialPreparationSlots <- struct{}{}
	_, err := PrepareObservationMaterialFiles(ctx, cfg, r)
	<-observationMaterialPreparationSlots
	if !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("parallel material preparation not bounded", err)
	}
	if _, err := PrepareObservationMaterialFiles(ctx, cfg, r); err != nil {
		t.Fatal("busy slot did not recover", err)
	}
	preparationCounts(t, f, 0, 0)
	if entries, err := os.ReadDir(r.OutputRoot); err != nil || len(entries) != 0 {
		t.Fatal("preflight or bad config wrote output", err)
	}
}

// Called by the EXISTING mandatory synthetic CLI test. No new optional gate or
// CI environment variable: the current command must exercise both old and new
// forms before that test succeeds.
func testObservationMaterialPreparationCLI(t *testing.T, binary string, reuse ...bool) {
	t.Helper()
	f, cfg, r, _ := observationMaterialPreparationFixture(t)
	if len(reuse) != 0 && reuse[0] {
		useWalletReusePreparationFixture(t, f, &r)
	}
	for name, raw := range map[string][]byte{"template.json": r.Template, "scope-input.json": r.Scope} {
		if err := os.WriteFile(filepath.Join(f.root, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	args := []string{"prepare", "-receiver", cfg.ReceiverPath, "-funding", cfg.FundingPath, "-authority", cfg.AuthorityPath,
		"-evidence-root", cfg.EvidenceRoot, "-group-id=1", "-month=2026-09", "-material", r.Material.Path, "-material-sha256", r.Material.SHA256,
		"-template", filepath.Join(f.root, "template.json"), "-scope", filepath.Join(f.root, "scope-input.json"),
		"-scope-source", r.ScopeSourceID, "-output-root", r.OutputRoot}
	for _, extra := range [][]string{{"-plan", "not-used.json"}, {"-observation-source", "manual-source"}, {"-material-sha256", strings.Repeat("a", 64)}, {"-material-sha256="}, {"-material="}} {
		bad := append(append([]string{}, args...), extra...)
		if raw, err := exec.CommandContext(t.Context(), binary, bad...).CombinedOutput(); err == nil || bytes.Contains(raw, []byte(f.root)) {
			t.Fatal("invalid CLI combination accepted or leaked private path", err)
		}
	}
	var preview AutomaticPreparationReceipt
	for _, write := range []bool{false, true} {
		current := append([]string{}, args...)
		if write {
			current = append(current, "-write")
		}
		raw, err := exec.CommandContext(t.Context(), binary, current...).CombinedOutput()
		if err != nil {
			t.Fatalf("material CLI: %v %s", err, raw)
		}
		var receipt AutomaticPreparationReceipt
		if json.Unmarshal(raw, &receipt) != nil || receipt.Complete != write {
			t.Fatal("material CLI receipt mismatch")
		}
		if !write {
			preview = receipt
		} else if receipt.ManifestSHA256 != preview.ManifestSHA256 {
			t.Fatal("CLI write changed inspected inputs")
		}
		preparationCounts(t, f, 0, 0)
	}
	stage := []string{"-receiver", cfg.ReceiverPath, "-funding", cfg.FundingPath, "-authority", cfg.AuthorityPath,
		"-evidence-root", cfg.EvidenceRoot, "-group-id=1", "-month=2026-09", "-expected-head=", "-plan", filepath.Join(r.OutputRoot, "plan.json"),
		"-preparation", filepath.Join(r.OutputRoot, "input.json")}
	if raw, err := exec.CommandContext(t.Context(), binary, stage...).CombinedOutput(); err != nil {
		t.Fatalf("material CLI output rejected by existing stage inspection: %v %s", err, raw)
	}
	preparationCounts(t, f, 0, 0)
}
