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
)

func automaticPreparationFileFixture(t *testing.T) (generationFixture, DeploymentConfig, AutomaticPreparationRequest) {
	t.Helper()
	f := observedPreparationFixture(t, "")
	_, authority, authorityPath := usageAuthorityFixture(t)
	owner, _, err := LoadUsageGenerationOwner(context.Background(), authority, f.plan.Owner.GroupID, f.plan.Owner.AuthVersion)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	if err := os.Chmod(f.receiverPath, 0600); err != nil {
		t.Fatal(err)
	}
	cfg := DeploymentConfig{ReceiverPath: f.receiverPath, FundingPath: f.path, AuthorityPath: authorityPath, EvidenceRoot: f.root,
		Rollout: GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: owner.GroupID, Month: f.plan.Funding.Month}}}}
	doc := RetainedAutomaticScope{Version: 1, Owner: owner, Month: f.plan.Funding.Month, SourceIdentity: f.plan.Funding.SourceIdentity, Scope: automaticScopeFixture(t, f)}
	scope, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(f.root, "prepared")
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	return f, cfg, AutomaticPreparationRequest{Plan: mustGenerationPlan(t, f), Template: f.raw, Scope: scope,
		ObservationSourceID: "observed_wallet", ScopeSourceID: syntheticScopeID, OutputRoot: output}
}

func TestAutomaticPreparationFilesReadOnlyWriteAndGenerate(t *testing.T) {
	f, cfg, request := automaticPreparationFileFixture(t)
	before := map[string]string{}
	for _, path := range []string{f.path, f.receiverPath, cfg.AuthorityPath, f.input.Sources[0].Path} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		before[path] = generationHash(raw)
	}
	ctx := context.Background()
	preview, err := PrepareAutomaticGenerationFiles(ctx, cfg, request)
	if err != nil || preview.Complete || preview.Directory != "" || preview.Mode != "checked_input_no_files_written" {
		t.Fatal("inspection failed", preview, err)
	}
	again, err := PrepareAutomaticGenerationFiles(ctx, cfg, request)
	if err != nil || !reflect.DeepEqual(preview, again) {
		t.Fatal("read-only inspection not deterministic", err)
	}
	if entries, err := os.ReadDir(request.OutputRoot); err != nil || len(entries) != 0 {
		t.Fatal("inspection wrote files", err)
	}
	preparationCounts(t, f, 0, 0)
	request.Write = true
	receipt, err := PrepareAutomaticGenerationFiles(ctx, cfg, request)
	if err != nil || !receipt.Complete || receipt.Directory != request.OutputRoot || receipt.Mode != automaticPreparationMode ||
		receipt.PlanSHA256 != preview.PlanSHA256 || receipt.PreparationSHA256 != preview.PreparationSHA256 || receipt.ManifestSHA256 != preview.ManifestSHA256 {
		t.Fatal("explicit copy mismatch", receipt, err)
	}
	read := func(name string) []byte {
		t.Helper()
		path := filepath.Join(request.OutputRoot, name)
		st, err := os.Stat(path)
		if err != nil || st.Mode().Perm() != 0600 {
			t.Fatal("private mode not retained", err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	manifestRaw := read("manifest.json")
	var m automaticPreparationManifest
	if json.Unmarshal(manifestRaw, &m) != nil || generationHash(manifestRaw) != receipt.ManifestSHA256 || m.Version != 1 || m.Owner != f.plan.Owner ||
		m.TemplateSHA256 != generationHash(f.raw) || m.ScopeInputSHA256 != generationHash(request.Scope) {
		t.Fatal("manifest not bound to original inputs")
	}
	plan, input, scope := read("plan.json"), read("input.json"), read("scope.json")
	if generationHash(plan) != receipt.PlanSHA256 || generationHash(input) != receipt.PreparationSHA256 || generationHash(scope) != receipt.ScopeSHA256 {
		t.Fatal("output hashes differ")
	}
	for path, digest := range before {
		raw, err := os.ReadFile(path)
		if err != nil || generationHash(raw) != digest {
			t.Fatal("preparation wrote database or original evidence", path, err)
		}
	}
	if _, err := os.Stat(cfg.ReceiverPath + ".runtime.lock"); !os.IsNotExist(err) {
		t.Fatal("read-only command created runtime lock", err)
	}
	preparationCounts(t, f, 0, 0)
	if out, err := PrepareAutomaticGenerationFiles(ctx, cfg, request); err == nil || out.Complete {
		t.Fatal("repeated write overwrote prior output", err)
	}
	checked, err := PrepareGenerationInput(ctx, cfg, plan, input, "", false)
	if err != nil || checked.Applied {
		t.Fatal("formal staging inspection rejected complete output", err)
	}
	// Activation is explicit and separate, only in this synthetic test.
	selected, err := PrepareGenerationInput(ctx, cfg, plan, input, "", true)
	if err != nil || !selected.Applied {
		t.Fatal(err)
	}
	useWalletPreparation(t, &f, WalletPreparation{Plan: plan, Preparation: input})
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(ctx, job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, artifact, 1)
}

func TestAutomaticPreparationFilesRejectInvalidOrAmbiguousInput(t *testing.T) {
	for _, kind := range []string{"plan_field", "scope_field", "scope_duplicate", "scope_case", "wrong_owner", "wrong_month", "wrong_source", "wrong_target", "wallet_unknown", "missing_scope", "observation", "source_id", "outside", "public", "symlink", "nonempty", "authority", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			f, cfg, r := automaticPreparationFileFixture(t)
			r.Write = true
			ctx := context.Background()
			var doc RetainedAutomaticScope
			if json.Unmarshal(r.Scope, &doc) != nil {
				t.Fatal("fixture scope")
			}
			switch kind {
			case "plan_field":
				r.Plan = append(bytes.TrimSuffix(r.Plan, []byte("}")), []byte(`,"unknown":1}`)...)
			case "scope_field":
				r.Scope = append(bytes.TrimSuffix(r.Scope, []byte("}")), []byte(`,"unknown":1}`)...)
			case "scope_duplicate":
				r.Scope = bytes.Replace(r.Scope, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
			case "scope_case":
				r.Scope = bytes.Replace(r.Scope, []byte(`"version":1`), []byte(`"VERSION":1`), 1)
			case "wrong_owner":
				doc.Owner.AuthVersion++
			case "wrong_month":
				doc.Month = "2026-08"
			case "wrong_source":
				doc.SourceIdentity = "another-source"
			case "wrong_target":
				cfg.Rollout.Targets[0].GroupID++
			case "wallet_unknown":
				doc.Scope.Accounts[0].WalletOnly = false
			case "missing_scope":
				r.Scope = nil
			case "observation":
				r.ObservationSourceID = "missing"
			case "source_id":
				r.ScopeSourceID = "bad#reference"
			case "outside":
				r.OutputRoot = t.TempDir()
				if err := os.Chmod(r.OutputRoot, 0700); err != nil {
					t.Fatal(err)
				}
			case "public":
				if err := os.Chmod(r.OutputRoot, 0755); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				r.OutputRoot = filepath.Join(f.root, "alias")
				if err := os.Symlink("prepared", r.OutputRoot); err != nil {
					t.Fatal(err)
				}
			case "nonempty":
				if err := os.WriteFile(filepath.Join(r.OutputRoot, "keep"), []byte("keep"), 0600); err != nil {
					t.Fatal(err)
				}
			case "authority":
				auth := generationDB(t, cfg.AuthorityPath, false)
				if _, err := auth.Exec(`UPDATE customer_groups SET portal_auth_ver=2 WHERE id=1`); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			if strings.HasPrefix(kind, "wrong_") && kind != "wrong_target" || kind == "wallet_unknown" {
				var err error
				r.Scope, err = json.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
			}
			out, err := PrepareAutomaticGenerationFiles(ctx, cfg, r)
			if err == nil || out.Directory != "" || out.Complete {
				t.Fatal("invalid input started writing", out, err)
			}
			entries, err := os.ReadDir(filepath.Join(f.root, "prepared"))
			want := 0
			if kind == "nonempty" {
				want = 1
			}
			if err != nil || len(entries) != want {
				t.Fatal("invalid attempt changed output", err)
			}
			preparationCounts(t, f, 0, 0)
		})
	}
}

func TestAutomaticPreparationFilesInterruptedCopiesNeverComplete(t *testing.T) {
	for _, kind := range []string{"disk", "corrupt", "corrupt_previous", "permission", "membership", "source", "cancel", "replaced_directory", "marker_exists"} {
		t.Run(kind, func(t *testing.T) {
			f, cfg, request := automaticPreparationFileFixture(t)
			request.Write = true
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			auth := generationDB(t, cfg.AuthorityPath, false)
			writer := func(ctx context.Context, root *os.Root, name string, raw []byte) error {
				if kind == "disk" && name == "input.json" {
					return errors.New("synthetic disk full")
				}
				if kind == "corrupt" && name == "scope.json" {
					return writeArchiveFile(ctx, root, name, []byte("corrupt"))
				}
				if err := writeArchiveFile(ctx, root, name, raw); err != nil {
					return err
				}
				if name == "manifest.pending" {
					switch kind {
					case "corrupt_previous":
						return os.WriteFile(filepath.Join(request.OutputRoot, "plan.json"), []byte("later corruption"), 0600)
					case "permission":
						_, err := auth.Exec(`UPDATE customer_groups SET portal_auth_ver=2 WHERE id=1`)
						return err
					case "membership":
						_, err := auth.Exec(`UPDATE tracked_users SET group_id=2 WHERE user_id=201`)
						return err
					case "source":
						return os.WriteFile(f.input.Sources[0].Path, []byte("changed synthetic source"), 0600)
					case "cancel":
						cancel()
					case "replaced_directory":
						if err := os.Rename(request.OutputRoot, request.OutputRoot+"-moved"); err != nil {
							return err
						}
						return os.Mkdir(request.OutputRoot, 0700)
					case "marker_exists":
						return writeArchiveFile(ctx, root, "manifest.json", []byte("foreign marker"))
					}
				}
				return nil
			}
			r, err := prepareAutomaticGenerationFiles(ctx, cfg, request, writer)
			if err == nil || r.Complete || r.Directory != request.OutputRoot || r.Mode != "incomplete_private_input_do_not_select" {
				t.Fatal("failed copy marked complete", r, err)
			}
			marker := filepath.Join(request.OutputRoot, "manifest.json")
			if kind == "marker_exists" {
				if raw, err := os.ReadFile(marker); err != nil || string(raw) != "foreign marker" {
					t.Fatal("replaced existing completion marker", err)
				}
			} else if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("failed copy gained completion marker", err)
			}
			preparationCounts(t, f, 0, 0)
		})
	}
}

func TestAutomaticPreparationFilesBusyRecoveryAndNoInitialization(t *testing.T) {
	f, cfg, request := automaticPreparationFileFixture(t)
	automaticPreparationFileSlots <- struct{}{}
	out, err := PrepareAutomaticGenerationFiles(context.Background(), cfg, request)
	<-automaticPreparationFileSlots
	if !errors.Is(err, ErrGenerationBusy) || out.Directory != "" || out.Complete {
		t.Fatal("busy preparation was not bounded", err)
	}
	if _, err := PrepareAutomaticGenerationFiles(context.Background(), cfg, request); err != nil {
		t.Fatal("busy preparation did not recover", err)
	}
	for _, missing := range []string{"receiver", "funding", "authority"} {
		bad := cfg
		newPath := filepath.Join(f.root, missing+"-must-not-create.sqlite")
		switch missing {
		case "receiver":
			bad.ReceiverPath = newPath
		case "funding":
			bad.FundingPath = newPath
		case "authority":
			bad.AuthorityPath = newPath
		}
		if _, err := PrepareAutomaticGenerationFiles(context.Background(), bad, request); err == nil {
			t.Fatal("missing database accepted", missing)
		}
		if _, err := os.Lstat(newPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("preparation initialized missing database", missing, err)
		}
	}
	preparationCounts(t, f, 0, 0)
}

func TestAutomaticPreparationFilesCLIRoundTrip(t *testing.T) {
	bin := os.Getenv("MONTHLY_STAGE_TEST_BINARY")
	if bin == "" {
		t.Skip("set local stage binary for real CLI round trip")
	}
	if !filepath.IsAbs(bin) {
		t.Fatal("absolute binary required")
	}
	t.Run("retained_normalized", func(t *testing.T) { testRetainedPreparationCLI(t, bin) })
	t.Run("observation_material", func(t *testing.T) { testObservationMaterialPreparationCLI(t, bin) })
	t.Run("wallet_reuse_material", func(t *testing.T) { testObservationMaterialPreparationCLI(t, bin, true) })
	t.Run("current_organization_collection", func(t *testing.T) { testOrganizationCollectionCLI(t, bin) })
	t.Run("organization_history", func(t *testing.T) { testOrganizationHistoryCLI(t, bin) })
	t.Run("organization_populated_history", func(t *testing.T) { testOrganizationHistoryCLI(t, bin, true) })
	f, cfg, request := automaticPreparationFileFixture(t)
	inputs := t.TempDir()
	for name, raw := range map[string][]byte{"plan.json": request.Plan, "template.json": request.Template, "scope.json": request.Scope} {
		if err := os.WriteFile(filepath.Join(inputs, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	args := []string{"prepare", "-receiver", cfg.ReceiverPath, "-funding", cfg.FundingPath, "-authority", cfg.AuthorityPath,
		"-evidence-root", cfg.EvidenceRoot, "-group-id=1", "-month=2026-09", "-plan", filepath.Join(inputs, "plan.json"),
		"-template", filepath.Join(inputs, "template.json"), "-scope", filepath.Join(inputs, "scope.json"),
		"-observation-source", request.ObservationSourceID, "-scope-source", request.ScopeSourceID, "-output-root", request.OutputRoot}
	for _, write := range []bool{false, true} {
		a := append([]string(nil), args...)
		if write {
			a = append(a, "-write")
		}
		raw, err := exec.CommandContext(t.Context(), bin, a...).CombinedOutput()
		if err != nil {
			t.Fatalf("prepare CLI: %v %s", err, raw)
		}
		var receipt AutomaticPreparationReceipt
		if json.Unmarshal(raw, &receipt) != nil || receipt.Complete != write {
			t.Fatal("CLI receipt mismatch")
		}
		preparationCounts(t, f, 0, 0)
	}
	stageArgs := []string{"-receiver", cfg.ReceiverPath, "-funding", cfg.FundingPath, "-authority", cfg.AuthorityPath, "-evidence-root", cfg.EvidenceRoot,
		"-group-id=1", "-month=2026-09", "-expected-head=", "-plan", filepath.Join(request.OutputRoot, "plan.json"), "-preparation", filepath.Join(request.OutputRoot, "input.json")}
	if raw, err := exec.CommandContext(t.Context(), bin, stageArgs...).CombinedOutput(); err != nil {
		t.Fatalf("prepared files rejected by real stage CLI: %v %s", err, raw)
	}
	preparationCounts(t, f, 0, 0)
}
