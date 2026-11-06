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
	"testing"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func retainedPreparationFileFixture(t *testing.T) (generationFixture, DeploymentConfig, RetainedPreparationRequest) {
	t.Helper()
	f := selectionFixture(t)
	_, authority, path := usageAuthorityFixture(t)
	owner, _, err := LoadUsageGenerationOwner(t.Context(), authority, f.plan.Owner.GroupID, f.plan.Owner.AuthVersion)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	addRetainedScopeFixture(t, &f, nil)
	if err := os.Chmod(f.receiverPath, 0600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(f.root, "retained-ready")
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := DeploymentConfig{ReceiverPath: f.receiverPath, FundingPath: f.path, AuthorityPath: path, EvidenceRoot: f.root,
		Rollout: GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: owner.GroupID, Month: f.plan.Funding.Month}}}}
	return f, cfg, RetainedPreparationRequest{Plan: mustGenerationPlan(t, f), Preparation: f.raw, OutputRoot: output}
}

func TestRetainedPreparationFilesInspectSealAndReceive(t *testing.T) {
	f, cfg, request := retainedPreparationFileFixture(t)
	before := map[string][]byte{}
	for _, p := range []string{f.receiverPath, f.path, cfg.AuthorityPath, f.input.Sources[0].Path, f.input.Sources[len(f.input.Sources)-1].Path} {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		before[p] = raw
	}
	ctx := t.Context()
	preview, err := PrepareRetainedGenerationFiles(ctx, cfg, request)
	if err != nil || preview.Complete || preview.Directory != "" {
		t.Fatal("retained inspection failed", preview, err)
	}
	again, err := PrepareRetainedGenerationFiles(ctx, cfg, request)
	if err != nil || !reflect.DeepEqual(again, preview) {
		t.Fatal("inspection is not repeatable", err)
	}
	if err := automaticPreparationOutputEmptyMustCheck(request.OutputRoot); err != nil {
		t.Fatal("inspection wrote outputs", err)
	}
	preparationCounts(t, f, 0, 0)
	request.Write = true
	receipt, err := PrepareRetainedGenerationFiles(ctx, cfg, request)
	if err != nil || !receipt.Complete || receipt.ManifestSHA256 != preview.ManifestSHA256 {
		t.Fatal("retained package not sealed", receipt, err)
	}
	for path, raw := range before {
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(after, raw) {
			t.Fatal("packaging changed original database or evidence", err)
		}
	}
	preparationCounts(t, f, 0, 0)
	if out, err := PrepareRetainedGenerationFiles(ctx, cfg, request); err == nil || out.Complete {
		t.Fatal("completed package was overwritten")
	}
	manifestRaw, err := os.ReadFile(filepath.Join(request.OutputRoot, "manifest.json"))
	var manifest automaticPreparationManifest
	if err != nil || json.Unmarshal(manifestRaw, &manifest) != nil || manifest.PreparationKind != retainedPreparationKind ||
		manifest.UsageSHA256 != "" || manifest.WalletSHA256 != "" || manifest.FundingSHA256 != "" {
		t.Fatal("retained package invented raw observation attestation")
	}
	d, err := openDeployment(ctx, cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.closeResources(); err != nil {
			t.Error(err)
		}
	}()
	planRaw, inputRaw, plan, err := d.readPreparedPackage(ctx, PreparedInputInboxEntry{GroupID: f.plan.Owner.GroupID, Month: f.plan.Funding.Month,
		Directory: filepath.Base(request.OutputRoot), ManifestSHA256: receipt.ManifestSHA256}, syntheticScopeID)
	if err != nil {
		t.Fatal("runtime inbox cannot read product package", err)
	}
	parsed, err := monitor.DecodeMonthlyPreparationInput(inputRaw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepareGenerationInputOnDeployment(ctx, d, plan, planRaw, parsed, inputRaw, "", false); err != nil {
		t.Fatal("product package cannot pass shared receiver preflight", err)
	}
	if !reflect.DeepEqual(parsed.Accounts, f.input.Accounts) {
		t.Fatal("packager changed original account money/history")
	}
	manifest.PreparationKind = "unknown_future_kind"
	unknown, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(request.OutputRoot, "manifest.json"), unknown, 0600); err != nil {
		t.Fatal(err)
	}
	_, _, _, err = d.readPreparedPackage(ctx, PreparedInputInboxEntry{GroupID: f.plan.Owner.GroupID, Month: f.plan.Funding.Month,
		Directory: filepath.Base(request.OutputRoot), ManifestSHA256: generationHash(unknown)}, syntheticScopeID)
	if !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("unknown package kind was silently accepted", err)
	}
}

func automaticPreparationOutputEmptyMustCheck(path string) error {
	entries, err := os.ReadDir(path)
	if err == nil && len(entries) != 0 {
		err = ErrGenerationIntegrity
	}
	return err
}

func TestRetainedPreparationFilesRefusesIncompleteOrChangedInput(t *testing.T) {
	for _, kind := range []string{"missing_scope", "unknown_plan_field", "wrong_group", "wrong_head", "changed_money", "scope_tamper", "outside", "nonempty", "public", "symlink", "disabled", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			f, cfg, r := retainedPreparationFileFixture(t)
			ctx := t.Context()
			r.Write = true
			switch kind {
			case "missing_scope":
				f.plan.AutomaticScopeSourceID = ""
				r.Plan = mustGenerationPlan(t, f)
			case "unknown_plan_field":
				r.Plan = append(bytes.TrimSuffix(r.Plan, []byte("}")), []byte(`,"unknown":1}`)...)
			case "wrong_group":
				cfg.Rollout.Targets[0].GroupID++
			case "wrong_head":
				r.ExpectedHead = generationHash([]byte("not the current head"))
			case "changed_money":
				f.input.Accounts[0].Spans[0].ConsumeQuota++
				usageInputUpdate(t, &f)
				r.Preparation, r.Plan = f.raw, mustGenerationPlan(t, f)
			case "scope_tamper":
				if err := os.WriteFile(f.input.Sources[len(f.input.Sources)-1].Path, []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "outside":
				r.OutputRoot = t.TempDir()
			case "nonempty":
				if err := os.WriteFile(filepath.Join(r.OutputRoot, "keep"), []byte("keep"), 0600); err != nil {
					t.Fatal(err)
				}
			case "public":
				if err := os.Chmod(r.OutputRoot, 0755); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				alias := filepath.Join(f.root, "alias")
				if err := os.Symlink(r.OutputRoot, alias); err != nil {
					t.Fatal(err)
				}
				r.OutputRoot = alias
			case "disabled":
				cfg.Rollout.Enabled = false
			case "canceled":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			}
			out, err := PrepareRetainedGenerationFiles(ctx, cfg, r)
			if err == nil || out.Complete {
				t.Fatal("unsafe input was sealed", kind)
			}
			if _, err := os.Stat(filepath.Join(r.OutputRoot, "manifest.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid input obtained completion marker")
			}
			preparationCounts(t, f, 0, 0)
		})
	}
}

func TestRetainedPreparationFilesInterruptedNeverComplete(t *testing.T) {
	for _, kind := range []string{"disk", "corrupt", "previous", "permission", "membership", "source", "cancel", "directory", "marker"} {
		t.Run(kind, func(t *testing.T) {
			f, cfg, r := retainedPreparationFileFixture(t)
			r.Write = true
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			auth := generationDB(t, cfg.AuthorityPath, false)
			writer := func(ctx context.Context, root *os.Root, name string, raw []byte) error {
				if kind == "disk" && name == "input.json" {
					return errors.New("synthetic disk error")
				}
				if kind == "corrupt" && name == "scope.json" {
					return writeArchiveFile(ctx, root, name, []byte("corrupt"))
				}
				if err := writeArchiveFile(ctx, root, name, raw); err != nil {
					return err
				}
				if name == "manifest.pending" {
					switch kind {
					case "previous":
						return os.WriteFile(filepath.Join(r.OutputRoot, "plan.json"), []byte("corrupt previous file"), 0600)
					case "permission":
						_, err := auth.Exec(`UPDATE customer_groups SET portal_auth_ver=2 WHERE id=1`)
						return err
					case "membership":
						_, err := auth.Exec(`UPDATE tracked_users SET group_id=2 WHERE user_id=201`)
						return err
					case "source":
						return os.WriteFile(f.input.Sources[0].Path, []byte("changed"), 0600)
					case "cancel":
						cancel()
					case "directory":
						if err := os.Rename(r.OutputRoot, r.OutputRoot+"-moved"); err != nil {
							return err
						}
						return os.Mkdir(r.OutputRoot, 0700)
					case "marker":
						return writeArchiveFile(ctx, root, "manifest.json", []byte("foreign marker"))
					}
				}
				return nil
			}
			out, err := prepareRetainedGenerationFiles(ctx, cfg, r, writer)
			if err == nil || out.Complete || out.Directory != r.OutputRoot || out.Mode != "incomplete_private_input_do_not_select" {
				t.Fatal("interrupted output reported success", out, err)
			}
			marker := filepath.Join(r.OutputRoot, "manifest.json")
			if kind == "marker" {
				if raw, err := os.ReadFile(marker); err != nil || string(raw) != "foreign marker" {
					t.Fatal("foreign marker was overwritten")
				}
			} else if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("incomplete output gained completion marker")
			}
			preparationCounts(t, f, 0, 0)
		})
	}
}

// Runs as a subtest of the EXISTING CI command-line gate, so this path cannot
// silently become an env-gated test that never executes in CI.
func testRetainedPreparationCLI(t *testing.T, binary string) {
	t.Helper()
	f, cfg, r := retainedPreparationFileFixture(t)
	inputs := t.TempDir()
	for name, raw := range map[string][]byte{"plan.json": r.Plan, "input.json": r.Preparation} {
		if err := os.WriteFile(filepath.Join(inputs, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	args := []string{"prepare-retained", "-receiver", cfg.ReceiverPath, "-funding", cfg.FundingPath, "-authority", cfg.AuthorityPath,
		"-evidence-root", cfg.EvidenceRoot, "-group-id=1", "-month=2026-09", "-expected-head=", "-output-root", r.OutputRoot,
		"-plan", filepath.Join(inputs, "plan.json"), "-preparation", filepath.Join(inputs, "input.json")}
	for _, write := range []bool{false, true} {
		call := append([]string{}, args...)
		if write {
			call = append(call, "-write")
		}
		raw, err := exec.CommandContext(t.Context(), binary, call...).CombinedOutput()
		if err != nil {
			t.Fatalf("retained CLI failed: %v %s", err, raw)
		}
		var receipt AutomaticPreparationReceipt
		if json.Unmarshal(raw, &receipt) != nil || receipt.Complete != write {
			t.Fatal("retained CLI receipt differs")
		}
		preparationCounts(t, f, 0, 0)
	}
	stageArgs := []string{"-receiver", cfg.ReceiverPath, "-funding", cfg.FundingPath, "-authority", cfg.AuthorityPath,
		"-evidence-root", cfg.EvidenceRoot, "-group-id=1", "-month=2026-09", "-expected-head=",
		"-plan", filepath.Join(r.OutputRoot, "plan.json"), "-preparation", filepath.Join(r.OutputRoot, "input.json")}
	if raw, err := exec.CommandContext(t.Context(), binary, stageArgs...).CombinedOutput(); err != nil {
		t.Fatalf("retained package rejected by stage CLI: %v %s", err, raw)
	}
	preparationCounts(t, f, 0, 0)
}
