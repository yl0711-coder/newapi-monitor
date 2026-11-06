package monthlydraft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func normalizedScopePreparationFixture(t *testing.T) generationFixture {
	t.Helper()
	f := observedPreparationFixture(t, "")
	out, err := f.g.PrepareObservedInput(context.Background(), f.plan, f.raw, "observed_wallet")
	if err != nil {
		t.Fatal(err)
	}
	useWalletPreparation(t, &f, WalletPreparation{Plan: out.Plan, Preparation: out.Preparation})
	return f
}

func TestRetainedScopePreparationReadOnlyDeterministicAndGeneration(t *testing.T) {
	f := normalizedScopePreparationFixture(t)
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
	scope := automaticScopeFixture(t, f)
	path := filepath.Join(f.root, "prepared-scope.json")
	original := bytes.Clone(f.raw)
	before := map[string]string{}
	for _, p := range []string{f.path, f.receiverPath, f.input.Sources[0].Path, authorityPath} {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		before[p] = generationHash(raw)
	}
	scopeBefore, err := json.Marshal(scope)
	if err != nil {
		t.Fatal(err)
	}
	out, err := f.g.PrepareRetainedScopeInput(context.Background(), f.plan, f.raw, syntheticScopeID, path, scope)
	if err != nil {
		t.Fatal(err)
	}
	again, err := f.g.PrepareRetainedScopeInput(context.Background(), f.plan, f.raw, syntheticScopeID, path, scope)
	if err != nil || !reflect.DeepEqual(out, again) {
		t.Fatal("repeated preparation changed identity", err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("preparation wrote output", err)
	}
	if after, _ := json.Marshal(scope); !bytes.Equal(after, scopeBefore) || !bytes.Equal(f.raw, original) {
		t.Fatal("caller input modified")
	}
	for p, digest := range before {
		raw, err := os.ReadFile(p)
		if err != nil || generationHash(raw) != digest {
			t.Fatal("preparation changed original database/evidence", err)
		}
	}
	preparationCounts(t, f, 0, 0)
	var doc RetainedAutomaticScope
	if json.Unmarshal(out.Scope, &doc) != nil || doc.Version != 1 || doc.Owner != f.plan.Owner || doc.Month != f.plan.Funding.Month ||
		doc.SourceIdentity != f.plan.Funding.SourceIdentity || !reflect.DeepEqual(doc.Scope, scope) {
		t.Fatal("new scope not bound to exact retained identity")
	}
	prepared, err := monitor.DecodeMonthlyPreparationInput(out.Preparation)
	if err != nil || len(prepared.Sources) != len(f.input.Sources)+1 || !reflect.DeepEqual(prepared.Accounts, f.input.Accounts) {
		t.Fatal("packaging changed money/history", err)
	}
	last := prepared.Sources[len(prepared.Sources)-1]
	if last.ID != syntheticScopeID || last.Path != path || last.SHA256 != generationHash(out.Scope) {
		t.Fatal("new source not hash bound")
	}
	// Only the explicit synthetic caller writes new files. Admission may retain
	// a task, but source verification/loading must refuse the missing file.
	useWalletPreparation(t, &f, WalletPreparation{Plan: out.Plan, Preparation: out.Preparation})
	if err := f.g.verifyFiles(context.Background(), f.input.Sources); err == nil {
		t.Fatal("missing scope treated as a retained file")
	}
	if _, err := PrepareGenerationInput(context.Background(), cfg, out.Plan, out.Preparation, "", false); err == nil {
		t.Fatal("formal inspector accepted absent scope")
	}
	if err := os.WriteFile(path, out.Scope, 0600); err != nil {
		t.Fatal(err)
	}
	checked, err := PrepareGenerationInput(context.Background(), cfg, out.Plan, out.Preparation, "", false)
	if err != nil || checked.Applied || checked.Changed || checked.PlanSHA256 != generationHash(out.Plan) {
		t.Fatal("formal inspector did not accept exact prepared output", err)
	}
	preparationCounts(t, f, 0, 0)
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(context.Background(), job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, artifact, 1)
	load, err := NewRetainedAutomaticScopeLoader(f.g, syntheticScopeID)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := load(context.Background(), job.Request)
	if err != nil || !reflect.DeepEqual(loaded, scope) {
		t.Fatal("formal output not accepted by fixed-source loader", err)
	}
}

func TestRetainedScopePreparationRejectsWithoutPartialOutput(t *testing.T) {
	for _, scenario := range []string{"unprepared", "existing_selection", "existing_id", "existing_path", "outside", "relative", "unclean", "existing_file", "symlink", "parent_symlink", "public_parent", "missing_parent", "wrong_group", "wrong_month", "wrong_member", "unknown_wallet", "wallet_observation_not_scope", "self_reference", "bad_correction", "changed_source", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			f := normalizedScopePreparationFixture(t)
			scope := automaticScopeFixture(t, f)
			path := filepath.Join(f.root, "new-scope.json")
			id := syntheticScopeID
			ctx := context.Background()
			switch scenario {
			case "unprepared":
				f.input.Accounts[0].Spans = nil
				usageInputUpdate(t, &f)
			case "existing_selection":
				f.plan.AutomaticScopeSourceID = f.input.Sources[0].ID
			case "existing_id":
				id = f.input.Sources[0].ID
			case "existing_path":
				path = f.input.Sources[0].Path
			case "outside":
				path = filepath.Join(t.TempDir(), "scope.json")
			case "relative":
				path = "scope.json"
			case "unclean":
				path = f.root + "/./scope.json"
			case "existing_file":
				if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink("absent", path); err != nil {
					t.Fatal(err)
				}
			case "parent_symlink":
				if err := os.Symlink(".", filepath.Join(f.root, "alias")); err != nil {
					t.Fatal(err)
				}
				path = filepath.Join(f.root, "alias", "scope.json")
			case "public_parent":
				parent := filepath.Join(f.root, "public")
				if err := os.Mkdir(parent, 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(parent, 0755); err != nil {
					t.Fatal(err)
				}
				path = filepath.Join(parent, "scope.json")
			case "missing_parent":
				path = filepath.Join(f.root, "absent", "scope.json")
			case "wrong_group":
				scope.Group.GroupID++
			case "wrong_month":
				scope.Group.Months = []string{"2020-01"}
			case "wrong_member":
				scope.Group.Members[0].UserID++
			case "unknown_wallet":
				scope.Accounts[0].WalletOnly = false
			case "wallet_observation_not_scope":
				scope.Accounts[0].WalletSource = "observed_wallet#wallet:capture"
			case "self_reference":
				scope.Group.Source = id + "#directory"
			case "bad_correction":
				scope.Corrections = []AutomaticAccountCorrection{{UserID: scope.Accounts[0].UserID}}
			case "changed_source":
				f.input.Sources[0].SHA256 = strings.Repeat("a", 64)
				usageInputUpdate(t, &f)
			case "canceled":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			out, err := f.g.PrepareRetainedScopeInput(ctx, f.plan, f.raw, id, path, scope)
			if err == nil || !reflect.DeepEqual(out, RetainedScopePreparation{}) {
				t.Fatal("invalid preparation returned partial output", err)
			}
			preparationCounts(t, f, 0, 0)
			if scenario == "existing_file" {
				if raw, err := os.ReadFile(path); err != nil || string(raw) != "keep" {
					t.Fatal("existing output overwritten", err)
				}
			}
		})
	}
}

func TestRetainedScopePreparationBusyReadOnlyAndRecovery(t *testing.T) {
	f := normalizedScopePreparationFixture(t)
	f.g.readOnly = true
	scope := automaticScopeFixture(t, f)
	path := filepath.Join(f.root, "scope.json")
	retainedScopePreparationSlots <- struct{}{}
	out, err := f.g.PrepareRetainedScopeInput(context.Background(), f.plan, f.raw, syntheticScopeID, path, scope)
	<-retainedScopePreparationSlots
	if !errors.Is(err, ErrGenerationBusy) || !reflect.DeepEqual(out, RetainedScopePreparation{}) {
		t.Fatal("busy preparation was not bounded", err)
	}
	if out, err := f.g.PrepareRetainedScopeInput(context.Background(), f.plan, f.raw, syntheticScopeID, path, scope); err != nil || len(out.Scope) == 0 {
		t.Fatal("read-only recovery failed", err)
	}
	for _, id := range []string{"", "../scope", " scope", "scope#ref", strings.Repeat("x", 129)} {
		if out, err := f.g.PrepareRetainedScopeInput(context.Background(), f.plan, f.raw, id, path, scope); err == nil || !reflect.DeepEqual(out, RetainedScopePreparation{}) {
			t.Fatal("invalid ID accepted", id, err)
		}
	}
	if err := f.g.Close(); err != nil {
		t.Fatal(err)
	}
	if out, err := f.g.PrepareRetainedScopeInput(context.Background(), f.plan, f.raw, syntheticScopeID, path, scope); err == nil || !reflect.DeepEqual(out, RetainedScopePreparation{}) {
		t.Fatal("closed generation accepted", err)
	}
}

func TestRetainedScopePreparationReservesNewEvidenceBudget(t *testing.T) {
	f := normalizedScopePreparationFixture(t)
	var size int64
	for _, source := range f.input.Sources {
		info, err := os.Stat(source.Path)
		if err != nil {
			t.Fatal(err)
		}
		size += info.Size()
	}
	path := filepath.Join(f.root, "budget-padding")
	padding, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = padding.Close() })
	if err := padding.Truncate(generationEvidenceLimit - size); err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	if _, err := io.Copy(h, padding); err != nil {
		t.Fatal(err)
	}
	if err := padding.Close(); err != nil {
		t.Fatal(err)
	}
	f.input.Sources = append(f.input.Sources, monitor.MonthlyPreparationSource{ID: "budget_padding", Path: path, SHA256: hex.EncodeToString(h.Sum(nil))})
	usageInputUpdate(t, &f)
	// Existing sources alone exactly fit. The new, unwritten scope must still
	// count against the SAME total budget, rather than raising the limit.
	out, err := f.g.PrepareRetainedScopeInput(context.Background(), f.plan, f.raw, syntheticScopeID,
		filepath.Join(f.root, "scope.json"), automaticScopeFixture(t, f))
	if !errors.Is(err, ErrGenerationCapacity) || !reflect.DeepEqual(out, RetainedScopePreparation{}) {
		t.Fatal("new scope escaped total evidence budget", err)
	}
	preparationCounts(t, f, 0, 0)
}
