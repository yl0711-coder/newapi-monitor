package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

const syntheticScopeID = "automatic_scope"

func addRetainedScopeFixture(t *testing.T, f *generationFixture, change func(*RetainedAutomaticScope)) string {
	t.Helper()
	doc := RetainedAutomaticScope{Version: 1, Owner: f.plan.Owner, Month: f.plan.Funding.Month,
		SourceIdentity: f.plan.Funding.SourceIdentity, Scope: automaticScopeFixture(t, *f)}
	if change != nil {
		change(&doc)
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.root, "automatic-scope.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	f.input.Sources = append(f.input.Sources, monitor.MonthlyPreparationSource{ID: syntheticScopeID, Path: path, SHA256: generationHash(raw)})
	f.plan.AutomaticScopeSourceID = syntheticScopeID
	usageInputUpdate(t, f)
	return path
}

func TestRetainedAutomaticScopeExactReadOnlyReloadAndContext(t *testing.T) {
	f := generationFixtureNew(t)
	want := automaticScopeFixture(t, f)
	path := addRetainedScopeFixture(t, &f, nil)
	job := generationSubmit(t, f)
	load, err := NewRetainedAutomaticScopeLoader(f.g, syntheticScopeID)
	if err != nil {
		t.Fatal(err)
	}
	before := map[string][]byte{}
	for _, p := range []string{path, f.path, f.receiverPath} {
		before[p], err = os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
	}
	out, err := load(context.Background(), job.Request)
	if err != nil || !reflect.DeepEqual(out, want) {
		t.Fatal("pinned historical scope mismatch", err)
	}
	out.Group.Members[0].Username = "caller-mutated"
	again, err := load(context.Background(), job.Request)
	if err != nil || !reflect.DeepEqual(again, want) {
		t.Fatal("caller mutation persisted", err)
	}
	metadata, err := NewObservedAutomaticContext(f.g, load)
	if err != nil {
		t.Fatal(err)
	}
	got, err := metadata(context.Background(), job.Request)
	if err != nil || len(got.BundleContext) == 0 {
		t.Fatal("retained scope did not pass original financial validators", err)
	}
	wrong := want
	wrong.Group.Name = "another configured scope"
	bypass, err := NewObservedAutomaticContext(f.g, func(context.Context, monthlytasks.Request) (AutomaticScope, error) { return wrong, nil })
	if err != nil {
		t.Fatal(err)
	}
	if output, err := bypass(context.Background(), job.Request); !errors.Is(err, ErrGenerationEvidence) || len(output.BundleContext) != 0 {
		t.Fatal("callback bypassed pinned scope selection", err)
	}
	for p, raw := range before {
		after, err := os.ReadFile(p)
		if err != nil || !bytes.Equal(raw, after) {
			t.Fatal("scope loader wrote private source/receiver", err)
		}
	}
	// The loader is file-backed, not a process-local success cache.
	if err := os.WriteFile(path, []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if scope, err := load(context.Background(), job.Request); err == nil || len(scope.Accounts) != 0 {
		t.Fatal("changed source reused cached scope")
	}
}

func TestRetainedAutomaticScopeSelectionParticipatesInTaskIdentity(t *testing.T) {
	f := generationFixtureNew(t)
	addRetainedScopeFixture(t, &f, nil)
	// Both files may exist in a manifest, but exactly one is selected by plan.
	second := f.input.Sources[len(f.input.Sources)-1]
	second.ID = "other_scope"
	f.input.Sources = append(f.input.Sources, second)
	usageInputUpdate(t, &f)
	first := generationSubmit(t, f)
	// Production admits only one active task per organization. Complete this
	// private draft before exercising a distinct selection; do not relax that gate.
	if _, err := f.g.Run(context.Background(), first.Request.Owner, first.ID); err != nil {
		t.Fatal(err)
	}
	f.plan.AutomaticScopeSourceID = second.ID
	other := generationSubmit(t, f)
	if first.ID == other.ID || first.Request.InputSHA256 == other.Request.InputSHA256 {
		t.Fatal("metadata selection did not change immutable task identity")
	}
	load, err := NewRetainedAutomaticScopeLoader(f.g, syntheticScopeID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := load(context.Background(), other.Request); !errors.Is(err, ErrGenerationEvidence) {
		t.Fatal("startup selector replaced a queued task selection", err)
	}
	if _, err := load(context.Background(), first.Request); err != nil {
		t.Fatal("selection change invalidated original retained plan", err)
	}
	for _, id := range []string{"missing_scope", "../bad"} {
		f.plan.AutomaticScopeSourceID = id
		if _, err := f.g.Submit(context.Background(), f.plan, f.raw); !errors.Is(err, ErrGenerationInput) {
			t.Fatal("unbound plan selection accepted", err)
		}
	}
}

func TestRetainedAutomaticScopeRejectsWrongBindingAndMetadata(t *testing.T) {
	for name, mutate := range map[string]func(*RetainedAutomaticScope){
		"version":             func(d *RetainedAutomaticScope) { d.Version++ },
		"organization":        func(d *RetainedAutomaticScope) { d.Owner.GroupID++ },
		"credential revision": func(d *RetainedAutomaticScope) { d.Owner.AuthVersion++ },
		"membership":          func(d *RetainedAutomaticScope) { d.Owner.MemberManifest = strings.Repeat("c", 64) },
		"month":               func(d *RetainedAutomaticScope) { d.Month = "2026-08" },
		"origin":              func(d *RetainedAutomaticScope) { d.SourceIdentity = strings.Repeat("c", 64) },
		"self directory":      func(d *RetainedAutomaticScope) { d.Scope.Group.Source = syntheticScopeID + "#directory" },
		"self wallet":         func(d *RetainedAutomaticScope) { d.Scope.Accounts[0].WalletSource = syntheticScopeID },
		"self registration": func(d *RetainedAutomaticScope) {
			d.Scope.Accounts[0].Registration = &monitor.MonthlyBundleRegistration{Source: syntheticScopeID + "#registration"}
		},
		"self reconstruction": func(d *RetainedAutomaticScope) {
			d.Scope.Accounts[0].Reconstructions = []monitor.MonthlySnapshotReconstruction{{ScopeSource: syntheticScopeID + "#scope"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := generationFixtureNew(t)
			addRetainedScopeFixture(t, &f, mutate)
			job := generationSubmit(t, f)
			load, err := NewRetainedAutomaticScopeLoader(f.g, syntheticScopeID)
			if err != nil {
				t.Fatal(err)
			}
			if scope, err := load(context.Background(), job.Request); err == nil || len(scope.Accounts) != 0 {
				t.Fatal("invalid retained scope returned partial financial metadata")
			}
		})
	}
}

func TestRetainedAutomaticScopeMissingUnsafeAndAmbiguousSources(t *testing.T) {
	for _, scenario := range []string{"missing", "unknown", "duplicate", "case alias", "trailing", "null", "public", "symlink", "hardlink", "oversize"} {
		t.Run(scenario, func(t *testing.T) {
			f := generationFixtureNew(t)
			path := addRetainedScopeFixture(t, &f, nil)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "unknown":
				raw = append([]byte(`{"unknown":1,`), raw[1:]...)
			case "duplicate":
				raw = append([]byte(`{"version":1,`), raw[1:]...)
			case "case alias":
				raw = bytes.Replace(raw, []byte(`"version"`), []byte(`"Version"`), 1)
			case "trailing":
				raw = append(raw, []byte(`{}`)...)
			case "null":
				raw = []byte(`null`)
			case "oversize":
				raw = bytes.Repeat([]byte(" "), monitor.MonthlyPreparationInputLimit+1)
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			// Re-pin malformed bytes so this tests strict parsing, not only hash.
			f.input.Sources[len(f.input.Sources)-1].SHA256 = generationHash(raw)
			usageInputUpdate(t, &f)
			job := generationSubmit(t, f)
			switch scenario {
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "public":
				if err := os.Chmod(path, 0644); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				other := filepath.Join(t.TempDir(), "outside.json")
				if err := os.Rename(path, other); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(other, path); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(path, filepath.Join(t.TempDir(), "alias.json")); err != nil {
					t.Fatal(err)
				}
			}
			load, err := NewRetainedAutomaticScopeLoader(f.g, syntheticScopeID)
			if err != nil {
				t.Fatal(err)
			}
			if scope, err := load(context.Background(), job.Request); err == nil || len(scope.Accounts) != 0 {
				t.Fatal("unsafe source returned metadata")
			}
		})
	}
}

func TestRetainedAutomaticScopeCancellationBusyAndMissingSelection(t *testing.T) {
	f := generationFixtureNew(t)
	addRetainedScopeFixture(t, &f, nil)
	job := generationSubmit(t, f)
	for _, id := range []string{"", "../scope", "scope#proof", "space id", strings.Repeat("a", 129)} {
		if load, err := NewRetainedAutomaticScopeLoader(f.g, id); err == nil || load != nil {
			t.Fatal("unsafe source selector accepted")
		}
	}
	if _, err := NewRetainedAutomaticScopeLoader(nil, syntheticScopeID); err == nil {
		t.Fatal("nil generation accepted")
	}
	load, err := NewRetainedAutomaticScopeLoader(f.g, syntheticScopeID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := load(nil, job.Request); err == nil {
		t.Fatal("nil context accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := load(canceled, job.Request); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	retainedScopeSlots <- struct{}{}
	_, err = load(context.Background(), job.Request)
	<-retainedScopeSlots
	if !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("busy source read queued", err)
	}
	missing, err := NewRetainedAutomaticScopeLoader(f.g, "not_in_manifest")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := missing(context.Background(), job.Request); !errors.Is(err, ErrGenerationEvidence) {
		t.Fatal(err)
	}
	if _, err := load(context.Background(), job.Request); err != nil {
		t.Fatal("canceled/busy read leaked capacity", err)
	}
	original := f.g.verifyCurrent
	f.g.verifyCurrent = func(ctx context.Context, _ monthlytasks.Request) error { <-ctx.Done(); return ctx.Err() }
	timed, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	_, err = load(timed, job.Request)
	stop()
	f.g.verifyCurrent = original
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("source authorization ignored caller deadline", err)
	}
	if _, err := load(context.Background(), job.Request); err != nil {
		t.Fatal("timeout leaked capacity", err)
	}
}

func TestRetainedAutomaticDeploymentExplicitEnableAndDependencies(t *testing.T) {
	ctx := context.Background()
	if d, err := OpenRetainedAutomaticDeployment(ctx, DeploymentConfig{ReceiverPath: "/must-not-open"}, "../bad", nil); err != nil || d != nil {
		t.Fatal("disabled entry inspected files or dependencies", err)
	}
	cfg := DeploymentConfig{Rollout: GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: "2026-09"}}}, ReceiverPath: "/must-not-open"}
	pub := func(context.Context, monthlytasks.Request, []byte) error {
		t.Fatal("constructor published")
		return nil
	}
	for _, id := range []string{"", "../scope", "scope#proof"} {
		if d, err := OpenRetainedAutomaticDeployment(ctx, cfg, id, pub); !errors.Is(err, ErrGenerationInput) || d != nil {
			t.Fatal("invalid scope source accepted", err)
		}
	}
	if d, err := OpenRetainedAutomaticDeployment(ctx, cfg, syntheticScopeID, nil); !errors.Is(err, ErrGenerationInput) || d != nil {
		t.Fatal("missing publisher accepted", err)
	}
}

func TestRetainedAutomaticDeploymentPublishesAndRestarts(t *testing.T) {
	f, cfg, auth := automaticDeploymentFixture(t)
	ctx := context.Background()
	previous, _, err := readGenerationSelection(ctx, f.receiver, 1, "2026-09")
	if err != nil {
		t.Fatal(err)
	}
	path := addRetainedScopeFixture(t, &f, nil)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	stageFixture(t, f, previous.Head)
	open := func() *Deployment {
		t.Helper()
		d, err := OpenRetainedAutomaticDeployment(ctx, cfg, syntheticScopeID, automaticDeploymentPublisher(auth))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { closeDeploymentTest(t, d) })
		return d
	}
	d := open()
	d.runtime.worker.pollMin, d.runtime.worker.pollMax = time.Millisecond, 10*time.Millisecond
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SubmitMonthly(ctx, 1, 1, "2026-09"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for d.runtime.worker.Status().Published != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	closeDeploymentTest(t, d)
	if d.runtime.worker.Status().Published != 1 {
		t.Fatal("retained-source publication did not finish", d.runtime.worker.Status())
	}
	var saved monitor.PortalMonthlyBill
	if err := auth.First(&saved, "group_id=1 AND month='2026-09'").Error; err != nil {
		t.Fatal(err)
	}
	reopened := open()
	if worked, err := reopened.runtime.worker.step(ctx); err != nil || worked {
		t.Fatal("restart reissued publication", worked, err)
	}
	closeDeploymentTest(t, reopened)
	var again monitor.PortalMonthlyBill
	if err := auth.First(&again, "group_id=1 AND month='2026-09'").Error; err != nil || !reflect.DeepEqual(saved, again) {
		t.Fatal("restart changed frozen bill", err)
	}
	if after, err := os.ReadFile(path); err != nil || !bytes.Equal(before, after) {
		t.Fatal("deployment changed scope source", err)
	}
}
