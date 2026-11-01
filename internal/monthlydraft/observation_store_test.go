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

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func unrelatedObservationDefault(t *testing.T, g *Generation) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "unrelated.sqlite")
	db := generationDB(t, path, false)
	if _, err := db.Exec("CREATE TABLE unrelated(value INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if err := monthlysource.InitFundingPageStore(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	g.funding = generationDB(t, path, true)
	return path
}

func TestObservationStoreLegacyCanonicalAndIdentity(t *testing.T) {
	f := observedPreparationFixture(t, "")
	_, legacy, _, err := canonicalGeneration(f.plan, f.raw, time.Now())
	if err != nil || bytes.Contains(legacy, []byte("observation_store_source_id")) {
		t.Fatal("legacy encoding changed", err)
	}
	// Compare against the old struct encoding with only the optional field
	// removed; the legacy task hash must not change just by upgrading code.
	var p GenerationPlan
	if err := json.Unmarshal(legacy, &p); err != nil {
		t.Fatal(err)
	}
	newPlan := p
	newPlan.ObservationStoreSourceID = "observed_wallet"
	_, selected, _, err := canonicalGeneration(newPlan, f.raw, time.Now())
	if err != nil || generationRequest(p, legacy) == generationRequest(newPlan, selected) {
		t.Fatal("source selection did not change immutable task identity", err)
	}
	restored := bytes.Replace(selected, []byte(`,"observation_store_source_id":"observed_wallet"`), nil, 1)
	if !bytes.Equal(restored, legacy) {
		t.Fatal("adding an empty optional field changed legacy canonical bytes")
	}
	for _, id := range []string{"missing", "../escape", " observed_wallet"} {
		newPlan.ObservationStoreSourceID = id
		if _, _, _, err := canonicalGeneration(newPlan, f.raw, time.Now()); !errors.Is(err, ErrGenerationInput) {
			t.Fatal("invalid source selection accepted", id, err)
		}
	}
	newPlan.ObservationStoreSourceID = "observed"
	if _, _, _, err := canonicalGeneration(newPlan, f.raw, time.Now()); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("wallet and observation DB selection may diverge", err)
	}
}

func TestObservationStoreIndependentInputsNormalizationGenerationReuse(t *testing.T) {
	first, second := observedPreparationFixture(t, ""), observedPreparationFixture(t, "")
	addTokenArchivesFromObserved(t, &first)
	addTokenArchivesFromObserved(t, &second)
	if first.plan.Windows[0].OlderScanID == second.plan.Windows[0].OlderScanID {
		t.Fatal("fixtures must have independent capture IDs")
	}
	// Retain the second input's files under the SAME service root. Neither
	// normalizing it nor generating it may append to or replace the first DB.
	dir := filepath.Join(first.root, "captures ?#")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for i := range second.input.Sources {
		source := &second.input.Sources[i]
		raw, err := os.ReadFile(source.Path)
		if err != nil {
			t.Fatal(err)
		}
		source.Path = filepath.Join(dir, source.ID+".source")
		if err := os.WriteFile(source.Path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	second.g, second.receiver, second.receiverPath = first.g, first.receiver, first.receiverPath
	unrelatedObservationDefault(t, first.g)
	defaultDB := first.g.funding
	var jobs []monthlytasks.Task
	var artifacts []GenerationArtifact
	for i, f := range []*generationFixture{&first, &second} {
		f.plan.ObservationStoreSourceID = "observed_wallet"
		usageInputUpdate(t, f)
		result, err := f.g.PrepareObservedInput(context.Background(), f.plan, f.raw, "observed_wallet")
		if err != nil {
			t.Fatalf("input %d normalized against wrong DB: %v", i, err)
		}
		if !generationHex(result.TokenSHA256, 32) {
			t.Fatal("token sources were not audited against the selected usage DB")
		}
		useWalletPreparation(t, f, WalletPreparation{Plan: result.Plan, Preparation: result.Preparation})
		job := generationSubmit(t, *f)
		artifact, err := f.g.Run(context.Background(), job.Request.Owner, job.ID)
		if err != nil {
			t.Fatalf("input %d generation: %v", i, err)
		}
		assertGenerationAmounts(t, artifact, 1)
		load, err := NewObservedAutomaticContext(f.g, func(context.Context, monthlytasks.Request) (AutomaticScope, error) {
			return automaticScopeFixture(t, *f), nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := load(context.Background(), job.Request); err != nil {
			t.Fatal("automatic handoff used default DB", err)
		}
		jobs, artifacts = append(jobs, job), append(artifacts, artifact)
	}
	for i, job := range jobs {
		cached, err := first.g.Run(context.Background(), job.Request.Owner, job.ID)
		if err != nil || !reflect.DeepEqual(cached, artifacts[i]) {
			t.Fatal("another input invalidated or changed cached bill", err)
		}
	}
	if first.g.funding != defaultDB || first.g.observationSource != nil {
		t.Fatal("shared Generation was mutated")
	}
	for _, f := range []generationFixture{first, second} {
		if err := f.g.verifyFiles(context.Background(), f.input.Sources); err != nil {
			t.Fatal("source evidence changed during normalization/generation", err)
		}
	}
}

func TestObservationStoreRejectsFileReplacementAfterBinding(t *testing.T) {
	f := observedPreparationFixture(t, "")
	f.plan.ObservationStoreSourceID = "observed_wallet"
	g, release, err := f.g.bindObservationStore(context.Background(), f.plan, f.input)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	raw, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	replacement := filepath.Join(f.root, "replacement.sqlite")
	if err := os.WriteFile(replacement, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, f.path); err != nil {
		t.Fatal(err)
	}
	if err := g.verifyFiles(context.Background(), f.input.Sources); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("even byte-identical inode replacement must invalidate an open binding", err)
	}
}

func TestObservationStoreRunFailureStillRecordsTaskFailure(t *testing.T) {
	f := observedPreparationFixture(t, "")
	f.plan.ObservationStoreSourceID = "observed_wallet"
	out, err := f.g.PrepareObservedInput(context.Background(), f.plan, f.raw, "observed_wallet")
	if err != nil {
		t.Fatal(err)
	}
	useWalletPreparation(t, &f, WalletPreparation{Plan: out.Plan, Preparation: out.Preparation})
	job := generationSubmit(t, f)
	if err := os.Chmod(f.path, 0644); err != nil {
		t.Fatal(err)
	}
	artifact, err := f.g.Run(context.Background(), job.Request.Owner, job.ID)
	if !errors.Is(err, ErrGenerationIntegrity) || !reflect.DeepEqual(artifact, GenerationArtifact{}) {
		t.Fatal("damaged source did not fail closed", err)
	}
	failed, err := f.g.tasks.Get(context.Background(), job.Request.Owner, job.ID)
	if err != nil || failed.State != "failed" {
		t.Fatal("source bind failure skipped task cleanup", err)
	}
	if err := f.g.receiver.Ping(); err != nil {
		t.Fatal("failed source binding closed shared receiver", err)
	}
}

func TestObservationStorePreparationRechecksBeforeCompletionMarker(t *testing.T) {
	for _, kind := range []string{"sidecar", "replacement"} {
		t.Run(kind, func(t *testing.T) {
			f, cfg, request := automaticPreparationFileFixture(t)
			f.plan.ObservationStoreSourceID = "observed_wallet"
			request.Plan, request.Write = mustGenerationPlan(t, f), true
			cfg.FundingPath = unrelatedObservationDefault(t, f.g)
			write := func(ctx context.Context, root *os.Root, name string, raw []byte) error {
				if err := writeArchiveFile(ctx, root, name, raw); err != nil {
					return err
				}
				if name != "scope.json" {
					return nil
				}
				if kind == "sidecar" {
					return os.WriteFile(f.path+"-wal", nil, 0600)
				}
				body, err := os.ReadFile(f.path)
				if err != nil {
					return err
				}
				replacement := filepath.Join(f.root, "replacement.sqlite")
				if err := os.WriteFile(replacement, body, 0600); err != nil {
					return err
				}
				return os.Rename(replacement, f.path)
			}
			receipt, err := prepareAutomaticGenerationFiles(context.Background(), cfg, request, write)
			if !errors.Is(err, ErrGenerationIntegrity) || receipt.Complete {
				t.Fatal("changed source obtained a completed package", err)
			}
			if _, err := os.Lstat(filepath.Join(request.OutputRoot, "manifest.json")); !os.IsNotExist(err) {
				t.Fatal("completion marker was published for changed source", err)
			}
			preparationCounts(t, f, 0, 0)
		})
	}
}

func TestObservationStoreAutomaticPreparationAndRunningAdmission(t *testing.T) {
	f, cfg, request := automaticPreparationFileFixture(t)
	f.plan.ObservationStoreSourceID = "observed_wallet"
	request.Plan = mustGenerationPlan(t, f)
	cfg.FundingPath = unrelatedObservationDefault(t, f.g)
	request.Write = true
	receipt, err := PrepareAutomaticGenerationFiles(context.Background(), cfg, request)
	if err != nil || !receipt.Complete {
		t.Fatal("producer used unrelated deployment default", err)
	}
	plan, err := os.ReadFile(filepath.Join(request.OutputRoot, "plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	input, err := os.ReadFile(filepath.Join(request.OutputRoot, "input.json"))
	if err != nil {
		t.Fatal(err)
	}
	preparationCounts(t, f, 0, 0)
	preview, err := PrepareGenerationInput(context.Background(), cfg, plan, input, "", false)
	if err != nil || preview.Applied {
		t.Fatal("read-only selection inspection used default DB", err)
	}
	preparationCounts(t, f, 0, 0)
	d, err := OpenRetainedAutomaticDeployment(context.Background(), cfg, syntheticScopeID,
		func(context.Context, monthlytasks.Request, []byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDeploymentTest(t, d) })
	if err := d.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	selected, err := d.PrepareMonthlyInput(context.Background(), plan, input, "", true)
	if err != nil || !selected.Applied || !selected.Changed {
		t.Fatal("running selection used default DB", err)
	}
	preparationCounts(t, f, 1, 1)
	again, err := d.PrepareMonthlyInput(context.Background(), plan, input, selected.SelectionHead, true)
	if err != nil || again.Changed {
		t.Fatal("same immutable input not idempotent", err)
	}
	preparationCounts(t, f, 1, 1)
}

func TestObservationStoreRejectsUnsafeFilesWithoutWrites(t *testing.T) {
	for _, scenario := range []string{"digest", "missing", "symlink", "hardlink", "public", "wal", "shm", "journal", "receiver", "cancel", "source_argument", "parent_symlink", "parent_public"} {
		t.Run(scenario, func(t *testing.T) {
			f := observedPreparationFixture(t, "")
			f.plan.ObservationStoreSourceID = "observed_wallet"
			var source *monitor.MonthlyPreparationSource
			for i := range f.input.Sources {
				if f.input.Sources[i].ID == "observed_wallet" {
					source = &f.input.Sources[i]
				}
			}
			ctx, sourceID := context.Background(), "observed_wallet"
			switch scenario {
			case "digest":
				source.SHA256 = strings.Repeat("a", 64)
			case "missing":
				source.Path += ".missing"
			case "symlink":
				path := filepath.Join(f.root, "alias.sqlite")
				if err := os.Symlink(source.Path, path); err != nil {
					t.Fatal(err)
				}
				source.Path = path
			case "hardlink":
				if err := os.Link(source.Path, filepath.Join(f.root, "other-link.sqlite")); err != nil {
					t.Fatal(err)
				}
			case "public":
				if err := os.Chmod(source.Path, 0644); err != nil {
					t.Fatal(err)
				}
			case "wal", "shm", "journal":
				if err := os.WriteFile(source.Path+"-"+scenario, nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "receiver":
				source.Path = f.receiverPath
				raw, err := os.ReadFile(source.Path)
				if err != nil {
					t.Fatal(err)
				}
				source.SHA256 = generationHash(raw)
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "source_argument":
				sourceID = "observed"
			case "parent_symlink", "parent_public":
				dir := filepath.Join(f.root, "parent")
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				raw, err := os.ReadFile(source.Path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "store.sqlite"), raw, 0600); err != nil {
					t.Fatal(err)
				}
				if scenario == "parent_public" {
					if err := os.Chmod(dir, 0755); err != nil {
						t.Fatal(err)
					}
				} else {
					alias := filepath.Join(f.root, "alias")
					if err := os.Symlink(dir, alias); err != nil {
						t.Fatal(err)
					}
					dir = alias
				}
				source.Path = filepath.Join(dir, "store.sqlite")
			}
			usageInputUpdate(t, &f)
			out, err := f.g.PrepareObservedInput(ctx, f.plan, f.raw, sourceID)
			if err == nil || !reflect.DeepEqual(out, ObservedPreparation{}) {
				t.Fatal("unsafe source produced material", err)
			}
			if scenario == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation was hidden", err)
			}
			preparationCounts(t, f, 0, 0)
			if scenario == "missing" {
				if _, err := os.Lstat(source.Path); !os.IsNotExist(err) {
					t.Fatal("missing source was created", err)
				}
			}
		})
	}
}

func TestObservationStoreScopedReadOnlyCleanupAndSidecarRecheck(t *testing.T) {
	f := observedPreparationFixture(t, "")
	f.plan.ObservationStoreSourceID = "observed_wallet"
	g, closeSource, err := f.g.bindObservationStore(context.Background(), f.plan, f.input)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSource()
	if _, err := g.funding.Exec("CREATE TABLE forbidden(value INTEGER)"); err == nil {
		t.Fatal("scoped source is writable")
	}
	if _, err := f.g.auditWindows(context.Background(), f.plan); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("unbound audit silently read the default", err)
	}
	nested, releaseNested, err := g.bindObservationStore(context.Background(), f.plan, f.input)
	if err != nil || nested != g {
		t.Fatal("nested call reopened/changed source", err)
	}
	releaseNested()
	if err := g.funding.Ping(); err != nil {
		t.Fatal("nested cleanup closed outer handle", err)
	}
	other := f.plan
	other.ObservationStoreSourceID = "observed"
	other.WalletObservations = nil
	if _, release, err := g.bindObservationStore(context.Background(), other, f.input); err == nil {
		release()
		t.Fatal("nested call changed database")
	}
	if err := os.WriteFile(f.path+"-wal", nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := g.verifyFiles(context.Background(), f.input.Sources); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("sidecar appearing after open bypassed hash check", err)
	}
	closeSource()
	if err := g.funding.Ping(); err == nil {
		t.Fatal("scoped source not released")
	}
	if err := f.g.funding.Ping(); err != nil {
		t.Fatal("release closed the shared default DB", err)
	}
	if err := f.g.receiver.Ping(); err != nil {
		t.Fatal("release closed receiver", err)
	}
}
