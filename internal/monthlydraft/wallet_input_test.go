package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func addWalletInputFixture(t *testing.T, f *generationFixture, scenario string) {
	t.Helper()
	ctx := context.Background()
	raw, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	// Only copy a synthetic fixture. In particular, don't modify a previously
	// sealed usage source or change an older source descriptor's digest.
	work := filepath.Join(t.TempDir(), "wallet-staging.sqlite")
	if err := os.WriteFile(work, raw, 0600); err != nil {
		t.Fatal(err)
	}
	db := generationDB(t, work, false)
	if err := monthlysource.InitWalletObservationStore(ctx, db); err != nil {
		t.Fatal(err)
	}
	selection := &WalletObservationSelection{SourceID: "observed_wallet"}
	end := f.plan.Windows[len(f.plan.Windows)-1].To
	for pass := int64(0); pass < 2; pass++ {
		started := (end+86400+pass*86400)*1000 + 100
		if scenario == "future" {
			started = time.Now().Add(time.Hour).UnixMilli() + pass*86400000
		}
		if scenario == "before_end" {
			started = (end-3600+pass*60)*1000 + 100
		}
		if scenario == "same_second" {
			started = (end+86400)*1000 + 100 + pass*100
		}
		o := monthlysource.WalletObservation{Version: 1, Basis: "database_observation_not_month_boundary", SourceIdentity: f.plan.Funding.SourceIdentity,
			StartedAtMS: started, CompletedAtMS: started + 1}
		if scenario == "identity" {
			o.SourceIdentity = strings.Repeat("a", 64)
		}
		for _, account := range f.input.Accounts {
			balance, used := account.Snapshots[0].BalanceQuota-pass, account.Snapshots[0].UsedQuota+pass
			if scenario == "precision" {
				balance, used = 9007199254740993+pass, 9007199254740995+pass
			}
			if scenario == "negative" {
				balance = -700000 - pass
			}
			if scenario == "zero" {
				balance, used = 0, 0
			}
			u := monthlysource.WalletUser{UserID: account.UserID, Exists: true, BalanceQuota: &balance, UsedQuota: &used}
			if scenario == "missing" && u.UserID == 202 {
				u = monthlysource.WalletUser{UserID: 202}
			}
			if scenario == "foreign" && u.UserID == 202 {
				u.UserID = 999
			}
			o.Users = append(o.Users, u)
		}
		id, err := monthlysource.SaveWalletObservation(ctx, db, o)
		if err != nil {
			t.Fatal(err)
		}
		selection.IDs = append(selection.IDs, id)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(work)
	if err != nil {
		t.Fatal(err)
	}
	f.path = filepath.Join(f.root, "sealed-wallet.sqlite")
	if err := os.WriteFile(f.path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	f.funding = generationDB(t, f.path, true)
	f.g.funding = f.funding
	f.sourceHash = generationHash(raw)
	f.input.Sources = append(f.input.Sources, monitor.MonthlyPreparationSource{ID: selection.SourceID, Path: f.path, SHA256: f.sourceHash})
	f.plan.WalletObservations = selection
	usageInputUpdate(t, f)
}

func preparedWalletFixture(t *testing.T) (generationFixture, WalletPreparation) {
	t.Helper()
	f := generationFixtureNew(t)
	addWalletInputFixture(t, &f, "")
	result, err := f.g.PrepareWalletObservations(context.Background(), f.plan, f.raw)
	if err != nil {
		t.Fatal(err)
	}
	return f, result
}

func useWalletPreparation(t *testing.T, f *generationFixture, result WalletPreparation) {
	t.Helper()
	var err error
	if err = json.Unmarshal(result.Plan, &f.plan); err != nil {
		t.Fatal(err)
	}
	f.input, err = monitor.DecodeMonthlyPreparationInput(result.Preparation)
	if err != nil {
		t.Fatal(err)
	}
	f.raw = result.Preparation
}

func TestWalletInputMachinePreparationGenerationAndReuse(t *testing.T) {
	f, result := preparedWalletFixture(t)
	ctx := context.Background()
	before := bytes.Clone(f.raw)
	again, err := f.g.PrepareWalletObservations(ctx, f.plan, f.raw)
	if err != nil || !reflect.DeepEqual(again, result) {
		t.Fatal("same template/captures not deterministic", err)
	}
	shuffled := f.plan
	shuffled.WalletObservations = &WalletObservationSelection{SourceID: f.plan.WalletObservations.SourceID, IDs: slices.Clone(f.plan.WalletObservations.IDs)}
	slices.Reverse(shuffled.WalletObservations.IDs)
	again, err = f.g.PrepareWalletObservations(ctx, shuffled, f.raw)
	if err != nil || !reflect.DeepEqual(again, result) || !bytes.Equal(f.raw, before) {
		t.Fatal("capture order changed identity, or caller input mutated", err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(result.Preparation)
	if err != nil {
		t.Fatal(err)
	}
	for i, a := range input.Accounts {
		if len(a.Snapshots) != 3 || a.Snapshots[0] != f.input.Accounts[i].Snapshots[0] {
			t.Fatal("existing independent boundary was replaced")
		}
		for _, b := range a.Snapshots[1:] {
			if b.TimeBasis != "collection_observation" || b.CutoffSource != "" || b.UserID != a.UserID {
				t.Fatal("capture promoted to historical cutoff or mixed account")
			}
		}
		independent := a
		independent.Snapshots = independent.Snapshots[:1]
		if !reflect.DeepEqual(independent, f.input.Accounts[i]) {
			t.Fatal("money, funding, actual refunds or independent evidence changed")
		}
	}
	var count int
	if err := f.receiver.QueryRow("SELECT COUNT(*) FROM portal_monthly_generation_inputs").Scan(&count); err != nil || count != 0 {
		t.Fatal("preparation queued or staged a task", err)
	}
	useWalletPreparation(t, &f, result)
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(ctx, job.Request.Owner, job.ID)
	if err != nil || artifact.WalletSHA256 != result.WalletSHA256 || !generationHex(artifact.WalletSHA256, 32) {
		t.Fatal("wallet observations not audited/pinned", err)
	}
	assertGenerationAmounts(t, artifact)
	f.g.calculate = func(context.Context, monthlysource.FundingMonthRequest, []monthlysource.FundingWindowObservation, []byte, time.Time) (Review, error) {
		t.Fatal("cache reuse recalculated frozen money")
		return Review{}, nil
	}
	if again, err := f.g.Run(ctx, job.Request.Owner, job.ID); err != nil || !reflect.DeepEqual(again, artifact) {
		t.Fatal("audited cache not reused", err)
	}
	load, err := NewObservedAutomaticContext(f.g, func(context.Context, monthlytasks.Request) (AutomaticScope, error) {
		return automaticScopeFixture(t, f), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	meta, err := load(ctx, job.Request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.PrepareReviewHandoff(ctx, job.Request.Owner, job.ID, meta.BundleContext); err != nil {
		t.Fatal("wallet check failed before handoff", err)
	}
	if raw, err := os.ReadFile(f.path); err != nil || generationHash(raw) != f.sourceHash {
		t.Fatal("preparation or generation wrote retained source", err)
	}
	if output, err := f.g.PrepareWalletObservations(ctx, f.plan, f.raw); err == nil || !reflect.DeepEqual(output, WalletPreparation{}) {
		t.Fatal("filled snapshot overwritten", err)
	}
}

func TestWalletInputRejectsChangedValuesAndProofPromotion(t *testing.T) {
	for _, scenario := range []string{"balance", "counter", "time", "user", "cutoff", "refund_proof", "basis_proof", "baseline_proof", "usage_proof", "movement_proof", "coverage_proof", "missing_snapshot", "unselected_claim"} {
		t.Run(scenario, func(t *testing.T) {
			f, result := preparedWalletFixture(t)
			useWalletPreparation(t, &f, result)
			a := &f.input.Accounts[0]
			b := &a.Snapshots[1]
			switch scenario {
			case "balance":
				b.BalanceQuota++
			case "counter":
				b.UsedQuota++
			case "time":
				b.At++
			case "user":
				b.UserID = 202
			case "cutoff":
				b.TimeBasis, b.CutoffSource = "source_log_cutoff", "fixture#not-a-real-cutoff"
			case "refund_proof":
				a.WalletMonths[0].Source = b.Source
			case "basis_proof":
				a.BasisSource = b.Source
			case "baseline_proof":
				a.Baseline.Source = b.Source
			case "usage_proof":
				a.Spans[0].Source = b.Source
			case "movement_proof":
				a.Movements[0].Source = b.Source
			case "coverage_proof":
				a.MovementCoverage[0].Source = b.Source
			case "missing_snapshot":
				a.Snapshots = a.Snapshots[:1]
			case "unselected_claim":
				b.Source = "observed_wallet#wallet:not-selected"
			}
			usageInputUpdate(t, &f)
			job := generationSubmit(t, f)
			f.g.calculate = func(context.Context, monthlysource.FundingMonthRequest, []monthlysource.FundingWindowObservation, []byte, time.Time) (Review, error) {
				t.Fatal("bad wallet input reached money calculation")
				return Review{}, nil
			}
			if artifact, err := f.g.Run(context.Background(), job.Request.Owner, job.ID); !errors.Is(err, ErrGenerationEvidence) || len(artifact.Reconciliation.Accounts) != 0 {
				t.Fatal("invalid wallet input returned partial draft", err)
			}
		})
	}
}

func TestWalletInputSealedSourceAndNoImplicitSetup(t *testing.T) {
	for _, scenario := range []string{"wal", "shm", "journal", "wrong_file", "alias", "no_table", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			f := generationFixtureNew(t)
			addWalletInputFixture(t, &f, "")
			ctx := context.Background()
			switch scenario {
			case "wal", "shm", "journal":
				if err := os.WriteFile(f.path+"-"+scenario, nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "wrong_file":
				f.input.Sources[1].Path = f.input.Sources[0].Path
				f.input.Sources[1].SHA256 = f.input.Sources[0].SHA256
			case "alias":
				alias := filepath.Join(f.root, "alias.sqlite")
				if err := os.Link(f.path, alias); err != nil {
					t.Fatal(err)
				}
				f.input.Sources[1].Path = alias
			case "no_table":
				db := generationDB(t, f.path, false)
				if _, err := db.Exec(`DROP TABLE portal_monthly_wallet_observations`); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				raw, err := os.ReadFile(f.path)
				if err != nil {
					t.Fatal(err)
				}
				f.input.Sources[1].SHA256 = generationHash(raw)
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			usageInputUpdate(t, &f)
			before, err := os.ReadFile(f.path)
			if err != nil {
				t.Fatal(err)
			}
			output, err := f.g.PrepareWalletObservations(ctx, f.plan, f.raw)
			if err == nil || !reflect.DeepEqual(output, WalletPreparation{}) {
				t.Fatal("invalid source produced partial output", err)
			}
			if scenario == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation lost", err)
			}
			if after, err := os.ReadFile(f.path); err != nil || !bytes.Equal(before, after) {
				t.Fatal("failure repaired or wrote source", err)
			}
			if scenario == "no_table" {
				var n int
				if err := f.funding.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name='portal_monthly_wallet_observations'`).Scan(&n); err != nil || n != 0 {
					t.Fatal("read path implicitly initialized storage", err)
				}
			}
		})
	}
}

func TestWalletInputScopeMissingTimesAndPrecision(t *testing.T) {
	for _, scenario := range []string{"identity", "missing", "foreign", "future", "before_end", "same_second", "negative", "precision", "zero"} {
		t.Run(scenario, func(t *testing.T) {
			f := generationFixtureNew(t)
			addWalletInputFixture(t, &f, scenario)
			result, err := f.g.PrepareWalletObservations(context.Background(), f.plan, f.raw)
			valid := scenario == "negative" || scenario == "precision" || scenario == "zero"
			if !valid {
				if err == nil || !reflect.DeepEqual(result, WalletPreparation{}) {
					t.Fatal("scope/time/missing amount produced partial output", err)
				}
				return
			}
			if err != nil {
				t.Fatal("valid signed/zero/exact integer observation rejected", err)
			}
			useWalletPreparation(t, &f, result)
			got := f.input.Accounts[0].Snapshots[1]
			want := int64(0)
			if scenario == "negative" {
				want = -700000
			} else if scenario == "precision" {
				want = 9007199254740993
			}
			if got.BalanceQuota != want {
				t.Fatal("negative, zero or precision changed", got.BalanceQuota)
			}
		})
	}
}

func TestWalletInputLegacyCanonicalSelectionAndArtifact(t *testing.T) {
	f := generationFixtureNew(t)
	legacyPlan, legacy, _, err := canonicalGeneration(f.plan, f.raw, time.Now())
	if err != nil || bytes.Contains(legacy, []byte("wallet_observations")) {
		t.Fatal("legacy plan format changed", err)
	}
	if digest, err := f.g.auditWalletInput(context.Background(), f.plan, f.input); err != nil || digest != "" {
		t.Fatal("legacy plan read uninitialized wallet table", err)
	}
	addWalletInputFixture(t, &f, "")
	for _, scenario := range []string{"empty", "too_many", "invalid", "upper", "duplicate", "no_source"} {
		t.Run(scenario, func(t *testing.T) {
			p := f.plan
			p.WalletObservations = &WalletObservationSelection{SourceID: f.plan.WalletObservations.SourceID, IDs: slices.Clone(f.plan.WalletObservations.IDs)}
			s := p.WalletObservations
			switch scenario {
			case "empty":
				s.IDs = nil
			case "too_many":
				s.IDs = make([]string, 65)
			case "invalid":
				s.IDs[0] = "bad-id"
			case "upper":
				s.IDs[0] = strings.Repeat("A", 64)
			case "duplicate":
				s.IDs[1] = s.IDs[0]
			case "no_source":
				s.SourceID = "unknown"
			}
			if _, _, _, err := canonicalGeneration(p, f.raw, time.Now()); err == nil {
				t.Fatal("invalid selected observation retained")
			}
		})
	}
	result, err := f.g.PrepareWalletObservations(context.Background(), f.plan, f.raw)
	if err != nil {
		t.Fatal(err)
	}
	useWalletPreparation(t, &f, result)
	p, raw, input, err := canonicalGeneration(f.plan, f.raw, time.Now())
	if err != nil || generationRequest(p, raw) == generationRequest(legacyPlan, legacy) {
		t.Fatal("new observation identity reused legacy task", err)
	}
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(context.Background(), job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	artifact.WalletSHA256 = ""
	bad, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeGenerationArtifact(generationRetained{plan: p, planRaw: raw, input: input, inputRaw: f.raw, artifact: bad}); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("missing wallet audit digest accepted as a new artifact", err)
	}
}

func TestWalletInputTimeoutBusyReopenAndSourceDamage(t *testing.T) {
	f, result := preparedWalletFixture(t)
	ctx := context.Background()
	f.funding.SetMaxOpenConns(1)
	conn, err := f.funding.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	output, err := f.g.PrepareWalletObservations(short, f.plan, f.raw)
	cancel()
	if closeErr := conn.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrGenerationEvidence) || !reflect.DeepEqual(output, WalletPreparation{}) {
		t.Fatal("timeout returned partial inputs or was classified as bad money", err)
	}
	walletInputSlots <- struct{}{}
	output, err = f.g.PrepareWalletObservations(ctx, f.plan, f.raw)
	<-walletInputSlots
	if !errors.Is(err, ErrGenerationBusy) || !reflect.DeepEqual(output, WalletPreparation{}) {
		t.Fatal("bounded wallet audit concurrency bypassed", err)
	}
	if output, err := f.g.PrepareWalletObservations(ctx, f.plan, f.raw); err != nil || !reflect.DeepEqual(output, result) {
		t.Fatal("wallet preparation did not recover", err)
	}
	useWalletPreparation(t, &f, result)
	job := generationSubmit(t, f)
	artifact, err := f.g.Run(ctx, job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := NewGeneration(f.receiver, f.funding, f.root, f.g.verifyCurrent)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if again, err := reopened.Run(ctx, job.Request.Owner, job.ID); err != nil || !reflect.DeepEqual(again, artifact) {
		t.Fatal("cold reopen didn't re-audit frozen draft", err)
	}
	// Deliberately damage ONLY this synthetic source. Later reads must refuse,
	// never repair it or use the old result to bypass the selected capture.
	damaged := generationDB(t, f.path, false)
	if _, err := damaged.Exec(`DROP TRIGGER monthly_wallet_no_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := damaged.Exec(`UPDATE portal_monthly_wallet_observations SET payload='{}' WHERE id=?`, f.plan.WalletObservations.IDs[0]); err != nil {
		t.Fatal(err)
	}
	if err := damaged.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.auditWalletInput(ctx, f.plan, f.input); err == nil {
		t.Fatal("invalid retained payload accepted")
	}
	if output, err := reopened.Run(ctx, job.Request.Owner, job.ID); err == nil || len(output.Reconciliation.Accounts) != 0 {
		t.Fatal("cached money bypassed source damage", err)
	}
	load, err := NewObservedAutomaticContext(reopened, func(context.Context, monthlytasks.Request) (AutomaticScope, error) {
		t.Fatal("source failure reached trusted scope loader")
		return AutomaticScope{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if meta, err := load(ctx, job.Request); err == nil || len(meta.BundleContext) != 0 {
		t.Fatal("damaged source produced publication metadata", err)
	}
	var saved []byte
	if err := f.receiver.QueryRow(`SELECT artifact FROM portal_monthly_generation_inputs WHERE id=?`, job.Request.InputSHA256).Scan(&saved); err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(artifact)
	if err != nil || !bytes.Equal(want, saved) {
		t.Fatal("failed validation rewrote saved artifact", err)
	}
	if after, err := os.ReadFile(f.path); err != nil || !bytes.Equal(after, before) {
		t.Fatal("failure repaired a damaged source", err)
	}
}

func TestWalletInputObservedBalancesNotMonthOrRefundProof(t *testing.T) {
	f, result := preparedWalletFixture(t)
	useWalletPreparation(t, &f, result)
	for i := range f.input.Accounts {
		a := &f.input.Accounts[i]
		a.Baseline.TimeBasis, a.Baseline.CutoffSource = "collection_observation", ""
		a.WalletMonths = nil
		for j := range a.Snapshots {
			a.Snapshots[j].TimeBasis, a.Snapshots[j].CutoffSource = "collection_observation", ""
		}
	}
	usageInputUpdate(t, &f)
	job := generationSubmit(t, f)
	if artifact, err := f.g.Run(context.Background(), job.Request.Owner, job.ID); !errors.Is(err, ErrGenerationEvidence) || len(artifact.Reconciliation.Accounts) != 0 {
		t.Fatal("raw collection observations invented missing historical evidence", err)
	}
}

func TestWalletInputPreparationGateAndScopeProof(t *testing.T) {
	f, cfg := preparationFixture(t)
	addWalletInputFixture(t, &f, "")
	cfg.FundingPath = f.path
	result, err := f.g.PrepareWalletObservations(context.Background(), f.plan, f.raw)
	if err != nil {
		t.Fatal(err)
	}
	useWalletPreparation(t, &f, result)
	before := make(map[string]string)
	for _, path := range []string{cfg.ReceiverPath, cfg.AuthorityPath, cfg.FundingPath} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		before[path] = generationHash(raw)
	}
	if output, err := PrepareGenerationInput(context.Background(), cfg, result.Plan, result.Preparation, "", false); err != nil || output.Applied {
		t.Fatal("prepared inputs didn't pass read-only selection check", err)
	}
	preparationCounts(t, f, 0, 0)
	for path, hash := range before {
		if raw, err := os.ReadFile(path); err != nil || generationHash(raw) != hash {
			t.Fatal("read-only preparation changed source or receiver", err)
		}
	}
	windows, err := f.g.auditWindows(context.Background(), f.plan)
	if err != nil {
		t.Fatal(err)
	}
	scope := automaticScopeFixture(t, f)
	scope.Accounts[0].WalletSource = f.input.Accounts[0].Snapshots[1].Source
	if output, err := observedAutomaticContext(context.Background(), f.plan, f.raw, windows, scope); !errors.Is(err, ErrGenerationEvidence) || len(output.BundleContext) != 0 {
		t.Fatal("wallet observation promoted to historical wallet-only proof", err)
	}
	f.input.Accounts[0].Snapshots[1].BalanceQuota++
	usageInputUpdate(t, &f)
	if output, err := PrepareGenerationInput(context.Background(), cfg, mustGenerationPlan(t, f), f.raw, "", false); !errors.Is(err, ErrGenerationEvidence) || output.Applied {
		t.Fatal("preparation gate accepted changed raw balance", err)
	}
	preparationCounts(t, f, 0, 0)
}
