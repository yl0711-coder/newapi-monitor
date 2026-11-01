package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func historyBindingInput(t *testing.T, f capturedHistoryFixture, ref monthlysource.MonthlyObservationFile) (GenerationPlan, monitor.MonthlyPreparationInput, []byte) {
	t.Helper()
	prepared, err := f.f.g.prepareAutomaticHistoryInput(t.Context(), ref, f.req)
	if err != nil {
		t.Fatal(err)
	}
	var p GenerationPlan
	if json.Unmarshal(prepared.observed.Plan, &p) != nil {
		t.Fatal("plan")
	}
	input, err := monitor.DecodeMonthlyPreparationInput(prepared.observed.Preparation)
	if err != nil {
		t.Fatal(err)
	}
	return p, input, prepared.observed.Preparation
}

func TestAutomaticHistoryBindingReopensOriginalsWithoutMutatingScopedHandle(t *testing.T) {
	f, _, ref := automaticHistoryOriginalFixture(t)
	p, input, _ := historyBindingInput(t, f, ref)
	if p.AutomaticHistorySourceID != automaticHistorySourceID {
		t.Fatal("normalizer omitted binding")
	}
	view, release, err := f.f.g.bindObservationStore(t.Context(), p, input)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	db, selected, info := view.funding, *view.observationSource, view.observationFile
	for range 2 {
		if _, err := view.auditWalletInput(t.Context(), p, input); err != nil {
			t.Fatal(err)
		}
		if view.funding != db || *view.observationSource != selected || view.observationFile != info || f.f.g.observationSource != nil {
			t.Fatal("shared/target source mutated")
		}
		if err := db.PingContext(t.Context()); err != nil {
			t.Fatal("caller's source handle closed", err)
		}
	}
	if db.Stats().InUse != 0 || f.f.receiver.Stats().InUse != 0 {
		t.Fatal("audit retained a DB connection")
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticHistoryBindingCannotBeStrippedOrMadeLegacy(t *testing.T) {
	f, _, ref := automaticHistoryOriginalFixture(t)
	p, input, raw := historyBindingInput(t, f, ref)
	for _, mutate := range []func(*GenerationPlan){
		func(p *GenerationPlan) { p.AutomaticHistorySourceID = "" },
		func(p *GenerationPlan) { p.AutomaticHistorySourceID = "another-source" },
		func(p *GenerationPlan) { p.Version = 1 },
		func(p *GenerationPlan) { p.ObservationStoreSourceID = "" },
		func(p *GenerationPlan) { p.CapturedUsageSourceID = "" },
		func(p *GenerationPlan) { p.WalletObservations = nil },
		func(p *GenerationPlan) { p.TokenUsageSources = nil },
	} {
		bad := p
		mutate(&bad)
		if _, _, _, err := canonicalGeneration(bad, raw, time.Now()); !errors.Is(err, ErrGenerationInput) {
			t.Fatal("source audit stripped from canonical identity", err)
		}
	}
	p.AutomaticHistorySourceID = ""
	input.Sources[0].ID = "renamed-originals"
	if err := canonicalAutomaticHistoryBinding(p, input); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("reserved arithmetic origin treated as legacy")
	}
	// Even an otherwise reconciled report is not financial scope approval.
	p.AutomaticHistorySourceID = automaticHistorySourceID
	if generationArtifactReconciled(p, monitor.MonthlyPreparationReport{}, &GenerationScopeReview{}) {
		t.Fatal("source-only input became publishable")
	}
}

func TestAutomaticHistoryBindingRejectsBalancedButAlteredHistory(t *testing.T) {
	f, _, ref := automaticHistoryOriginalFixture(t)
	p, _, raw := historyBindingInput(t, f, ref)
	for _, mutate := range []func(*monitor.MonthlyPreparationInput){
		func(i *monitor.MonthlyPreparationInput) { i.Accounts[0].Baseline.BalanceQuota++ },
		func(i *monitor.MonthlyPreparationInput) { i.Accounts[0].Baseline.UsedQuota++ },
		func(i *monitor.MonthlyPreparationInput) { i.Accounts[0].MovementCoverage[0].From++ },
		func(i *monitor.MonthlyPreparationInput) {
			i.Accounts[0].Spans[0].ConsumeQuota++
			i.Accounts[0].Spans[0].RefundQuota++
		},
		func(i *monitor.MonthlyPreparationInput) {
			// Forged equal credit/debit preserves the closing balance but not
			// the original funding ledger. Neither row may be admitted.
			for j, delta := range []int64{1, -1} {
				i.Accounts[0].Movements = append(i.Accounts[0].Movements, monitor.MonthlyPreparationMovement{
					UserID: i.Accounts[0].UserID, ID: int64(90000001 + j), At: i.Accounts[0].Baseline.At + 1,
					DeltaQuota: delta, Source: automaticHistorySourceID + "#forged-funding"})
			}
		},
		func(i *monitor.MonthlyPreparationInput) {
			s := i.Accounts[0].Snapshots[0]
			s.At++
			s.Source = automaticHistorySourceID + "#invented-cutoff"
			s.TimeBasis = "source_log_cutoff"
			s.CutoffSource = s.Source
			i.Accounts[0].Snapshots = append(i.Accounts[0].Snapshots, s)
		},
	} {
		i, err := monitor.DecodeMonthlyPreparationInput(raw)
		if err != nil {
			t.Fatal(err)
		}
		mutate(&i)
		if err := f.f.g.auditAutomaticHistoryBinding(t.Context(), p, i); !errors.Is(err, ErrGenerationEvidence) {
			t.Fatal("balanced altered history accepted", err)
		}
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticHistoryBindingNeverFallsBackToDefaultDatabase(t *testing.T) {
	f, _, ref := automaticHistoryOriginalFixture(t)
	p, input, _ := historyBindingInput(t, f, ref)
	unavailable, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := unavailable.Close(); err != nil {
		t.Fatal(err)
	}
	g := *f.f.g
	g.funding = unavailable
	view, release, err := g.bindObservationStore(t.Context(), p, input)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := view.auditWalletInput(t.Context(), p, input); err != nil {
		t.Fatal("explicit month original read fell back to unavailable default", err)
	}
	if _, err := g.auditWalletInput(t.Context(), p, input); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("unbound caller silently used another DB", err)
	}
}

func TestAutomaticHistoryBindingRechecksNonTargetOriginalOnEveryUse(t *testing.T) {
	f, boundary, _ := automaticHistoryOriginalFixture(t, "2026-08")
	older := SourcePreparationRequest{Owner: f.req.Owner, Month: "2026-08"}
	seedHistoryEmptyMonth(t, f, older, false)
	pipeline, err := newAutomaticHistoryPreparation(f.wallet, f.spool)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := collectCapturedSources(t.Context(), pipeline.raw, pipeline.core, older); err != nil {
		t.Fatal(err)
	}
	files, sealed, err := collectCapturedSources(t.Context(), pipeline.raw, pipeline.core, older)
	if err != nil || !sealed {
		t.Fatal("raw history unavailable", err)
	}
	ref := writeAutomaticHistoryManifest(t, f.f.root, automaticHistorySources{Version: 1, Mode: automaticHistorySourcesMode, Owner: f.req.Owner,
		SourceIdentity: f.wallet.funding.sourceIdentity, Boundary: boundary, Months: []CapturedSourceFiles{files, boundary}})
	p, input, _ := historyBindingInput(t, f, ref)
	if err := f.f.g.auditAutomaticHistoryBinding(t.Context(), p, input); err != nil {
		t.Fatal(err)
	}
	var m monthlysource.MonthlyObservationMaterial
	raw, err := os.ReadFile(files.Material.Path)
	if err != nil || json.Unmarshal(raw, &m) != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(m.Tokens[0].File.Path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.f.g.auditAutomaticHistoryBinding(t.Context(), p, input); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("successful prior review replaced original re-audit", err)
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticHistoryBindingRunAndCachedMarkerCannotSkipSourceAudit(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(map[bool]string{false: "queued", true: "synthetic-corrupt-ready-marker"}[cached], func(t *testing.T) {
			f, _, ref := automaticHistoryOriginalFixture(t)
			p, input, _ := historyBindingInput(t, f, ref)
			input.Accounts[0].Baseline.BalanceQuota++ // Rehashing is not independent monetary evidence.
			raw := materialPreparationJSON(t, input)
			p.PreparationSHA256 = generationHash(raw)
			// Explicit synthetic current-policy double to reach the monetary
			// Run/cache gate with deliberately corrupt internal state. The real
			// runtime selection/authorization callback is neither replaced nor
			// weakened; all fixture/shared handles keep their existing owner.
			g := *f.f.g
			g.verifyCurrent = func(_ context.Context, request monthlytasks.Request) error {
				if request.Owner != f.req.Owner || request.Month != f.req.Month {
					return ErrGenerationChanged
				}
				return nil
			}
			job, err := g.Submit(t.Context(), p, raw)
			if err != nil {
				t.Fatal(err)
			}
			if cached {
				_, lease, err := g.tasks.Claim(t.Context(), p.Owner, job.ID)
				if err != nil {
					t.Fatal(err)
				}
				// A deliberately invalid internal cached blob, not a published
				// bill. Its marker must not bypass the original financial audit.
				blob := []byte(`{}`)
				if err := saveGenerationArtifact(t.Context(), f.f.receiver, job.Request.InputSHA256, blob); err != nil {
					t.Fatal(err)
				}
				if err := g.tasks.Complete(t.Context(), lease, generationHash(blob)); err != nil {
					t.Fatal(err)
				}
			}
			called := false
			g.calculate = func(context.Context, monthlysource.FundingMonthRequest, []monthlysource.FundingWindowObservation, []byte, time.Time) (Review, error) {
				called = true
				return Review{}, nil
			}
			out, err := g.Run(t.Context(), p.Owner, job.ID)
			if !errors.Is(err, ErrGenerationEvidence) || called || len(out.Reconciliation.Accounts) != 0 {
				t.Fatal("queued/cached marker skipped source audit", err, called)
			}
		})
	}
}

func TestAutomaticHistoryBindingLegacyPlanBytesStayUnchanged(t *testing.T) {
	f := generationFixtureNew(t)
	_, got, _, err := canonicalGeneration(f.plan, f.raw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(got, []byte("automatic_history_source_id")) {
		t.Fatal("legacy bytes gained an opt-in field")
	}
	p := f.plan
	p.CalculationVersion = generationVersion
	want := materialPreparationJSON(t, p)
	if !bytes.Equal(got, want) {
		t.Fatal("legacy plan canonical bytes changed")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := f.g.auditAutomaticHistoryBinding(ctx, f.plan, f.input); err != nil {
		t.Fatal("legacy path gained new source reads", err)
	}
}
