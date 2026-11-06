package monthlydraft

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestGenerationAutomaticPublicationAndRestart(t *testing.T) {
	f := generationFixtureNew(t)
	auth, ro, _ := usageAuthorityFixture(t)
	if err := auth.AutoMigrate(&monitor.PortalMonthlyBill{}); err != nil {
		t.Fatal(err)
	}
	owner, _, err := LoadUsageGenerationOwner(context.Background(), ro, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	f.g.verifyCurrent = func(ctx context.Context, req monthlytasks.Request) error {
		current, _, err := LoadUsageGenerationOwner(ctx, ro, req.Owner.GroupID, req.Owner.AuthVersion)
		if err != nil {
			return err
		}
		if current != req.Owner {
			return ErrGenerationChanged
		}
		return nil
	}
	job := generationSubmit(t, f)
	loaded, published := 0, 0
	metadata := func(_ context.Context, req monthlytasks.Request) (AutomaticSourceContext, error) {
		if req != job.Request {
			t.Fatal("wrong source request")
		}
		loaded++
		return AutomaticSourceContext{BundleContext: handoffContext(t, f)}, nil
	}
	publish := func(ctx context.Context, req monthlytasks.Request, raw []byte) error {
		published++
		var e monitor.MonthlyBillEvidence
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		if e.ReviewedBy != "policy:automatic-wallet-v1" || e.Users[0].ConsumeQuota != 1500000 || e.Users[1].ConsumeQuota != 1500000 {
			t.Fatal("manual approval or changed usage")
		}
		_, err := monitor.ImportMonthlyBillForAuthorization(ctx, auth, raw, monitor.MonthlyBillExpectedAuthorization{
			GroupID: req.Owner.GroupID, AuthVersion: req.Owner.AuthVersion, MemberFingerprint: req.Owner.MemberFingerprint, MemberManifest: req.Owner.MemberManifest}, time.Now())
		return err
	}
	w, err := NewAutomaticLocalWorker(f.g, metadata, publish)
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := w.step(context.Background()); err != nil || !worked || w.Status().Published != 1 {
		t.Fatal("automatic pipeline", worked, err, w.Status())
	}
	var before monitor.PortalMonthlyBill
	if err := auth.First(&before, "group_id=1 AND month='2026-09'").Error; err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if worked, err := w.step(context.Background()); err != nil || worked {
			t.Fatal("ready bill recalculated on each poll", worked, err)
		}
	}
	if loaded != 1 || published != 1 {
		t.Fatal("unbounded finalization")
	}
	// Completed publication is durable: a new process does not re-read private
	// sources or invoke the publisher again.
	restarted, err := NewAutomaticLocalWorker(f.g, metadata, publish)
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := restarted.step(context.Background()); err != nil || worked || restarted.Status().Published != 0 || loaded != 1 || published != 1 {
		t.Fatal("restart", worked, err, restarted.Status())
	}
	var after monitor.PortalMonthlyBill
	if err := auth.First(&after, "group_id=1 AND month='2026-09'").Error; err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("restart changed published bill", err)
	}
	if body, err := os.ReadFile(f.path); err != nil || generationHash(body) != f.sourceHash {
		t.Fatal("source database changed", err)
	}
}

func TestGenerationAutomaticFailureIsBoundedAndRequiresFreshSources(t *testing.T) {
	for _, kind := range []string{"metadata", "integrity", "revoked"} {
		t.Run(kind, func(t *testing.T) {
			f := generationFixtureNew(t)
			job := generationSubmit(t, f)
			if _, err := f.g.Run(context.Background(), job.Request.Owner, job.ID); err != nil {
				t.Fatal(err)
			}
			calls, publishes := 0, 0
			load := func(context.Context, monthlytasks.Request) (AutomaticSourceContext, error) {
				calls++
				if kind == "metadata" {
					return AutomaticSourceContext{}, errors.New("synthetic temporary source failure")
				}
				return AutomaticSourceContext{BundleContext: handoffContext(t, f)}, nil
			}
			if kind == "integrity" {
				if err := os.WriteFile(f.input.Sources[0].Path, []byte("changed retained source"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "revoked" {
				f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error { return ErrGenerationChanged }
			}
			w, err := NewAutomaticLocalWorker(f.g, load, func(context.Context, monthlytasks.Request, []byte) error { publishes++; return nil })
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			w.publications.now = func() time.Time { return now }
			for range 8 {
				if _, err := w.step(context.Background()); err != nil {
					t.Fatal(err)
				}
				for id, deferral := range w.deferred {
					deferral.until = time.Time{}
					w.deferred[id] = deferral
				}
				now = now.Add(publicationRetryTime + time.Second)
			}
			if publishes != 0 || calls > 3 || w.Status().Published != 0 {
				t.Fatal("failed evidence published or retried indefinitely", calls, publishes)
			}
			if kind != "revoked" {
				if state, err := w.finalizationState(context.Background(), job.ID); err != nil || state != "needs_review" {
					t.Fatal("paused finalization advertised as ongoing", state, err)
				}
			}
		})
	}
}

func TestGenerationAutomaticRuntimeRequiresExplicitScopeAndDependencies(t *testing.T) {
	if r, err := NewAutomaticLocalRuntime(nil, nil, GenerationRollout{}, nil, nil); err != nil || r != nil {
		t.Fatal("disabled constructor did work", err)
	}
	f := generationFixtureNew(t)
	_, ro, _ := usageAuthorityFixture(t)
	load := func(context.Context, monthlytasks.Request) (AutomaticSourceContext, error) {
		t.Fatal("constructor loaded private sources")
		return AutomaticSourceContext{}, nil
	}
	publish := func(context.Context, monthlytasks.Request, []byte) error {
		t.Fatal("constructor published")
		return nil
	}
	cfg := GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: "2026-09"}}}
	for _, bad := range []GenerationRollout{{Enabled: true}, {Targets: cfg.Targets}, {Enabled: true, Targets: append(cfg.Targets, cfg.Targets...)}} {
		if r, err := NewAutomaticLocalRuntime(f.g, ro, bad, load, publish); err == nil || r != nil {
			t.Fatal("invalid rollout allowed")
		}
	}
	if r, err := NewAutomaticLocalRuntime(f.g, ro, cfg, nil, publish); err == nil || r != nil {
		t.Fatal("missing producer allowed")
	}
	if r, err := NewAutomaticLocalRuntime(f.g, ro, cfg, load, nil); err == nil || r != nil {
		t.Fatal("missing publisher allowed")
	}
	if r, err := NewAutomaticLocalRuntime(f.g, nil, cfg, load, publish); err == nil || r != nil {
		t.Fatal("missing authorization allowed")
	}
	r, err := NewAutomaticLocalRuntime(f.g, ro, cfg, load, publish)
	if err != nil || r.Phase() != "disabled" || r.worker.Status().Running {
		t.Fatal("runtime construction started work", err)
	}
	if !r.scope.allows(1, "2026-09") || r.scope.allows(2, "2026-09") || r.worker.scope.allows(1, "2026-08") {
		t.Fatal("scope not enforced on both admission and worker")
	}
	if state, err := r.worker.finalizationState(context.Background(), "not-yet-run"); err != nil || state != "finalizing" {
		t.Fatal("ready task polling would stop before automatic verification", state, err)
	}
	stopRuntime(t, r)
	generationAdmissionCounts(t, f.receiver, 0, 0)
}

func TestGenerationAutomaticSmallCorrectionPreservesRawConsumption(t *testing.T) {
	testGenerationAutomaticSmallCorrectionPreservesRawConsumption(t, false)
}

func TestTokenGenerationAutomaticSmallCorrectionPreservesRawConsumption(t *testing.T) {
	testGenerationAutomaticSmallCorrectionPreservesRawConsumption(t, true)
}

func testGenerationAutomaticSmallCorrectionPreservesRawConsumption(t *testing.T, withTokens bool) {
	f := generationFixtureNew(t)
	auth, ro, _ := usageAuthorityFixture(t)
	if err := auth.AutoMigrate(&monitor.PortalMonthlyBill{}); err != nil {
		t.Fatal(err)
	}
	owner, _, err := LoadUsageGenerationOwner(context.Background(), ro, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	// Synthetic independent wallet/counter evidence: exactly one quota less
	// was settled than the original log. Do not alter that raw log amount.
	a := &f.input.Accounts[0]
	a.Snapshots[0].BalanceQuota++
	a.Snapshots[0].UsedQuota--
	body, err := json.Marshal(f.input.Accounts)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.input.Sources[0].Path, body, 0600); err != nil {
		t.Fatal(err)
	}
	f.input.Sources[0].SHA256 = generationHash(body)
	second := filepath.Join(f.root, "second-synthetic-capture.json")
	secondBody, err := json.Marshal(struct {
		CapturedAt int64
		Balance    int64
		Used       int64
	}{a.Snapshots[0].At + 310, a.Snapshots[0].BalanceQuota, a.Snapshots[0].UsedQuota})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, secondBody, 0600); err != nil {
		t.Fatal(err)
	}
	f.input.Sources = append(f.input.Sources, monitor.MonthlyPreparationSource{ID: "second_capture", Path: second, SHA256: generationHash(secondBody)})
	f.raw = draftJSON(t, f.input)
	f.plan.PreparationSHA256 = generationHash(f.raw)
	if withTokens {
		addTokenArchives(t, &f)
		applyTokenInput(t, &f)
	}
	job := generationSubmit(t, f)
	contextRaw := handoffContext(t, f)
	bundle, err := monitor.PrepareMonthlyCandidateBundle(f.raw, contextRaw, time.Now())
	if err != nil || bundle.ReadyForReview != 1 {
		t.Fatal("synthetic independent candidate", err, bundle.Entries)
	}
	u := bundle.Entries[0].Candidate.Users[0]
	if u.ConsumeQuota != 1499999 || u.Accounting.LogConsumeQuota != 1500000 {
		t.Fatal("fixture failed to reproduce under-settlement")
	}
	u.ConsumeQuota = u.Accounting.LogConsumeQuota // only to bind the original log scope
	scope := monitor.MonthlyCorrectionScopeSHA256(u, job.Request.Month)
	proof := monitor.MonthlyBillBalanceCorrection{Version: 1, Quota: 1, Observations: []monitor.MonthlyBillCorrectionObservation{
		{UserID: u.UserID, Month: job.Request.Month, CapturedAt: u.Through + 10, SourceSHA256: f.input.Sources[0].SHA256, ScopeSHA256: scope, ClosingQuota: u.ClosingQuota, NetUsedQuota: 999999},
		{UserID: u.UserID, Month: job.Request.Month, CapturedAt: u.Through + 310, SourceSHA256: f.input.Sources[1].SHA256, ScopeSHA256: scope, ClosingQuota: u.ClosingQuota, NetUsedQuota: 999999},
	}}
	corrections := []AutomaticAccountCorrection{{UserID: u.UserID, Evidence: proof}}
	// An unretained hash or another account cannot be used as correction proof.
	bad := append([]AutomaticAccountCorrection(nil), corrections...)
	bad[0].UserID = 99999
	if _, err := automaticCorrectedCandidate(*bundle.Entries[0].Candidate, bad, f.raw); !errors.Is(err, ErrGenerationEvidence) {
		t.Fatal("foreign correction accepted", err)
	}
	if bundle.Entries[0].Candidate.Users[0].ConsumeQuota != 1499999 {
		t.Fatal("failed normalization mutated original candidate")
	}
	bad[0] = corrections[0]
	bad[0].Evidence.Observations = append([]monitor.MonthlyBillCorrectionObservation(nil), proof.Observations...)
	bad[0].Evidence.Observations[0].SourceSHA256 = generationHash([]byte("not retained"))
	if _, err := automaticCorrectedCandidate(*bundle.Entries[0].Candidate, bad, f.raw); !errors.Is(err, ErrGenerationEvidence) {
		t.Fatal("unretained correction capture accepted", err)
	}
	worker, err := NewAutomaticLocalWorker(f.g, func(context.Context, monthlytasks.Request) (AutomaticSourceContext, error) {
		return AutomaticSourceContext{BundleContext: contextRaw, Corrections: corrections}, nil
	}, func(ctx context.Context, req monthlytasks.Request, evidence []byte) error {
		var e monitor.MonthlyBillEvidence
		if err := json.Unmarshal(evidence, &e); err != nil {
			return err
		}
		version := 4
		if withTokens {
			version = 5
		}
		if e.Version != version || e.Users[0].ConsumeQuota != 1500000 || e.Users[0].RefundQuota != 500000 || e.Users[0].BalanceCorrection == nil || e.Users[0].BalanceCorrection.Quota != 1 || e.Users[1].BalanceCorrection != nil {
			t.Fatal("raw money changed or correction mixed between accounts")
		}
		_, err := monitor.ImportMonthlyBillForAuthorization(ctx, auth, evidence, monitor.MonthlyBillExpectedAuthorization{GroupID: req.Owner.GroupID, AuthVersion: req.Owner.AuthVersion, MemberFingerprint: req.Owner.MemberFingerprint, MemberManifest: req.Owner.MemberManifest}, time.Now())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := worker.step(context.Background()); err != nil || !worked || worker.Status().Published != 1 {
		t.Fatal("small correction pipeline failed", worked, err, worker.Status())
	}
	// Unstable projections use bounded retries, not a permanent evidence verdict
	// on the first sampling mismatch and not a changed customer charge.
	// Use a distinct retained input/task. A saved task must never be reactivated.
	versionPath := filepath.Join(f.root, "unstable-synthetic-version.json")
	versionBody := []byte(`{"version":"unstable-capture-test"}`)
	if err := os.WriteFile(versionPath, versionBody, 0600); err != nil {
		t.Fatal(err)
	}
	f.input.Sources = append(f.input.Sources, monitor.MonthlyPreparationSource{ID: "unstable_version", Path: versionPath, SHA256: generationHash(versionBody)})
	f.raw = draftJSON(t, f.input)
	f.plan.PreparationSHA256 = generationHash(f.raw)
	generationSubmit(t, f)
	contextRaw = handoffContext(t, f)
	unstable := append([]AutomaticAccountCorrection(nil), corrections...)
	unstable[0].Evidence.Observations = append([]monitor.MonthlyBillCorrectionObservation(nil), proof.Observations...)
	unstable[0].Evidence.Observations[1].ClosingQuota++
	attempts, unexpectedPublishes := 0, 0
	unstableWorker, err := NewAutomaticLocalWorker(f.g, func(context.Context, monthlytasks.Request) (AutomaticSourceContext, error) {
		attempts++
		return AutomaticSourceContext{BundleContext: contextRaw, Corrections: unstable}, nil
	}, func(context.Context, monthlytasks.Request, []byte) error {
		unexpectedPublishes++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	unstableWorker.publications.now = func() time.Time { return now }
	for range 5 {
		if _, err := unstableWorker.step(context.Background()); err != nil {
			t.Fatal(err)
		}
		for id, deferral := range unstableWorker.deferred {
			deferral.until = time.Time{}
			unstableWorker.deferred[id] = deferral
		}
		now = now.Add(publicationRetryTime + time.Second)
	}
	if attempts != 3 || unexpectedPublishes != 0 {
		t.Fatal("unstable capture did not retry exactly within budget", attempts, unexpectedPublishes)
	}
}
