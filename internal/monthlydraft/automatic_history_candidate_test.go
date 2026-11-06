package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func historyCandidateFixture(t *testing.T, residual bool) (capturedHistoryFixture, automaticHistoryContextInput, monthlysource.MonthlyObservationFile) {
	t.Helper()
	f, _, ref := automaticHistoryOriginalFixtureWithTail(t, func(o *monthlysource.WalletTailObservation, phase int) {
		sameSecondHistoryTail(o, phase)
		if !residual {
			*o.User.BalanceQuota -= 1000000
		} // A synthetic zero opening; no funding or usage is changed.
	})
	contextDirectoryLabels(t, f)
	if err := os.Rename(f.sources.History.Path, f.sources.History.Path+".unused"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.sources.Scope.Path, f.sources.Scope.Path+".unused"); err != nil {
		t.Fatal(err)
	}
	prepared, err := f.f.g.prepareAutomaticHistoryContextInput(t.Context(), f.auth, ref, f.req)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.f.root, "retained-directory.json")
	if err := os.WriteFile(path, prepared.directory, 0600); err != nil {
		t.Fatal(err)
	}
	return f, prepared, monthlysource.MonthlyObservationFile{Path: path, SHA256: generationHash(prepared.directory)}
}

func TestAutomaticHistoryCandidateOriginalsToStrictCalculator(t *testing.T) {
	f, prepared, dir := historyCandidateFixture(t, false)
	got, err := f.f.g.prepareAutomaticHistoryCandidate(t.Context(), prepared, dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.bundle.ReadyForReview != 1 || got.bundle.Blocked != 0 || len(got.bundle.Entries) != 1 || got.bundle.Entries[0].Candidate == nil {
		t.Fatal("source-to-candidate not completed", got.bundle)
	}
	candidate := got.bundle.Entries[0].Candidate
	if len(candidate.Users) != 2 || len(candidate.ReconstructedUsers) != 2 {
		t.Fatal("partial organization")
	}
	for i, u := range candidate.Users {
		if u.UserID != int64(201+i) || u.OpeningQuota != 0 || u.ClosingQuota != -1000000 || u.ConsumeQuota != 1500000 || u.RefundQuota != 500000 || len(u.Events) != 0 {
			t.Fatal("closed-month ledger changed", u)
		}
		r := got.scope.Accounts[i].Reconstructions[0]
		if r.SameView == nil || r.SameView.CompletedAtMS%1000 != 37 || len(r.SameView.Funding) != 2 || r.SameView.Funding[1].At != r.ObservedAt || r.SameView.Funding[1].DeltaQuota != 750000 {
			t.Fatal("same-second original lost/retimed", r)
		}
	}
	if _, err := monitor.PrepareAutomaticMonthlyBill(*candidate, time.Now().Unix(), time.Now()); err != nil {
		t.Fatal("candidate fails existing monetary publication policy", err)
	}
	if _, err := monitor.PrepareMonthlyCandidateBundle(got.input, got.metadata, time.Now()); err == nil {
		t.Fatal("plain JSON supplied same-view authority")
	}
	if generationArtifactReconciled(got.plan, got.bundle.Reconciliation, nil) {
		t.Fatal("local candidate skipped retained scope admission")
	}
	if _, err := json.Marshal(got); err == nil {
		t.Fatal("adapter serialized into approval")
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticHistoryCandidateRejectsUnresolvedOriginAndChangedOriginal(t *testing.T) {
	t.Run("residual", func(t *testing.T) {
		f, p, d := historyCandidateFixture(t, true)
		if got, err := f.f.g.prepareAutomaticHistoryCandidate(t.Context(), p, d); !errors.Is(err, ErrGenerationEvidence) || len(got.input) != 0 {
			t.Fatal("origin residual silently accepted", err)
		}
	})
	t.Run("changed-original", func(t *testing.T) {
		f, p, d := historyCandidateFixture(t, false)
		file := p.input.originals.months[0].material.Tokens[0].File
		if err := os.WriteFile(file.Path, []byte(`{}`), 0600); err != nil {
			t.Fatal(err)
		}
		if got, err := f.f.g.prepareAutomaticHistoryCandidate(t.Context(), p, d); err == nil || len(got.input) != 0 {
			t.Fatal("cached view replaced original audit")
		}
	})
}

func TestAutomaticHistoryCandidateRejectsPlainOrDeserializedWitness(t *testing.T) {
	f, p, d := historyCandidateFixture(t, false)
	got, err := f.f.g.prepareAutomaticHistoryCandidate(t.Context(), p, d)
	if err != nil {
		t.Fatal(err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(got.input)
	if err != nil {
		t.Fatal(err)
	}
	bound, release, err := f.f.g.bindObservationStore(t.Context(), got.plan, input)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	windows, err := bound.auditWindows(t.Context(), got.plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observedAutomaticContext(t.Context(), got.plan, got.input, windows, got.scope); !errors.Is(err, ErrGenerationEvidence) {
		t.Fatal("missing native witness accepted", err)
	}
	valid := historyCandidateWitness{input: generationHash(got.input), scope: generationHash(materialPreparationJSON(t, got.scope)), plan: generationHash(materialPreparationJSON(t, got.plan))}
	validCtx := context.WithValue(t.Context(), historyCandidateWitnessKey{}, valid)
	wrongPlan := got.plan
	wrongPlan.Owner.AuthVersion++
	if _, err := observedAutomaticContext(validCtx, wrongPlan, got.input, windows, got.scope); !errors.Is(err, ErrGenerationEvidence) {
		t.Fatal("native witness crossed plan/authorization version", err)
	}
	w := historyCandidateWitness{input: generationHash(got.input), scope: generationHash(materialPreparationJSON(t, got.scope)), plan: "unused"}
	if _, err := json.Marshal(w); err == nil {
		t.Fatal("witness serialized")
	}
	if err := json.Unmarshal([]byte(`{}`), &w); err == nil || w.input != "" {
		t.Fatal("witness decoded")
	}
	ctx := context.WithValue(t.Context(), historyCandidateWitnessKey{}, w)
	if _, err := observedAutomaticContext(ctx, got.plan, got.input, windows, got.scope); !errors.Is(err, ErrGenerationEvidence) {
		t.Fatal("empty witness reused", err)
	}
}

func TestAutomaticHistoryCandidateVerifierCopiesAndFailure(t *testing.T) {
	f, p, d := historyCandidateFixture(t, false)
	got, err := f.f.g.prepareAutomaticHistoryCandidate(t.Context(), p, d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := monitor.PrepareMonthlyCandidateBundleWithSameView(got.input, got.metadata, time.Now(), nil); err == nil {
		t.Fatal("nil verifier accepted")
	}
	failure := errors.New("synthetic original verifier failure")
	if out, err := monitor.PrepareMonthlyCandidateBundleWithSameView(got.input, got.metadata, time.Now(), func([]byte, []byte) error { return failure }); !errors.Is(err, failure) || len(out.Entries) != 0 {
		t.Fatal("verifier failure leaked partial candidate", err)
	}
	input, metadata := bytes.Clone(got.input), bytes.Clone(got.metadata)
	out, err := monitor.PrepareMonthlyCandidateBundleWithSameView(input, metadata, time.Now(), func(i, c []byte) error { clear(i); clear(c); return nil }) // Explicit test double, not the production source verifier.
	if err != nil || out.ReadyForReview != 1 || !bytes.Equal(input, got.input) || !bytes.Equal(metadata, got.metadata) {
		t.Fatal("verifier mutated calculator/caller buffers", err)
	}
}

func TestAutomaticHistoryCandidateSameViewStrictShapeAndScope(t *testing.T) {
	f, p, d := historyCandidateFixture(t, false)
	got, err := f.f.g.prepareAutomaticHistoryCandidate(t.Context(), p, d)
	if err != nil {
		t.Fatal(err)
	}
	// A deliberately permissive TRANSPORT verifier double reaches structural
	// calculator checks. Production always uses the audited original witness.
	calculate := func(raw []byte) (monitor.MonthlyPreparationBundle, error) {
		return monitor.PrepareMonthlyCandidateBundleWithSameView(got.input, raw, time.Now(), func([]byte, []byte) error { return nil })
	}
	for _, mutate := range []func(*monitor.MonthlyBundleContext){
		func(c *monitor.MonthlyBundleContext) { c.Accounts[0].Reconstructions[0].SameView.CompletedAtMS = 0 },
		func(c *monitor.MonthlyBundleContext) { c.Accounts[0].Reconstructions[0].SameView.CompletedAtMS += 1000 },
		func(c *monitor.MonthlyBundleContext) {
			v := c.Accounts[0].Reconstructions[0].SameView
			v.OriginalSHA256[1] = v.OriginalSHA256[0]
		},
		func(c *monitor.MonthlyBundleContext) { c.Accounts[0].Reconstructions[0].SameView.Version = 2 },
		func(c *monitor.MonthlyBundleContext) {
			v := c.Accounts[0].Reconstructions[0].SameView
			v.Funding[1].ID = v.Funding[0].ID
		},
		func(c *monitor.MonthlyBundleContext) {
			v := c.Accounts[0].Reconstructions[0].SameView
			v.Funding[0].At = v.Boundary - 1
		},
		func(c *monitor.MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].SameView.Funding[0].DeltaQuota = 0
		},
		func(c *monitor.MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].SameView.Funding[0].Source = "missing#fund"
		},
		func(c *monitor.MonthlyBundleContext) { c.Accounts[0].Reconstructions[0].BridgeDirection = "forward" },
		func(c *monitor.MonthlyBundleContext) {
			c.Accounts[1].Reconstructions[0].SameView.Funding[0].ID = c.Accounts[0].Reconstructions[0].SameView.Funding[0].ID
		},
	} {
		var c monitor.MonthlyBundleContext
		if json.Unmarshal(got.metadata, &c) != nil {
			t.Fatal("context")
		}
		mutate(&c)
		out, err := calculate(materialPreparationJSON(t, c))
		if err == nil && out.ReadyForReview != 0 {
			t.Fatal("invalid same-view data produced partial/full customer candidate")
		}
	}
	for _, raw := range [][]byte{
		bytes.Replace(got.metadata, []byte(`"same_view":`), []byte(`"Same_View":`), 1),
		bytes.Replace(got.metadata, []byte(`"completed_at_ms":`), []byte(`"completed_at_ms":null,"unused":`), 1),
		bytes.Replace(got.metadata, []byte(`"funding":[`), []byte(`"funding":null,"unused":[`), 1),
	} {
		if _, err := calculate(raw); err == nil {
			t.Fatal("noncanonical shape accepted")
		}
	}
	if strings.Contains(string(got.input), "same_view") {
		t.Fatal("view tail impersonated a preparation time-coverage span")
	}
}
