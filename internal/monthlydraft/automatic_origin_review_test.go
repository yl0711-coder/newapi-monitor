package monthlydraft

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func originReviewFixture(t *testing.T, cash, counter int64) (capturedHistoryFixture, monthlysource.MonthlyObservationFile) {
	t.Helper()
	f, _, ref := automaticHistoryOriginalFixtureWithTail(t, func(o *monthlysource.WalletTailObservation, phase int) {
		sameSecondHistoryTail(o, phase)
		*o.User.BalanceQuota -= 1000000
		*o.User.BalanceQuota += cash
		*o.User.UsedQuota += counter
	})
	return f, ref
}

func TestAutomaticOriginReviewOppositeResidualKeepsOriginalMoneyAndNoApproval(t *testing.T) {
	f, ref := originReviewFixture(t, 1, -1)
	if _, err := f.f.g.auditAutomaticHistorySources(t.Context(), ref, f.req); err == nil {
		t.Fatal("ordinary audit admitted a negative origin counter")
	}
	r, err := f.f.g.reviewAutomaticOrigins(t.Context(), ref, f.req)
	if err != nil || len(r.accounts) != 2 {
		t.Fatal(err)
	}
	for _, a := range r.accounts {
		if a.OriginBalanceQuota != 1 || a.OriginUsedQuota != -1 || a.RegistrationMonthLogNet != 1000000 || a.Status != "bounded_policy_candidate_not_approval" {
			t.Fatal("wrong independent residual classification", a)
		}
	}
	for _, a := range r.originals.history.Report().Accounts {
		m := a.Months[0]
		if !a.OriginRequiresResolution || m.ConsumeQuota != 1500000 || m.RefundQuota != 500000 || m.ClosingBalanceQuota != -999999 || m.ClosingUsedQuota != 999999 {
			t.Fatal("original cash/counter/charges were modified", a.UserID)
		}
	}
	if _, err := f.f.g.prepareAutomaticHistoryInput(t.Context(), ref, f.req); err == nil {
		t.Fatal("diagnostics loosened financial normalization")
	}
	if _, err := json.Marshal(r); err == nil {
		t.Fatal("review serialized as approval")
	}
	if json.Unmarshal([]byte(`{}`), &r) == nil || len(r.accounts) != 0 {
		t.Fatal("decoded diagnostic retained approval")
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticOriginReviewClassifiesOnlyExistingBoundsAndDirection(t *testing.T) {
	for _, c := range []struct {
		cash, counter int64
		status        string
	}{
		{0, 0, "no_difference"}, {101, -101, "outside_existing_amount_limits"}, {1, 0, "not_opposite_residual"}, {-1, 1, "not_opposite_residual"},
	} {
		f, ref := originReviewFixture(t, c.cash, c.counter)
		r, err := f.f.g.reviewAutomaticOrigins(t.Context(), ref, f.req)
		if err != nil || len(r.accounts) != 2 {
			t.Fatal(err)
		}
		for _, a := range r.accounts {
			if a.Status != c.status {
				t.Fatal("unsupported difference treated as eligible", a)
			}
		}
		generationAdmissionCounts(t, f.f.receiver, 0, 0)
	}
	// Do not mistake the registration month for a proven incident/recognition
	// period. An empty August cannot pass the ratio gate; only September can.
	f, boundary, _ := automaticHistoryOriginalFixtureWithTail(t, func(o *monthlysource.WalletTailObservation, phase int) {
		sameSecondHistoryTail(o, phase)
		*o.User.BalanceQuota -= 999999
		*o.User.UsedQuota--
	}, "2026-08")
	older := SourcePreparationRequest{Owner: f.req.Owner, Month: "2026-08"}
	seedHistoryEmptyMonth(t, f, older, false)
	p, err := newAutomaticHistoryPreparation(f.wallet, f.spool)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := collectCapturedSources(t.Context(), p.raw, p.core, older); err != nil {
		t.Fatal(err)
	}
	files, sealed, err := collectCapturedSources(t.Context(), p.raw, p.core, older)
	if err != nil || !sealed {
		t.Fatal("original August unavailable", err)
	}
	ref := writeAutomaticHistoryManifest(t, f.f.root, automaticHistorySources{Version: 1, Mode: automaticHistorySourcesMode, Owner: f.req.Owner, SourceIdentity: f.wallet.funding.sourceIdentity, Boundary: boundary, Months: []CapturedSourceFiles{files, boundary}})
	r, err := f.f.g.reviewAutomaticOrigins(t.Context(), ref, f.req)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range r.accounts {
		if a.RegistrationMonth != "2026-08" || a.RegistrationMonthLogNet != 0 || a.Status != "bounded_policy_candidate_not_approval" || len(a.EligibleRecognitionMonths) != 1 || a.EligibleRecognitionMonths[0] != "2026-09" {
			t.Fatal("arbitrary recognition month or relaxed ratio", a)
		}
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticOriginReviewReopensOriginalsAndRejectsUnbalancedCounter(t *testing.T) {
	f, ref := originReviewFixture(t, 2, -1)
	if r, err := f.f.g.reviewAutomaticOrigins(t.Context(), ref, f.req); err == nil || len(r.accounts) != 0 {
		t.Fatal("non-opposite negative origin was admitted")
	}
	f, ref = originReviewFixture(t, 1, -1)
	r, err := f.f.g.reviewAutomaticOrigins(t.Context(), ref, f.req)
	if err != nil {
		t.Fatal(err)
	}
	path := r.originals.months[0].material.Tokens[0].File.Path
	if err := os.WriteFile(path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if changed, err := f.f.g.reviewAutomaticOrigins(t.Context(), ref, f.req); err == nil || len(changed.accounts) != 0 {
		t.Fatal("cached arithmetic replaced fresh original verification")
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticOriginReviewDoesNotNetIndependentAccounts(t *testing.T) {
	f, _, ref := automaticHistoryOriginalFixtureWithTail(t, func(o *monthlysource.WalletTailObservation, phase int) {
		sameSecondHistoryTail(o, phase)
		*o.User.BalanceQuota -= 1000000
		if o.User.UserID == 201 {
			*o.User.BalanceQuota++
			*o.User.UsedQuota--
		} else {
			*o.User.BalanceQuota--
			*o.User.UsedQuota++
		}
	})
	r, err := f.f.g.reviewAutomaticOrigins(t.Context(), ref, f.req)
	if err != nil || len(r.accounts) != 2 {
		t.Fatal(err)
	}
	if r.accounts[0].UserID != 201 || r.accounts[0].Status != "bounded_policy_candidate_not_approval" || r.accounts[1].UserID != 202 || r.accounts[1].Status != "not_opposite_residual" {
		t.Fatal("independent differences were netted", r.accounts)
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}
