package monitor

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestMonthlyReconstructionMalformedSelectionCannotFallback(t *testing.T) {
	i, c := monthlyReconstructionFixture()
	raw, meta := monthlyBundleRaw(t, i, c)
	for name, bad := range map[string][]byte{
		"alias":          bytes.Replace(meta, []byte(`"reconstructions"`), []byte(`"Reconstructions"`), 1),
		"null selection": bytes.Replace(meta, []byte(`"reconstructions":[`), []byte(`"reconstructions":null,"unused":[`), 1),
		"null amount":    bytes.Replace(meta, []byte(`"balance_quota":-1750000`), []byte(`"balance_quota":null`), 1),
		"amount alias":   bytes.Replace(meta, []byte(`"used_quota"`), []byte(`"Used_quota"`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := PrepareMonthlyCandidateBundle(raw, bad, time.Unix(preparationAt("2026-10-20"), 0)); err == nil {
				t.Fatal("malformed reconstruction accepted")
			}
		})
	}
}

func TestMonthlyReconstructionKeepsAccountsIndependentAndUnknownStatistics(t *testing.T) {
	i, c := monthlyReconstructionFixture()
	i.Accounts[0].Spans[0].Requests = nil
	i.Accounts[0].Spans[0].Tokens = nil
	b := monthlyBundleRun(t, i, c)
	if b.ReadyForReview != 1 || b.Entries[0].Candidate.Users[0].Statistics == nil || b.Entries[0].Candidate.Users[0].Requests != 0 {
		t.Fatal("unknown statistics changed money or became a guessed count")
	}
	other := i.Accounts[0]
	other.UserID, other.GroupID, other.Username = 9001, 9002, "independent"
	other.Baseline.UserID = other.UserID
	other.Spans = append([]MonthlyPreparationSpan(nil), other.Spans...)
	other.Snapshots = append([]MonthlyPreparationBalance(nil), other.Snapshots...)
	other.Movements = append([]MonthlyPreparationMovement(nil), other.Movements...)
	other.MovementCoverage = append([]MonthlyPreparationCoverage(nil), other.MovementCoverage...)
	for j := range other.Spans {
		other.Spans[j].UserID = other.UserID
	}
	for j := range other.Snapshots {
		other.Snapshots[j].UserID = other.UserID
	}
	for j := range other.Movements {
		other.Movements[j].UserID = other.UserID
		other.Movements[j].ID += 100
	}
	for j := range other.MovementCoverage {
		other.MovementCoverage[j].UserID = other.UserID
	}
	i.Accounts = append(i.Accounts, other)
	metadata := monthlyBundleFixture(i)
	metadata.Accounts[0].Reconstructions = c.Accounts[0].Reconstructions
	metadata.Accounts[1].Reconstructions = append([]MonthlySnapshotReconstruction(nil), c.Accounts[0].Reconstructions...)
	metadata.Accounts[1].Reconstructions[0].UserID = other.UserID
	metadata.Accounts[1].Reconstructions[0].FundingEventIDs = []int64{101, 102}
	metadata.Accounts[1].Reconstructions[0].BalanceQuota = preparationInt(-1749999)
	mixed := monthlyBundleRun(t, i, metadata)
	if mixed.ReadyForReview != 1 || mixed.Blocked != 1 {
		t.Fatal("other account residual leaked into valid customer", mixed)
	}
	want, got := b.Entries[0].Candidate.Users[0], mixed.Entries[0].Candidate.Users[0]
	wantAccounting, gotAccounting := *want.Accounting, *got.Accounting
	// Adding a second account changes the input/context fingerprint, not money.
	wantAccounting.MovementSource, gotAccounting.MovementSource = "", ""
	wantAccounting.ReconciliationSource, gotAccounting.ReconciliationSource = "", ""
	want.Accounting, got.Accounting = &wantAccounting, &gotAccounting
	if !reflect.DeepEqual(want, got) {
		t.Fatal("valid customer's money/statistics/provenance changed")
	}
}

func monthlyReconstructionFixture() (MonthlyPreparationInput, MonthlyBundleContext) {
	input := preparationFixture()
	a := &input.Accounts[0]
	a.Months = []string{"2026-06"}
	for i := range a.Snapshots {
		a.Snapshots[i].TimeBasis = monthlyCollectionObservation
	}
	c := monthlyBundleFixture(input)
	c.Accounts[0].Reconstructions = []MonthlySnapshotReconstruction{{
		UserID: a.UserID, Month: "2026-06", ObservedAt: preparationAt("2026-08-01"),
		SnapshotSource: "logs#independent-balance", ScopeSource: "logs#transaction-and-query-scope", MonthSource: "logs#independent-june", BridgeSource: "logs#independent-july", FundingSource: "logs#independent-funding", OpeningBasisSource: "logs#baseline-proof",
		BalanceQuota: preparationInt(-1750000), UsedQuota: preparationInt(6750000),
		MonthConsumeQuota: preparationInt(3000000), MonthRefundQuota: preparationInt(0), BridgeConsumeQuota: preparationInt(4000000), BridgeRefundQuota: preparationInt(250000), FundingEventIDs: []int64{1, 2},
	}}
	return input, c
}

func TestMonthlyReconstructionAdjacentMonthsPreserveGrossAndRefunds(t *testing.T) {
	i, c := monthlyReconstructionFixture()
	i.Accounts[0].Months = []string{"2026-06", "2026-07"}
	c.Groups[0].Months = append([]string(nil), i.Accounts[0].Months...)
	july := c.Accounts[0].Reconstructions[0]
	july.Month = "2026-07"
	july.MonthConsumeQuota = preparationInt(4000000)
	july.MonthRefundQuota = preparationInt(250000)
	july.BridgeConsumeQuota = preparationInt(0)
	july.BridgeRefundQuota = preparationInt(0)
	c.Accounts[0].Reconstructions = append(c.Accounts[0].Reconstructions, july)
	b := monthlyBundleRun(t, i, c)
	if b.ReadyForReview != 2 || b.Blocked != 0 {
		t.Fatal("adjacent reconstruction failed", b)
	}
	first, second := b.Entries[0].Candidate.Users[0], b.Entries[1].Candidate.Users[0]
	if first.ClosingQuota != second.OpeningQuota || second.ClosingQuota != -1750000 || second.ConsumeQuota != 4000000 || second.RefundQuota != 250000 || len(second.Events) != 1 || second.Events[0].DeltaQuota != -500000 || second.Events[0].ID == first.Events[0].ID {
		t.Fatal("adjacent continuity, refund or signed funding changed", first, second)
	}
}

func TestMonthlyReconstructionExactCandidateNotOriginalSnapshot(t *testing.T) {
	input, c := monthlyReconstructionFixture()
	before, _ := json.Marshal(input)
	b := monthlyBundleRun(t, input, c)
	if b.ReadyForReview != 1 || b.Blocked != 0 || b.Reconciliation.Accounts[0].Months[0].Status != "no_matching_balance_anchor" {
		t.Fatal("did not keep observation separate from explicit reconstruction", b)
	}
	e := b.Entries[0]
	u := e.Candidate.Users[0]
	if u.OpeningQuota != 500000 || u.ClosingQuota != 2500000 || u.ReverseClosingQuota != 2500000 || u.ConsumeQuota != 3000000 || u.RefundQuota != 0 || len(u.Events) != 1 || len(e.Candidate.ReconstructedUsers) != 1 || e.Candidate.ReconstructedUsers[0] != u.UserID || u.Accounting.ClosingSource != "historical-reconstruction:logs#independent-balance" {
		t.Fatal("changed money or lost reconstruction identity", u)
	}
	if !reflect.DeepEqual(b, monthlyBundleRun(t, input, c)) {
		t.Fatal("unstable reconstruction")
	}
	after, _ := json.Marshal(input)
	if string(before) != string(after) {
		t.Fatal("input rewritten")
	}
	without := c
	without.Accounts = append([]MonthlyBundleAccountContext(nil), c.Accounts...)
	without.Accounts[0].Reconstructions = nil
	if b := monthlyBundleRun(t, input, without); b.ReadyForReview != 0 {
		t.Fatal("ordinary observation bypasses reconstruction")
	}
}

func TestMonthlyReconstructionRejectsMissingConflictingAndAdjustedEvidence(t *testing.T) {
	for name, mutate := range map[string]func(*MonthlyPreparationInput, *MonthlyBundleContext){
		"missing balance": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].BalanceQuota = nil
		},
		"missing refund": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].BridgeRefundQuota = nil
		},
		"wrong account": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) { c.Accounts[0].Reconstructions[0].UserID++ },
		"wrong month": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].Month = "2026-07"
		},
		"month absent from account": func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) { i.Accounts[0].Months = []string{"2026-07"} },
		"missing scope": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].ScopeSource = "absent#scope"
		},
		"duplicate month": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions = append(c.Accounts[0].Reconstructions, c.Accounts[0].Reconstructions[0])
		},
		"missing event": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].FundingEventIDs = []int64{1}
		},
		"duplicate event": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].FundingEventIDs = []int64{1, 2, 2}
		},
		"extra event": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].FundingEventIDs = []int64{1, 2, 3}
		},
		"missing event set": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			c.Accounts[0].Reconstructions[0].FundingEventIDs = nil
		},
		"unreconciled balance": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			*c.Accounts[0].Reconstructions[0].BalanceQuota++
		},
		"unreconciled counter": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			*c.Accounts[0].Reconstructions[0].UsedQuota++
		},
		"net equal but gross changed": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			*c.Accounts[0].Reconstructions[0].MonthConsumeQuota++
			*c.Accounts[0].Reconstructions[0].MonthRefundQuota++
		},
		"negative refund": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			*c.Accounts[0].Reconstructions[0].MonthRefundQuota = -1
		},
		"overflow": func(_ *MonthlyPreparationInput, c *MonthlyBundleContext) {
			*c.Accounts[0].Reconstructions[0].BridgeConsumeQuota = math.MaxInt64
		},
		"no month logs": func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) {
			i.Accounts[0].Spans = i.Accounts[0].Spans[1:]
		},
		"no funding coverage": func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) { i.Accounts[0].MovementCoverage = nil },
		"conflicting precise source": func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) {
			i.Accounts[0].Snapshots[0].TimeBasis = monthlySourceLogCutoff
			i.Accounts[0].Snapshots[0].CutoffSource = "logs#reviewed"
			i.Accounts[0].Snapshots[0].BalanceQuota++
		},
		"wallet mode also selected": func(i *MonthlyPreparationInput, _ *MonthlyBundleContext) {
			i.Accounts[0].WalletMonths = []MonthlyPreparationWalletMonth{{UserID: i.Accounts[0].UserID, Month: "2026-06", Source: "logs#refund"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			i, c := monthlyReconstructionFixture()
			mutate(&i, &c)
			raw, meta := monthlyBundleRaw(t, i, c)
			b, err := PrepareMonthlyCandidateBundle(raw, meta, time.Unix(preparationAt("2026-10-20"), 0))
			if err == nil && (b.ReadyForReview != 0 || b.Entries[0].Candidate != nil) {
				t.Fatal("invalid reconstruction accepted", b)
			}
		})
	}
}

func TestMonthlyReconstructionRequiresExplicitReviewAndExactContext(t *testing.T) {
	i, c := monthlyReconstructionFixture()
	raw, meta := monthlyBundleRaw(t, i, c)
	now := time.Unix(preparationAt("2026-10-20"), 0)
	b, err := PrepareMonthlyCandidateBundle(raw, meta, now)
	if err != nil {
		t.Fatal(err)
	}
	b.SourceIntegrity = "retained_files_sha256_verified_not_fact_attestation"
	b.Reconciliation.SourceIntegrity = b.SourceIntegrity
	br := monthlyReviewJSON(t, b)
	r := MonthlyCandidateReview{Version: 1, GroupID: i.Accounts[0].GroupID, Month: "2026-06", BundleSHA256: monthlyDigest(br), CandidateSHA256: b.Entries[0].CandidateSHA256, Decision: "approve", ReviewedBy: "synthetic-reviewer", ReviewedAt: now.Unix(), SourcesReviewed: true, MemberScopeReviewed: true, WalletBasisReviewed: true}
	if _, err := ReviewMonthlyCandidate(raw, meta, br, monthlyReviewJSON(t, r), now); err == nil {
		t.Fatal("ordinary approval accepted reconstruction")
	}
	r.ReconstructionReviewed = true
	got, err := ReviewMonthlyCandidate(raw, meta, br, monthlyReviewJSON(t, r), now)
	if err != nil {
		t.Fatal(err)
	}
	var e MonthlyBillEvidence
	if err := json.Unmarshal(got, &e); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(e.Users, b.Entries[0].Candidate.Users) {
		t.Fatal("review changed money or provenance")
	}
	c.Accounts[0].Reconstructions[0].BridgeSource = "logs#changed"
	_, changed := monthlyBundleRaw(t, i, c)
	if _, err := ReviewMonthlyCandidate(raw, changed, br, monthlyReviewJSON(t, r), now); err == nil {
		t.Fatal("old approval used after evidence changed")
	}
}
