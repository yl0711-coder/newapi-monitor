package monitor

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

type monthlyReviewInputs struct {
	input, context, bundle, review []byte
	now                            time.Time
}

func monthlyReviewJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func monthlyReviewApproval(bundleRaw []byte, entry MonthlyBundleEntry, now time.Time) MonthlyCandidateReview {
	return MonthlyCandidateReview{Version: 1, GroupID: entry.GroupID, Month: entry.Month, BundleSHA256: monthlyDigest(bundleRaw), CandidateSHA256: entry.CandidateSHA256, Decision: "approve", ReviewedBy: "synthetic-local-review-only", ReviewedAt: now.Unix(), SourcesReviewed: true, MemberScopeReviewed: true, WalletBasisReviewed: true}
}

func monthlyReviewFixture(t *testing.T) monthlyReviewInputs {
	t.Helper()
	input := preparationFixture()
	raw, meta := monthlyBundleRaw(t, input, monthlyBundleFixture(input))
	now := time.Unix(preparationAt("2026-10-21"), 0)
	b, err := PrepareMonthlyCandidateBundle(raw, meta, now)
	if err != nil {
		t.Fatal(err)
	}
	// Synthetic sources only. Real source file verification belongs to the CLI.
	b.SourceIntegrity = "retained_files_sha256_verified_not_fact_attestation"
	b.Reconciliation.SourceIntegrity = b.SourceIntegrity
	bundle := monthlyReviewJSON(t, b)
	return monthlyReviewInputs{raw, meta, bundle, monthlyReviewJSON(t, monthlyReviewApproval(bundle, b.Entries[0], now)), now}
}

func TestMonthlyReviewExactConversionAndDeterminism(t *testing.T) {
	f := monthlyReviewFixture(t)
	before := [][]byte{bytes.Clone(f.input), bytes.Clone(f.context), bytes.Clone(f.bundle), bytes.Clone(f.review)}
	raw, err := ReviewMonthlyCandidate(f.input, f.context, f.bundle, f.review, f.now)
	if err != nil {
		t.Fatal(err)
	}
	e, err := decodeMonthlyBillEvidence(raw)
	if err != nil {
		t.Fatal(err)
	}
	var bundle MonthlyPreparationBundle
	if err := json.Unmarshal(f.bundle, &bundle); err != nil {
		t.Fatal(err)
	}
	candidate := bundle.Entries[0].Candidate
	if e.Version != 2 || e.GroupID != candidate.GroupID || e.Month != candidate.Month || e.ReviewedBy != "synthetic-local-review-only" || e.ReviewedAt != f.now.Unix() || !reflect.DeepEqual(e.Users, candidate.Users) {
		t.Fatal("conversion changed scope, money or explicit review metadata")
	}
	for _, digest := range []string{monthlyDigest(f.input), monthlyDigest(f.context), monthlyDigest(f.bundle), bundle.Entries[0].CandidateSHA256, monthlyDigest(f.review)} {
		if !strings.Contains(e.SourceReference, digest) {
			t.Fatal("missing traceable source digest")
		}
	}
	if _, _, err := buildPublishedMonthlyBill(e, f.now); err != nil {
		t.Fatal(err)
	}
	repeated, err := ReviewMonthlyCandidate(f.input, f.context, f.bundle, f.review, f.now.Add(time.Hour))
	if err != nil || !bytes.Equal(raw, repeated) {
		t.Fatal("repeat generated different import bytes", err)
	}
	for i, after := range [][]byte{f.input, f.context, f.bundle, f.review} {
		if !bytes.Equal(before[i], after) {
			t.Fatal("conversion modified an input")
		}
	}
}

func TestMonthlyReviewRequiresExplicitScopeAndApproval(t *testing.T) {
	for name, mutate := range map[string]func(*MonthlyCandidateReview){
		"version":             func(r *MonthlyCandidateReview) { r.Version = 2 },
		"no customer":         func(r *MonthlyCandidateReview) { r.GroupID = 0 },
		"wrong customer":      func(r *MonthlyCandidateReview) { r.GroupID++ },
		"wrong month":         func(r *MonthlyCandidateReview) { r.Month = "2026-07" },
		"implicit month":      func(r *MonthlyCandidateReview) { r.Month = "" },
		"open month":          func(r *MonthlyCandidateReview) { r.Month = "2026-10" },
		"malformed month":     func(r *MonthlyCandidateReview) { r.Month = "2026-6" },
		"no decision":         func(r *MonthlyCandidateReview) { r.Decision = "" },
		"rejected":            func(r *MonthlyCandidateReview) { r.Decision = "reject" },
		"sources unreviewed":  func(r *MonthlyCandidateReview) { r.SourcesReviewed = false },
		"scope unreviewed":    func(r *MonthlyCandidateReview) { r.MemberScopeReviewed = false },
		"wallet unreviewed":   func(r *MonthlyCandidateReview) { r.WalletBasisReviewed = false },
		"missing reviewer":    func(r *MonthlyCandidateReview) { r.ReviewedBy = "" },
		"padded reviewer":     func(r *MonthlyCandidateReview) { r.ReviewedBy = " operator " },
		"control character":   func(r *MonthlyCandidateReview) { r.ReviewedBy = "operator\n" },
		"overlong reviewer":   func(r *MonthlyCandidateReview) { r.ReviewedBy = strings.Repeat("a", 129) },
		"missing timestamp":   func(r *MonthlyCandidateReview) { r.ReviewedAt = 0 },
		"before month closed": func(r *MonthlyCandidateReview) { r.ReviewedAt = preparationAt("2026-06-30") },
		"future timestamp":    func(r *MonthlyCandidateReview) { r.ReviewedAt++ },
		"bundle digest":       func(r *MonthlyCandidateReview) { r.BundleSHA256 = strings.Repeat("a", 64) },
		"candidate digest":    func(r *MonthlyCandidateReview) { r.CandidateSHA256 = strings.Repeat("a", 64) },
		"uppercase digest":    func(r *MonthlyCandidateReview) { r.CandidateSHA256 = strings.ToUpper(r.CandidateSHA256) },
	} {
		t.Run(name, func(t *testing.T) {
			f := monthlyReviewFixture(t)
			var r MonthlyCandidateReview
			if err := json.Unmarshal(f.review, &r); err != nil {
				t.Fatal(err)
			}
			mutate(&r)
			if raw, err := ReviewMonthlyCandidate(f.input, f.context, f.bundle, monthlyReviewJSON(t, r), f.now); err == nil || raw != nil {
				t.Fatal("invalid review produced import file")
			}
		})
	}
}

func TestMonthlyReviewRejectsMalformedOrStaleInputs(t *testing.T) {
	for name, mutate := range map[string]func(*monthlyReviewInputs){
		"unknown review field": func(f *monthlyReviewInputs) { f.review = append([]byte(`{"apply":true,`), f.review[1:]...) },
		"duplicate field":      func(f *monthlyReviewInputs) { f.review = append([]byte(`{"version":1,`), f.review[1:]...) },
		"case alias": func(f *monthlyReviewInputs) {
			f.review = bytes.Replace(f.review, []byte(`"decision"`), []byte(`"Decision"`), 1)
		},
		"null confirmation": func(f *monthlyReviewInputs) {
			f.review = bytes.Replace(f.review, []byte(`"sources_reviewed":true`), []byte(`"sources_reviewed":null`), 1)
		},
		"null actor": func(f *monthlyReviewInputs) {
			f.review = bytes.Replace(f.review, []byte(`"reviewed_by":"synthetic-local-review-only"`), []byte(`"reviewed_by":null`), 1)
		},
		"trailing review":  func(f *monthlyReviewInputs) { f.review = append(f.review, []byte("{}")...) },
		"oversized review": func(f *monthlyReviewInputs) { f.review = bytes.Repeat([]byte(" "), MonthlyReviewRequestLimit+1) },
		"input changed":    func(f *monthlyReviewInputs) { f.input = append(f.input, '\n') },
		"context changed":  func(f *monthlyReviewInputs) { f.context = append(f.context, '\n') },
		"bundle changed":   func(f *monthlyReviewInputs) { f.bundle = append(f.bundle, '\n') },
	} {
		t.Run(name, func(t *testing.T) {
			f := monthlyReviewFixture(t)
			mutate(&f)
			if raw, err := ReviewMonthlyCandidate(f.input, f.context, f.bundle, f.review, f.now); err == nil || raw != nil {
				t.Fatal("stale or malformed review accepted")
			}
		})
	}
}

func TestMonthlyReviewRecomputesRatherThanTrustingEditedBundle(t *testing.T) {
	for name, mutate := range map[string]func(*MonthlyPreparationBundle){
		"balanced money tamper": func(b *MonthlyPreparationBundle) {
			u := &b.Entries[0].Candidate.Users[0]
			u.OpeningQuota++
			u.ClosingQuota++
			u.ReverseClosingQuota++
		},
		"account name tamper":        func(b *MonthlyPreparationBundle) { b.Entries[0].Members[0].Username = "other-account" },
		"omitted organization month": func(b *MonthlyPreparationBundle) { b.Entries = b.Entries[:2]; b.Blocked = 0 },
		"altered original report":    func(b *MonthlyPreparationBundle) { b.Reconciliation.Accounts[0].Months[0].Amounts.Consume = "0.000000" },
		"self claimed integrity":     func(b *MonthlyPreparationBundle) { b.SourceIntegrity = "not_checked_by_pure_calculator" },
	} {
		t.Run(name, func(t *testing.T) {
			f := monthlyReviewFixture(t)
			var b MonthlyPreparationBundle
			if err := json.Unmarshal(f.bundle, &b); err != nil {
				t.Fatal(err)
			}
			mutate(&b)
			b.Entries[0].CandidateSHA256 = monthlyDigest(monthlyReviewJSON(t, b.Entries[0].Candidate))
			f.bundle = monthlyReviewJSON(t, b)
			f.review = monthlyReviewJSON(t, monthlyReviewApproval(f.bundle, b.Entries[0], f.now))
			if raw, err := ReviewMonthlyCandidate(f.input, f.context, f.bundle, f.review, f.now); err == nil || raw != nil {
				t.Fatal("edited balanced payload bypassed source recomputation")
			}
		})
	}
	f := monthlyReviewFixture(t)
	var b MonthlyPreparationBundle
	if err := json.Unmarshal(f.bundle, &b); err != nil {
		t.Fatal(err)
	}
	r := monthlyReviewApproval(f.bundle, b.Entries[2], f.now)
	r.CandidateSHA256 = strings.Repeat("a", 64)
	if raw, err := ReviewMonthlyCandidate(f.input, f.context, f.bundle, monthlyReviewJSON(t, r), f.now); err == nil || raw != nil {
		t.Fatal("explicit approval hid an unresolved month")
	}
}
