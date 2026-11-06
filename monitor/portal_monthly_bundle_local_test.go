package monitor

// Explicit opt-in retained-data acceptance. No production database, credentials
// or external service is loaded. Candidate review below is TEST ONLY; real
// candidates remain non-importable until an operator actually reviews them.
import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

func monthlyBundleAcceptanceEntries(t *testing.T, b MonthlyPreparationBundle, expected []monthlyBatchEntry) []monthlyBatchEntry {
	t.Helper()
	if b.Version != 1 || b.Mode != "local_candidate_bundle_not_publication" || len(b.Entries) != len(expected) {
		t.Fatal("unexpected bundle scope")
	}
	byKey := map[string]monthlyBatchEntry{}
	for _, e := range expected {
		var scope struct {
			GroupID int64 `json:"group_id"`
		}
		if err := json.Unmarshal(e.Evidence, &scope); err != nil || scope.GroupID <= 0 {
			t.Fatal("invalid comparison scope", err)
		}
		key := fmt.Sprintf("%d/%s", scope.GroupID, e.Month)
		if _, ok := byKey[key]; ok {
			t.Fatal("duplicate comparison scope")
		}
		byKey[key] = e
	}
	ready, blocked := 0, 0
	result := []monthlyBatchEntry{}
	for _, entry := range b.Entries {
		key := fmt.Sprintf("%d/%s", entry.GroupID, entry.Month)
		want, ok := byKey[key]
		if !ok || want.Account != entry.Account || len(want.Members) != len(entry.Members) {
			t.Fatal("customer name or scope changed", key)
		}
		delete(byKey, key)
		for i, member := range entry.Members {
			if member.UserID != want.Members[i].ID || member.Username != want.Members[i].Username {
				t.Fatal("member attribution changed", key)
			}
		}
		if len(want.Issues) != 0 {
			if entry.Status != "blocked" || entry.Candidate != nil || len(entry.Issues) == 0 {
				t.Fatal("unresolved historical month became a bill", key)
			}
			blocked++
			result = append(result, want)
			continue
		}
		if entry.Candidate == nil || entry.Status != "needs_review" || len(entry.Issues) != 0 {
			t.Fatal("previously reconciled bill not prepared", key, entry.Issues)
		}
		candidate := entry.Candidate
		raw, err := json.Marshal(candidate)
		if err != nil || monthlyDigest(raw) != entry.CandidateSHA256 {
			t.Fatal("candidate hash", err)
		}
		if _, err := decodeMonthlyBillEvidence(raw); err == nil {
			t.Fatal("unreviewed candidate accepted")
		}
		old, err := decodeMonthlyBillEvidence(want.Evidence)
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(old.Users, func(i, j int) bool { return old.Users[i].UserID < old.Users[j].UserID })
		if len(old.Users) != len(candidate.Users) {
			t.Fatal("account count changed", key)
		}
		for i, previous := range old.Users {
			current := candidate.Users[i]
			if previous.Accounting == nil || current.Accounting == nil || previous.Accounting.LogConsumeQuota != current.Accounting.LogConsumeQuota || previous.Accounting.LogRefundQuota != current.Accounting.LogRefundQuota {
				t.Fatal("log reference totals changed", key)
			}
			// Reference descriptions differ in the new formal preparation tool;
			// every monetary field, count and attributed source operation must not.
			previous.Accounting, current.Accounting = nil, nil
			previous.Events = append([]MonthlyBillCreditEvidence{}, previous.Events...)
			current.Events = append([]MonthlyBillCreditEvidence{}, current.Events...)
			for _, events := range [][]MonthlyBillCreditEvidence{previous.Events, current.Events} {
				sort.Slice(events, func(i, j int) bool { return events[i].ID < events[j].ID })
			}
			if !reflect.DeepEqual(previous, current) {
				t.Fatalf("account money or events changed %s/user=%d", key, current.UserID)
			}
		}
		reviewed := MonthlyBillEvidence{Version: 2, GroupID: candidate.GroupID, Month: candidate.Month, ReviewedBy: "synthetic-local-test-only", ReviewedAt: time.Now().Unix(), SourceReference: candidate.SourceReference, Users: candidate.Users}
		want.Evidence, err = json.Marshal(reviewed)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, want)
		ready++
	}
	if len(byKey) != 0 || ready != b.ReadyForReview || blocked != b.Blocked {
		t.Fatal("summary or scope mismatch")
	}
	return result
}

func TestMonthlyBundleSyntheticPublicationJourney(t *testing.T) {
	input := preparationFixture()
	b := monthlyBundleRun(t, input, monthlyBundleFixture(input))
	var expected []monthlyBatchEntry
	for i, values := range []struct {
		month, opening, closing, credit, adjustment, amount, refund, net string
		openingQ, closingQ, consumeQ, refundQ, requests, tokens          int64
		event                                                            MonthlyBillCreditEvidence
	}{
		{"2026-06", "1.000000", "5.000000", "10.000000", "0.000000", "6.000000", "0.000000", "6.000000", 500000, 2500000, 3000000, 0, 20, 200, MonthlyBillCreditEvidence{ID: 1, At: preparationAt("2026-06-01") + 10, DeltaQuota: 5000000, Kind: "recharge", Method: "后台充值"}},
		{"2026-07", "5.000000", "-3.500000", "0.000000", "-1.000000", "8.000000", "0.500000", "7.500000", 2500000, -1750000, 4000000, 250000, 10, 500, MonthlyBillCreditEvidence{ID: 2, At: preparationAt("2026-07-01"), DeltaQuota: -500000, Kind: "adjustment", Method: "余额扣减"}},
	} {
		_, end, err := monthlyWindow(values.month, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		u := MonthlyBillUserEvidence{UserID: 102, Complete: true, WalletOnly: true, Through: end.Unix(), ExpectedCreditEvents: 1, OpeningQuota: values.openingQ, ClosingQuota: values.closingQ, ReverseClosingQuota: values.closingQ, ConsumeQuota: values.consumeQ, RefundQuota: values.refundQ, Requests: values.requests, Tokens: values.tokens, Events: []MonthlyBillCreditEvidence{values.event}, Reversals: []MonthlyBillReversal{}, Accounting: &MonthlyBillAccountingEvidence{Basis: "independent_wallet_reconciliation", OpeningSource: "independent-test", ClosingSource: "independent-test", MovementSource: "independent-test", RefundSource: "independent-test", ReconciliationSource: "independent-test", LogConsumeQuota: values.consumeQ, LogRefundQuota: values.refundQ}}
		evidence := MonthlyBillEvidence{Version: 2, GroupID: 5, Month: values.month, ReviewedBy: "synthetic-test", ReviewedAt: time.Now().Unix(), SourceReference: "independent-test", Users: []MonthlyBillUserEvidence{u}}
		raw, err := json.Marshal(evidence)
		if err != nil {
			t.Fatal(err)
		}
		expected = append(expected, monthlyBatchEntry{Account: b.Entries[i].Account, Month: values.month, Members: []monthlyBatchMember{{ID: 102, Username: "account-102"}}, Evidence: raw, Expected: []monthlyMemberStatement{{UserID: 102, Username: "account-102", Opening: values.opening, Closing: values.closing, CreditTotal: values.credit, Adjustment: values.adjustment, Consumption: monthlyConsumption{Requests: values.requests, Tokens: values.tokens, Amount: values.amount, Refund: values.refund, Net: values.net}}}})
	}
	expected = append(expected, monthlyBatchEntry{Account: "synthetic", Month: "2026-08", Members: []monthlyBatchMember{{ID: 102, Username: "account-102"}}, Evidence: json.RawMessage(`{"group_id":5,"month":"2026-08"}`), Issues: []string{"independent history mismatch"}})
	monthlyBatchLocalJourney(t, monthlyBundleAcceptanceEntries(t, b, expected), t.TempDir())
}

func TestMonthlyBundleRetainedLocalJourney(t *testing.T) {
	bundlePath, expectedPath, out := os.Getenv("MONTHLY_CANDIDATE_BUNDLE"), os.Getenv("MONTHLY_BATCH_INPUT"), os.Getenv("MONTHLY_BATCH_OUTPUT")
	if bundlePath == "" || expectedPath == "" || out == "" {
		t.Skip("explicit local candidate, independent comparison and new output paths required")
	}
	for _, path := range []string{bundlePath, expectedPath, out} {
		if !filepath.IsAbs(path) {
			t.Fatal("absolute local paths required")
		}
	}
	raw, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	var b MonthlyPreparationBundle
	if err := json.Unmarshal(raw, &b); err != nil {
		t.Fatal(err)
	}
	if b.SourceIntegrity != "retained_files_sha256_verified_not_fact_attestation" {
		t.Fatal("CLI source verification required")
	}
	raw, err = os.ReadFile(expectedPath)
	if err != nil {
		t.Fatal(err)
	}
	var expected []monthlyBatchEntry
	if err := json.Unmarshal(raw, &expected); err != nil {
		t.Fatal(err)
	}
	monthlyBatchLocalJourney(t, monthlyBundleAcceptanceEntries(t, b, expected), out)
}
