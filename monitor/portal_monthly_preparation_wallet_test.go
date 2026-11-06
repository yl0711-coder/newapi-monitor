package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"math/rand"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func monthlyWalletFixture() MonthlyPreparationInput {
	jun, jul := preparationAt("2026-06-01"), preparationAt("2026-07-01")
	return MonthlyPreparationInput{Version: 1,
		Sources: []MonthlyPreparationSource{{ID: "logs", Path: "/synthetic/wallet-source.json", SHA256: strings.Repeat("a", 64)}},
		Accounts: []MonthlyPreparationAccount{{UserID: 1, GroupID: 1, Username: "synthetic-wallet", Months: []string{"2026-06"}, CounterBasis: "net_consumption", BasisSource: "logs#basis",
			Baseline:         MonthlyPreparationBalance{UserID: 1, At: jun, BalanceQuota: 50000000, UsedQuota: 0, Source: "logs#opening", TimeBasis: monthlySourceLogCutoff, CutoffSource: "logs#opening-cutoff"},
			Snapshots:        []MonthlyPreparationBalance{{UserID: 1, At: jul, BalanceQuota: 100000000, UsedQuota: 200000000, Source: "logs#closing", TimeBasis: monthlySourceLogCutoff, CutoffSource: "logs#closing-cutoff"}},
			Spans:            []MonthlyPreparationSpan{{UserID: 1, From: jun, To: jul, ConsumeQuota: 200009000, RefundQuota: 0, Requests: preparationInt(10), Tokens: preparationInt(100), Source: "logs#usage"}},
			Movements:        []MonthlyPreparationMovement{{UserID: 1, ID: 1, At: jun + 10, DeltaQuota: 250000000, Source: "logs#credit"}},
			MovementCoverage: []MonthlyPreparationCoverage{{UserID: 1, From: jun, To: jul, Source: "logs#coverage"}},
			WalletMonths:     []MonthlyPreparationWalletMonth{{UserID: 1, Month: "2026-06", RefundQuota: 0, Source: "logs#actual-refunds"}},
		}},
	}
}

func TestMonthlyWalletUnderchargeUsesActualDebit(t *testing.T) {
	input := monthlyWalletFixture()
	before := monthlyReviewJSON(t, input)
	r := prepareTest(t, input).Accounts[0]
	row := r.Months[0]
	want := MonthlyPreparationAmounts{Opening: "100.000000", Increase: "500.000000", Decrease: "0.000000", Consume: "400.000000", Refund: "0.000000", Closing: "200.000000"}
	if r.Error != "" || row.Status != "reconciled_draft" || row.Amounts == nil || *row.Amounts != want || row.LogConsume != "400.018000" || row.Basis != monthlyExactWalletBasis || row.OpeningSource != "logs#opening" || row.RefundSource != "logs#actual-refunds" {
		t.Fatalf("actual debit did not survive preparation: %+v", r)
	}
	if r.Anchors[0].Status != "mismatch" || r.Anchors[0].BalanceGapQuota != "9000" {
		t.Fatal("internal log comparison disappeared", r.Anchors)
	}
	bundle := monthlyBundleRun(t, input, monthlyBundleFixture(input))
	if bundle.ReadyForReview != 1 || bundle.Blocked != 0 || bundle.Entries[0].Status != "needs_review" {
		t.Fatal("actual wallet month blocked", bundle.Entries)
	}
	u := bundle.Entries[0].Candidate.Users[0]
	if u.ConsumeQuota != 200000000 || u.ClosingQuota != 100000000 || u.ReverseClosingQuota != 100000000 || u.Accounting.LogConsumeQuota != 200009000 || u.Accounting.RefundSource != "logs#actual-refunds" {
		t.Fatal("bundle substituted logs for debit", u)
	}
	if !bytes.Equal(before, monthlyReviewJSON(t, input)) || !reflect.DeepEqual(bundle, monthlyBundleRun(t, input, monthlyBundleFixture(input))) {
		t.Fatal("preparation mutated input or became nondeterministic")
	}
}

func TestMonthlyWalletExactAmountsWithRefundsTransfersAndNegativeBalances(t *testing.T) {
	// All expected figures are integer quota; never round, use a tolerance, or
	// let a negative balance become a zero. Fixed seed makes failures repeatable.
	rng := rand.New(rand.NewSource(20261021))
	for i := 0; i < 200; i++ {
		input := monthlyWalletFixture()
		a := &input.Accounts[0]
		opening, credit, debit := rng.Int63n(10000000)-5000000, rng.Int63n(10000000)+1, -(rng.Int63n(10000000) + 1)
		gross, refund, gap := rng.Int63n(10000000), rng.Int63n(10000000), rng.Int63n(9001)
		closing := opening + credit + debit - gross + refund
		a.Baseline.BalanceQuota, a.Baseline.UsedQuota = opening, 1000000000
		a.Snapshots[0].BalanceQuota, a.Snapshots[0].UsedQuota = closing, a.Baseline.UsedQuota+gross-refund
		a.Spans[0].ConsumeQuota, a.Spans[0].RefundQuota = gross+gap, refund
		a.WalletMonths[0].RefundQuota = refund
		a.Movements[0].DeltaQuota = credit
		a.Movements = append(a.Movements, MonthlyPreparationMovement{UserID: 1, ID: 2, At: a.Baseline.At + 20, DeltaQuota: debit, Source: "logs#transfer-out"})
		b := monthlyBundleRun(t, input, monthlyBundleFixture(input))
		if b.Blocked != 0 || b.ReadyForReview != 1 {
			t.Fatalf("case %d rejected: %+v", i, b.Entries)
		}
		u := b.Entries[0].Candidate.Users[0]
		if u.OpeningQuota != opening || u.ClosingQuota != closing || u.ConsumeQuota != gross || u.RefundQuota != refund || u.ReverseClosingQuota != closing || u.Accounting.LogConsumeQuota != gross+gap || len(u.Events) != 2 || u.Events[1].DeltaQuota != debit {
			t.Fatalf("case %d changed exact amounts: %+v", i, u)
		}
	}
}

func TestMonthlyWalletMissingContradictoryOrForeignProofNeverFallsBack(t *testing.T) {
	for name, change := range map[string]func(*MonthlyPreparationAccount){
		"no refund proof":          func(a *MonthlyPreparationAccount) { a.WalletMonths = nil },
		"unknown refund source":    func(a *MonthlyPreparationAccount) { a.WalletMonths[0].Source = "absent#source" },
		"foreign refund":           func(a *MonthlyPreparationAccount) { a.WalletMonths[0].UserID++ },
		"foreign opening":          func(a *MonthlyPreparationAccount) { a.Baseline.UserID++ },
		"foreign closing":          func(a *MonthlyPreparationAccount) { a.Snapshots[0].UserID++ },
		"unknown month":            func(a *MonthlyPreparationAccount) { a.WalletMonths[0].Month = "2026-07" },
		"duplicate refund proof":   func(a *MonthlyPreparationAccount) { a.WalletMonths = append(a.WalletMonths, a.WalletMonths[0]) },
		"negative refund":          func(a *MonthlyPreparationAccount) { a.WalletMonths[0].RefundQuota = -1 },
		"counter discrepancy":      func(a *MonthlyPreparationAccount) { a.Snapshots[0].UsedQuota++ },
		"closing after boundary":   func(a *MonthlyPreparationAccount) { a.Snapshots[0].At++ },
		"opening before boundary":  func(a *MonthlyPreparationAccount) { a.Baseline.At-- },
		"funding coverage gap":     func(a *MonthlyPreparationAccount) { a.MovementCoverage[0].To-- },
		"consumption coverage gap": func(a *MonthlyPreparationAccount) { a.Spans[0].To-- },
		"net debit above logs":     func(a *MonthlyPreparationAccount) { a.Spans[0].ConsumeQuota = 199999999 },
		"net amount overflow": func(a *MonthlyPreparationAccount) {
			a.Baseline.BalanceQuota = math.MaxInt64
			a.Snapshots[0].BalanceQuota = math.MinInt64
		},
		"gross amount overflow": func(a *MonthlyPreparationAccount) { a.WalletMonths[0].RefundQuota = math.MaxInt64 },
	} {
		t.Run(name, func(t *testing.T) {
			input := monthlyWalletFixture()
			change(&input.Accounts[0])
			b := monthlyBundleRun(t, input, monthlyBundleFixture(input))
			if b.Blocked != 1 || b.ReadyForReview != 0 || b.Entries[0].Candidate != nil {
				t.Fatal("invalid wallet proof produced candidate", b.Entries)
			}
		})
	}
	input := monthlyWalletFixture()
	a := &input.Accounts[0]
	// Add a later globally log-matching snapshot. It must not waive the
	// explicitly chosen month's contradictory exact wallet counter.
	a.Snapshots[0].UsedQuota++
	a.Spans = append(a.Spans, MonthlyPreparationSpan{UserID: 1, From: preparationAt("2026-07-01"), To: preparationAt("2026-08-01"), Requests: preparationInt(0), Tokens: preparationInt(0), Source: "logs#empty-july"})
	a.MovementCoverage[0].To = preparationAt("2026-08-01")
	a.Snapshots = append(a.Snapshots, MonthlyPreparationBalance{UserID: 1, At: preparationAt("2026-08-01"), BalanceQuota: 99991000, UsedQuota: 200009000, Source: "logs#later-matched"})
	r := prepareTest(t, input).Accounts[0]
	if r.Anchors[1].Status != "matched" || r.Months[0].Status != "wallet_counter_mismatch" || r.Months[0].Amounts != nil {
		t.Fatal("bad direct proof silently fell back to logs", r)
	}
}

func TestMonthlyWalletLaterExactMonthDoesNotInheritOlderLogGap(t *testing.T) {
	input := monthlyWalletFixture()
	a := &input.Accounts[0]
	a.Months = []string{"2026-07"}
	a.WalletMonths[0].Month = "2026-07"
	a.MovementCoverage[0].To = preparationAt("2026-08-01")
	a.Spans = append(a.Spans, MonthlyPreparationSpan{UserID: 1, From: preparationAt("2026-07-01"), To: preparationAt("2026-08-01"), ConsumeQuota: 5000000, Requests: preparationInt(2), Tokens: preparationInt(20), Source: "logs#july"})
	a.Snapshots = append(a.Snapshots, MonthlyPreparationBalance{UserID: 1, At: preparationAt("2026-08-01"), BalanceQuota: 95000000, UsedQuota: 205000000, Source: "logs#august", TimeBasis: monthlySourceLogCutoff, CutoffSource: "logs#august-cutoff"})
	b := monthlyBundleRun(t, input, monthlyBundleFixture(input))
	if b.ReadyForReview != 1 || b.Blocked != 0 {
		t.Fatal("old gap poisoned exact later boundaries", b.Entries)
	}
	u := b.Entries[0].Candidate.Users[0]
	if u.OpeningQuota != 100000000 || u.ConsumeQuota != 5000000 || u.ClosingQuota != 95000000 || u.Accounting.OpeningSource != "logs#closing" {
		t.Fatal("historical gap was pushed into later month", u)
	}
}

func TestMonthlyWalletInputRequiresExplicitFields(t *testing.T) {
	base := string(monthlyReviewJSON(t, monthlyWalletFixture()))
	for name, raw := range map[string]string{
		"null collection":       strings.Replace(base, `"wallet_months":[{"user_id":1,"month":"2026-06","refund_quota":0,"source":"logs#actual-refunds"}]`, `"wallet_months":null`, 1),
		"wrong collection case": strings.Replace(base, `"wallet_months"`, `"Wallet_Months"`, 1),
		"missing refund":        strings.Replace(base, `"refund_quota":0,"source":"logs#actual-refunds"`, `"source":"logs#actual-refunds"`, 1),
		"null refund":           strings.Replace(base, `"refund_quota":0,"source":"logs#actual-refunds"`, `"refund_quota":null,"source":"logs#actual-refunds"`, 1),
		"wrong refund case":     strings.Replace(base, `"refund_quota":0,"source":"logs#actual-refunds"`, `"Refund_Quota":0,"source":"logs#actual-refunds"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if raw == base {
				t.Fatal("invalid mutation")
			}
			if _, err := DecodeMonthlyPreparationInput([]byte(raw)); err == nil {
				t.Fatal("absent evidence became a zero refund")
			}
		})
	}
}

func TestMonthlyWalletActualDebitReviewPublishDownloadJourney(t *testing.T) {
	input := monthlyWalletFixture()
	raw, meta := monthlyBundleRaw(t, input, monthlyBundleFixture(input))
	now := time.Unix(preparationAt("2026-10-21"), 0)
	b, err := PrepareMonthlyCandidateBundle(raw, meta, now)
	if err != nil {
		t.Fatal(err)
	}
	b.SourceIntegrity = "retained_files_sha256_verified_not_fact_attestation"
	b.Reconciliation.SourceIntegrity = b.SourceIntegrity
	bundle := monthlyReviewJSON(t, b)
	approval := monthlyReviewJSON(t, monthlyReviewApproval(bundle, b.Entries[0], now))
	evidence, err := ReviewMonthlyCandidate(raw, meta, bundle, approval, now)
	if err != nil {
		t.Fatal(err)
	}
	m, _, _ := newPortalTestMonitor(t)
	t.Cleanup(m.Close)
	m.cfg.PortalMonthlyStatementEnabled, m.cfg.LocalSnapshotOnly, m.cfg.AlertsDisabled = true, true, true
	g := CustomerGroup{ID: 1, Name: "synthetic wallet", PortalEmail: "wallet@example.invalid"}
	other := CustomerGroup{ID: 2, Name: "other synthetic", PortalEmail: "other@example.invalid"}
	for _, group := range []CustomerGroup{g, other} {
		if err := m.storeDB.Create(&group).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, user := range []TrackedUser{{UserID: 1, GroupID: 1, Username: "synthetic-wallet"}, {UserID: 2, GroupID: 2, Username: "other"}} {
		if err := m.storeDB.Create(&user).Error; err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ImportMonthlyBill(context.Background(), m.storeDB, evidence, true, now); err != nil {
		t.Fatal(err)
	}
	var record PortalMonthlyBill
	if err := m.storeDB.First(&record, "group_id = ? AND month = ?", 1, "2026-06").Error; err != nil {
		t.Fatal(err)
	}
	if _, err := ImportMonthlyBill(context.Background(), m.storeDB, evidence, true, now); err != nil {
		t.Fatal(err)
	}
	var again PortalMonthlyBill
	if err := m.storeDB.First(&again, "group_id = ? AND month = ?", 1, "2026-06").Error; err != nil || !reflect.DeepEqual(record, again) {
		t.Fatal("repeat changed bill", err)
	}
	r := gin.New()
	m.RegisterPortalRoutes(r)
	cookie := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(g.ID, g.PortalAuthVer, time.Now().Unix())}
	w := portalDo(r, "GET", "/api/monthly-statement?month=2026-06", "", cookie)
	if w.Code != 200 {
		t.Fatal("customer read failed", w.Code, w.Body.String())
	}
	var reply struct{ Data monthlyStatement }
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	p := reply.Data
	if p.Consumption.Amount != "400.000000" || p.Consumption.Net != "400.000000" || *p.MonthOpeningBalance != "100.000000" || *p.MonthEndBalance != "200.000000" || p.Credits.KnownTotal != "500.000000" || !monthlyStatementAmountsComplete(p) {
		t.Fatal("customer money changed", p)
	}
	// The random opaque publication ID can coincidentally contain "9000".
	// Exempt only that identifier; retain unknown response/data fields in the
	// privacy scan rather than projecting them away through the known DTO.
	var publicBody, publicData map[string]json.RawMessage
	if json.Unmarshal(w.Body.Bytes(), &publicBody) != nil || json.Unmarshal(publicBody["data"], &publicData) != nil {
		t.Fatal("invalid customer response")
	}
	delete(publicData, "publication")
	publicBody["data"] = monthlyReviewJSON(t, publicData)
	publicForScan := monthlyReviewJSON(t, publicBody)
	for _, secret := range []string{"400.018", "log_consume_quota", "logs#", "wallet_counter_mismatch", "9000"} {
		if bytes.Contains(publicForScan, []byte(secret)) {
			t.Fatal("private reconciliation leaked", secret)
		}
	}
	file := portalDo(r, "GET", "/api/monthly-statement/download?month=2026-06&publication="+p.Publication, "", cookie)
	if file.Code != 200 {
		t.Fatal("customer download failed", file.Code)
	}
	e, err := decodeMonthlyBillEvidence(evidence)
	if err != nil {
		t.Fatal(err)
	}
	monthlyAuditProjection(t, p, e, file.Body.Bytes())
	monthlyBatchCheckMemberSheet(t, file.Body.Bytes(), p.Members)
	foreign := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(other.ID, other.PortalAuthVer, time.Now().Unix())}
	if w := portalDo(r, "GET", "/api/monthly-statement?month=2026-06&group_id=1", "", foreign); w.Code != 404 {
		t.Fatal("another organization saw bill", w.Code)
	}
}
