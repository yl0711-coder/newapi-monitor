package monthlydraft

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

var fixtureNow = time.Date(2026, 10, 21, 12, 0, 0, 0, time.FixedZone("CST", 8*3600))

func draftFixture(t *testing.T) (monthlysource.FundingMonthRequest, []monthlysource.FundingWindowObservation, monitor.MonthlyPreparationInput) {
	t.Helper()
	r, windows, input, _ := draftFixtureRetained(t)
	return r, windows, input
}

func draftFixtureRetained(t *testing.T) (monthlysource.FundingMonthRequest, []monthlysource.FundingWindowObservation, monitor.MonthlyPreparationInput, string) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "observations.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	if err := monthlysource.InitFundingPageStore(ctx, db); err != nil {
		t.Fatal(err)
	}
	pin, err := monthlysource.WalletSourceIdentity("synthetic-draft", "synthetic-draft")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, fixtureNow.Location())
	from, to := start.Unix(), start.AddDate(0, 1, 0).Unix()
	req := monthlysource.FundingMonthRequest{Month: "2026-09", SourceIdentity: pin, Rules: monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000}, UserIDs: []int64{201, 202}}
	// 800 CNY transfer: two distinct source operations, operator 9, neither
	// operator ownership nor zero organization-wide net may replace the rows.
	rows := []monthlysource.FundingLog{
		{ID: 1, UserID: 9, CreatedAt: from, Type: 3, Other: `{"op":{"action":"user.quota_subtract","params":{"target_user_id":201,"quota":"400000000 点额度"}}}`},
		{ID: 2, UserID: 9, CreatedAt: from, Type: 3, Other: `{"op":{"action":"user.quota_add","params":{"target_user_id":202,"quota":"400000000 点额度"}}}`},
		{ID: 3, UserID: 201, CreatedAt: from + 1, Type: 1, Content: "使用在线充值成功，充值金额: $10.000000，支付金额：1.00"},
	}
	base := (to + 86400) * 1000
	var windows []monthlysource.FundingWindowObservation
	for at := from; at < to; at += 3600 {
		i := int64(len(windows))
		var scans []string
		for pass := int64(0); pass < 2; pass++ {
			scan, err := monthlysource.NewFundingScan(pin, req.Rules, req.UserIDs, at, at+3600)
			if err != nil {
				t.Fatal(err)
			}
			p := monthlysource.FundingPage{Version: 1, Scan: scan, FenceID: 100, EndReached: true, StartedAtMS: base + i*20 + pass*300000, CompletedAtMS: base + i*20 + pass*300000 + 10, Rows: []monthlysource.FundingLog{}}
			if at == from {
				p.Rows = rows
				p.Next = monthlysource.FundingCursor{At: from + 1, ID: 3}
			}
			if _, err := monthlysource.SaveFundingPage(ctx, db, p); err != nil {
				t.Fatal(err)
			}
			scans = append(scans, scan.ID)
		}
		o, err := monthlysource.ReviewFundingWindow(ctx, db, scans[0], scans[1])
		if err != nil {
			t.Fatal(err)
		}
		windows = append(windows, o)
	}
	input := monitor.MonthlyPreparationInput{Version: 1, Sources: []monitor.MonthlyPreparationSource{{ID: "fixture", Path: "/synthetic/not-a-production-source.json", SHA256: strings.Repeat("a", 64)}}}
	for _, id := range req.UserIDs {
		input.Accounts = append(input.Accounts, monitor.MonthlyPreparationAccount{
			UserID: id, GroupID: 1, Username: fmt.Sprintf("synthetic-%d", id), Months: []string{req.Month}, CounterBasis: "net_consumption", BasisSource: "fixture#counter-semantics",
			Baseline:         monitor.MonthlyPreparationBalance{UserID: id, At: from, BalanceQuota: 500000000, UsedQuota: 1000000000, Source: "fixture#exact-opening", TimeBasis: "source_log_cutoff", CutoffSource: "fixture#opening-cutoff-proof"},
			Snapshots:        []monitor.MonthlyPreparationBalance{{UserID: id, At: to, BalanceQuota: 899000000, UsedQuota: 1001000000, Source: "fixture#exact-closing", TimeBasis: "source_log_cutoff", CutoffSource: "fixture#closing-cutoff-proof"}},
			Spans:            []monitor.MonthlyPreparationSpan{{UserID: id, From: from, To: to, ConsumeQuota: 1500000, RefundQuota: 500000, Source: "fixture#usage"}},
			MovementCoverage: []monitor.MonthlyPreparationCoverage{{UserID: id, From: from, To: to, Source: "fixture#independent-funding-coverage"}},
			WalletMonths:     []monitor.MonthlyPreparationWalletMonth{{UserID: id, Month: req.Month, RefundQuota: 500000, Source: "fixture#actual-refund"}},
		})
	}
	a, b := &input.Accounts[0], &input.Accounts[1]
	a.Snapshots[0].BalanceQuota = 104000000 // 1000 - 800 + 10 - 3 + 1 = 208 CNY
	a.Movements = []monitor.MonthlyPreparationMovement{
		{UserID: 201, ID: 1, At: from, DeltaQuota: -400000000, Source: "fixture#transfer-out"},
		{UserID: 201, ID: 3, At: from + 1, DeltaQuota: 5000000, Source: "fixture#topup-credited-not-payment"},
	}
	b.Movements = []monitor.MonthlyPreparationMovement{{UserID: 202, ID: 2, At: from, DeltaQuota: 400000000, Source: "fixture#transfer-in"}}
	return req, windows, input, path
}

func draftJSON(t *testing.T, input monitor.MonthlyPreparationInput) []byte {
	t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func draftCopy(t *testing.T, input monitor.MonthlyPreparationInput) monitor.MonthlyPreparationInput {
	t.Helper()
	var result monitor.MonthlyPreparationInput
	if err := json.Unmarshal(draftJSON(t, input), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestObservedDraftWholeMonthMoneyAndFailures(t *testing.T) {
	req, windows, input := draftFixture(t)
	raw := draftJSON(t, input)
	good, err := ReconcileObservedMonth(context.Background(), req, windows, raw, fixtureNow)
	if err != nil {
		t.Fatal(err)
	}
	want := []monitor.MonthlyPreparationAmounts{
		{Opening: "1000.000000", Increase: "10.000000", Decrease: "-800.000000", Consume: "3.000000", Refund: "1.000000", Closing: "208.000000"},
		{Opening: "1000.000000", Increase: "800.000000", Decrease: "0.000000", Consume: "3.000000", Refund: "1.000000", Closing: "1798.000000"},
	}
	for i, a := range good.Reconciliation.Accounts {
		row := a.Months[0]
		if a.Error != "" || row.Status != "reconciled_draft" || row.Amounts == nil || *row.Amounts != want[i] || row.Requests != nil || row.Tokens != nil {
			t.Fatal("funding/actual refund/counter/unknown stats changed", a)
		}
	}
	if good.Reconciliation.SourceIntegrity != "not_checked_by_pure_calculator" || good.Mode != "local_observed_month_draft_not_publication" || string(raw) != string(draftJSON(t, input)) {
		t.Fatal("draft promoted itself to verified publication or mutated input")
	}
	// Exercise the public API with genuine audited page chains, not fabricated
	// report booleans. Every failure of account 201 must leave 202 unchanged.
	for name, change := range map[string]func(*monitor.MonthlyPreparationAccount){
		"one_point":                func(a *monitor.MonthlyPreparationAccount) { a.Movements[0].DeltaQuota++ },
		"same_total_wrong_ID":      func(a *monitor.MonthlyPreparationAccount) { a.Movements[0].ID = 7 },
		"same_total_wrong_time":    func(a *monitor.MonthlyPreparationAccount) { a.Movements[0].At++ },
		"same_total_wrong_account": func(a *monitor.MonthlyPreparationAccount) { a.Movements[0].UserID = 202 },
		"netted_rows": func(a *monitor.MonthlyPreparationAccount) {
			a.Movements[0].DeltaQuota += a.Movements[1].DeltaQuota
			a.Movements = a.Movements[:1]
		},
		"duplicate_row": func(a *monitor.MonthlyPreparationAccount) { a.Movements = append(a.Movements, a.Movements[0]) },
		"missing_row":   func(a *monitor.MonthlyPreparationAccount) { a.Movements = a.Movements[1:] },
		"extra_row": func(a *monitor.MonthlyPreparationAccount) {
			m := a.Movements[0]
			m.ID = 7
			a.Movements = append(a.Movements, m)
		},
		"payment_not_credit": func(a *monitor.MonthlyPreparationAccount) { a.Movements[1].DeltaQuota = 500000 },
		"no_coverage": func(a *monitor.MonthlyPreparationAccount) {
			a.MovementCoverage = []monitor.MonthlyPreparationCoverage{}
		},
		"coverage_gap":        func(a *monitor.MonthlyPreparationAccount) { a.MovementCoverage[0].To-- },
		"no_closing":          func(a *monitor.MonthlyPreparationAccount) { a.Snapshots = []monitor.MonthlyPreparationBalance{} },
		"approximate_closing": func(a *monitor.MonthlyPreparationAccount) { a.Snapshots[0].At++ },
		"collection_time_not_cutoff": func(a *monitor.MonthlyPreparationAccount) {
			a.Snapshots[0].TimeBasis, a.Snapshots[0].CutoffSource = "collection_observation", ""
		},
		"legacy_unknown_cutoff": func(a *monitor.MonthlyPreparationAccount) {
			a.Snapshots[0].TimeBasis, a.Snapshots[0].CutoffSource = "", ""
		},
		"counter_diff":     func(a *monitor.MonthlyPreparationAccount) { a.Snapshots[0].UsedQuota++ },
		"no_usage":         func(a *monitor.MonthlyPreparationAccount) { a.Spans = []monitor.MonthlyPreparationSpan{} },
		"no_actual_refund": func(a *monitor.MonthlyPreparationAccount) { a.WalletMonths = nil; a.Spans[0].ConsumeQuota++ },
	} {
		t.Run(name, func(t *testing.T) {
			bad := draftCopy(t, input)
			change(&bad.Accounts[0])
			r, err := ReconcileObservedMonth(context.Background(), req, windows, draftJSON(t, bad), fixtureNow)
			if err != nil {
				t.Fatal(err)
			}
			a := r.Reconciliation.Accounts[0]
			for _, row := range a.Months {
				if row.Amounts != nil || row.Status == "reconciled_draft" {
					t.Fatal("invalid money evidence produced amount", a)
				}
			}
			if !reflect.DeepEqual(good.Reconciliation.Accounts[1], r.Reconciliation.Accounts[1]) {
				t.Fatal("independent customer contaminated")
			}
		})
	}
	t.Run("undercharge_independently_proven", func(t *testing.T) {
		less := draftCopy(t, input)
		less.Accounts[0].Spans[0].ConsumeQuota += 909785
		r, err := ReconcileObservedMonth(context.Background(), req, windows, draftJSON(t, less), fixtureNow)
		if err != nil || r.Reconciliation.Accounts[0].Months[0].Amounts == nil || *r.Reconciliation.Accounts[0].Months[0].Amounts != want[0] || r.Reconciliation.Accounts[0].Anchors[0].Status != "mismatch" {
			t.Fatal("actual smaller debit replaced or internal discrepancy hidden", err)
		}
	})
	t.Run("negative_balance_preserved", func(t *testing.T) {
		x := draftCopy(t, input)
		x.Accounts[0].Baseline.BalanceQuota = 395500000
		x.Accounts[0].Snapshots[0].BalanceQuota = -500000
		r, err := ReconcileObservedMonth(context.Background(), req, windows, draftJSON(t, x), fixtureNow)
		if err != nil || r.Reconciliation.Accounts[0].Months[0].Amounts == nil || r.Reconciliation.Accounts[0].Months[0].Amounts.Closing != "-1.000000" || r.Reconciliation.Accounts[0].Months[0].Amounts.Consume != "3.000000" {
			t.Fatal("negative balance was clamped or changed consumption", err)
		}
	})
	t.Run("overcharge_not_automatically_billed", func(t *testing.T) {
		x := draftCopy(t, input)
		x.Accounts[0].Spans[0].ConsumeQuota--
		r, err := ReconcileObservedMonth(context.Background(), req, windows, draftJSON(t, x), fixtureNow)
		if err != nil || r.Reconciliation.Accounts[0].Months[0].Amounts != nil || r.Reconciliation.Accounts[0].Months[0].Status != "wallet_debit_above_logs" {
			t.Fatal("apparent extra debit became a bill", err)
		}
	})
	t.Run("missing_scan_no_fabricated_coverage", func(t *testing.T) {
		r, err := ReconcileObservedMonth(context.Background(), req, windows[:len(windows)-1], raw, fixtureNow)
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range r.Reconciliation.Accounts {
			if a.Months[0].Amounts != nil || a.Months[0].Status != "funding_incomplete_observation" || len(a.Anchors) != 0 {
				t.Fatal("unobserved funding allowed through existing calculator", a)
			}
		}
	})
	for name, edit := range map[string]func(*monitor.MonthlyPreparationInput){
		"extra_account":   func(x *monitor.MonthlyPreparationInput) { x.Accounts = append(x.Accounts, x.Accounts[0]) },
		"missing_account": func(x *monitor.MonthlyPreparationInput) { x.Accounts = x.Accounts[:1] },
		"foreign_account": func(x *monitor.MonthlyPreparationInput) { x.Accounts[0].UserID = 999 },
		"foreign_month":   func(x *monitor.MonthlyPreparationInput) { x.Accounts[0].Months = []string{"2026-08"} },
		"extra_month": func(x *monitor.MonthlyPreparationInput) {
			x.Accounts[0].Months = append(x.Accounts[0].Months, "2026-08")
		},
	} {
		t.Run(name, func(t *testing.T) {
			x := draftCopy(t, input)
			edit(&x)
			if r, err := ReconcileObservedMonth(context.Background(), req, windows, draftJSON(t, x), fixtureNow); err == nil || !reflect.DeepEqual(r, Review{}) {
				t.Fatal("mixed-scope review returned candidate", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if r, err := ReconcileObservedMonth(ctx, req, windows, raw, fixtureNow); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(r, Review{}) {
		t.Fatal("cancelled draft returned partial money", err)
	}
	draftSlots <- struct{}{}
	_, err = ReconcileObservedMonth(context.Background(), req, windows, raw, fixtureNow)
	<-draftSlots
	if !errors.Is(err, ErrDraftBusy) {
		t.Fatal("draft single-flight missing", err)
	}
	if _, err := ReconcileObservedMonth(context.Background(), req, windows, []byte(`{}`), fixtureNow); err == nil {
		t.Fatal("malformed preparation input accepted")
	}
	if _, err := ReconcileObservedMonth(context.Background(), req, []monthlysource.FundingWindowObservation{{}}, raw, fixtureNow); err == nil {
		t.Fatal("unaudited observations accepted")
	}
	again, err := ReconcileObservedMonth(context.Background(), req, windows, raw, fixtureNow)
	if err != nil || !reflect.DeepEqual(again, good) {
		t.Fatal("failed calls leaked slots or changed later result", err)
	}
}
