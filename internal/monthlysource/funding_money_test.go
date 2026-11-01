package monthlysource

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
)

func fundingMoneyMonth(t *testing.T, rows []FundingLog) (*sql.DB, FundingMonthRequest, []FundingWindowObservation) {
	t.Helper()
	db, _ := fundingTestStore(t)
	req, from, to := fundingMonthFixture(t, "2026-09", []int64{201, 202})
	base := (to + 86400) * 1000
	var windows []FundingWindowObservation
	for at := from; at < to; at += 3600 {
		i := int64(len(windows))
		var selected []FundingLog
		if at == from {
			selected = rows
		}
		windows = append(windows, fundingMonthObservation(t, db, req, at, at+3600, selected, selected, base+i*20, base+300000+i*20, 1000, 1000))
	}
	return db, req, windows
}

func TestFundingMoneyExactIndependentAmountsAndDetachedReports(t *testing.T) {
	credit := fundingMonthRow(1, 201, "400000000 点额度")
	debit := fundingMonthRow(2, 201, "400000000 点额度")
	debit.Other = strings.Replace(debit.Other, "user.quota_add", "user.quota_subtract", 1)
	rows := []FundingLog{credit, debit, fundingMonthRow(3, 202, "400000000 点额度"), fundingMonthRow(4, 201, "1 点额度"),
		{ID: 5, UserID: 201, Type: 4, Content: "通用安全验证成功 (验证方式: 2FA)"},
		fundingMonthRow(6, 999, "100 点额度"),
		// RC26 profile edits share the admin audit stream but do not alter
		// wallets. They must neither block this month nor add a money row.
		{ID: 7, UserID: 9, Type: 3, Content: "Updated user synthetic (ID: 201)", Other: fundingTestAudit("user.update", `{"id":201,"target_user_id":201,"username":"synthetic"}`)},
	}
	db, req, windows := fundingMoneyMonth(t, rows)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := ReviewFundingMoney(context.Background(), req, windows)
	if err != nil || got.Accounts[0].Status != "observed_month" || got.Accounts[1].Status != "observed_month" {
		t.Fatal("full observed month rejected", err, got.Accounts)
	}
	a, b := got.Accounts[0], got.Accounts[1]
	if *a.Totals != (FundingMoneyTotals{IncreaseQuota: 400000001, DecreaseQuota: -400000000}) || *b.Totals != (FundingMoneyTotals{IncreaseQuota: 400000000}) || len(a.Movements) != 3 || len(b.Movements) != 1 {
		t.Fatal("transfer netted, operator/outside account used, or one-point precision lost", a, b)
	}
	for i, id := range []int64{1, 2, 4} {
		m := a.Movements[i]
		if m.LogID != id || m.WindowIndex != 0 || !validDigest(m.PageID) || !validDigest(m.SourceDigest) || m.At != got.Windows.From+1 {
			t.Fatal("movement identity lost", m)
		}
	}
	before, _ := json.Marshal(got)
	got.Accounts[0].Totals.IncreaseQuota = 0
	got.Accounts[0].Movements[0].DeltaQuota = 0
	got.Windows.Accounts[0].Windows[0].Selected = false
	again, err := ReviewFundingMoney(context.Background(), req, windows)
	after, _ := json.Marshal(again)
	if err != nil || string(before) != string(after) {
		t.Fatal("editable report changed private evidence", err)
	}
	for _, rawField := range []string{"admin_info", "target_user_id", "quota_add", "quota_subtract", "2FA", "publishable", "complete\""} {
		if strings.Contains(string(before), rawField) {
			t.Fatal("raw metadata or publication assertion leaked", rawField)
		}
	}
}

func TestFundingMoneyIncompleteAndReviewNeverBecomeZero(t *testing.T) {
	db, req, windows := fundingMoneyMonth(t, []FundingLog{fundingMonthRow(1, 201, "1 点额度")})
	got, err := ReviewFundingMoney(context.Background(), req, windows[1:])
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range got.Accounts {
		if a.Status != "incomplete_observation" || a.Totals != nil || len(a.Movements) != 0 {
			t.Fatal("incomplete account reported monetary zero", a)
		}
	}
	from, to := got.Windows.From, got.Windows.To
	base := (to + 86400) * 1000
	for _, tc := range []struct {
		name string
		row  FundingLog
	}{
		{"unknown_amount", fundingMonthRow(2, 201, "unknown")},
		{"separate_ledger", FundingLog{ID: 2, UserID: 201, Type: 4, Content: "邀请用户赠送 1 点额度"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := tc.row
			w := append([]FundingWindowObservation(nil), windows...)
			w[0] = fundingMonthObservation(t, db, req, from, from+3600, []FundingLog{row}, []FundingLog{row}, base, base+300000, 1000, 1000)
			r, err := ReviewFundingMoney(context.Background(), req, w)
			if err != nil || r.Accounts[0].Totals != nil || r.Accounts[0].Status != "incomplete_observation" || r.Accounts[1].Status != "observed_month" || r.Accounts[1].Totals == nil || *r.Accounts[1].Totals != (FundingMoneyTotals{}) {
				t.Fatal("review item became zero or affected independent account", err, r.Accounts)
			}
		})
	}
	for _, name := range []string{"changed_amount", "cross_window_duplicate"} {
		t.Run(name, func(t *testing.T) {
			w := append([]FundingWindowObservation(nil), windows...)
			before := []FundingLog{fundingMonthRow(1, 201, "1 点额度")}
			after := []FundingLog{fundingMonthRow(1, 201, "2 点额度")}
			if name == "changed_amount" {
				w[0] = fundingMonthObservation(t, db, req, from, from+3600, before, after, base, base+300000, 1000, 1000)
			} else {
				w[1] = fundingMonthObservation(t, db, req, from+3600, from+7200, before, before, base+20, base+300020, 1000, 1000)
			}
			r, err := ReviewFundingMoney(context.Background(), req, w)
			if err != nil || r.Accounts[0].Totals != nil || len(r.Accounts[0].Movements) != 0 || r.Accounts[1].Totals == nil {
				t.Fatal("changed/duplicate source selected for money or affected unrelated account", err, r.Accounts)
			}
		})
	}
}

func TestFundingMoneyExactOverflowIsAccountLocal(t *testing.T) {
	db, req, windows := fundingMoneyMonth(t, []FundingLog{fundingMonthRow(1, 201, "9223372036854775807 点额度"), fundingMonthRow(2, 201, "1 点额度"), fundingMonthRow(3, 202, "1 点额度")})
	_ = db.Close()
	r, err := ReviewFundingMoney(context.Background(), req, windows)
	if err != nil || r.Accounts[0].Status != "amount_out_of_range" || r.Accounts[0].Totals != nil || len(r.Accounts[0].Movements) != 0 || r.Accounts[1].Totals == nil || r.Accounts[1].Totals.IncreaseQuota != 1 {
		t.Fatal("overflow wrapped or affected independent account", err, r.Accounts)
	}
	// The exact maximum itself remains valid and is not converted to float64.
	delete(windows[0].data.newerRecords, 2) // white-box arithmetic boundary only
	r, err = ReviewFundingMoney(context.Background(), req, windows)
	if err != nil || r.Accounts[0].Totals == nil || r.Accounts[0].Totals.IncreaseQuota != math.MaxInt64 {
		t.Fatal("valid exact maximum lost", err, r.Accounts)
	}
}

func TestFundingMoneyCancellationBusyAndUntrustedInput(t *testing.T) {
	req, _, _ := fundingMonthFixture(t, "2026-09", []int64{201})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if r, err := ReviewFundingMoney(ctx, req, nil); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(r, FundingMoneyReview{}) {
		t.Fatal("cancelled review returned partial result", err)
	}
	fundingMoneySlots <- struct{}{}
	_, err := ReviewFundingMoney(context.Background(), req, nil)
	<-fundingMoneySlots
	if !errors.Is(err, ErrFundingMoneyBusy) {
		t.Fatal("money single-flight missing", err)
	}
	if _, err := ReviewFundingMoney(context.Background(), req, []FundingWindowObservation{{}}); err == nil {
		t.Fatal("fabricated observation accepted")
	}
	if _, err := ReviewFundingMoney(context.Background(), req, nil); err != nil {
		t.Fatal("failed call leaked single-flight slot", err)
	}
}
