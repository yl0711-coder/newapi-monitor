package monthlysource

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestFundingMonthFullCalendarTwoHundredAccountsAndBoundaryLogs(t *testing.T) {
	db, _ := fundingTestStore(t)
	ids := make([]int64, WalletBatchLimit)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	request, from, to := fundingMonthFixture(t, "2026-08", ids)
	base := (to + 86400) * 1000
	windows := make([]FundingWindowObservation, 0, 744)
	for at := from; at < to; at += 3600 {
		i := int64(len(windows))
		var rows []FundingLog
		if at == from || at+3600 == to {
			// Valid accounts 1..200, and the first/last second of the month.
			row := fundingMonthRow(i+1, 1, "10 点额度")
			row.CreatedAt = at
			if at != from {
				row.CreatedAt = to - 1
			}
			rows = []FundingLog{row}
		}
		windows = append(windows, fundingMonthObservation(t, db, request, at, at+3600, rows, rows, base+i*20, base+300000+i*20, 1000, 1000))
	}
	// Month assembly itself must need no receiver connection at all.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	got, err := reviewFundingMonth(context.Background(), request, windows, fundingMonthTestNow)
	if err != nil || len(got.MissingWindows) != 0 || len(got.Conflicts) != 0 || len(got.Accounts) != WalletBatchLimit {
		t.Fatal("complete bounded month assembly failed", err)
	}
	for _, account := range got.Accounts {
		if account.ObservedSeconds != to-from || account.SelectedSeconds != to-from || account.MatchingSeconds != to-from || len(account.Windows) != 744 {
			t.Fatal("full calendar not covered once", account.UserID)
		}
		for i, window := range account.Windows {
			version := got.Windows[window.WindowIndex]
			if window.WindowIndex != i || !window.Selected || version.NewerScanID == version.OlderScanID || !validDigest(version.NewerHeadID) || !validDigest(version.OlderHeadID) {
				t.Fatal("version lost in full month")
			}
		}
	}
	elapsed := time.Since(started)
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > 8<<20 {
		t.Fatal("shared window version metadata expanded into each account")
	}
	for _, forbidden := range []string{"complete", "publishable", "balance", "delta_quota", "quota_add", "admin_info"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatal("observation report acquired financial proof or raw contents", forbidden)
		}
	}
	t.Logf("744 windows / 200 accounts / 148,800 account-windows assembled in %s without DB access; serialized internal report %d bytes; synthetic sample only", elapsed, len(raw))
	started = time.Now()
	money, err := ReviewFundingMoney(context.Background(), request, windows)
	if err != nil || len(money.Accounts) != WalletBatchLimit {
		t.Fatal("full-sized money assembly failed", err)
	}
	for i, a := range money.Accounts {
		want := int64(0)
		if i == 0 {
			want = 20
			if len(a.Movements) != 2 || a.Movements[0].At != from || a.Movements[1].At != to-1 || a.Movements[1].WindowIndex != 743 {
				t.Fatal("first/last second money attribution changed")
			}
		}
		if a.Status != "observed_month" || a.Totals == nil || a.Totals.IncreaseQuota != want || a.Totals.DecreaseQuota != 0 {
			t.Fatal("full-sized money amounts changed", a.UserID)
		}
	}
	t.Logf("744 windows / 200 accounts including exact funding amounts in %s, receiver already closed; empty observations still not wallet proof", time.Since(started))
}

func TestFundingMonthInvalidRequestAndContextHelpers(t *testing.T) {
	request, from, to := fundingMonthFixture(t, "2026-09", []int64{201})
	for _, tc := range []struct {
		name string
		edit func(*FundingMonthRequest)
	}{
		{"wrong_rules", func(r *FundingMonthRequest) { r.Rules.Producer = "future" }},
		{"wrong_unit", func(r *FundingMonthRequest) { r.Rules.QuotaPerUnit++ }},
		{"missing_source", func(r *FundingMonthRequest) { r.SourceIdentity = "" }},
		{"missing_accounts", func(r *FundingMonthRequest) { r.UserIDs = nil }},
		{"duplicate_accounts", func(r *FundingMonthRequest) { r.UserIDs = []int64{201, 201} }},
		{"zero_account", func(r *FundingMonthRequest) { r.UserIDs = []int64{0} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := request
			tc.edit(&bad)
			if got, err := reviewFundingMonth(context.Background(), bad, nil, fundingMonthTestNow); err == nil || !reflect.DeepEqual(got, FundingMonthReview{}) {
				t.Fatal("invalid scope/source profile accepted", err)
			}
		})
	}
	db, _ := fundingTestStore(t)
	base := (to + 86400) * 1000
	o := fundingMonthObservation(t, db, request, from, from+3600, nil, nil, base, base+300000, 100, 100)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := validateFundingMonthWindows(ctx, request, request.UserIDs, []FundingWindowObservation{o}, from, to); err == nil {
		t.Fatal("validation ignored cancellation")
	}
	o.data.newerRecords[1] = fundingObservedRecord{target: 201, targetKnown: true}
	if _, err := fundingMonthConflicts(ctx, []*fundingWindowObservationData{o.data}, request.UserIDs); err == nil {
		t.Fatal("identity indexing ignored cancellation")
	}
	if _, err := fundingConflictIndex(ctx, []FundingCrossWindowConflict{{LogID: 1, AffectedUserIDs: []int64{201}}}); err == nil {
		t.Fatal("conflict expansion ignored cancellation")
	}
	if !reflect.DeepEqual(fundingUnionIDs([]int64{3, 1}, []int64{2, 3}), []int64{1, 2, 3}) || fundingUnionIDs(nil, nil) != nil {
		t.Fatal("issue identity deduplication changed")
	}
}

func TestFundingMonthIdentityBudgetCountsAllVersions(t *testing.T) {
	// White-box resource stress: start from genuinely audited empty windows,
	// then populate only the compact identity structures (no fake money).
	db, _ := fundingTestStore(t)
	request, from, to := fundingMonthFixture(t, "2026-09", []int64{201})
	base := (to + 86400) * 1000
	for _, changedOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed_only_%t", changedOnly), func(t *testing.T) {
			var windows []FundingWindowObservation
			for i := int64(0); i < 6; i++ {
				o := fundingMonthObservation(t, db, request, from+i*3600, from+(i+1)*3600, nil, nil, base+i*20, base+300000+i*20, 200000, 200000)
				if changedOnly {
					o.data.comparison.Changes = make([]FundingObservedChange, 20000)
				} else {
					for id := int64(1); id <= 20000; id++ {
						o.data.newerRecords[i*20000+id] = fundingObservedRecord{target: 201, targetKnown: true}
					}
				}
				windows = append(windows, o)
			}
			got, err := reviewFundingMonth(context.Background(), request, windows, fundingMonthTestNow)
			if err == nil || !strings.Contains(err.Error(), "identity budget") || !reflect.DeepEqual(got, FundingMonthReview{}) {
				t.Fatal("source identity/change budget bypassed", err)
			}
		})
	}
}

func TestFundingMonthAmbiguityCannotExpandWithoutLimit(t *testing.T) {
	db, _ := fundingTestStore(t)
	ids := make([]int64, WalletBatchLimit)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	request, from, to := fundingMonthFixture(t, "2026-09", ids)
	base := (to + 86400) * 1000
	var windows []FundingWindowObservation
	for i := int64(0); i < 6; i++ {
		rows := make([]FundingLog, 200)
		for j := range rows {
			row := fundingMonthRow(i*200+int64(j+1), 1, "1 点额度")
			row.Other = `{"op":{"action":"future.multi_account","params":{}}}`
			rows[j] = row
		}
		windows = append(windows, fundingMonthObservation(t, db, request, from+i*3600, from+(i+1)*3600, rows, rows, base+i*20, base+300000+i*20, 2000, 2000))
	}
	got, err := reviewFundingMonth(context.Background(), request, windows, fundingMonthTestNow)
	if err == nil || !strings.Contains(err.Error(), "issue-reference budget") || !reflect.DeepEqual(got, FundingMonthReview{}) {
		t.Fatal("unknown targets expanded to unbounded per-account report", err)
	}
	// Cross-window targets can similarly form a Cartesian expansion. Bound
	// the index before allocating the full account x window x log product.
	conflicts := make([]FundingCrossWindowConflict, 6)
	for i := range conflicts {
		conflicts[i] = FundingCrossWindowConflict{LogID: int64(i + 1), AffectedUserIDs: ids, Windows: make([]FundingInterval, 200)}
	}
	if _, err := fundingConflictIndex(context.Background(), conflicts); err == nil || !strings.Contains(err.Error(), "conflict-reference budget") {
		t.Fatal("conflict expansion lacks a budget", err)
	}
}
