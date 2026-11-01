package monthlysource

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func tokenMonthFixture() (FundingMonthRequest, []TokenUsageWindowPair, time.Time) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, fundingMonthZone)
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, fundingMonthZone)
	req := FundingMonthRequest{SourceIdentity: strings.Repeat("a", 64), Rules: fundingTestRules(), UserIDs: []int64{201}, Month: "2026-09"}
	pairs := []TokenUsageWindowPair{}
	for day := 0; day < 30; day++ {
		from := start.AddDate(0, 0, day).Unix()
		started := now.Add(-24*time.Hour).UnixMilli() + int64(day)*1000
		o := TokenUsageWindowObservation{Version: 1, Basis: tokenUsageBasis, Request: UsageWindowRequest{SourceIdentity: req.SourceIdentity, Rules: req.Rules, UserID: 201, From: from, To: from + 86400}, StartedAtMS: started, CompletedAtMS: started + 1, Wallet: WalletUser{UserID: 201}, Totals: []UsageWindowRow{}, Rows: []TokenUsageWindowRow{}}
		if day < 5 {
			token, kind, q, name := int64(11), 2, int64(500001), "same name"
			if day == 1 {
				token = 12
			}
			if day == 2 {
				kind, q = 6, 2
			}
			if day == 3 {
				token, q, name = 0, 1, "legacy name"
			}
			if day == 4 {
				q, name = 3, "renamed"
			}
			r := UsageWindowRow{UserID: 201, Type: kind, BillingSource: "wallet", Records: 1, Quota: q, MinQuota: q, MaxQuota: q, MinID: int64(day + 1), MaxID: int64(day + 1), FirstAt: from + 60, LastAt: from + 60}
			o.Totals = append(o.Totals, r)
			o.Rows = append(o.Rows, TokenUsageWindowRow{UsageWindowRow: r, TokenID: token, Name: name})
		}
		b := o
		b.StartedAtMS = o.CompletedAtMS + 300000
		b.CompletedAtMS = b.StartedAtMS + 1
		// Detach rows, so modifying one repeat in a test cannot change the first.
		b.Rows = append([]TokenUsageWindowRow{}, o.Rows...)
		b.Totals = append([]UsageWindowRow{}, o.Totals...)
		pairs = append(pairs, TokenUsageWindowPair{Older: o, Newer: b})
	}
	return req, pairs, now
}

func TestTokenUsageMonthExactCalendarMoneyNamesAndDetached(t *testing.T) {
	req, pairs, now := tokenMonthFixture()
	before, _ := json.Marshal(pairs)
	got, err := SummarizeTokenUsageMonth(context.Background(), req, pairs, now)
	if err != nil || got.ConsumeQuota != 1000006 || got.RefundQuota != 2 || len(got.Rows) != 3 || len(got.Sources) != 60 || got.UserID != 201 {
		t.Fatal(got, err)
	}
	if got.Rows[1].TokenID != 11 || got.Rows[1].ConsumeQuota != 500004 || got.Rows[1].RefundQuota != 2 || got.Rows[1].Name != "renamed" || got.Rows[2].TokenID != 12 || got.Rows[0].TokenID != 0 {
		t.Fatal("token identity, rename or legacy amount changed", got.Rows)
	}
	after, _ := json.Marshal(pairs)
	if string(before) != string(after) {
		t.Fatal("caller source modified")
	}
	for i, j := 0, len(pairs)-1; i < j; i, j = i+1, j-1 {
		pairs[i], pairs[j] = pairs[j], pairs[i]
	}
	again, err := SummarizeTokenUsageMonth(context.Background(), req, pairs, now)
	if err != nil || !reflect.DeepEqual(got, again) {
		t.Fatal("input order changed normalized month", err)
	}
	got.Rows[0].ConsumeQuota = 0
	got.Sources[0] = "changed"
	if reflect.DeepEqual(got, again) {
		t.Fatal("reports alias input/output")
	}
}

func TestTokenUsageMonthRejectsGapsChangesScopeAndZeroAssumptions(t *testing.T) {
	for _, scenario := range []string{"missing_empty_day", "duplicate_day", "same_log_different_day", "wrong_account", "wrong_source", "wrong_unit", "wrong_month", "missing_end", "changed_token", "changed_account", "too_soon", "same_capture", "future_capture", "subscription", "unclassified", "overflow", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			req, pairs, now := tokenMonthFixture()
			ctx := context.Background()
			switch scenario {
			case "missing_empty_day":
				pairs = append(pairs[:10], pairs[11:]...)
			case "duplicate_day":
				pairs = append(pairs, pairs[0])
			case "same_log_different_day":
				for _, o := range []*TokenUsageWindowObservation{&pairs[1].Older, &pairs[1].Newer} {
					o.Rows[0].MinID, o.Rows[0].MaxID = 1, 1
					o.Totals[0] = o.Rows[0].UsageWindowRow
				}
			case "wrong_account":
				req.UserIDs[0] = 202
			case "wrong_source":
				req.SourceIdentity = strings.Repeat("b", 64)
			case "wrong_unit":
				req.Rules.QuotaPerUnit = 1
			case "wrong_month":
				req.Month = "2026-08"
			case "missing_end":
				pairs = pairs[:29]
			case "changed_token":
				pairs[0].Newer.Rows[0].TokenID = 99
			case "changed_account":
				pairs[0].Newer.Totals[0].Quota++
			case "too_soon":
				pairs[0].Newer.StartedAtMS--
				pairs[0].Newer.CompletedAtMS--
			case "same_capture":
				pairs[0].Newer = pairs[0].Older
			case "future_capture":
				pairs[0].Newer.StartedAtMS = now.UnixMilli()
				pairs[0].Newer.CompletedAtMS = now.UnixMilli() + 1
			case "subscription", "unclassified":
				value := "subscription"
				if scenario == "unclassified" {
					value = "unspecified"
				}
				pairs[0].Older.Rows[0].BillingSource = value
				pairs[0].Older.Totals[0].BillingSource = value
				pairs[0].Newer.Rows[0].BillingSource = value
				pairs[0].Newer.Totals[0].BillingSource = value
			case "overflow":
				for i := 0; i < 2; i++ {
					for _, o := range []*TokenUsageWindowObservation{&pairs[i].Older, &pairs[i].Newer} {
						o.Rows[0].Quota = math.MaxInt64
						o.Rows[0].MinQuota = math.MaxInt64
						o.Rows[0].MaxQuota = math.MaxInt64
						o.Totals[0] = o.Rows[0].UsageWindowRow
					}
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if report, err := SummarizeTokenUsageMonth(ctx, req, pairs, now); err == nil || report.Rows != nil || report.ConsumeQuota != 0 {
				t.Fatal("bad source produced partial monthly amounts", report, err)
			}
		})
	}
}
