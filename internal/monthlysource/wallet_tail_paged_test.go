package monthlysource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestWalletTailPagedOneViewExactGrossRefundAndFundingOrdering(t *testing.T) {
	source, d, now := pageTailSource(t, "user_usage", nil)
	b := d.r.Wallet.Boundary
	var consume, refund, cn, rn int64
	for i := int64(1); i <= walletTailPageRows+37; i++ {
		item := pageTailItem{id: i, at: b + 10 + i/10000, typ: 2, user: 201, quota: i % 11, billing: "wallet"}
		if i%7 == 0 {
			item.typ = 6
		}
		if i == 1 {
			item.quota = 9007199254740993
		}
		if item.typ == 2 {
			consume += item.quota
			cn++
		} else {
			refund += item.quota
			rn++
		}
		d.items = append(d.items, item)
	}
	for i := int64(1); i <= 401; i++ {
		item := pageTailItem{id: 90000 + i, at: b + 1, typ: 1, user: 201, content: "使用Creem充值成功，充值额度: 7，支付金额：0.01"}
		if i%2 == 0 {
			item.typ, item.user, item.other = 3, 999, fundingTestAudit("user.quota_add", `{"target_user_id":201,"quota":"3 点额度"}`)
		}
		d.items = append(d.items, item)
	}
	d.items = append(d.items, pageTailItem{id: 999991, at: b + 1, typ: 1, user: 202, content: "foreign-private-topup"}, pageTailItem{id: 999992, at: b + 1, typ: 2, user: 202, quota: 999999, billing: "wallet"})
	o, err := captureWalletTailPaged(t.Context(), source, d.r.Wallet, func() time.Time { return now }, walletTailJournalPages)
	if err != nil {
		t.Fatal(err)
	}
	if ValidateWalletTailObservation(o) != nil || len(o.Usage) != 2 || len(o.Funding) != 401 || o.Usage[0].Quota != consume || o.Usage[1].Quota != refund || o.Usage[0].Records != cn || o.Usage[1].Records != rn {
		t.Fatal("same-view collection lost, rounded or netted source money", err)
	}
	for i, row := range o.Funding {
		if row.ID != int64(90001+i) || row.Type == 3 && row.UserID != 999 {
			t.Fatal("funding order or actor changed", row)
		}
	}
	users, identities := 0, 0
	for _, q := range d.s.queries {
		if strings.Contains(q, "FROM users") {
			users++
		}
		if strings.Contains(q, "@@server_uuid") {
			identities++
		}
		if strings.Contains(q, "logs") && !strings.Contains(q, "MAX_EXECUTION_TIME(2000)") {
			t.Fatal("unbounded source SQL")
		}
		if strings.Contains(q, "SUM(") && (!strings.Contains(q, "WITH usage_keys AS") || !strings.Contains(q, "LIMIT 8192")) {
			t.Fatal("whole-month aggregation before page limit")
		}
	}
	if users != 1 || identities != 1 || !d.s.options.ReadOnly || d.s.options.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) || !d.s.committed || source.Stats().InUse != 0 {
		t.Fatal("wallet/view reread or source connection leaked", users, identities, d.s.options)
	}
	raw, _ := json.Marshal(o)
	if strings.Contains(string(raw), "foreign-private-topup") {
		t.Fatal("foreign topup leaked")
	}
}

func TestWalletTailPagedFreshViewIncludesBackfillAndPreservesExistingBoundaryGate(t *testing.T) {
	source, d, now := pageTailSource(t, "user_usage", nil)
	b := d.r.Wallet.Boundary
	var initialNet int64
	for i := int64(1); i <= walletTailPageRows+3; i++ {
		d.items = append(d.items, pageTailItem{id: i, at: b + 10, typ: 2, user: 201, quota: i % 5, billing: "wallet"})
		initialNet += i % 5
	}
	old, err := captureWalletTailPaged(t.Context(), source, d.r.Wallet, func() time.Time { return now }, walletTailJournalPages)
	if err != nil {
		t.Fatal(err)
	}
	// A late event before the old cursor, a refund and a topup enter the NEXT
	// complete source view. No old-cache amount or timestamp is imported.
	d.items = append(d.items,
		pageTailItem{id: 90001, at: b + 1, typ: 2, user: 201, quota: 17, billing: "wallet"},
		pageTailItem{id: 90002, at: b + 1, typ: 6, user: 201, quota: 11, billing: "wallet"},
		pageTailItem{id: 90003, at: b + 1, typ: 1, user: 201, content: "使用Creem充值成功，充值额度: 37，支付金额：0.01"})
	d.s.data[0][2] = int64(12345 - 17 + 11 + 37)
	d.s.data[0][3] = int64(9007199254740993 + 17 - 11)
	now = now.Add(5 * time.Minute)
	fresh, err := captureWalletTailPaged(t.Context(), source, d.r.Wallet, func() time.Time { return now }, walletTailJournalPages)
	if err != nil {
		t.Fatal(err)
	}
	point, err := DeriveWalletTailBoundary(old, fresh)
	if err != nil || point.BalanceQuota() != 12345+initialNet || point.UsedQuota() != 9007199254740993-initialNet {
		t.Fatal("existing independent-view money gate disagrees", point, err)
	}
	if fresh.Usage[0].Quota != old.Usage[0].Quota+17 || fresh.Usage[1].Quota != 11 || len(fresh.Funding) != 1 {
		t.Fatal("late event was omitted")
	}
	// A money mismatch must STILL be rejected, not hidden as a cache correction.
	*fresh.User.BalanceQuota += 1
	if _, err := DeriveWalletTailBoundary(old, fresh); err == nil {
		t.Fatal("existing mismatch gate weakened")
	}
}

func TestWalletTailPagedPreservesAllBillingClassesWithoutAssumingWallet(t *testing.T) {
	source, d, now := pageTailSource(t, "user_usage", nil)
	for i, class := range []string{"wallet", "subscription", "unspecified", "invalid_json", "invalid_source"} {
		for _, typ := range []int64{2, 6} {
			d.items = append(d.items, pageTailItem{id: int64(i*10) + typ, at: d.r.Wallet.Boundary + 1, typ: typ, user: 201, quota: 2, billing: class})
		}
	}
	o, err := captureWalletTailPaged(t.Context(), source, d.r.Wallet, func() time.Time { return now }, walletTailJournalPages)
	if err != nil || len(o.Usage) != usageWindowRowsLimit || ValidateWalletTailObservation(o) != nil {
		t.Fatal(o, err)
	}
	newer := o
	newer.StartedAtMS += 300000
	newer.CompletedAtMS += 300000
	if _, err := DeriveWalletTailBoundary(o, newer); err == nil {
		t.Fatal("unknown/subscription source became wallet money")
	}
}

func TestWalletTailPagedFailureHasNoPartialViewOrBudgetReset(t *testing.T) {
	for _, fault := range []string{"nil_context", "cancel", "identity", "index", "body", "funding_index", "commit", "budget", "cross_month", "too_long"} {
		t.Run(fault, func(t *testing.T) {
			source, d, now := pageTailSource(t, "user_usage", []pageTailItem{{id: 1, at: 1790784001, typ: 2, user: 201, quota: 3, billing: "wallet"}})
			ctx, request, budget := t.Context(), d.r.Wallet, walletTailJournalPages
			calls := 0
			clock := func() time.Time {
				calls++
				if calls > 1 && fault == "too_long" {
					return now.Add(5001 * time.Millisecond)
				}
				if calls > 1 && fault == "cross_month" {
					return now.AddDate(0, 1, 0)
				}
				return now
			}
			switch fault {
			case "nil_context":
				ctx = nil
			case "cancel":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			case "identity":
				request.SourceIdentity = strings.Repeat("a", 64)
			case "index":
				d.fail = "index"
			case "body":
				d.fail = "body"
			case "funding_index":
				d.onQuery = func(q string) {
					if strings.Contains(q, "idx_created_at_type") {
						d.fail = "index"
					}
				}
			case "commit":
				d.s.commitErr = errors.New("synthetic-private-failure")
			case "budget":
				budget = 1
			}
			o, err := captureWalletTailPaged(ctx, source, request, clock, budget)
			if err == nil || !reflect.DeepEqual(o, WalletTailObservation{}) || d.s.committed || source.Stats().InUse != 0 || strings.Contains(err.Error(), "synthetic-private-failure") {
				t.Fatal("failure returned partial or leaked source data", o, err)
			}
			wantStage := "tail_usage"
			if fault == "funding_index" {
				wantStage = "tail_funding"
			}
			if (fault == "index" || fault == "funding_index") && (!errors.Is(err, context.DeadlineExceeded) || WalletTailFailureStage(err) != wantStage) {
				t.Fatal("lost safe failure stage", err)
			}
		})
	}
}

func TestWalletTailPagedFundingLimitsRemainWholeViewLimits(t *testing.T) {
	for _, bound := range []string{"rows", "bytes"} {
		t.Run(bound, func(t *testing.T) {
			source, d, now := pageTailSource(t, "global_funding", nil)
			n, other := walletTailRowsLimit+1, ""
			if bound == "bytes" {
				n, other = 601, strings.Repeat("x", 4000)
			}
			for i := 1; i <= n; i++ {
				d.items = append(d.items, pageTailItem{id: int64(i), at: d.r.Wallet.Boundary + 1, typ: 3, user: 999, content: "actor", other: other})
			}
			o, err := captureWalletTailPaged(t.Context(), source, d.r.Wallet, func() time.Time { return now }, walletTailJournalPages)
			if err == nil || !reflect.DeepEqual(o, WalletTailObservation{}) || d.s.committed || WalletTailFailureStage(err) != "tail_funding" {
				t.Fatal("per-page budget enlarged original whole-view budget", o, err)
			}
		})
	}
}

func TestWalletTailPagedGrossOverflowIsNotNettedAgainstRefund(t *testing.T) {
	totals := map[string]*pagedTailUsageTotal{}
	rows := []UsageWindowRow{{UserID: 201, Type: 2, BillingSource: "wallet", Records: 1, Quota: math.MaxInt64}, {UserID: 201, Type: 2, BillingSource: "wallet", Records: 1, Quota: 1}, {UserID: 201, Type: 6, BillingSource: "wallet", Records: 1, Quota: math.MaxInt64}}
	if err := foldPagedTailUsage(totals, rows); err != nil {
		t.Fatal(err)
	}
	if _, err := finishPagedTailUsage(totals); err == nil {
		t.Fatal("overflowing gross amount was hidden in net consumption")
	}
}
