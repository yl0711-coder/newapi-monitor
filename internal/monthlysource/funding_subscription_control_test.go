package monthlysource

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestFundingSubscriptionControlsDoNotBlockUnrelatedWalletAccounts(t *testing.T) {
	for _, tc := range []struct{ method, route, path string }{
		{"POST", "/api/subscription/admin/users/:id/subscriptions", "/api/subscription/admin/users/147/subscriptions"},
		{"DELETE", "/api/subscription/admin/user_subscriptions/:id", "/api/subscription/admin/user_subscriptions/147"},
	} {
		db, _ := fundingTestStore(t)
		control := subscriptionControlRow(t, tc.method, tc.route, tc.path)
		control.ID, control.CreatedAt = 1, 1780000010
		rows := []FundingLog{control, fundingAuditRow(2, 201, "7 点额度"), fundingAuditRow(3, 202, "11 点额度")}
		older := saveFundingAuditFixture(t, db, fundingAuditFixture(t, rows, 1780100000000))
		newer := saveFundingAuditFixture(t, db, fundingAuditFixture(t, rows, 1780100300010))
		c, err := CompareFundingScans(context.Background(), db, older, newer)
		if err != nil || !c.ObservationsMatch || len(c.UnscopedReviewLogIDs) != 0 || len(c.Accounts) != 2 {
			t.Fatal("non-wallet subscription audit still blocks unrelated account scope", err, c)
		}
		for _, account := range c.Accounts {
			if !account.ObservationsMatch || len(account.ReviewLogIDs) != 0 || len(account.SeparateLedgerLogIDs) != 0 {
				t.Fatal("non-wallet audit was assigned to a wallet account", account)
			}
		}
		var retained int
		if err := db.QueryRow("SELECT COUNT(*) FROM portal_monthly_funding_records").Scan(&retained); err != nil || retained != 6 {
			t.Fatal("classification erased source audit records", retained, err)
		}
		// This source route really can debit the wallet. The exact reviewed
		// admin-control exemption must never hide its unresolved money.
		paid := subscriptionControlRow(t, "POST", "/api/subscription/balance/pay", "/api/subscription/balance/pay")
		paid.ID, paid.CreatedAt = 4, 1780000010
		mixed := append(append([]FundingLog(nil), rows...), paid)
		a := saveFundingAuditFixture(t, db, fundingAuditFixture(t, mixed, 1780100400000))
		b := saveFundingAuditFixture(t, db, fundingAuditFixture(t, mixed, 1780100700010))
		c, err = CompareFundingScans(context.Background(), db, a, b)
		if err != nil || !reflect.DeepEqual(c.UnscopedReviewLogIDs, []int64{4}) {
			t.Fatal("wallet purchase lost required review", err, c)
		}
	}
}

func subscriptionControlRow(t *testing.T, method, route, actualPath string) FundingLog {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"admin_info": map[string]any{"admin_id": 9},
		"op":         map[string]any{"action": "generic", "params": map[string]any{"method": method, "route": route}},
		"audit_info": map[string]any{"method": method, "route": route, "path": actualPath, "params": map[string]any{"id": "147"}, "status": 200, "success": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	return fundingTestRow(3, method+" "+route, string(raw))
}

func TestFundingSubscriptionAdminControlsNeverCreateWalletMoney(t *testing.T) {
	for _, tc := range []struct{ method, route, path string }{
		{"POST", "/api/subscription/admin/users/:id/subscriptions", "/api/subscription/admin/users/147/subscriptions"},
		{"DELETE", "/api/subscription/admin/user_subscriptions/:id", "/api/subscription/admin/user_subscriptions/147"},
	} {
		row := subscriptionControlRow(t, tc.method, tc.route, tc.path)
		original := row
		got := NormalizeFundingLog(row, fundingTestRules())
		if got.Status != FundingNonWallet || got.DeltaQuota != nil || got.TargetUserID != 0 || got.SourceDigest == "" || !reflect.DeepEqual(row, original) {
			t.Fatal("subscription control changed money, guessed actor as target or mutated original", got)
		}
		failed := row
		failed.Other = strings.ReplaceAll(strings.ReplaceAll(row.Other, `"status":200`, `"status":403`), `"success":true`, `"success":false`)
		if got := NormalizeFundingLog(failed, fundingTestRules()); got.Status != FundingNonWallet || got.DeltaQuota != nil {
			t.Fatal("failed non-wallet control turned into a wallet movement", got)
		}
		for _, kind := range []string{"content", "path-id", "noncanonical-id", "missing-id", "extra-id-field", "extra-op-money", "extra-envelope-money", "missing-audit", "missing-result", "null-result", "duplicate-id", "wrong-method", "balance-purchase", "future-route"} {
			bad := row
			switch kind {
			case "content":
				bad.Content = "unknown operation"
			case "path-id":
				bad.Other = strings.Replace(row.Other, `"id":"147"`, `"id":"148"`, 1)
			case "noncanonical-id":
				bad.Other = strings.Replace(row.Other, `"id":"147"`, `"id":"0147"`, 1)
			case "missing-id":
				bad.Other = strings.Replace(row.Other, `"params":{"id":"147"}`, `"params":{}`, 1)
			case "extra-id-field":
				bad.Other = strings.Replace(row.Other, `"id":"147"`, `"id":"147","quota":500000`, 1)
			case "extra-op-money":
				bad.Other = strings.Replace(row.Other, `"method":"`+tc.method+`","route":"`+tc.route+`"}`, `"method":"`+tc.method+`","route":"`+tc.route+`","quota":500000}`, 1)
			case "extra-envelope-money":
				bad.Other = strings.TrimSuffix(row.Other, "}") + `,"quota_change":500000}`
			case "missing-audit":
				bad.Other = `{"op":{"action":"generic","params":{"method":"` + tc.method + `","route":"` + tc.route + `"}}}`
			case "missing-result":
				bad.Other = strings.Replace(row.Other, `,"success":true`, "", 1)
			case "null-result":
				bad.Other = strings.Replace(row.Other, `"success":true`, `"success":null`, 1)
			case "duplicate-id":
				bad.Other = strings.Replace(row.Other, `"id":"147"`, `"id":"147","id":"148"`, 1)
			case "wrong-method":
				bad.Other = strings.ReplaceAll(row.Other, `"`+tc.method+`"`, `"PUT"`)
				bad.Content = "PUT " + tc.route
			case "balance-purchase":
				bad.Other = strings.ReplaceAll(row.Other, tc.route, "/api/subscription/balance/pay")
				bad.Content = tc.method + " /api/subscription/balance/pay"
			case "future-route":
				bad.Other = strings.ReplaceAll(row.Other, tc.route, tc.route+"/future")
				bad.Content = tc.method + " " + tc.route + "/future"
			}
			if bad == row {
				t.Fatal("negative fixture did not change the input", kind)
			}
			if got := NormalizeFundingLog(bad, fundingTestRules()); got.Status != FundingReview || got.DeltaQuota != nil {
				t.Fatal("unreviewed/money-affecting operation hidden", kind, got)
			}
		}
	}
	paid := fundingTestRow(1, "使用余额购买订阅成功，套餐: synthetic", "")
	if got := NormalizeFundingLog(paid, fundingTestRules()); got.Status != FundingReview || got.DeltaQuota != nil {
		t.Fatal("subscription wallet purchase silently excluded", got)
	}
}
