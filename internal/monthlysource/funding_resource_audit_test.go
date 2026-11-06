package monthlysource

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestFundingReviewedResourceAuditsDoNotCreateMoney(t *testing.T) {
	for _, tc := range []struct{ action, params string }{
		{"channel.status_update", `{"id":77,"status":2,"changed":true}`},
		{"channel.update", `{"id":77,"name":"synthetic","changed_fields":["models","group"]}`},
		{"channel.update", `{"id":77,"name":"synthetic","changed_fields":[]}`},
		{"channel.create", `{"name":"synthetic","type":1,"count":2}`},
		{"option.update", `{"key":"ModelRatio"}`},
		{"channel.copy", `{"sourceId":77,"id":78,"name":"synthetic-copy"}`},
		{"channel.delete", `{"id":77,"name":"synthetic-deleted"}`},
	} {
		row := fundingTestRow(3, "source resource audit", fundingTestAudit(tc.action, tc.params))
		got := NormalizeFundingLog(row, fundingTestRules())
		if got.Status != FundingNonWallet || got.DeltaQuota != nil || got.TargetUserID != 0 || got.SourceDigest == "" {
			t.Fatal("resource event changed wallet or remained globally unknown", tc.action, got)
		}
		row.Other = strings.Replace(row.Other, tc.params, strings.TrimSuffix(tc.params, "}")+`,"quota":"1 点额度"}`, 1)
		if got := NormalizeFundingLog(row, fundingTestRules()); got.Status != FundingReview || got.DeltaQuota != nil {
			t.Fatal("future resource money field silently ignored", tc.action, got)
		}
	}
}

func TestFundingResourceAuditsRejectUnreviewedOrAmbiguousShapes(t *testing.T) {
	for _, tc := range []struct{ action, params string }{
		{"channel.future", `{"id":77}`},
		{"channel.update", `{"id":77,"name":"x","changed_fields":["quota"]}`},
		{"channel.update", `{"id":77,"name":"x","changed_fields":null}`},
		{"channel.update", `{"id":77,"name":"x","changed_fields":["models","models"]}`},
		{"channel.update", `{"id":77,"id":78,"name":"x","changed_fields":[]}`},
		{"channel.status_update", `{"id":77,"status":3,"changed":true}`},
		{"channel.status_update", `{"id":77,"status":2,"changed":null}`},
		{"channel.create", `{"name":"x","type":1,"count":null}`},
		{"option.update", `{"key":null}`},
		{"option.update", `{"key":"ModelRatio","value":"opaque"}`},
		{"channel.copy", `{"sourceId":77,"id":77,"name":"same-id"}`},
		{"channel.copy", `{"source_id":77,"id":78,"name":"wrong-shape"}`},
		{"option.payment_compliance", `{"confirmed":true}`},
		{"user.create", `{"target_user_id":202,"role":1,"username":"x","quota":10}`},
		{"user.create", `{"target_user_id":null,"role":1,"username":"x"}`},
		{"user.create", `{"role":1,"username":"x"}`},
	} {
		row := fundingTestRow(3, "cannot classify from text", fundingTestAudit(tc.action, tc.params))
		got := NormalizeFundingLog(row, fundingTestRules())
		if got.Status != FundingReview || got.DeltaQuota != nil || got.TargetUserID != 0 {
			t.Fatal("ambiguous event lost its review requirement", tc, got)
		}
	}
}

func TestFundingGenericRoutesRequireExactReviewedAuditContext(t *testing.T) {
	for _, route := range []string{"PUT /api/channel/", "POST /api/channel/fetch_models", "POST /api/channel/upstream_updates/detect"} {
		method, path, _ := strings.Cut(route, " ")
		params := `{"method":"` + method + `","route":"` + path + `"}`
		audit := `{"method":"` + method + `","route":"` + path + `","path":"` + path + `","status":200,"success":true}`
		row := fundingTestRow(3, route, `{"op":{"action":"generic","params":`+params+`},"audit_info":`+audit+`}`)
		if got := NormalizeFundingLog(row, fundingTestRules()); got.Status != FundingNonWallet || got.DeltaQuota != nil {
			t.Fatal("reviewed fallback blocked unrelated wallet", got)
		}
		for _, kind := range []string{"missing-audit", "different-route", "different-content", "future-field", "duplicate-key", "wallet-route", "null-result"} {
			bad := row
			switch kind {
			case "missing-audit":
				bad.Other = fundingTestAudit("generic", params)
			case "different-route":
				bad.Other = strings.Replace(row.Other, `"path":"`+path+`"`, `"path":"/api/user/manage"`, 1)
			case "different-content":
				bad.Content = "some unrelated operation"
			case "future-field":
				bad.Other = strings.Replace(row.Other, params, strings.TrimSuffix(params, "}")+`,"quota":100}`, 1)
			case "duplicate-key":
				bad.Other = strings.Replace(row.Other, `"status":200`, `"status":200,"status":500`, 1)
			case "wallet-route":
				bad.Other = strings.ReplaceAll(row.Other, path, "/api/user/manage")
				bad.Content = method + " /api/user/manage"
			case "null-result":
				bad.Other = strings.Replace(row.Other, `"success":true`, `"success":null`, 1)
			}
			if got := NormalizeFundingLog(bad, fundingTestRules()); got.Status != FundingReview || got.DeltaQuota != nil {
				t.Fatal("unreviewed generic operation excluded", kind, got)
			}
		}
	}
}

func TestFundingReviewedComplianceAndDynamicKeyAudit(t *testing.T) {
	compliance := fundingTestRow(3, "POST /api/option/payment_compliance", `{"op":{"action":"option.payment_compliance","params":{}},"audit_info":{"method":"POST","route":"/api/option/payment_compliance","path":"/api/option/payment_compliance","status":200,"success":true}}`)
	key := fundingTestRow(3, "POST /api/channel/:id/key", `{"op":{"action":"generic","params":{"method":"POST","route":"/api/channel/:id/key"}},"audit_info":{"method":"POST","route":"/api/channel/:id/key","path":"/api/channel/77/key","params":{"id":"77"},"status":403,"success":false}}`)
	for _, row := range []FundingLog{compliance, key} {
		if got := NormalizeFundingLog(row, fundingTestRules()); got.Status != FundingNonWallet || got.DeltaQuota != nil || got.TargetUserID != 0 {
			t.Fatal("reviewed route blocked wallets or created money", got)
		}
	}
	absent := compliance
	absent.Other = strings.Replace(compliance.Other, `,"params":{}`, "", 1)
	if got := NormalizeFundingLog(absent, fundingTestRules()); got.Status != FundingNonWallet || got.DeltaQuota != nil {
		t.Fatal("source-omitted empty params incorrectly blocked wallet", got)
	}
	explicitNull := compliance
	explicitNull.Other = strings.Replace(compliance.Other, `"params":{}`, `"params":null`, 1)
	if got := NormalizeFundingLog(explicitNull, fundingTestRules()); got.Status != FundingReview {
		t.Fatal("explicit null treated as source-omitted params", got)
	}
	absent.Other = `{"op":{"action":"option.payment_compliance"}}`
	if got := NormalizeFundingLog(absent, fundingTestRules()); got.Status != FundingReview {
		t.Fatal("missing route proof silently excluded", got)
	}
	for _, action := range []string{"user.quota_add", "generic", "channel.update"} {
		bad := fundingTestRow(3, "unknown", `{"op":{"action":"`+action+`"}}`)
		if got := NormalizeFundingLog(bad, fundingTestRules()); got.Status != FundingReview || got.DeltaQuota != nil {
			t.Fatal("missing money/generic params excluded", action, got)
		}
	}
	for _, kind := range []string{"id-path-mismatch", "noncanonical-id", "extra-param", "missing-params", "wallet-endpoint", "wrong-method", "missing-result"} {
		bad := key
		switch kind {
		case "id-path-mismatch":
			bad.Other = strings.Replace(key.Other, `"id":"77"`, `"id":"78"`, 1)
		case "noncanonical-id":
			bad.Other = strings.Replace(key.Other, `"id":"77"`, `"id":"077"`, 1)
		case "extra-param":
			bad.Other = strings.Replace(key.Other, `"id":"77"`, `"id":"77","quota":1`, 1)
		case "missing-params":
			bad.Other = strings.Replace(key.Other, `"params":{"id":"77"},`, "", 1)
		case "wallet-endpoint":
			bad.Other = strings.ReplaceAll(key.Other, "/api/channel/", "/api/user/")
			bad.Content = "POST /api/user/:id/key"
		case "wrong-method":
			bad.Other = strings.ReplaceAll(key.Other, `"POST"`, `"PUT"`)
			bad.Content = "PUT /api/channel/:id/key"
		case "missing-result":
			bad.Other = strings.Replace(key.Other, `"success":false`, `"success":null`, 1)
		}
		if got := NormalizeFundingLog(bad, fundingTestRules()); got.Status != FundingReview || got.DeltaQuota != nil {
			t.Fatal("unreviewed dynamic operation ignored", kind, got)
		}
	}
	bad := compliance
	bad.Other = strings.Replace(compliance.Other, `"params":{}`, `"params":{"quota":500000}`, 1)
	if got := NormalizeFundingLog(bad, fundingTestRules()); got.Status != FundingReview {
		t.Fatal("compliance schema drift hidden", got)
	}
}

func TestFundingCreatedUserReviewRemainsAccountLocal(t *testing.T) {
	created := FundingLog{ID: 1, UserID: 100, CreatedAt: 1780000010, Type: 3, Other: fundingTestAudit("user.create", `{"target_user_id":202,"role":1,"username":"synthetic"}`)}
	r := NormalizeFundingLog(created, fundingTestRules())
	if r.Status != FundingReview || r.TargetUserID != 202 || r.DeltaQuota != nil {
		t.Fatal("user creation invented balance or guessed actor", r)
	}
	db, _ := fundingTestStore(t)
	rows := []FundingLog{created, fundingAuditRow(2, 201, "7 点额度")}
	older := saveFundingAuditFixture(t, db, fundingAuditFixture(t, rows, 1780100000000))
	newer := saveFundingAuditFixture(t, db, fundingAuditFixture(t, rows, 1780100005000))
	comparison, err := CompareFundingScans(context.Background(), db, older, newer)
	if err != nil || len(comparison.UnscopedReviewLogIDs) != 0 || len(comparison.Accounts[0].ReviewLogIDs) != 0 || !reflect.DeepEqual(comparison.Accounts[1].ReviewLogIDs, []int64{1}) {
		t.Fatal("creation blocked another account or became verified zero", err, comparison)
	}
}
