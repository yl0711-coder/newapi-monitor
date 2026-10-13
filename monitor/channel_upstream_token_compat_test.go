package monitor

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func tokenCompatSub2Item(t *testing.T, extra string) sub2PricingUsageItem {
	t.Helper()
	raw := fmt.Sprintf(`{"id":91,"created_at":"2026-08-24T10:20:30+08:00","model":"model-a","input_tokens":2,"output_tokens":3,"actual_cost":"0.123456","rate_multiplier":"0.15","group":{"id":48,"name":"same-group","rate_multiplier":"0.7"}%s}`, extra)
	item, err := decodeSub2PricingUsageItem(json.RawMessage(raw))
	if err != nil {
		t.Fatal(err)
	}
	return item
}

func TestSub2TokenIdentityDoesNotChangeLegacyMoneyOrCheckpoints(t *testing.T) {
	account := ChannelUpstreamAccount{Domain: "sub2.example", Provider: upstreamProviderSub2API, BaseURL: "https://sub2.example", Account: "acct"}
	base := tokenCompatSub2Item(t, "")
	before, err := buildSub2PricingEvidence(account, []sub2PricingUsageItem{base}, base.CreatedAt+100)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := sub2PricingFingerprint([]sub2PricingUsageItem{base}, 1)
	for _, tt := range []struct{ extra, token, state string }{
		{"", "", "missing"}, {`,"api_key_id":null`, "", "missing"},
		{`,"api_key_id":575`, "575", "valid"}, {`,"api_key_id":"574"`, "574", "valid"},
		{`,"api_key_id":9007199254740993`, "9007199254740993", "valid"},
		{`,"api_key_id":0`, "", "invalid"}, {`,"api_key_id":-1`, "", "invalid"},
		{`,"api_key_id":"wrong"`, "", "invalid"}, {`,"api_key_id":{}`, "", "invalid"},
		{`,"api_key_id":575.0`, "", "invalid"},
		{`,"api_key":{"id":575,"name":"mutable","key":"not-a-real-key"}`, "", "missing"},
		{`,"api_key_id":575,"api_key":{"id":574,"name":"renamed"}`, "575", "valid"},
	} {
		t.Run(tt.extra, func(t *testing.T) {
			item := tokenCompatSub2Item(t, tt.extra)
			if item.UpstreamTokenID != tt.token || item.UpstreamTokenState != tt.state {
				t.Fatalf("identity %q/%s", item.UpstreamTokenID, item.UpstreamTokenState)
			}
			after, err := buildSub2PricingEvidence(account, []sub2PricingUsageItem{item}, base.CreatedAt+100)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("legacy pricing changed: %v", err)
			}
			if sub2PricingFingerprint([]sub2PricingUsageItem{item}, 1) != fingerprint {
				t.Fatal("legacy in-progress checkpoint would be invalidated")
			}
			if item.ActualCostMicros != 123456 {
				t.Fatal("actual_cost must not be multiplied by the rate again")
			}
		})
	}
}

func TestSub2DistinctTokensRetainIdentityWithoutDuplicatingAccountTotal(t *testing.T) {
	a := tokenCompatSub2Item(t, `,"api_key_id":575`)
	b := tokenCompatSub2Item(t, `,"api_key_id":574`)
	b.ID++
	if a.UpstreamTokenID == b.UpstreamTokenID || a.SourceGroup != b.SourceGroup {
		t.Fatal("same group must not erase different token identities")
	}
	account := ChannelUpstreamAccount{Domain: "sub2.example", Provider: upstreamProviderSub2API, BaseURL: "https://sub2.example", Account: "acct"}
	rows, err := buildSub2PricingEvidence(account, []sub2PricingUsageItem{a, b}, a.CreatedAt+100)
	if err != nil || len(rows) != 1 || rows[0].SourceRows != 2 || rows[0].FinalQuota != 246912 {
		t.Fatalf("existing account aggregation changed: %+v, %v", rows, err)
	}
}
