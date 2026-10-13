package monitor

import (
	"context"
	"reflect"
	"testing"
)

// These tests exercise the existing monetary pipeline, not an activated new
// Sub2 collector. Optional configuration must not silently become a binding.
func TestUpstreamTokenAcceptanceSameGroupSeparatedAndAccountScoped(t *testing.T) {
	account := ChannelUpstreamAccount{Domain: "upstream.example", Provider: upstreamProviderNewAPI,
		BaseURL: "https://upstream.example", UserID: 31, BalanceUnit: 500000}
	const hour int64 = 3600
	key := []byte("0123456789abcdef0123456789abcdef")
	items := []newAPIPricingUsageItem{
		{CreatedAt: hour + 1, QuotaExact: 123456, QuotaExactKnown: true, TokensExactKnown: true,
			PromptTokens: 11, CompletionTokens: 3, Pricing: newAPIPricingAttributes{TokenID: 575, GroupName: "same-group", ModelName: "model-a", BillingMode: "token"}},
		{CreatedAt: hour + 2, QuotaExact: 654321, QuotaExactKnown: true, TokensExactKnown: true,
			PromptTokens: 19, CompletionTokens: 7, Pricing: newAPIPricingAttributes{TokenID: 574, GroupName: "same-group", ModelName: "model-a", BillingMode: "token"}},
	}
	rows, state, err := buildNewAPICostHourEvidence(account, items, hour, 7200, key, "test-v1")
	if err != nil || len(rows) != 2 || rows[0].SourceRef == rows[1].SourceRef {
		t.Fatalf("same group erased distinct tokens: rows=%d err=%v", len(rows), err)
	}
	if state.EvidenceChargeUnits != 777777 || state.Requests != 2 || rows[0].ChargeUnits+rows[1].ChargeUnits != 777777 {
		t.Fatal("token separation must conserve account cost and request count")
	}
	// Reordering/re-fetching the same complete input is deterministic. This is
	// not a claim that concatenating duplicate pages is safe.
	again, againState, err := buildNewAPICostHourEvidence(account, []newAPIPricingUsageItem{items[1], items[0]}, hour, 7200, key, "test-v1")
	if err != nil || !reflect.DeepEqual(rows, again) || !reflect.DeepEqual(state, againState) {
		t.Fatalf("same scan is not deterministic: %v", err)
	}
	account.UserID++
	other, _, err := buildNewAPICostHourEvidence(account, items, hour, 7200, key, "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range rows {
		for _, b := range other {
			if a.SourceRef == b.SourceRef {
				t.Fatal("identical numeric token IDs in different accounts collided")
			}
		}
	}
	// One unidentifiable row must prevent partial evidence from being mistaken
	// for a complete token-level hour. The legacy account ledger is independent.
	items[1].Pricing.TokenID = 0
	partial, _, err := buildNewAPICostHourEvidence(account, items, hour, 7200, key, "test-v1")
	if err == nil || len(partial) != 0 {
		t.Fatal("missing identity exposed a misleading partial token total")
	}
}

func TestUpstreamTokenAcceptanceConfigCannotReplaceConfirmedHistory(t *testing.T) {
	m, account := tokenConfigFixture(t)
	// Exercise configuration while the existing cost feature is enabled, not
	// merely while all monetary workers are switched off.
	m.cfg.ChannelCostClosureEnabled = true
	m.cfg.ChannelCostClosureDomains = []string{account.Domain}
	const hour int64 = 3600
	source, err := channelCostSourceRef([]byte("0123456789abcdef0123456789abcdef"), account.Provider,
		newAPIUpstreamAccountEpoch(account), "test-source", "575")
	if err != nil {
		t.Fatal(err)
	}
	confirmed := ChannelCostSourceBinding{Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account),
		SourceRef: source, Provider: account.Provider, SourceRefKind: "test-source", HMACKeyID: "test-v1",
		LocalChannelID: 185, ValidFrom: hour, Status: "confirmed", AllocationMode: "allocated", MappingSource: "manual"}
	if err := m.saveCostSourceBinding(context.Background(), confirmed); err != nil {
		t.Fatal(err)
	}
	var before []ChannelCostSourceBinding
	if err := m.storeDB.Find(&before).Error; err != nil {
		t.Fatal(err)
	}
	// Configure the same token on two physical channels, then change and clear
	// one. None of these intents can retroactively rewrite confirmed ownership.
	for _, step := range []struct {
		channel int
		id      string
		rev     int64
	}{{185, "575", 0}, {186, "575", 0}, {185, "574", 1}, {185, "", 2}} {
		view, err := m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(account, step.channel, step.id, step.rev), "tester", 7200)
		if err != nil || view.AttributionVerified {
			t.Fatalf("unverified configuration was activated: %v", err)
		}
		var after []ChannelCostSourceBinding
		if err := m.storeDB.Find(&after).Error; err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("optional setting rewrote historical ownership: %v", err)
		}
	}
	for _, model := range []any{&ChannelEconomicsDirtyHour{}, &ChannelCostDirtyHour{}, &ChannelEconomicsHourPublication{}, &ChannelFinanceVersion{}} {
		var n int64
		if err := m.storeDB.Model(model).Count(&n).Error; err != nil || n != 0 {
			t.Fatalf("configuration triggered monetary writes to %T: count=%d err=%v", model, n, err)
		}
	}
}
