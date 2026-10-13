package monitor

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func newAPITokenStoreFixture(t *testing.T) (tokenReconcileFixture, upstreamTokenBindingCommitInput, int64) {
	t.Helper()
	m, a := tokenConfigFixture(t)
	a.Provider = upstreamProviderNewAPI
	a.BalanceUnit = 500000
	if err := m.storeDB.Save(&a).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.AutoMigrate(&ChannelUpstreamPricingHourEvidence{}, &ChannelUpstreamPricingHourState{}, &ChannelUpstreamUsageHour{}, &ChannelUpstreamPricingPageCheckpoint{}); err != nil {
		t.Fatal(err)
	}
	day := cstDayStart(1787541630)
	now := day + 86405
	f := tokenReconcileFixture{m: m, account: a, day: day, key: []byte("0123456789abcdef0123456789abcdef")}
	m.cfg.ChannelCostClosureEnabled = true
	m.cfg.ChannelCostClosureDomains = []string{a.Domain}
	m.cfg.ChannelCostHMACKey = string(f.key)
	m.cfg.ChannelCostHMACKeyID = "v1"
	if err := m.validateChannelCostKeyRegistry(); err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"575", "574"} {
		if _, err := m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(a, 185+i, id, 0), "fixture", day-1); err != nil {
			t.Fatal(err)
		}
	}
	for hour := day; hour < day+86400; hour += 3600 {
		var items []newAPIPricingUsageItem
		for i, id := range []int64{575, 574} {
			if hour == day+10*3600 || (hour == day+11*3600 && i == 0) {
				quota := int64(125000 * (i + 1))
				if hour == day+11*3600 {
					quota = 0
				}
				items = append(items, newAPIPricingUsageItem{CreatedAt: hour + 1, QuotaExact: quota, QuotaExactKnown: true, PromptTokens: 12, CompletionTokens: 3, TokensExactKnown: true,
					Pricing: newAPIPricingAttributes{TokenID: id, GroupName: "same-group", ModelName: "model-a", BillingMode: "token", GroupRatio: "1", GroupRatioCanonical: "1", GroupRatioState: pricingRatioValid, UserGroupRatioState: pricingRatioMissing, DiscountRatioState: pricingRatioMissing, EffectiveRatio: "1", EffectiveRatioSource: "group_ratio", EvidenceCapability: "full_rate", OtherValid: true}})
			}
		}
		pricing, p, err := buildNewAPIPricingHour(a, items, hour, now)
		if err != nil {
			t.Fatal(err)
		}
		cost, s, err := buildNewAPICostHourEvidence(a, items, hour, now, f.key, "v1")
		if err != nil {
			t.Fatal(err)
		}
		p.Status, p.VerifiedScans, p.ReconcileStatus = "verified", 2, "matched"
		p.LegacyRequests, p.LegacyTokens, p.LegacyQuota = p.EligibleRequests, p.Tokens, p.FinalQuota
		s.Status, s.VerifiedScans, s.ReconcileStatus = "verified", 2, "matched"
		s.ControlChargeUnits = s.EvidenceChargeUnits
		u := ChannelUpstreamUsageHour{Domain: a.Domain, Provider: a.Provider, HourTs: hour, BucketSeconds: 3600, Requests: p.EligibleRequests, Tokens: p.Tokens, Quota: float64(p.FinalQuota), CostUSD: float64(p.FinalQuota) / 500000, FetchedAt: now}
		if err := validateNewAPITokenProofHour(a, hour, now, "v1", cost, s, pricing, p, u); err != nil {
			t.Fatalf("fixture hour %d: %v", hour, err)
		}
		if len(pricing) > 0 {
			if err := m.storeDB.Create(&pricing).Error; err != nil {
				t.Fatal(err)
			}
		}
		if len(cost) > 0 {
			if err := m.storeDB.Create(&cost).Error; err != nil {
				t.Fatal(err)
			}
		}
		for _, row := range []any{&p, &s, &u} {
			if err := m.storeDB.Create(row).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	proof, err := readNewAPITokenProof(m.storeDB, a, day, now, "v1")
	if err != nil || proof.View.ReadyHours != 24 || proof.Hash == "" {
		t.Fatalf("fixture proof %+v %v", proof.View, err)
	}
	plan, err := m.previewUpstreamTokenBindingPlan(context.Background(), a.Domain, 185, now)
	if err != nil {
		t.Fatal(err)
	}
	return f, upstreamTokenBindingCommitInput{Domain: a.Domain, ChannelID: 185, AccountEpoch: newAPIUpstreamAccountEpoch(a), PlanDigest: plan.Digest, ProofDayTs: day, ProofSuccessHash: proof.Hash}, now
}

func TestUpstreamTokenNewAPIReadinessAndAtomicBindingPreserveMoney(t *testing.T) {
	f, in, now := newAPITokenStoreFixture(t)
	var before []ChannelUpstreamCostHourEvidence
	if err := f.m.storeDB.Order("hour_ts,dimension_hash").Find(&before).Error; err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, channel := range []int{185, 186} {
		v, err := f.m.loadUpstreamTokenReconciliation(ctx, f.account.Domain, channel, f.day, now)
		if err != nil || v.Status != "evidence_ready" || !v.TokenSeen || v.AttributionVerified || v.ReadyHours != 24 {
			t.Fatalf("%+v %v", v, err)
		}
	}
	got, err := f.m.commitNewAPITokenBindingPlan(ctx, in, "tester", now)
	if err != nil || got.ChangedSources != 2 || got.EffectiveFrom <= now {
		t.Fatalf("%+v %v", got, err)
	}
	if got, err := f.m.commitNewAPITokenBindingPlan(ctx, in, "tester", now); !errors.Is(err, errUpstreamTokenConflict) || got != (upstreamTokenBindingCommitResult{}) {
		t.Fatal("stale plan accepted")
	}
	var after []ChannelUpstreamCostHourEvidence
	if err := f.m.storeDB.Order("hour_ts,dimension_hash").Find(&after).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("binding rewrote money")
	}
	var total, requests int64
	for _, row := range after {
		total += row.ChargeUnits
		requests += row.Requests
	}
	if total != 375000 || requests != 3 {
		t.Fatal("zero charge or account total lost", total, requests)
	}
	for _, model := range []any{&ChannelFinanceVersion{}, &ChannelEconomicsDirtyHour{}, &ChannelCostDirtyHour{}, &ChannelEconomicsHourPublication{}} {
		tokenCostAssertCount(t, f, model, 0)
	}
}

func TestUpstreamTokenNewAPIRejectsStaleAndIncompleteEvidence(t *testing.T) {
	for _, name := range []string{"cost-row", "pricing-row", "control", "one-scan", "missing-hour", "future-completion", "key", "cost-recovery", "cost-checkpoint", "pricing-checkpoint", "proof-hash", "unseen-token", "partial-unit", "empty-unit", "snapshot-changed"} {
		t.Run(name, func(t *testing.T) {
			f, in, now := newAPITokenStoreFixture(t)
			exec := func(sql string, args ...any) {
				t.Helper()
				if err := f.m.storeDB.Exec(sql, args...).Error; err != nil {
					t.Fatal(err)
				}
			}
			switch name {
			case "cost-row":
				exec("UPDATE channel_upstream_cost_hour_evidence SET charge_units=charge_units+1")
			case "pricing-row":
				exec("UPDATE channel_upstream_pricing_hour_evidence SET final_quota=final_quota+1")
			case "control":
				exec("UPDATE channel_upstream_usage_hours SET requests=requests+1")
			case "one-scan":
				exec("UPDATE channel_upstream_cost_hour_states SET verified_scans=1")
			case "missing-hour":
				if err := f.m.storeDB.Where("hour_ts=?", f.day).Delete(&ChannelUpstreamCostHourState{}).Error; err != nil {
					t.Fatal(err)
				}
			case "future-completion":
				exec("UPDATE channel_upstream_cost_hour_states SET completed_at=?", now+1)
			case "key":
				exec("UPDATE channel_upstream_cost_hour_evidence SET hmac_key_id='wrong'")
			case "cost-recovery":
				if err := f.m.storeDB.Create(&ChannelCostDirtyHour{Domain: f.account.Domain, AccountEpoch: in.AccountEpoch, HourTs: f.day, Status: "pending", Reason: "failed"}).Error; err != nil {
					t.Fatal(err)
				}
			case "cost-checkpoint":
				if err := f.m.storeDB.Create(&ChannelCostPageCheckpoint{Domain: f.account.Domain, AccountEpoch: in.AccountEpoch, SemanticsVersion: channelCostEvidenceSemanticsVersion, HourTs: f.day}).Error; err != nil {
					t.Fatal(err)
				}
			case "pricing-checkpoint":
				if err := f.m.storeDB.Create(&ChannelUpstreamPricingPageCheckpoint{Domain: f.account.Domain, AccountEpoch: in.AccountEpoch, SemanticsVersion: upstreamPricingSemanticsVersion, HourTs: f.day}).Error; err != nil {
					t.Fatal(err)
				}
			case "proof-hash":
				in.ProofSuccessHash = fmt.Sprintf("%064x", 1)
			case "unseen-token":
				if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, 186, "999", 1), "tester", now); err != nil {
					t.Fatal(err)
				}
				p, err := f.m.previewUpstreamTokenBindingPlan(context.Background(), f.account.Domain, 185, now)
				if err != nil {
					t.Fatal(err)
				}
				in.PlanDigest = p.Digest
			case "partial-unit":
				exec("UPDATE channel_upstream_cost_hour_states SET charge_units_per_usd='250000'")
			case "empty-unit":
				exec("UPDATE channel_upstream_cost_hour_states SET charge_units_per_usd='' WHERE hour_ts=?", f.day)
			case "snapshot-changed":
				// Totals are identical, but a newly completed scan invalidates the
				// previously reviewed snapshot; the caller must read it again.
				exec("UPDATE channel_upstream_cost_hour_states SET updated_at=updated_at+1")
			}
			got, err := f.m.commitNewAPITokenBindingPlan(context.Background(), in, "tester", now)
			if err == nil || got != (upstreamTokenBindingCommitResult{}) {
				t.Fatalf("accepted %s %+v %v", name, got, err)
			}
			tokenCostAssertCount(t, f, &ChannelCostSourceBinding{}, 0)
		})
	}
}

func TestUpstreamTokenNewAPISharedTokenDoesNotAssignTwice(t *testing.T) {
	f, in, now := newAPITokenStoreFixture(t)
	if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, 186, "575", 1), "tester", now); err != nil {
		t.Fatal(err)
	}
	p, err := f.m.previewUpstreamTokenBindingPlan(context.Background(), f.account.Domain, 185, now)
	if err != nil {
		t.Fatal(err)
	}
	in.PlanDigest = p.Digest
	got, err := f.m.commitNewAPITokenBindingPlan(context.Background(), in, "tester", now)
	if err != nil || got.ChangedSources != 1 {
		t.Fatalf("shared commit %+v %v", got, err)
	}
	var rows []ChannelCostSourceBinding
	if err := f.m.storeDB.Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].LocalChannelID != 0 || rows[0].AllocationMode != "shared" {
		t.Fatalf("shared token allocated twice: %+v", rows)
	}
	tokenCostAssertCount(t, f, &ChannelEconomicsHourPublication{}, 0)
}

func TestUpstreamTokenNewAPIFrozenUnitAndNoConfigurationFallback(t *testing.T) {
	f, in, now := newAPITokenStoreFixture(t)
	if err := f.m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", f.account.Domain).Update("balance_unit", 250000).Error; err != nil {
		t.Fatal(err)
	}
	// The current balance unit must not reinterpret old charges.
	if got, err := f.m.commitNewAPITokenBindingPlan(context.Background(), in, "tester", now); err != nil || got.ChangedSources != 2 {
		t.Fatalf("frozen unit not preserved %+v %v", got, err)
	}
	for _, channel := range []int{185, 186} {
		if _, err := f.m.saveUpstreamTokenConfig(context.Background(), tokenConfigInput(f.account, channel, "", 1), "tester", now+1); err != nil {
			t.Fatal(err)
		}
	}
	p, err := f.m.previewUpstreamTokenBindingPlan(context.Background(), f.account.Domain, 185, now+1)
	if err != nil {
		t.Fatal(err)
	}
	in.PlanDigest = p.Digest
	in.ProofDayTs = 0
	in.ProofSuccessHash = ""
	// Empty settings and explicit clearing must work without a proof ledger.
	if err := f.m.storeDB.Migrator().DropTable(&ChannelUpstreamCostHourEvidence{}); err != nil {
		t.Fatal(err)
	}
	v, err := f.m.loadUpstreamTokenReconciliation(context.Background(), f.account.Domain, 185, f.day, now+1)
	if err != nil || v.Status != "unconfigured" {
		t.Fatalf("empty config queried proof %+v %v", v, err)
	}
	if got, err := f.m.commitNewAPITokenBindingPlan(context.Background(), in, "tester", now+1); err != nil || got.ChangedSources != 2 {
		t.Fatalf("clear depended on proof %+v %v", got, err)
	}
}

func TestUpstreamTokenNewAPIBindingFailureRollsBackWholeBatch(t *testing.T) {
	f, in, now := newAPITokenStoreFixture(t)
	p, err := f.m.previewUpstreamTokenBindingPlan(context.Background(), f.account.Domain, 185, now)
	if err != nil {
		t.Fatal(err)
	}
	sql := fmt.Sprintf("CREATE TRIGGER fail_newapi_binding BEFORE INSERT ON channel_cost_source_bindings WHEN NEW.source_ref='%s' BEGIN SELECT RAISE(ABORT,'synthetic failure'); END", p.Changes[1].After.SourceRef)
	if err := f.m.storeDB.Exec(sql).Error; err != nil {
		t.Fatal(err)
	}
	got, err := f.m.commitNewAPITokenBindingPlan(context.Background(), in, "tester", now)
	if err == nil || got != (upstreamTokenBindingCommitResult{}) {
		t.Fatal("partial success", got, err)
	}
	tokenCostAssertCount(t, f, &ChannelCostSourceBinding{}, 0)
}
