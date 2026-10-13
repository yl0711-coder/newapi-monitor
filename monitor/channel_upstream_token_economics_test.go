package monitor

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

func sub2TokenEconomicsFixture(t *testing.T, paid, credit float64) (tokenReconcileFixture, int64) {
	t.Helper()
	f, in := tokenCostStoreFixture(t, -1)
	ctx := context.Background()
	if _, err := f.m.commitSub2TokenCostCandidates(ctx, in, f.day+86401); err != nil {
		t.Fatal(err)
	}
	if err := f.m.storeDB.AutoMigrate(&StabilityHourSample{}, &ChannelTestHourSample{}, &StabilityHourIngestState{}, &ChannelDomainCost{}, &ChannelFinanceVersion{}); err != nil {
		t.Fatal(err)
	}
	hour := f.day + 36000
	for i, id := range []string{"575", "574"} {
		source, err := channelCostSourceRef(f.key, f.account.Provider, in.AccountEpoch, sub2TokenSourceKind, id)
		if err != nil {
			t.Fatal(err)
		}
		// Explicit historical binding in synthetic data, not automatically
		// backdated by the optional configuration feature.
		binding := ChannelCostSourceBinding{Domain: f.account.Domain, AccountEpoch: in.AccountEpoch, Provider: f.account.Provider, SourceRef: source, SourceRefKind: sub2TokenSourceKind, HMACKeyID: "v1", ValidFrom: f.day, LocalChannelID: 185 + i, Status: "confirmed", AllocationMode: "allocated", MappingSource: "manual"}
		if err := f.m.storeDB.Create(&binding).Error; err != nil {
			t.Fatal(err)
		}
		if err := f.m.storeDB.Create(&StabilityHourSample{HourTs: hour, ChannelID: 185 + i, ModelName: "model-a", Grp: "local", TrafficClassVersion: stabilityTrafficClassificationVersion, Success: 1, Quota: 500000}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := f.m.storeDB.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete", TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	snapshot, _ := json.Marshal(channelFinanceVersionSnapshot{Domain: f.account.Domain, UpstreamRechargePaid: paid, UpstreamRechargeCredit: credit})
	if err := f.m.storeDB.Create(&ChannelFinanceVersion{Domain: f.account.Domain, Version: 1, EffectiveAt: f.day, SnapshotJSON: string(snapshot)}).Error; err != nil {
		t.Fatal(err)
	}
	return f, hour
}

func TestSub2TokenEconomicsRechargeRatiosAndFrozenUnit(t *testing.T) {
	for _, tc := range []struct {
		name          string
		paid, credit  float64
		first, second int64
	}{
		{"one", 1, 1, 123456, 234567}, {"discount", 1, 10, 12346, 23457}, {"premium", 7, 1, 864192, 1641969}, {"fraction", 2, 7, 35273, 67019}, {"decimal", 1.5, 2.5, 74074, 140740},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, hour := sub2TokenEconomicsFixture(t, tc.paid, tc.credit)
			f.account.BalanceUnit = 7 // not a multiplier for Sub2 actual_cost
			if err := f.m.storeDB.Save(&f.account).Error; err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			for i := 0; i < 2; i++ {
				if err := f.m.publishSub2TokenEconomicsHour(ctx, f.account, hour, "local acceptance", f.day+86405+int64(i)); err != nil {
					t.Fatal(err)
				}
			}
			var rows []ChannelEconomicsHourPublication
			if err := f.m.storeDB.Order("local_channel_id").Find(&rows).Error; err != nil {
				t.Fatal(err)
			}
			if len(rows) != 2 {
				t.Fatalf("repeat publication changed count: %d", len(rows))
			}
			var cost, revenue int64
			for i, r := range rows {
				want := []int64{tc.first, tc.second}[i]
				raw := []int64{123456, 234567}[i]
				if r.LocalChannelID != 185+i || r.UpstreamChargeUnit != sub2TokenChargeUnit || r.ChargeUnitsPerUSD != "1000000" || r.UpstreamChargeUnits != raw || r.UpstreamCostMicroUSD != raw || r.CorrectedCostMicroUSD != want || r.ProfitMicroUSD != 1000000-want || !r.ProfitKnown || r.CoverageStatus != "verified_complete" {
					t.Fatalf("wrong unit or correction: %+v", r)
				}
				cost += r.UpstreamCostMicroUSD
				revenue += r.RevenueMicroUSD
			}
			if cost != 358023 || revenue != 2000000 {
				t.Fatal("raw money not conserved", cost, revenue)
			}
		})
	}
}

func TestSub2TokenEconomicsSharedAndLegacyWorkerIsolation(t *testing.T) {
	f, hour := sub2TokenEconomicsFixture(t, 1, 1)
	ctx := context.Background()
	// Existing production paths remain NewAPI-only until explicit activation.
	if err := f.m.publishChannelEconomicsHour(ctx, f.account, hour, "old worker", f.day+86405); err != nil {
		t.Fatal(err)
	}
	if err := f.m.enqueueMissingChannelEconomicsHours(ctx, f.account, 16); err != nil {
		t.Fatal(err)
	}
	tokenCostAssertCount(t, f, &ChannelEconomicsHourPublication{}, 0)
	tokenCostAssertCount(t, f, &ChannelEconomicsDirtyHour{}, 0)
	if err := f.m.storeDB.Model(&ChannelCostSourceBinding{}).Where("domain=?", f.account.Domain).Updates(map[string]any{"allocation_mode": "shared", "local_channel_id": 0}).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.m.publishSub2TokenEconomicsHour(ctx, f.account, hour, "shared", f.day+86405); err != nil {
		t.Fatal(err)
	}
	var rows []ChannelEconomicsHourPublication
	if err := f.m.storeDB.Order("local_channel_id").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0].LocalChannelID != 0 || rows[0].UpstreamChargeUnits != 358023 || rows[0].ProfitKnown || rows[0].CoverageStatus != "unallocated_cost" || rows[1].UpstreamChargeUnits != 0 || rows[2].UpstreamChargeUnits != 0 {
		t.Fatalf("shared money duplicated: %+v", rows)
	}
}

func TestSub2TokenEconomicsInvalidEvidencePreservesPublishedReport(t *testing.T) {
	for _, kind := range []string{"state-unit", "row-unit", "row-hash", "control", "pricing", "key", "one-scan", "provisional", "rounding", "readonly", "disabled", "stats-only"} {
		t.Run(kind, func(t *testing.T) {
			f, hour := sub2TokenEconomicsFixture(t, 1, 1)
			ctx := context.Background()
			if err := f.m.publishSub2TokenEconomicsHour(ctx, f.account, hour, "before", f.day+86405); err != nil {
				t.Fatal(err)
			}
			var before, after []ChannelEconomicsHourPublication
			if err := f.m.storeDB.Order("publication_id").Find(&before).Error; err != nil {
				t.Fatal(err)
			}
			exec := func(sql string, args ...any) {
				t.Helper()
				if err := f.m.storeDB.Exec(sql, args...).Error; err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "state-unit":
				exec("UPDATE channel_upstream_cost_hour_states SET charge_units_per_usd='500000'")
			case "row-unit":
				exec("UPDATE channel_upstream_cost_hour_evidence SET charge_unit='newapi_quota'")
			case "row-hash":
				exec("UPDATE channel_upstream_cost_hour_evidence SET charge_units=charge_units+1")
			case "control":
				exec("UPDATE channel_upstream_usage_hours SET cost_usd=cost_usd+1")
			case "pricing":
				exec("UPDATE channel_upstream_pricing_hour_states SET final_quota=final_quota+1")
			case "key":
				f.m.cfg.ChannelCostHMACKey = "different-key-0123456789abcdef0123456789"
			case "one-scan":
				exec("UPDATE channel_upstream_cost_hour_states SET verified_scans=1")
			case "provisional":
				exec("UPDATE channel_upstream_usage_hours SET provisional=1")
			case "rounding":
				exec("UPDATE channel_upstream_cost_hour_states SET reconcile_status='rounding_only'")
			case "readonly":
				f.m.cfg.LocalSnapshotOnly = true
			case "disabled":
				f.m.cfg.ChannelCostClosureEnabled = false
			case "stats-only":
				f.account.UsageAdapter = upstreamUsageAdapterSub2Stats
			}
			if err := f.m.publishSub2TokenEconomicsHour(ctx, f.account, hour, "invalid", f.day+86406); err == nil {
				t.Fatal("invalid evidence published", kind)
			}
			if err := f.m.storeDB.Order("publication_id").Find(&after).Error; err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("failed validation replaced published report")
			}
		})
	}
}

func TestSub2TokenEconomicsVerifiedEmptyHour(t *testing.T) {
	f, _ := sub2TokenEconomicsFixture(t, 1, 10)
	hour := f.day + 9*3600
	for i := 0; i < 2; i++ {
		if err := f.m.publishSub2TokenEconomicsHour(context.Background(), f.account, hour, "empty", f.day+86405+int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	tokenCostAssertCount(t, f, &ChannelEconomicsHourPublication{}, 0)
	tokenCostAssertCount(t, f, &ChannelEconomicsHourManifestPublication{}, 1)
}
