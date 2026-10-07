package monitor

import (
	"math"
	"reflect"
	"strconv"
	"testing"
)

func TestTokenForceLegacyBillsUseFaceValueAndConfiguredRecharge(t *testing.T) {
	const domain = "hainahn.com"
	scope := stabilityScope{FromTs: 3600, ToTs: 10800}
	accounts := map[string]ChannelUpstreamAccountView{domain: {Configured: true, Provider: upstreamProviderTokenForce, UsageSyncEnabled: true, Currency: "USD", NativeCurrency: "CNY", UnitPerUSD: 999}}
	for _, unit := range []float64{1, 7.2, 8} {
		for _, tc := range []struct {
			paid, credit float64
			wantMicro    int64
		}{{1, 1, 3_000_000}, {1, 10, 300_000}, {7, 1, 21_000_000}} {
			name := strconv.FormatFloat(unit, 'f', -1, 64) + "_" + strconv.FormatFloat(tc.paid, 'f', -1, 64) + ":" + strconv.FormatFloat(tc.credit, 'f', -1, 64)
			t.Run(name, func(t *testing.T) {
				rows := []ChannelUpstreamUsageHour{
					{Domain: domain, Provider: upstreamProviderTokenForce, HourTs: 3600, BucketSeconds: 3600, Requests: 1, Quota: 1, CostUSD: 1 / unit, UnitPerUSD: unit},
					{Domain: domain, Provider: upstreamProviderTokenForce, HourTs: 7200, BucketSeconds: 3600, Requests: 1, Quota: 2, CostUSD: 2 / unit, UnitPerUSD: unit},
				}
				original := append([]ChannelUpstreamUsageHour(nil), rows...)
				versions := map[string][]channelRechargeVersion{domain: {{Version: 1, EffectiveAt: 3600, Paid: tc.paid, Credit: tc.credit, Valid: true}}}
				metrics, money, err := projectFinanceBillWindow(rows, scope, scope.ToTs, accounts, versions)
				if err != nil || !metrics[domain].Complete || metrics[domain].CostUSD != 3 ||
					money[domain].Raw.MicroUSD != "3000000" || money[domain].RechargeCorrected.MicroUSD != strconv.FormatInt(tc.wantMicro, 10) {
					t.Fatal("legacy currency conversion leaked into platform cost", metrics, money, err)
				}
				if !reflect.DeepEqual(original, rows) {
					t.Fatal("projection rewrote historical source evidence")
				}
			})
		}
	}
}

func TestTokenForceLegacyBillUsesDatedTermsNotTodaysUnit(t *testing.T) {
	const domain = "hainahn.com"
	rows := []ChannelUpstreamUsageHour{
		{Domain: domain, Provider: upstreamProviderTokenForce, HourTs: 3600, BucketSeconds: 3600, Quota: 1, CostUSD: 1 / 7.2, UnitPerUSD: 7.2},
		{Domain: domain, Provider: upstreamProviderTokenForce, HourTs: 7200, BucketSeconds: 3600, Quota: 2, CostUSD: 2 / 8.0, UnitPerUSD: 8},
	}
	accounts := map[string]ChannelUpstreamAccountView{domain: {Configured: true, Provider: upstreamProviderTokenForce, UsageSyncEnabled: true, UnitPerUSD: 100}}
	versions := map[string][]channelRechargeVersion{domain: {
		{Version: 1, EffectiveAt: 3600, Paid: 1, Credit: 1, Valid: true},
		{Version: 2, EffectiveAt: 7200, Paid: 7, Credit: 1, Valid: true},
	}}
	_, money, err := projectFinanceBillWindow(rows, stabilityScope{FromTs: 3600, ToTs: 10800}, 10800, accounts, versions)
	if err != nil || money[domain].Raw.MicroUSD != "3000000" || money[domain].RechargeCorrected.MicroUSD != "15000000" {
		t.Fatal("original face value or dated recharge terms were lost", money, err)
	}
}

func TestTokenForceBillFaceValueRejectsUnprovedOrInvalidAmounts(t *testing.T) {
	base := ChannelUpstreamUsageHour{Provider: upstreamProviderTokenForce, Quota: 72, CostUSD: 10, UnitPerUSD: 7.2}
	for _, tc := range []struct {
		name   string
		change func(*ChannelUpstreamUsageHour)
	}{
		{"mismatch", func(r *ChannelUpstreamUsageHour) { r.CostUSD = 9 }},
		{"missing_unit", func(r *ChannelUpstreamUsageHour) { r.UnitPerUSD = 0 }},
		{"nan_unit", func(r *ChannelUpstreamUsageHour) { r.UnitPerUSD = math.NaN() }},
		{"infinite_unit", func(r *ChannelUpstreamUsageHour) { r.UnitPerUSD = math.Inf(1) }},
		{"negative_quota", func(r *ChannelUpstreamUsageHour) { r.Quota = -72 }},
		{"nan_quota", func(r *ChannelUpstreamUsageHour) { r.Quota = math.NaN() }},
		{"infinite_quota", func(r *ChannelUpstreamUsageHour) { r.Quota = math.Inf(1) }},
		{"negative_cost", func(r *ChannelUpstreamUsageHour) { r.CostUSD = -10 }},
		{"nan_cost", func(r *ChannelUpstreamUsageHour) { r.CostUSD = math.NaN() }},
		{"infinite_cost", func(r *ChannelUpstreamUsageHour) { r.CostUSD = math.Inf(1) }},
		{"missing_raw_amount", func(r *ChannelUpstreamUsageHour) { r.Quota = 0 }},
		{"division_overflow", func(r *ChannelUpstreamUsageHour) { r.UnitPerUSD = math.SmallestNonzeroFloat64 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := base
			tc.change(&row)
			if _, ok := tokenForceBillFaceValue(row); ok {
				t.Fatal("invalid evidence became an accounting amount", row)
			}
		})
	}
	zero, ok := tokenForceBillFaceValue(ChannelUpstreamUsageHour{Provider: upstreamProviderTokenForce})
	if !ok || zero.CostUSD != 0 || zero.UnitPerUSD != tokenForceLedgerUnit {
		t.Fatal("observed zero bucket was lost", zero)
	}
}

func TestTokenForceLegacyBalanceViewKeepsRawAmount(t *testing.T) {
	row := ChannelUpstreamAccount{Domain: "hainahn.com", Provider: upstreamProviderTokenForce, BalanceKnown: true, BalanceRaw: 720, BalanceUSD: 100, BalanceUnit: 7.2}
	view := (&Monitor{}).channelUpstreamAccountView(row)
	if view.BalanceUSD == nil || *view.BalanceUSD != 720 || view.BalanceRaw == nil || *view.BalanceRaw != 720 || view.UnitPerUSD != tokenForceLedgerUnit {
		t.Fatal("source currency conversion leaked into balance display", view)
	}
	if row.BalanceUSD != 100 || row.BalanceUnit != 7.2 {
		t.Fatal("view changed stored account evidence")
	}
}

func TestTokenForcePersistenceDoesNotGuessHistoricalFXBoundary(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	account := ChannelUpstreamAccount{Domain: "hainahn.com", Provider: upstreamProviderTokenForce, BalanceUnit: 7.2, BalanceUnitEffectiveAt: 100000}
	if err := m.storeDB.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	rows := []ChannelUpstreamUsageHour{{Domain: account.Domain, Provider: account.Provider, HourTs: 3600, BucketSeconds: 3600, Quota: 72, CostUSD: 72, UnitPerUSD: tokenForceLedgerUnit}}
	if err := m.persistUpstreamUsageWindow(t.Context(), account.Domain, 3600, 7200, rows, 100001); err != nil {
		t.Fatal(err)
	}
	var stored ChannelUpstreamUsageHour
	if err := m.storeDB.First(&stored, "domain = ? AND hour_ts = ?", account.Domain, int64(3600)).Error; err != nil {
		t.Fatal(err)
	}
	if stored.CostUSD != 72 || stored.Quota != 72 || stored.UnitPerUSD != tokenForceLedgerUnit {
		t.Fatal("native bill was blocked or changed by obsolete FX boundary", stored)
	}
}
