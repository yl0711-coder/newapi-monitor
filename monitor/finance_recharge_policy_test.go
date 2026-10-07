package monitor

import (
	"context"
	"math"
	"math/big"
	"strconv"
	"testing"
)

type financeRechargePolicyTestCase struct {
	name, paidText, creditText string
	paid, credit               float64
}

func financeRechargePolicyCases() []financeRechargePolicyTestCase {
	return []financeRechargePolicyTestCase{
		{"one_to_one", "1", "1", 1, 1},
		{"one_to_ten", "1", "10", 1, 10},
		{"seven_to_one", "7", "1", 7, 1},
		{"one_to_five", "1", "5", 1, 5},
		{"one_to_seven", "1", "7", 1, 7},
		{"decimal_credit", "1", "7.14", 1, 7.14},
		{"decimal_paid", "2.5", "8", 2.5, 8},
		{"both_decimal", "123.45", "678.9", 123.45, 678.9},
		{"non_terminating", "100", "333", 100, 333},
		{"decimal_surcharge", "2.75", "0.4", 2.75, 0.4},
	}
}

// Independent decimal oracle with explicitly specified half-up micro-unit
// rounding. Do not obtain expected amounts by calling production pricing.
func financeRechargePolicyOracle(t *testing.T, rawMicro int64, paid, credit string) int64 {
	t.Helper()
	p, pOK := new(big.Rat).SetString(paid)
	c, cOK := new(big.Rat).SetString(credit)
	if !pOK || !cOK || p.Sign() <= 0 || c.Sign() <= 0 {
		t.Fatal("invalid independent test terms")
	}
	value := new(big.Rat).Mul(new(big.Rat).SetInt64(rawMicro), p)
	value.Quo(value, c)
	q, remainder := new(big.Int), new(big.Int)
	q.QuoRem(value.Num(), value.Denom(), remainder)
	if new(big.Int).Lsh(remainder, 1).Cmp(value.Denom()) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	if !q.IsInt64() {
		t.Fatal("independent expected amount exceeds int64")
	}
	return q.Int64()
}

// All accounting paths share configured paid:credit terms, not an enumeration
// of ratios or source-currency conversion.
func TestFinanceRechargeConfiguredRatioIgnoresCurrencyLabels(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	rows, err := m.loadChannelUpstreamUsageRows(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range financeRechargePolicyCases() {
		t.Run(tc.name, func(t *testing.T) {
			cost, _, ok := adjustedUpstreamUsageCost(1, ChannelDomainCost{RechargePaid: tc.paid, RechargeCredit: tc.credit}, true)
			if !ok || math.Abs(cost-tc.paid/tc.credit) > 1e-12 {
				t.Fatalf("channel cost=%v, want=%v", cost, tc.paid/tc.credit)
			}
			wantUnit := financeRechargePolicyOracle(t, 1_000_000, tc.paidText, tc.creditText)
			micro, err := correctedCostMicroUSD(1_000_000, tc.paid, tc.credit)
			if err != nil || micro != wantUnit {
				t.Fatalf("ledger cost=%d, want=%d: %v", micro, wantUnit, err)
			}
			versions := map[string][]channelRechargeVersion{}
			for domain := range accounts {
				versions[domain] = []channelRechargeVersion{{Version: 1, EffectiveAt: scope.FromTs, Paid: tc.paid, Credit: tc.credit, Valid: true}}
			}
			for _, currency := range []string{"USD", "CNY", "", "arbitrary_source_label"} {
				t.Run("currency_"+currency, func(t *testing.T) {
					labelled := map[string]ChannelUpstreamAccountView{}
					for domain, account := range accounts {
						account.Currency, account.NativeCurrency = currency, currency
						labelled[domain] = account
					}
					metrics, amounts, err := projectFinanceBillWindow(rows, scope, scope.ToTs, labelled, versions)
					if err != nil {
						t.Fatal(err)
					}
					for domain, unit := range map[string]int64{"hour.example": 1, "day.example": 10} {
						want := financeRechargePolicyOracle(t, unit*1_000_000, tc.paidText, tc.creditText) +
							financeRechargePolicyOracle(t, 2*unit*1_000_000, tc.paidText, tc.creditText)
						metric := metrics[domain]
						if !metric.Complete || !metric.AdjustedCostAvailable ||
							math.Abs(metric.AdjustedCostUSD-float64(3*unit)*tc.paid/tc.credit) > 1e-9 ||
							amounts[domain].RechargeCorrected.MicroUSD != strconv.FormatInt(want, 10) ||
							amounts[domain].Raw.MicroUSD != strconv.FormatInt(3*unit*1_000_000, 10) {
							t.Fatalf("%s: currency or ratio changed raw/corrected accounting: %+v %+v", domain, metric, amounts[domain])
						}
					}
				})
			}
		})
	}
}

func TestFinanceRechargeConfiguredRatioKeepsMonthDayAndCacheConsistent(t *testing.T) {
	for _, tc := range financeRechargePolicyCases() {
		t.Run(tc.name, func(t *testing.T) {
			m, scope, accounts := dailyBillFixture(t)
			for domain, account := range accounts {
				createChannelRechargeVersion(t, m, domain, 1, scope.FromTs, tc.paid, tc.credit)
				account.Currency, account.NativeCurrency = "CNY", "CNY"
				accounts[domain] = account
			}
			finance := channelFinanceSnapshot{hasSettings: true, settings: ChannelFinanceSetting{FXBenchmark: 7, SiteRechargePaid: 1, SiteRechargeCredit: 1}}
			wantDays := []int64{}
			var wantPeriod int64
			for _, day := range []int64{1, 2} {
				want := financeRechargePolicyOracle(t, day*1_000_000, tc.paidText, tc.creditText) +
					financeRechargePolicyOracle(t, day*10_000_000, tc.paidText, tc.creditText)
				wantDays = append(wantDays, want)
				wantPeriod += want
			}
			for attempt := 0; attempt < 2; attempt++ {
				component, cached, err := m.buildFinancePeriodComponent(context.Background(), scope, scope.ToTs+86400, "platform-ratio-policy", accounts, finance, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
				if err != nil || cached != (attempt == 1) {
					t.Fatalf("build/cache attempt=%d, cached=%v: %v", attempt, cached, err)
				}
				if component.Statement.KnownRawCorrectedUpstreamCost.MicroUSD != strconv.FormatInt(wantPeriod, 10) ||
					component.Statement.UpstreamBilledCost == nil || component.Statement.UpstreamBilledCost.MicroUSD != "33000000" || len(component.Days) != 2 {
					t.Fatalf("period raw/corrected cost changed: %+v", component.Statement)
				}
				var sum int64
				for i, day := range component.Days {
					value, ok := financeMoneyInt64(day.RechargeCorrection.KnownCost)
					if !ok || !day.RechargeCorrection.Complete || day.RechargeCorrection.Cost == nil || value != wantDays[i] {
						t.Fatalf("day %d: %+v", i, day.RechargeCorrection)
					}
					sum += value
				}
				if sum != wantPeriod {
					t.Fatal("day/month corrected amounts differ")
				}
			}
		})
	}
}

func TestFinanceRechargeBillProjectionMatchesDecimalHalfUp(t *testing.T) {
	scope := stabilityScope{FromTs: 3600, ToTs: 7200}
	accounts := map[string]ChannelUpstreamAccountView{"bill.example": {Configured: true, UsageSyncEnabled: true, Provider: upstreamProviderNewAPI, UsageGranularity: "hour"}}
	for _, tc := range financeRechargePolicyCases() {
		t.Run(tc.name, func(t *testing.T) {
			versions := map[string][]channelRechargeVersion{"bill.example": {{Version: 1, EffectiveAt: scope.FromTs, Paid: tc.paid, Credit: tc.credit, Valid: true}}}
			for _, rawMicro := range []int64{1, 3, 4, 11, 17, 35, 52, 75, 88, 168, 225, 280, 101234567} {
				cost := float64(rawMicro) / 1_000_000
				rows := []ChannelUpstreamUsageHour{{Domain: "bill.example", HourTs: scope.FromTs, BucketSeconds: 3600, CostUSD: cost, Quota: cost * quotaPerUSD, UnitPerUSD: quotaPerUSD, Provider: upstreamProviderNewAPI}}
				metrics, amounts, err := projectFinanceBillWindow(rows, scope, scope.ToTs, accounts, versions)
				want := financeRechargePolicyOracle(t, rawMicro, tc.paidText, tc.creditText)
				if err != nil || !metrics["bill.example"].AdjustedCostAvailable || amounts["bill.example"].RechargeCorrected.MicroUSD != strconv.FormatInt(want, 10) {
					t.Fatalf("raw micro=%d paid=%s credit=%s: projected=%s want=%d error=%v", rawMicro, tc.paidText, tc.creditText, amounts["bill.example"].RechargeCorrected.MicroUSD, want, err)
				}
			}
		})
	}
}

func TestFinanceRechargeRatiosPreserveEquivalentTermsAndMicroRounding(t *testing.T) {
	for paid := int64(1); paid <= 17; paid++ {
		for credit := int64(1); credit <= 23; credit++ {
			for _, cost := range []int64{0, 1, 17_725_003, 101_234_567} {
				want := financeRechargePolicyOracle(t, cost, strconv.FormatInt(paid, 10), strconv.FormatInt(credit, 10))
				for _, scale := range []int64{1, 3, 100} {
					actual, err := correctedCostMicroUSD(cost, float64(paid*scale), float64(credit*scale))
					if err != nil || actual != want {
						t.Fatalf("cost=%d paid=%d credit=%d scale=%d: got=%d want=%d err=%v", cost, paid, credit, scale, actual, want, err)
					}
				}
			}
		}
	}
}
