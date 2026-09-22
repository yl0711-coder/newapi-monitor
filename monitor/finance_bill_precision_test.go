package monitor

import (
	"context"
	"math"
	"reflect"
	"testing"
)

func TestFinanceBillPrecisionKeepsChannelMetrics(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	rows, err := m.loadChannelUpstreamUsageRows(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"valid", "invalid", "overlap", "provisional", "other_provider"} {
		t.Run(mode, func(t *testing.T) {
			input := append([]ChannelUpstreamUsageHour(nil), rows...)
			switch mode {
			case "invalid":
				input[0].CostUSD = -1
			case "overlap":
				input[0].BucketSeconds = 2 * 86400
			case "provisional":
				input[0].Provisional = true
			case "other_provider":
				input[0].Provider = "not-the-configured-provider"
			}
			original, err := projectChannelUpstreamUsageWindow(input, scope, scope.ToTs, accounts, nil)
			if err != nil {
				t.Fatal(err)
			}
			actual, amounts, err := projectFinanceBillWindow(input, scope, scope.ToTs, accounts, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(original, actual) {
				t.Fatal("finance precision altered shared metric semantics")
			}
			for domain, metric := range actual {
				if metric.IntegrityStatus != upstreamUsageIntegrityComplete && amounts[domain].Raw.MicroUSD != "" {
					t.Fatal("invalid domain retained a partial integer amount")
				}
			}
		})
	}
}

func TestFinanceBillPrecisionRejectsOverflow(t *testing.T) {
	for _, value := range []float64{math.NaN(), math.Inf(1), -1, float64(math.MaxInt64) / 1_000_000} {
		if _, err := financeMoneyFromUSD(value); err == nil {
			t.Fatalf("unsafe conversion accepted: %v", value)
		}
	}
	m, scope, accounts := dailyBillFixture(t)
	rows, err := m.loadChannelUpstreamUsageRows(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	for index := range rows {
		if rows[index].Domain == "day.example" {
			rows[index].CostUSD = 5e12 // Each fits; the sum does not.
		}
	}
	if _, _, err := projectFinanceBillWindow(rows, scope, scope.ToTs, accounts, nil); err == nil {
		t.Fatal("integer sum overflow was accepted")
	}
}

func TestFinanceBillPrecisionClosedDayFallback(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	scope.ToTs -= 3600
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain=?", "day.example").Update("cost_usd", 0.0000006).Error; err != nil {
		t.Fatal(err)
	}
	component, _, err := m.buildFinancePeriodComponent(context.Background(), scope, scope.ToTs+86400, "precision-partial", accounts, channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if component.Statement.UpstreamBilledCost != nil || component.Statement.KnownUpstreamBilledCost.MicroUSD != "3000001" {
		t.Fatalf("partial-day precision/coverage: %+v", component.Statement)
	}
	var total int64
	for _, day := range component.Days {
		value, ok := financeMoneyInt64(day.Statement.KnownUpstreamBilledCost)
		if !ok {
			t.Fatal("missing known bill")
		}
		total += value
	}
	if total != 3_000_001 {
		t.Fatal("closed daily fallback differs from day facts")
	}
}
