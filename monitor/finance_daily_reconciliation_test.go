package monitor

import (
	"encoding/json"
	"math"
	"testing"
)

func TestFinanceDailyCostReconciliation(t *testing.T) {
	for _, mode := range []string{"mixed", "zero", "unknown", "missing_zero", "supplier_offset", "parent_mismatch", "unknown_source", "duplicate", "negative", "overflow"} {
		t.Run(mode, func(t *testing.T) {
			day := cstDayStart(1788192000)
			days := []financeDailyView{{From: day, ledgerSourceCosts: map[string]int64{"ledger": 3}}, {From: day + 86400, ledgerSourceCosts: map[string]int64{"ledger": 4}}}
			bills := map[int64]financeDailyBill{
				day:         {CorrectionByDomain: map[string]int64{"recharge": 1}},
				day + 86400: {CorrectionByDomain: map[string]int64{"recharge": 2}},
			}
			details := []financeCostDetailView{
				{Domain: "recharge", CorrectionSource: "充值版本", KnownCorrectedCost: economicsMoney(3)},
				{Domain: "ledger", CorrectionSource: "经济事实账", KnownCorrectedCost: economicsMoney(7)},
				{Domain: "missing"},
			}
			parent := economicsMoney(10)
			switch mode {
			case "zero", "missing_zero":
				days = days[:1]
				details = details[:1]
				details[0].KnownCorrectedCost = economicsMoney(0)
				bills[day].CorrectionByDomain["recharge"] = 0
				parent = economicsMoney(0)
				if mode == "missing_zero" {
					delete(bills[day].CorrectionByDomain, "recharge")
				}
			case "unknown":
				details = nil
				parent = channelEconomicsMoneyView{}
			case "supplier_offset":
				details[0].KnownCorrectedCost = economicsMoney(4)
				details[1].KnownCorrectedCost = economicsMoney(6)
			case "parent_mismatch":
				parent = economicsMoney(11)
			case "unknown_source":
				details[0].CorrectionSource = "future-source"
			case "duplicate":
				details = append(details, details[0])
			case "negative":
				bills[day].CorrectionByDomain["recharge"] = -1
			case "overflow":
				details[0].KnownCorrectedCost = economicsMoney(math.MaxInt64)
				bills[day].CorrectionByDomain["recharge"] = math.MaxInt64
			}
			applyFinanceDailyCostReconciliation(days, bills, details, parent)
			for index, result := range days {
				v := result.CostReconciliation
				switch mode {
				case "mixed":
					if v.Status != "reconciled" || v.KnownTotal != economicsMoney(int64(4+index*2)) {
						t.Fatalf("mixed sources not reconciled: %+v", v)
					}
				case "zero":
					if v.Status != "reconciled" || v.KnownTotal.MicroUSD != "0" {
						t.Fatalf("verified zero lost: %+v", v)
					}
				case "unknown":
					if v.Status != "unavailable" || v.KnownTotal.MicroUSD != "" {
						t.Fatalf("unknown invented as zero: %+v", v)
					}
				default:
					if v.Status != "source_mismatch" || v.KnownTotal.MicroUSD != "" {
						t.Fatalf("invalid cost published: %+v", v)
					}
				}
			}
			payload, err := json.Marshal(days)
			if err != nil {
				t.Fatal(err)
			}
			var cached []financeDailyView
			if err := json.Unmarshal(payload, &cached); err != nil {
				t.Fatal(err)
			}
			if cached[0].CostReconciliation != days[0].CostReconciliation {
				t.Fatal("cache lost reconciliation")
			}
		})
	}
}
