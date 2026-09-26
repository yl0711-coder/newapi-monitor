package monitor

import (
	"context"
	"encoding/json"
	"math"
	"testing"
)

func TestFinanceDailyLedgerIncludesUnpairedCostWithoutDuplicatingRecharge(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	for index, cost := range []int64{2_000_000, 5_000_000} {
		pub := insertEconomicsReportHour(t, m, "hour.example", "epoch", scope.FromTs+int64(index)*86400, index+1, 0, cost, cost, 0)
		if index == 1 {
			if err := m.storeDB.Model(&ChannelEconomicsHourPublication{}).Where("publication_id=?", pub.PublicationID).
				Updates(map[string]any{"profit_known": false, "coverage_status": "local_revenue_missing"}).Error; err != nil {
				t.Fatal(err)
			}
		}
		insertEconomicsReportManifest(t, m, pub.Domain, pub.AccountEpoch, pub.HourTs, pub)
	}
	build := func() financePeriodComponent {
		component, _, err := m.buildFinancePeriodComponent(context.Background(), scope, scope.ToTs+86400, "ledger-daily", accounts, channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return component
	}
	component := build()
	if component.Statement.KnownRawCorrectedUpstreamCost.MicroUSD != "7000000" {
		t.Fatalf("wrong month cost: %+v", component.Statement)
	}
	for index, expected := range []string{"2000000", "5000000"} {
		day := component.Days[index]
		if day.CostReconciliation.Status != "reconciled" || day.CostReconciliation.KnownTotal.MicroUSD != expected {
			t.Fatalf("ledger cost not reconciled to parent: %+v", day.CostReconciliation)
		}
		if day.LedgerCorrection.KnownCost.MicroUSD != expected || day.LedgerCorrection.Cost != nil || day.LedgerCorrection.Status != "incomplete" {
			t.Fatalf("unpaired cost lost or published exact: %+v", day.LedgerCorrection)
		}
		if day.RechargeCorrection.KnownCost.MicroUSD != "" {
			t.Fatal("fallback counted as recharge cost")
		}
	}
	if component.Days[1].Statement.PairedCorrectedCost.MicroUSD != "" {
		t.Fatal("cost-only evidence became paired customer cost")
	}
	// The same parent query now chooses recharge bills. The historical ledger
	// must no longer contribute, including after a cached component existed.
	createChannelRechargeVersion(t, m, "hour.example", 1, scope.FromTs, 1, 1)
	component = build()
	if component.Statement.KnownRawCorrectedUpstreamCost.MicroUSD != "3000000" {
		t.Fatal("parent source did not switch")
	}
	for _, day := range component.Days {
		if day.CostReconciliation.Status != "reconciled" || day.CostReconciliation.KnownTotal != day.RechargeCorrection.KnownCost {
			t.Fatal("source switch did not refresh reconciled cost")
		}
		if day.LedgerCorrection.Status != "not_required" || day.LedgerCorrection.KnownCost.MicroUSD != "" {
			t.Fatal("ledger and recharge double counted")
		}
		if day.RechargeCorrection.KnownCost.MicroUSD == "" {
			t.Fatal("recharge source missing")
		}
	}
}

func TestFinanceDailyLedgerProjectionBoundaries(t *testing.T) {
	for _, mode := range []string{"exact", "partial", "zero", "missing_day", "mismatch", "offsetting_mismatch", "overflow", "negative", "unselected"} {
		t.Run(mode, func(t *testing.T) {
			days := []financeDailyView{{ledgerSourceCosts: map[string]int64{"a": 2}}, {ledgerSourceCosts: map[string]int64{"a": 5}}}
			details := []financeCostDetailView{{Domain: "a", CorrectionSource: "经济事实账", KnownCorrectedCost: economicsMoney(7), CorrectedCost: financeMoneyPointer(economicsMoney(7))}}
			switch mode {
			case "partial":
				details[0].CorrectedCost = nil
			case "zero":
				days[0].ledgerSourceCosts["a"] = 0
				days[1].ledgerSourceCosts["a"] = 0
				details[0].KnownCorrectedCost = economicsMoney(0)
				details[0].CorrectedCost = financeMoneyPointer(economicsMoney(0))
			case "missing_day":
				days[1].ledgerSourceCosts = nil
			case "mismatch":
				details[0].KnownCorrectedCost = economicsMoney(8)
			case "offsetting_mismatch":
				details[0].KnownCorrectedCost = economicsMoney(8)
				details = append(details, financeCostDetailView{Domain: "b", CorrectionSource: "经济事实账", KnownCorrectedCost: economicsMoney(0)})
				days[0].ledgerSourceCosts["b"] = 1
			case "overflow":
				days[0].ledgerSourceCosts["a"] = math.MaxInt64
			case "negative":
				days[0].ledgerSourceCosts["a"] = -1
			case "unselected":
				details[0].CorrectionSource = "充值版本"
			}
			err := applyFinanceDailyLedgerCorrection(days, details)
			if mode == "overflow" || mode == "negative" {
				if err == nil {
					t.Fatal("invalid amount accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, day := range days {
				v := day.LedgerCorrection
				switch mode {
				case "exact", "zero":
					if v.Cost == nil || v.Status != "complete" {
						t.Fatalf("valid projection lost: %+v", v)
					}
				case "partial":
					if v.Cost != nil || v.KnownCost.MicroUSD == "" || v.Status != "incomplete" {
						t.Fatalf("partial projection: %+v", v)
					}
				case "unselected":
					if v.KnownCost.MicroUSD != "" || v.Status != "not_required" {
						t.Fatal("unselected source included")
					}
				default:
					if v.Status != "source_mismatch" || v.KnownCost.MicroUSD != "" || v.Cost != nil {
						t.Fatalf("mismatch published: %+v", v)
					}
				}
			}
			encoded, err := json.Marshal(days)
			if err != nil {
				t.Fatal(err)
			}
			var cached []financeDailyView
			if err = json.Unmarshal(encoded, &cached); err != nil {
				t.Fatal(err)
			}
			if cached[0].LedgerCorrection.KnownCost != days[0].LedgerCorrection.KnownCost || cached[0].LedgerCorrection.Status != days[0].LedgerCorrection.Status {
				t.Fatal("cache roundtrip lost projection")
			}
		})
	}
}

func TestFinanceDailyLedgerUnknownCostIsNotZero(t *testing.T) {
	m, scope, _ := dailyBillFixture(t)
	pub := insertEconomicsReportHour(t, m, "hour.example", "epoch", scope.FromTs, 1, 0, 5, 0, 0)
	if err := m.storeDB.Model(&ChannelEconomicsHourPublication{}).Where("publication_id=?", pub.PublicationID).Updates(map[string]any{"corrected_cost_known": false, "profit_known": false, "coverage_status": "finance_version_conflict"}).Error; err != nil {
		t.Fatal(err)
	}
	insertEconomicsReportManifest(t, m, pub.Domain, pub.AccountEpoch, pub.HourTs, pub)
	ledger, err := m.buildChannelEconomicsReportMode(context.Background(), scope, "", true)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := ledger.dailyPublishedCosts[cstDayStart(scope.FromTs)][pub.Domain]; found {
		t.Fatal("unknown cost was represented as known zero")
	}
}
