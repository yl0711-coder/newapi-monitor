package monitor

import (
	"context"
	"math"
	"testing"
)

func TestAICodeWithDailyUnitRequiresExistingContract(t *testing.T) {
	for _, unit := range []float64{0, 1, -1, 2, 7, math.NaN(), math.Inf(1)} {
		got, err := aiCodeWithDailyUnit(unit)
		if unit == 0 || unit == 1 {
			if err != nil || got != 1 {
				t.Fatalf("unit=%v got=%v err=%v", unit, got, err)
			}
		} else if err == nil {
			t.Fatalf("invalid unit accepted: %v", unit)
		}
	}
}

func TestAICodeWithDailyUpgradeResumesMixedStagesWithoutRepricing(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "compatible", true: "inconsistent"}[invalid], func(t *testing.T) {
			m := newStabilityTestMonitor(t)
			day := recordTestDay()
			row := ChannelUpstreamAccount{Domain: "daily-evidence.test", Provider: upstreamProviderAICodeWith, BalanceUnit: 1}
			round := AICodeWithUsageRound{Domain: row.Domain, RoundID: "upgrade", Kind: "tail", CredentialSetVersion: "v1", WindowFrom: day, WindowTo: day + 86400, TotalKeys: 2, Status: upstreamStatusPending}
			if err := m.storeDB.Create(&round).Error; err != nil {
				t.Fatal(err)
			}
			old := ChannelUpstreamUsageHour{Domain: row.Domain, HourTs: day, BucketSeconds: 86400, Quota: 5, CostUSD: 5}
			if err := m.storeDB.Create(&old).Error; err != nil {
				t.Fatal(err)
			}
			for i, slot := range []string{"legacy", "new"} {
				state := AICodeWithKeySyncState{Domain: row.Domain, SlotID: slot, CredentialSetVersion: "v1", TailRoundID: round.RoundID}
				if err := m.storeDB.Create(&state).Error; err != nil {
					t.Fatal(err)
				}
				for offset := int64(0); offset < 2; offset++ {
					// Include a zero-cost day: its missing unit is valid only under
					// the fixed contract, not an invented quota/cost division.
					part := AICodeWithUsageStage{Domain: row.Domain, RoundID: round.RoundID, SlotID: slot, HourTs: day + offset*86400, BucketSeconds: 86400, UnitPerUSD: float64(i), Quota: float64(3 * (1 - offset)), CostUSD: float64(3 * (1 - offset))}
					if invalid && i == 0 && offset == 0 {
						part.CostUSD = 1
					}
					if err := m.storeDB.Create(&part).Error; err != nil {
						t.Fatal(err)
					}
				}
			}
			// Both legacy and current keys cover the same two-day frozen window.
			round.WindowTo = day + 2*86400
			if err := m.storeDB.Save(&round).Error; err != nil {
				t.Fatal(err)
			}
			published, err := m.publishAICodeWithRound(context.Background(), &row, round, round.WindowTo)
			if invalid {
				if err == nil || published {
					t.Fatalf("inconsistent evidence published: %v", err)
				}
			} else if err != nil || !published {
				t.Fatalf("compatible pending round blocked: %v", err)
			}
			var rows []ChannelUpstreamUsageHour
			if err := m.storeDB.Order("hour_ts ASC").Find(&rows).Error; err != nil {
				t.Fatal(err)
			}
			var stages int64
			if err := m.storeDB.Model(&AICodeWithUsageStage{}).Count(&stages).Error; err != nil {
				t.Fatal(err)
			}
			if invalid {
				if len(rows) != 1 || rows[0].CostUSD != 5 || stages != 4 {
					t.Fatalf("failed publication lost history or resume evidence: rows=%+v stages=%d", rows, stages)
				}
			} else if len(rows) != 2 || rows[0].UnitPerUSD != 1 || rows[0].CostUSD != 6 || rows[1].UnitPerUSD != 1 || rows[1].CostUSD != 0 || stages != 0 {
				t.Fatalf("resumed publication lost evidence: rows=%+v stages=%d", rows, stages)
			}
		})
	}
}
