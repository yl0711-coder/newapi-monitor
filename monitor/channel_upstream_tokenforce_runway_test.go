package monitor

import (
	"math"
	"testing"
	"time"
)

func TestTokenForceRunwayUsesFaceValueNotFXOrRechargeCorrection(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		name := "complete"
		if invalid {
			name = "invalid_source"
		}
		t.Run(name, func(t *testing.T) {
			m := newChannelUpstreamTestMonitor(t)
			now := time.Date(2026, 9, 20, 12, 0, 0, 0, cstLocation).Unix()
			policy := upstreamBalancePolicyFor(defaultAlertConfig())
			from, to := upstreamBalanceWindow(now, policy.Lookback)
			account := ChannelUpstreamAccount{Domain: "hainahn.com", Provider: upstreamProviderTokenForce,
				Enabled: true, UsageSyncEnabled: true, Status: upstreamStatusOK, LastSuccessAt: now,
				BalanceKnown: true, BalanceRaw: 144, BalanceUSD: 20, BalanceUnit: 7.2}
			rows := make([]ChannelUpstreamUsageHour, 0, (to-from)/3600)
			for hour := from; hour < to; hour += 3600 {
				rows = append(rows, ChannelUpstreamUsageHour{Domain: account.Domain, Provider: account.Provider,
					HourTs: hour, BucketSeconds: 3600, Requests: 1, Quota: 3, CostUSD: 3 / 7.2, UnitPerUSD: 7.2})
			}
			if invalid {
				rows[0].CostUSD = 9 // Inconsistent with the stored original quota/unit pair.
			}
			if err := m.storeDB.CreateInBatches(rows, 200).Error; err != nil {
				t.Fatal(err)
			}
			if err := m.storeDB.Create(&ChannelDomainCost{Domain: account.Domain, RechargePaid: 1, RechargeCredit: 10}).Error; err != nil {
				t.Fatal(err)
			}
			view := m.channelUpstreamAccountView(account)
			estimates, err := m.loadUpstreamBurnEstimates(t.Context(), now, policy, map[string]ChannelUpstreamAccountView{account.Domain: view})
			if err != nil {
				t.Fatal(err)
			}
			estimate := estimates[account.Domain]
			assessment := assessUpstreamBalance(view, estimate, policy, now, 5)
			if invalid {
				if assessment.Available || estimate.Reason == "" {
					t.Fatal("invalid legacy conversion yielded a misleading runway", assessment)
				}
				return
			}
			if !assessment.Available || estimate.CoveragePct != 100 || math.Abs(estimate.AverageDailyCostUSD-72) > 1e-9 ||
				assessment.EstimatedRunwayDays == nil || *assessment.EstimatedRunwayDays != 2 {
				t.Fatal("runway must use 144 native balance / 72 native daily consumption, without FX or recharge ratio", estimate, assessment)
			}
		})
	}
}
