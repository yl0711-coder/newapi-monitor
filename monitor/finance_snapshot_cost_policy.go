package monitor

import "encoding/json"

// Upgrade display may reuse unchanged providers' dated amounts, but must not
// expose currency-converted TokenForce costs under the new paid/credit policy.
// This is a pure snapshot check: no SQLite or provider calls on the fast path.
func financePriorCostPolicyCompatible(payload []byte) (bool, error) {
	var report struct {
		Statement   financeStatementView `json:"statement"`
		Periods     []financePeriodView  `json:"periods"`
		Days        []financeDailyView   `json:"days"`
		CostDetails []struct {
			Provider string `json:"provider"`
		} `json:"cost_details"`
	}
	if err := json.Unmarshal(payload, &report); err != nil {
		return false, err
	}
	for _, detail := range report.CostDetails {
		switch detail.Provider {
		case upstreamProviderNewAPI, upstreamProviderSub2API, upstreamProviderAICodeWith, "openox":
		default:
			return false, nil // Includes TokenForce, missing and unreviewed providers.
		}
	}
	if len(report.CostDetails) > 0 {
		return true, nil
	}
	// Old sparse snapshots without provider attribution cannot prove that
	// account-level bill/corrected amounts use an unchanged source unit.
	accountCost := func(statement financeStatementView) bool {
		return statement.UpstreamBilledCost != nil || statement.KnownUpstreamBilledCost.MicroUSD != "" ||
			statement.RawCorrectedUpstreamCost != nil || statement.KnownRawCorrectedUpstreamCost.MicroUSD != "" ||
			statement.CorrectedUpstreamCost != nil || statement.KnownCorrectedUpstreamCost.MicroUSD != ""
	}
	if accountCost(report.Statement) {
		return false, nil
	}
	for _, period := range report.Periods {
		if accountCost(period.Statement) {
			return false, nil
		}
	}
	for _, day := range report.Days {
		if accountCost(day.Statement) {
			return false, nil
		}
	}
	return true, nil
}
