package monitor

import "math"

// TokenForce amounts are account face values, in the platform's unified
// display unit. Only dated paid/credit terms turn those amounts into cost.
const tokenForceLedgerUnit = 1.0

// Old buckets retain their original conversion evidence in SQLite. Validate
// that evidence before projecting the original Quota (costInCny) as face value.
// Never rewrite the source row or infer an amount from today's account setting.
func tokenForceBillFaceValue(row ChannelUpstreamUsageHour) (ChannelUpstreamUsageHour, bool) {
	if math.IsNaN(row.Quota) || math.IsInf(row.Quota, 0) || row.Quota < 0 ||
		math.IsNaN(row.CostUSD) || math.IsInf(row.CostUSD, 0) || row.CostUSD < 0 {
		return row, false
	}
	if row.Quota == 0 {
		if row.CostUSD != 0 {
			return row, false
		}
	} else {
		if !validUpstreamEconomicUnit(row.UnitPerUSD) {
			return row, false
		}
		expected := row.Quota / row.UnitPerUSD
		if math.IsNaN(expected) || math.IsInf(expected, 0) ||
			math.Abs(row.CostUSD-expected) > math.Max(1e-9, math.Abs(expected)*1e-9) {
			return row, false
		}
	}
	row.CostUSD, row.UnitPerUSD = row.Quota, tokenForceLedgerUnit
	return row, true
}
