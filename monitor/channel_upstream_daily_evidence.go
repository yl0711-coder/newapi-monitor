package monitor

import (
	"fmt"
	"math"
)

// Spring's configured ledger contract is 1:1, not an FX conversion. Zero is
// the legacy missing field; other values require an explicit audited repair,
// never silent repricing of historical bills.
func aiCodeWithDailyUnit(unit float64) (float64, error) {
	if unit == 0 || unit == 1 {
		return 1, nil
	}
	return 0, fmt.Errorf("AICodeWith 日账单换算单位不符合 1:1 约定，请核对账户换算配置")
}

// An upgrade may resume a round containing pre-upgrade unit=0 stages. Infer
// only from their own amounts and the fixed contract, without changing costs
// or deleting/refetching an already staged key.
func aiCodeWithDailyStageUnit(part AICodeWithUsageStage) (float64, error) {
	unit, err := aiCodeWithDailyUnit(part.UnitPerUSD)
	if err != nil {
		return 0, err
	}
	if math.IsNaN(part.Quota) || math.IsInf(part.Quota, 0) || part.Quota < 0 ||
		math.IsNaN(part.CostUSD) || math.IsInf(part.CostUSD, 0) || part.CostUSD < 0 ||
		math.Abs(part.Quota-part.CostUSD) > 1e-9*math.Max(1, math.Abs(part.Quota)) {
		return 0, fmt.Errorf("AICodeWith 日账单暂存金额与换算依据不一致，保留已发布账单")
	}
	return unit, nil
}
