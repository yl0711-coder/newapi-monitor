package monitor

import (
	"math/big"
	"time"
)

// Internal operator actions, never customer financial data or auto-adjustment
// instructions. The same rules apply to every account and every closed month.
type MonthlyResolutionCase struct {
	UserID     int64  `json:"user_id"`
	GroupID    int64  `json:"group_id"`
	Month      string `json:"month"`
	Code       string `json:"code"`
	NextAction string `json:"next_action"`
}

type MonthlyResolutionPlan struct {
	Mode            string                  `json:"mode"`
	InputSHA256     string                  `json:"input_sha256"`
	SourceIntegrity string                  `json:"source_integrity"`
	Cases           []MonthlyResolutionCase `json:"cases"`
}

// PlanMonthlyResolution diagnoses retained evidence without writes, retries,
// tolerance thresholds or per-user exceptions. Changing evidence is required
// before repeating a failed calculation; requesting this plan cannot publish.
func PlanMonthlyResolution(raw []byte, now time.Time) (MonthlyResolutionPlan, error) {
	input, err := DecodeMonthlyPreparationInput(raw)
	if err != nil {
		return MonthlyResolutionPlan{}, err
	}
	r, err := PrepareMonthlyReconciliation(raw, now)
	if err != nil {
		return MonthlyResolutionPlan{}, err
	}
	p := MonthlyResolutionPlan{Mode: "internal_resolution_plan_not_publication", InputSHA256: r.InputSHA256, SourceIntegrity: r.SourceIntegrity, Cases: []MonthlyResolutionCase{}}
	for i, account := range r.Accounts {
		a := input.Accounts[i]
		if account.Error != "" {
			for _, month := range a.Months {
				p.Cases = append(p.Cases, MonthlyResolutionCase{a.UserID, a.GroupID, month, "repair_input", "修正该账号的无效来源或范围；不修改其他账号，不以重复计算代替修复"})
			}
			continue
		}
		for _, row := range account.Months {
			c := MonthlyResolutionCase{UserID: a.UserID, GroupID: a.GroupID, Month: row.Month}
			c.Code, c.NextAction = monthlyResolutionAction(a, account, row, now)
			p.Cases = append(p.Cases, c)
		}
	}
	return p, nil
}

func monthlyResolutionAction(a MonthlyPreparationAccount, report MonthlyPreparationAccountReport, row MonthlyPreparationRow, now time.Time) (string, string) {
	switch row.Status {
	case "invalid_month":
		return "select_closed_month", "选择已结束且不早于账号基准的月份"
	case "incomplete_sources":
		if row.LogConsume == "" {
			return "complete_usage_coverage", "补齐该账号缺失或重叠的消费区间；保留已取得区间，补齐后重算"
		}
		return "complete_funding_or_bridge", "消费已取得；补齐资金流水或月界与余额依据间的衔接区间，不重复拉取已完整消费"
	case "incomplete_wallet_sources":
		return "verify_exact_wallet_boundaries", "核验该月期初、期末的结算截点及实际退款依据；采集时间等于零点不代表精确月界"
	case "wallet_counter_mismatch":
		return "balance_counter_conflict", "核对余额变动、资金流水和累计用量；不把冲突差额改为消费或自动调整"
	case "wallet_debit_above_logs":
		return "debit_above_logs_review", "核对消费覆盖与实际扣费差异；不自动向客户追加费用"
	case "invalid_wallet_debit":
		return "repair_wallet_evidence", "核对退款、实际扣费和金额范围，修正依据后重算"
	case "reconciled_draft":
		if row.Basis == monthlyExactWalletBasis {
			return "ready_for_review", "进入原有独立审核流程；核验通过后生成固定账单版本，不自动发布"
		}
		for _, b := range a.Snapshots {
			if b.At == row.AnchorAt && b.hasReviewedCutoff() && a.Baseline.hasReviewedCutoff() {
				return "ready_for_review", "金额已核平；进入原有独立来源审核，不自动发布"
			}
		}
		return "verify_legacy_cutoff", "旧草稿金额已核平；补核验所用余额截点的时序依据，不改消费或重复分配差额"
	case "no_matching_balance_anchor":
		return monthlyResidualAction(a, report, row.Month, now)
	default:
		return "review_unclassified_evidence", "保留现有结果并核对未识别的证据状态，不自动重试或发布"
	}
}

func monthlyResidualAction(a MonthlyPreparationAccount, report MonthlyPreparationAccountReport, month string, now time.Time) (string, string) {
	_, end, err := monthlyWindow(month, now)
	if err != nil || !a.Baseline.hasReviewedCutoff() {
		return "verify_cutoff_evidence", "先核验基准及覆盖月末的结算截点；普通采样残差不属于已确认的消费差异"
	}
	verified := map[int64]bool{}
	for _, b := range a.Snapshots {
		if b.hasReviewedCutoff() && b.At >= end.Unix() {
			verified[b.At] = true
		}
	}
	var residual string
	count, varying, conflicting, above := 0, false, false, false
	for _, check := range report.Anchors {
		if !verified[check.At] || (check.Status != "matched" && check.Status != "mismatch") {
			continue
		}
		balance, bok := new(big.Int).SetString(check.BalanceGapQuota, 10)
		counter, cok := new(big.Int).SetString(check.CounterGapQuota, 10)
		if !bok || !cok || new(big.Int).Add(balance, counter).Sign() != 0 {
			conflicting = true
		}
		if bok && balance.Sign() < 0 {
			above = true
		}
		if count > 0 && residual != check.BalanceGapQuota {
			varying = true
		}
		residual = check.BalanceGapQuota
		count++
	}
	if conflicting {
		return "balance_counter_conflict", "有效截点的余额与累计用量差额不互相对应，核对资金及计数依据"
	}
	if above {
		return "debit_above_logs_review", "有效截点存在可能多扣或漏记的差异，单独核验，不自动补收"
	}
	if count == 0 {
		return "verify_cutoff_evidence", "补齐覆盖月末且具有完整衔接流水的结算截点；不以普通采样凑平"
	}
	if varying {
		return "varying_residual_review", "不同有效截点的差额仍变化，分离时序或新增差异，不把最新残差一次分配到历史月份"
	}
	if count == 1 {
		return "confirm_residual_with_more_evidence", "只有一个有效差异截点；补独立依据核实差额是否持续，不据此自动归期"
	}
	return "historical_adjustment_review", "多个有效截点残差一致；形成单账号历史处理提案，确认金额及归期后再走审核，消费原日志不改"
}
