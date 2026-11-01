package monitor

// An operator's proposed historical allocation is not an observed monthly
// debit. Keep this preview separate from candidates/imports and customer DTOs.
import (
	"errors"
	"math/big"
	"sort"
	"time"
)

type MonthlyHistoricalSettlementRequest struct {
	InputSHA256      string `json:"input_sha256"`
	UserID           int64  `json:"user_id"`
	AnchorAt         int64  `json:"anchor_at"`
	SettlementMonth  string `json:"settlement_month"`
	ExpectedGapQuota int64  `json:"expected_gap_quota"`
}

type MonthlyHistoricalSettlementPreview struct {
	Mode                 string                             `json:"mode"`
	Status               string                             `json:"status"`
	Reason               string                             `json:"reason,omitempty"`
	SourceIntegrity      string                             `json:"source_integrity"`
	Request              MonthlyHistoricalSettlementRequest `json:"request"`
	ObservedBalanceGap   string                             `json:"observed_balance_gap_quota"`
	ObservedCounterGap   string                             `json:"observed_counter_gap_quota"`
	Months               []MonthlyHistoricalSettlementRow   `json:"months"`
	ExcludedObservations []MonthlyPreparationBalance        `json:"excluded_observations,omitempty"`
}

// Preview-only: an explicit proposed balance adjustment is not consumption,
// recharge, refund, or a new wallet transaction. Never embed it in customer
// publication DTOs until an independently reviewed issuance path exists.
type MonthlyHistoricalSettlementRow struct {
	MonthlyPreparationRow
	BalanceAdjustment string `json:"balance_adjustment,omitempty"`
}

// PrepareMonthlyHistoricalSettlementPreview never changes source facts and
// never returns a publication candidate. Even with a reviewed source cutoff,
// matching a balance/counter does not prove cause or which month incurred it. The requested
// allocation remains a proposal even when its arithmetic is consistent.
func PrepareMonthlyHistoricalSettlementPreview(raw []byte, request MonthlyHistoricalSettlementRequest, now time.Time) (MonthlyHistoricalSettlementPreview, error) {
	p := MonthlyHistoricalSettlementPreview{Mode: "historical_allocation_preview_not_publication", Status: "blocked", SourceIntegrity: "not_checked_by_pure_calculator", Request: request, Months: []MonthlyHistoricalSettlementRow{}}
	if !validMonthlyPublication(request.InputSHA256) || request.InputSHA256 != monthlyDigest(raw) || request.UserID <= 0 || request.AnchorAt <= 0 || request.ExpectedGapQuota <= 0 {
		return p, errors.New("历史处理请求的输入指纹、账号、截点或明确差额无效")
	}
	start, end, err := monthlyWindow(request.SettlementMonth, now)
	if err != nil || len(request.SettlementMonth) != 7 || start.Format("2006-01") != request.SettlementMonth || request.AnchorAt < end.Unix() {
		return p, errors.New("历史处理月份未结束或余额截点不能覆盖该月")
	}
	input, err := DecodeMonthlyPreparationInput(raw)
	if err != nil {
		return p, err
	}
	report, err := PrepareMonthlyReconciliation(raw, now)
	if err != nil {
		return p, err
	}
	var a *MonthlyPreparationAccount
	var r *MonthlyPreparationAccountReport
	for i := range input.Accounts {
		if input.Accounts[i].UserID == request.UserID {
			a = &input.Accounts[i]
			r = &report.Accounts[i]
			break
		}
	}
	if a == nil || r.Error != "" {
		return p, errors.New("历史处理账号不存在或原始核对输入无效")
	}
	if len(a.WalletMonths) != 0 {
		return p, errors.New("已有独立月界核算的账号不使用历史分配预览")
	}
	var anchor *MonthlyPreparationAnchorCheck
	var target *MonthlyPreparationRow
	for i := range r.Anchors {
		if r.Anchors[i].At == request.AnchorAt {
			anchor = &r.Anchors[i]
		}
	}
	for i := range r.Months {
		if r.Months[i].Month == request.SettlementMonth {
			target = &r.Months[i]
		}
	}
	if anchor == nil || target == nil {
		return p, errors.New("选定的留存余额截点或月份不存在")
	}
	p.ObservedBalanceGap, p.ObservedCounterGap = anchor.BalanceGapQuota, anchor.CounterGapQuota
	if !a.Baseline.hasReviewedCutoff() {
		p.Reason = "历史基准缺少已核验的日志结算截点依据；不使用采集时间推定历史扣费"
		return p, nil
	}
	reviewed := make(map[int64]bool)
	for _, snapshot := range a.Snapshots {
		if snapshot.At > request.AnchorAt {
			continue
		}
		if snapshot.hasReviewedCutoff() {
			reviewed[snapshot.At] = true
		} else {
			p.ExcludedObservations = append(p.ExcludedObservations, snapshot)
		}
	}
	if !reviewed[request.AnchorAt] {
		p.Reason = "选定余额仅为采集观察或截点依据未核验；不能将观察残差自动分配为某月消费调整"
		return p, nil
	}
	if anchor.Status != "mismatch" || target.Status != "no_matching_balance_anchor" {
		p.Reason = "选定范围不是可单独核对的历史余额差异；不覆盖已核平月份或其他错误"
		return p, nil
	}
	gap := big.NewInt(request.ExpectedGapQuota)
	if anchor.BalanceGapQuota != gap.String() || anchor.CounterGapQuota != new(big.Int).Neg(new(big.Int).Set(gap)).String() {
		p.Reason = "选定快照的余额及累计扣费差额与明确处理金额不一致；不能将其他差额倒灌到该月"
		return p, nil
	}
	// Independent earlier anchors must not contradict the proposed allocation.
	// An intra-month observation can only contain part of the same residual.
	for _, check := range r.Anchors {
		if check.At > request.AnchorAt || !reviewed[check.At] {
			continue
		}
		b, bok := new(big.Int).SetString(check.BalanceGapQuota, 10)
		c, cok := new(big.Int).SetString(check.CounterGapQuota, 10)
		if !bok || !cok || new(big.Int).Add(new(big.Int).Set(b), c).Sign() != 0 || b.Sign() < 0 || b.Cmp(gap) > 0 ||
			(check.At <= start.Unix() && b.Sign() != 0) || (check.At >= end.Unix() && b.Cmp(gap) != 0) {
			p.Reason = "其他留存余额截点与指定的历史归期规则冲突；需单独核对"
			return p, nil
		}
	}
	sort.Slice(a.Spans, func(i, j int) bool { return a.Spans[i].From < a.Spans[j].From })
	sort.Slice(a.MovementCoverage, func(i, j int) bool { return a.MovementCoverage[i].From < a.MovementCoverage[j].From })
	rows := []MonthlyHistoricalSettlementRow{}
	for _, original := range r.Months {
		from, to, windowErr := monthlyWindow(original.Month, now)
		if windowErr != nil {
			p.Reason = "输入含无效月份，未输出部分成功结果"
			return p, nil
		}
		if to.Unix() > request.AnchorAt {
			continue
		}
		if original.Status == "reconciled_draft" {
			rows = append(rows, MonthlyHistoricalSettlementRow{MonthlyPreparationRow: original})
			continue
		}
		if original.Status != "no_matching_balance_anchor" {
			p.Reason = "月份存在余额差异以外的缺口，未输出部分成功结果"
			return p, nil
		}
		before, rangeErr := monthlyPreparationRange(*a, a.Baseline.At, from.Unix())
		if rangeErr != nil {
			p.Reason = rangeErr.Error()
			return p, nil
		}
		v, rangeErr := monthlyPreparationRange(*a, from.Unix(), to.Unix())
		if rangeErr != nil {
			p.Reason = rangeErr.Error()
			return p, nil
		}
		opening, rangeErr := before.balance(a.Baseline.BalanceQuota)
		if rangeErr == nil && from.Unix() >= end.Unix() {
			rangeErr = addEconomicsInt64(&opening, request.ExpectedGapQuota)
		}
		if rangeErr != nil {
			p.Reason = "历史余额重建超出金额范围"
			return p, nil
		}
		var adjustment int64
		if original.Month == request.SettlementMonth {
			if v.consume < request.ExpectedGapQuota {
				p.Reason = "拟处理差额超过当月日志消费，超出本预览支持的历史差异范围"
				return p, nil
			}
			adjustment = request.ExpectedGapQuota
		}
		closing, rangeErr := v.balance(opening)
		if rangeErr == nil {
			rangeErr = addEconomicsInt64(&closing, adjustment)
		}
		if rangeErr != nil {
			p.Reason = "历史余额重建超出金额范围"
			return p, nil
		}
		row := original
		row.Status, row.Basis = "historical_allocation_proposal", "explicit_historical_balance_adjustment_not_observed_transaction"
		row.Reason = "仅表示指定历史归期规则下金额可核平，不证明差额实际发生月份或原因；不可直接导入发布"
		row.AnchorAt, row.AnchorSource = anchor.At, anchor.Source
		refs := map[string]bool{a.Baseline.CutoffSource: true, a.Baseline.Source: true, a.BasisSource: true}
		for ref := range before.sources {
			refs[ref] = true
		}
		for _, ref := range row.Sources {
			refs[ref] = true
		}
		for _, snapshot := range a.Snapshots {
			if snapshot.At <= request.AnchorAt && reviewed[snapshot.At] {
				refs[snapshot.Source], refs[snapshot.CutoffSource] = true, true
			}
		}
		row.Sources = monthlyPreparationRefs(refs)
		row.Amounts = &MonthlyPreparationAmounts{Opening: monthlyQuotaMoney(opening), Increase: monthlyQuotaMoney(v.increase), Decrease: monthlyQuotaMoney(v.decrease), Consume: monthlyQuotaMoney(v.consume), Refund: monthlyQuotaMoney(v.refund), Closing: monthlyQuotaMoney(closing)}
		proposal := MonthlyHistoricalSettlementRow{MonthlyPreparationRow: row}
		if adjustment != 0 {
			proposal.BalanceAdjustment = monthlyQuotaMoney(adjustment)
		}
		rows = append(rows, proposal)
	}
	p.Status, p.Months = "proposal_requires_internal_review", rows
	return p, nil
}
