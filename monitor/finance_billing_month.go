package monitor

// Monthly management accounting is a separately labelled projection. It never
// rewrites underlying business facts, CUR daily amounts or exact-window profit.
import (
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"time"
)

const financeAWSBillingMonthBasis = "aws_billing_month_v1"

type financeBillingResult struct {
	Basis         string                     `json:"basis"`
	Status        string                     `json:"status"`
	Reason        string                     `json:"reason"`
	AWSSource     string                     `json:"aws_source,omitempty"`
	AWSStatus     string                     `json:"aws_status,omitempty"`
	Revenue       *channelEconomicsMoneyView `json:"revenue"`
	UpstreamCost  *channelEconomicsMoneyView `json:"upstream_cost"`
	AWSCost       *channelEconomicsMoneyView `json:"aws_cost"`
	Profit        *channelEconomicsMoneyView `json:"profit"`
	MarginPercent *string                    `json:"margin_percent"`
}

var financeBillingUSD = regexp.MustCompile(`^-?[0-9]{1,12}(?:\.[0-9]{1,18})?$`)

// Round once per bill, half away from zero, to the report's integer micro-USD.
// Never use binary floating point for monetary calculations.
func financeBillingMoney(raw string) (channelEconomicsMoneyView, error) {
	if !financeBillingUSD.MatchString(raw) {
		return channelEconomicsMoneyView{}, fmt.Errorf("invalid monthly USD")
	}
	r, ok := new(big.Rat).SetString(raw)
	if !ok {
		return channelEconomicsMoneyView{}, fmt.Errorf("invalid monthly USD")
	}
	r.Mul(r, big.NewRat(1_000_000, 1))
	n := new(big.Int).Abs(r.Num())
	q, rem := new(big.Int), new(big.Int)
	q.QuoRem(n, r.Denom(), rem)
	if rem.Lsh(rem, 1).Cmp(r.Denom()) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	if r.Sign() < 0 {
		q.Neg(q)
	}
	if !q.IsInt64() {
		return channelEconomicsMoneyView{}, fmt.Errorf("monthly cost overflow")
	}
	return economicsMoney(q.Int64()), nil
}

func financeFullCalendarMonth(from, to int64) (time.Time, bool) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return time.Time{}, false
	}
	t := time.Unix(from, 0).In(loc)
	start := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, loc)
	return start, from == start.Unix() && to == start.AddDate(0, 1, 0).Unix()
}

func (s *financeCURSource) billingPeriod(p financePeriodView) *financeBillingResult {
	r := &financeBillingResult{Basis: financeAWSBillingMonthBasis, Status: "incomplete"}
	r.Revenue, r.UpstreamCost = copyFinanceBillingMoney(p.Statement.OperatingRevenue), copyFinanceBillingMoney(p.Statement.RawCorrectedUpstreamCost)
	month, full := financeFullCalendarMonth(p.From, p.To)
	if !full {
		r.Status, r.Reason = "partial_period", "非完整自然月：不扣入整月账单；当月及自定义区间仍查看日级已知费用"
		return r
	}
	if p.Period != month.Format("2006-01") && p.Period != "" {
		r.Reason = "月份标识与实际区间不一致"
		return r
	}
	if s.monthly != nil {
		for _, m := range s.monthly.Months {
			if m.Period != month.Format("2006-01") {
				continue
			}
			cost, err := financeBillingMoney(m.NetUSD)
			if err != nil {
				r.Reason = "AWS 月账金额无效"
				return r
			}
			r.AWSCost, r.AWSSource, r.AWSStatus = financeMoneyPointer(cost), "cost_explorer", "reconciled"
			if m.Estimated {
				r.AWSStatus = "provisional"
			}
		}
	}
	utc := time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC)
	for _, a := range s.review.Statements {
		if a.BillingPeriodStart != utc.Unix() || a.BillingPeriodEnd != utc.AddDate(0, 1, 0).Unix() {
			continue
		}
		if r.AWSCost != nil {
			r.AWSCost = nil
			r.Reason = "同月 AWS 来源冲突"
			return r
		}
		if !a.AllocationComplete || a.FirstUsageUnix > a.BillingPeriodStart || a.LastUsageThroughUnix < a.BillingPeriodEnd {
			r.Reason = "AWS 月账归属或账期覆盖尚未完整"
			return r
		}
		if a.ExcludedNanoUSD != 0 || a.NexusAPINanoUSD != a.SourceNanoUSD {
			r.Reason = "AWS 月账不是整账号全额归集，不能用于当前经营口径"
			return r
		}
		micro, err := nanoUSDToRoundedMicroUSD(a.NexusAPINanoUSD)
		if err != nil {
			r.Reason = "AWS 月账金额无效"
			return r
		}
		r.AWSCost, r.AWSSource, r.AWSStatus = financeMoneyPointer(economicsMoney(micro)), "cur", "provisional"
		if a.BillingFinalized {
			r.AWSStatus = "reconciled"
		}
	}
	finishFinanceBillingResult(r)
	return r
}

func copyFinanceBillingMoney(v *channelEconomicsMoneyView) *channelEconomicsMoneyView {
	if v == nil {
		return nil
	}
	c := *v
	return &c
}

func finishFinanceBillingResult(r *financeBillingResult) {
	r.Profit, r.MarginPercent = nil, nil
	if r.AWSCost == nil {
		r.Status, r.Reason = "incomplete", "缺少该月份可核验 AWS 成本"
		return
	}
	if r.Revenue == nil || r.UpstreamCost == nil {
		r.Status, r.Reason = "incomplete", "经营收入或修正上游总成本未完整，不能用已知部分计算利润"
		return
	}
	revenue, validRevenue := financeMoneyInt64(*r.Revenue)
	_, validUpstream := financeMoneyInt64(*r.UpstreamCost)
	_, validAWS := financeMoneyInt64(*r.AWSCost)
	if !validRevenue || !validUpstream || !validAWS {
		r.Status, r.Reason = "incomplete", "核算金额格式无效"
		return
	}
	profit, err := financeSubtract(*r.Revenue, *r.UpstreamCost)
	if err == nil {
		profit, err = financeSubtract(profit, *r.AWSCost)
	}
	if err != nil {
		r.Status, r.Reason = "incomplete", "利润金额超出安全范围"
		return
	}
	r.Profit = financeMoneyPointer(profit)
	if revenue > 0 {
		amount, _ := financeMoneyInt64(profit)
		n := new(big.Int).Mul(big.NewInt(amount), big.NewInt(100))
		margin := new(big.Rat).SetFrac(n, big.NewInt(revenue)).FloatString(2)
		r.MarginPercent = &margin
	}
	r.Status, r.Reason = "reconciled", "按 AWS 账单所属月归集；已核验月账口径，非发票审计"
	if r.AWSStatus == "provisional" {
		r.Status, r.Reason = "provisional", "AWS 账单尚未定稿，利润和毛利率为暂估，账单更新后需重算"
	}
}

func (s *financeCURSource) applyBillingMonths(report *financeOperatingReport) {
	if s == nil || !s.billingMonth || report == nil {
		return
	}
	for i := range report.Periods {
		report.Periods[i].BillingResult = s.billingPeriod(report.Periods[i])
	}
	total := &financeBillingResult{Basis: financeAWSBillingMonthBasis, Status: "incomplete", Reason: "缺少完整月度核算结果"}
	report.BillingResult = total
	periods := append([]financePeriodView(nil), report.Periods...)
	sort.Slice(periods, func(i, j int) bool { return periods[i].From < periods[j].From })
	if len(periods) == 0 {
		return
	}
	cursor := report.From
	var revenue, upstream, aws int64
	provisional := false
	for _, p := range periods {
		r := p.BillingResult
		if p.From != cursor || p.To > report.To || p.To <= p.From {
			total.Reason = "月度区间不连续或重叠，不能汇总"
			return
		}
		cursor = p.To
		if r.Status == "partial_period" {
			total.Status, total.Reason = "partial_period", "查询包含非完整月份，不计算整个区间的月账口径利润；可查看下方完整月份"
			return
		}
		if r.Profit == nil {
			total.Reason = "部分月份收入或成本未完整，不将完整月份小计当作全区间利润"
			return
		}
		for _, pair := range []struct {
			dst   *int64
			money *channelEconomicsMoneyView
		}{{&revenue, r.Revenue}, {&upstream, r.UpstreamCost}, {&aws, r.AWSCost}} {
			if pair.money == nil {
				total.Reason = "月度金额缺失"
				return
			}
			v, ok := financeMoneyInt64(*pair.money)
			if !ok || addFinanceCURInt64(pair.dst, v) != nil {
				total.Reason = "月度汇总金额超出安全范围"
				return
			}
		}
		provisional = provisional || r.Status == "provisional"
	}
	if cursor != report.To {
		total.Reason = "月度区间未覆盖完整查询范围"
		return
	}
	total.Revenue, total.UpstreamCost, total.AWSCost = financeMoneyPointer(economicsMoney(revenue)), financeMoneyPointer(economicsMoney(upstream)), financeMoneyPointer(economicsMoney(aws))
	total.AWSStatus = "reconciled"
	if provisional {
		total.AWSStatus = "provisional"
	}
	finishFinanceBillingResult(total)
}
