package monitor

// finance_cur_artifact.go is a deliberately narrow bridge from an offline,
// reviewed CUR 2.0 allocation artifact into the read-only finance report. It
// never downloads CUR data, reads AWS credentials, or writes Monitor state.

import (
	"fmt"
	"math"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/yl0711-coder/newapi-monitor/internal/financecur"
)

type financeCURCostView struct {
	Enabled              bool                       `json:"enabled"`
	Loaded               bool                       `json:"loaded"`
	Status               string                     `json:"status"`
	StatementID          string                     `json:"statement_id,omitempty"`
	BillingPeriodStart   int64                      `json:"billing_period_start,omitempty"`
	BillingPeriodEnd     int64                      `json:"billing_period_end,omitempty"`
	DataFrom             int64                      `json:"data_from,omitempty"`
	DataThrough          int64                      `json:"data_through,omitempty"`
	AllocationComplete   bool                       `json:"allocation_complete"`
	BillingFinalized     bool                       `json:"billing_finalized"`
	CoveragePPM          int64                      `json:"coverage_ppm,omitempty"`
	IssueCount           int64                      `json:"issue_count,omitempty"`
	UnallocatedNanoUSD   string                     `json:"unallocated_nano_usd,omitempty"`
	UnallocatedCost      channelEconomicsMoneyView  `json:"unallocated_cost"`
	ConflictNanoUSD      string                     `json:"conflict_nano_usd,omitempty"`
	ConflictCost         channelEconomicsMoneyView  `json:"conflict_cost"`
	IncludedDays         int                        `json:"included_days,omitempty"`
	KnownNanoUSD         string                     `json:"known_nano_usd,omitempty"`
	KnownCost            channelEconomicsMoneyView  `json:"known_cost"`
	ExactCost            *channelEconomicsMoneyView `json:"exact_cost"`
	RangeComplete        bool                       `json:"range_complete"`
	MonthRoundingMicro   string                     `json:"month_rounding_adjustment_micro_usd,omitempty"`
	DayRoundingMicro     string                     `json:"day_rounding_adjustment_micro_usd,omitempty"`
	ProductRoundingMicro string                     `json:"product_rounding_adjustment_micro_usd,omitempty"`
	Error                string                     `json:"error,omitempty"`
}

type financeCURProductCostView struct {
	ProductCode  string                     `json:"product_code"`
	Status       string                     `json:"status"`
	IncludedDays int                        `json:"included_days"`
	KnownNanoUSD string                     `json:"known_nano_usd"`
	KnownCost    channelEconomicsMoneyView  `json:"known_cost"`
	ExactCost    *channelEconomicsMoneyView `json:"exact_cost"`
	SharePercent string                     `json:"share_percent,omitempty"`
}

func (m *Monitor) loadFinanceCURCost(fromUnix, toUnix int64) financeCURCostView {
	artifact, view := m.loadFinanceCURArtifact()
	if !view.Loaded {
		return view
	}
	projected, err := projectVerifiedFinanceCURCost(artifact, fromUnix, toUnix)
	if err != nil {
		view.Status = "error"
		view.Loaded = false
		view.Error = err.Error()
		return view
	}
	return projected
}

func (m *Monitor) loadFinanceCURArtifact() (financecur.Artifact, financeCURCostView) {
	if !m.cfg.FinanceCURArtifactEnabled {
		return financecur.Artifact{}, financeCURCostView{Status: "disabled"}
	}
	view := financeCURCostView{Enabled: true, Status: "error"}
	file, err := os.Open(strings.TrimSpace(m.cfg.FinanceCURArtifactPath))
	if err != nil {
		view.Error = "无法打开本地核算产物"
		return financecur.Artifact{}, view
	}
	defer file.Close()
	artifact, err := financecur.ReadArtifact(file)
	if err != nil {
		view.Error = "本地核算产物完整性校验失败"
		return financecur.Artifact{}, view
	}
	view.Loaded = true
	view.Status = "draft"
	return artifact, view
}

func projectFinanceCURCost(artifact financecur.Artifact, fromUnix, toUnix int64) (financeCURCostView, error) {
	if err := financecur.VerifyArtifact(artifact); err != nil {
		return financeCURCostView{}, fmt.Errorf("本地核算产物完整性校验失败")
	}
	return projectVerifiedFinanceCURCost(artifact, fromUnix, toUnix)
}

func projectVerifiedFinanceCURCost(artifact financecur.Artifact, fromUnix, toUnix int64) (financeCURCostView, error) {
	if artifact.Timeline.TimeZone != "Asia/Shanghai" {
		return financeCURCostView{}, fmt.Errorf("AWS 成本核算时区不是 Asia/Shanghai")
	}
	return projectFinanceCURBuckets(artifact.Statement, artifact.Timeline.Days, fromUnix, toUnix)
}

func projectFinanceCURBuckets(statement financecur.Statement, buckets []financecur.TimeBucket, fromUnix, toUnix int64) (financeCURCostView, error) {
	if fromUnix >= toUnix {
		return financeCURCostView{}, fmt.Errorf("AWS 成本查询区间无效")
	}
	if statement.TimeZone != "Asia/Shanghai" {
		return financeCURCostView{}, fmt.Errorf("AWS 成本核算时区不是 Asia/Shanghai")
	}
	view := financeCURCostView{
		Enabled: true, Loaded: true, Status: "draft", StatementID: statement.StatementID,
		BillingPeriodStart: statement.BillingPeriodStart, BillingPeriodEnd: statement.BillingPeriodEnd,
		DataFrom: statement.FirstUsageUnix, DataThrough: statement.LastUsageThroughUnix,
		AllocationComplete: statement.AllocationComplete, BillingFinalized: statement.BillingFinalized,
		CoveragePPM: statement.CoveragePPM, IssueCount: statement.IssueCount,
		UnallocatedNanoUSD: strconv.FormatInt(statement.UnallocatedNanoUSD, 10),
		ConflictNanoUSD:    strconv.FormatInt(statement.ConflictNanoUSD, 10),
	}
	unallocatedMicro, err := nanoUSDToRoundedMicroUSD(statement.UnallocatedNanoUSD)
	if err != nil {
		return financeCURCostView{}, err
	}
	conflictMicro, err := nanoUSDToRoundedMicroUSD(statement.ConflictNanoUSD)
	if err != nil {
		return financeCURCostView{}, err
	}
	view.UnallocatedCost = economicsMoney(unallocatedMicro)
	view.ConflictCost = economicsMoney(conflictMicro)
	var knownNano int64
	allIncludedComplete := true
	coveredThrough := fromUnix
	contiguous := true
	ordered := append([]financecur.TimeBucket(nil), buckets...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].FromUnix < ordered[j].FromUnix })
	for _, bucket := range ordered {
		if bucket.ToUnix <= bucket.FromUnix {
			return financeCURCostView{}, fmt.Errorf("AWS 成本时间桶无效")
		}
		// Daily CUR amounts cannot be safely split again. Only include a bucket
		// fully contained by the requested range; partial boundary days remain
		// unknown rather than being prorated.
		if bucket.FromUnix < fromUnix || bucket.ToUnix > toUnix {
			continue
		}
		if bucket.FromUnix < coveredThrough {
			return financeCURCostView{}, fmt.Errorf("AWS 成本时间桶重叠")
		}
		contiguous = contiguous && bucket.FromUnix == coveredThrough
		coveredThrough = bucket.ToUnix
		if (bucket.NexusAPINanoUSD > 0 && knownNano > math.MaxInt64-bucket.NexusAPINanoUSD) ||
			(bucket.NexusAPINanoUSD < 0 && knownNano < math.MinInt64-bucket.NexusAPINanoUSD) {
			return financeCURCostView{}, fmt.Errorf("AWS 成本金额超出安全范围")
		}
		knownNano += bucket.NexusAPINanoUSD
		view.IncludedDays++
		allIncludedComplete = allIncludedComplete && bucket.Complete
	}
	view.KnownNanoUSD = strconv.FormatInt(knownNano, 10)
	knownMicro, err := nanoUSDToRoundedMicroUSD(knownNano)
	if err != nil {
		return financeCURCostView{}, err
	}
	if view.IncludedDays > 0 {
		view.KnownCost = economicsMoney(knownMicro)
	}
	view.RangeComplete = statement.Status == financecur.StatementStatusPublishable &&
		fromUnix >= statement.FirstUsageUnix && toUnix <= statement.LastUsageThroughUnix &&
		view.IncludedDays > 0 && allIncludedComplete && contiguous && coveredThrough == toUnix
	if view.RangeComplete {
		view.Status = "verified"
		view.ExactCost = financeMoneyPointer(view.KnownCost)
	}
	return view, nil
}

// applyFinanceCURArtifactToReport reads and verifies the artifact once, then
// projects the same immutable evidence into the total, month and day views.
// All projections are prepared before any statement is changed so one bad
// range cannot leave a partially updated report.
func (m *Monitor) applyFinanceCURArtifactToReport(report *financeOperatingReport) {
	if report == nil {
		return
	}
	artifact, state := m.loadFinanceCURArtifact()
	if !state.Loaded {
		report.CURCost = state
		return
	}
	total, err := projectVerifiedFinanceCURCost(artifact, report.From, report.To)
	if err != nil {
		report.CURCost = financeCURProjectionError(state, err)
		return
	}
	periods := make([]financeCURCostView, len(report.Periods))
	for index := range report.Periods {
		periods[index], err = projectVerifiedFinanceCURCost(artifact, report.Periods[index].From, report.Periods[index].To)
		if err != nil {
			report.CURCost = financeCURProjectionError(state, err)
			return
		}
	}
	days := make([]financeCURCostView, len(report.Days))
	for index := range report.Days {
		days[index], err = projectVerifiedFinanceCURCost(artifact, report.Days[index].From, report.Days[index].To)
		if err != nil {
			report.CURCost = financeCURProjectionError(state, err)
			return
		}
	}
	type productProjection struct {
		code string
		cost financeCURCostView
	}
	products := make([]productProjection, 0, len(artifact.Timeline.Products))
	for _, product := range artifact.Timeline.Products {
		cost, projectErr := projectFinanceCURBuckets(artifact.Statement, product.Days, report.From, report.To)
		if projectErr != nil {
			report.CURCost = financeCURProjectionError(state, projectErr)
			return
		}
		if cost.IncludedDays > 0 {
			products = append(products, productProjection{code: product.ProductCode, cost: cost})
		}
	}
	monthAdjustment, err := reconcileFinanceCURRoundedCosts(total, periods)
	if err != nil {
		report.CURCost = financeCURProjectionError(state, err)
		return
	}
	productCosts := make([]financeCURCostView, len(products))
	for index := range products {
		productCosts[index] = products[index].cost
	}
	productAdjustment, err := reconcileFinanceCURRoundedCosts(total, productCosts)
	if err != nil {
		report.CURCost = financeCURProjectionError(state, err)
		return
	}
	dayAdjustment, err := reconcileFinanceCURRoundedCosts(total, days)
	if err != nil {
		report.CURCost = financeCURProjectionError(state, err)
		return
	}
	if monthAdjustment != 0 {
		total.MonthRoundingMicro = strconv.FormatInt(monthAdjustment, 10)
	}
	if dayAdjustment != 0 {
		total.DayRoundingMicro = strconv.FormatInt(dayAdjustment, 10)
	}
	if productAdjustment != 0 {
		total.ProductRoundingMicro = strconv.FormatInt(productAdjustment, 10)
	}
	productViews := make([]financeCURProductCostView, len(products))
	for index := range products {
		status := "incomplete"
		if productCosts[index].ExactCost != nil {
			status = "verified"
		}
		productViews[index] = financeCURProductCostView{
			ProductCode: products[index].code, Status: status,
			IncludedDays: productCosts[index].IncludedDays,
			KnownNanoUSD: productCosts[index].KnownNanoUSD,
			KnownCost:    productCosts[index].KnownCost,
			ExactCost:    productCosts[index].ExactCost,
			SharePercent: financeCURSharePercent(productCosts[index].KnownNanoUSD, total.KnownNanoUSD),
		}
	}
	sort.SliceStable(productViews, func(i, j int) bool {
		left, _ := strconv.ParseInt(productViews[i].KnownNanoUSD, 10, 64)
		right, _ := strconv.ParseInt(productViews[j].KnownNanoUSD, 10, 64)
		if financeCURAbs(left) == financeCURAbs(right) {
			return productViews[i].ProductCode < productViews[j].ProductCode
		}
		return financeCURAbs(left) > financeCURAbs(right)
	})
	report.CURCost = total
	report.CURProducts = productViews
	applyFinanceCURCost(&report.Statement, total)
	for index := range report.Periods {
		applyFinanceCURCost(&report.Periods[index].Statement, periods[index])
		if periods[index].ExactCost == nil {
			report.Periods[index].Status = "incomplete"
		}
	}
	for index := range report.Days {
		applyFinanceCURCost(&report.Days[index].Statement, days[index])
		if days[index].ExactCost == nil {
			report.Days[index].Status = "incomplete"
		}
	}
}

func financeCURSharePercent(partRaw, totalRaw string) string {
	part, partOK := new(big.Int).SetString(partRaw, 10)
	total, totalOK := new(big.Int).SetString(totalRaw, 10)
	if !partOK || !totalOK || total.Sign() == 0 {
		return ""
	}
	numerator := new(big.Int).Mul(part, big.NewInt(100))
	return new(big.Rat).SetFrac(numerator, total).FloatString(2)
}

func financeCURAbs(value int64) uint64 {
	if value >= 0 {
		return uint64(value)
	}
	return uint64(-(value + 1)) + 1
}

// reconcileFinanceCURRoundedCosts preserves exact parent/child reconciliation
// after nano-USD is presented as micro-USD. The at-most-sub-cent residual is
// deterministically assigned to the final included child, a standard ledger
// convention that avoids changing the source or allocation evidence.
func reconcileFinanceCURRoundedCosts(parent financeCURCostView, children []financeCURCostView) (int64, error) {
	if parent.IncludedDays == 0 {
		return 0, nil
	}
	parentNano, err := strconv.ParseInt(parent.KnownNanoUSD, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("AWS 成本总额精度证据无效")
	}
	parentMicro, ok := financeMoneyInt64(parent.KnownCost)
	if !ok {
		return 0, fmt.Errorf("AWS 成本总额展示值无效")
	}
	var childNano, childMicro int64
	last := -1
	for index := range children {
		if children[index].IncludedDays == 0 {
			continue
		}
		nano, parseErr := strconv.ParseInt(children[index].KnownNanoUSD, 10, 64)
		if parseErr != nil || addFinanceCURInt64(&childNano, nano) != nil {
			return 0, fmt.Errorf("AWS 成本子期间精度证据无效")
		}
		micro, valid := financeMoneyInt64(children[index].KnownCost)
		if !valid || addFinanceCURInt64(&childMicro, micro) != nil {
			return 0, fmt.Errorf("AWS 成本子期间展示值无效")
		}
		last = index
	}
	if childNano != parentNano {
		return 0, fmt.Errorf("AWS 成本子期间与总额不一致")
	}
	if last < 0 {
		return 0, fmt.Errorf("AWS 成本总额缺少子期间")
	}
	adjustmentMoney, err := financeSubtract(economicsMoney(parentMicro), economicsMoney(childMicro))
	if err != nil {
		return 0, fmt.Errorf("AWS 成本舍入尾差超出安全范围")
	}
	adjustment, _ := financeMoneyInt64(adjustmentMoney)
	if adjustment == 0 {
		return 0, nil
	}
	lastMicro, _ := financeMoneyInt64(children[last].KnownCost)
	if (adjustment > 0 && lastMicro > math.MaxInt64-adjustment) ||
		(adjustment < 0 && lastMicro < math.MinInt64-adjustment) {
		return 0, fmt.Errorf("AWS 成本舍入尾差超出安全范围")
	}
	children[last].KnownCost = economicsMoney(lastMicro + adjustment)
	if children[last].ExactCost != nil {
		children[last].ExactCost = financeMoneyPointer(children[last].KnownCost)
	}
	return adjustment, nil
}

func addFinanceCURInt64(total *int64, value int64) error {
	if total == nil || (value > 0 && *total > math.MaxInt64-value) || (value < 0 && *total < math.MinInt64-value) {
		return fmt.Errorf("AWS 成本金额超出安全范围")
	}
	*total += value
	return nil
}

func financeCURProjectionError(base financeCURCostView, err error) financeCURCostView {
	return financeCURCostView{Enabled: base.Enabled, Status: "error", Error: err.Error()}
}

func nanoUSDToRoundedMicroUSD(nano int64) (int64, error) {
	const nanoPerMicro = int64(1_000)
	quotient, remainder := nano/nanoPerMicro, nano%nanoPerMicro
	if remainder >= nanoPerMicro/2 {
		if quotient == math.MaxInt64 {
			return 0, fmt.Errorf("AWS 成本金额超出安全范围")
		}
		quotient++
	} else if remainder <= -nanoPerMicro/2 {
		if quotient == math.MinInt64 {
			return 0, fmt.Errorf("AWS 成本金额超出安全范围")
		}
		quotient--
	}
	return quotient, nil
}

func applyFinanceCURCost(statement *financeStatementView, cur financeCURCostView) {
	if statement == nil {
		return
	}
	statement.OperatingProfit = nil
	statement.KnownOperatingProfit = channelEconomicsMoneyView{}
	if !cur.Loaded || cur.IncludedDays == 0 {
		return
	}
	statement.KnownAWSInfrastructureCost = cur.KnownCost
	statement.AWSInfrastructureCost = cur.ExactCost
	// Platform expenditure includes internal testing. The business-only cost
	// belongs to the customer contribution view, not the operating result.
	if statement.OperatingRevenue == nil || statement.RawCorrectedUpstreamCost == nil || statement.AWSInfrastructureCost == nil {
		return
	}
	profit, err := financeSubtract(*statement.OperatingRevenue, *statement.RawCorrectedUpstreamCost)
	if err != nil {
		return
	}
	profit, err = financeSubtract(profit, *statement.AWSInfrastructureCost)
	if err != nil {
		return
	}
	statement.KnownOperatingProfit = profit
	statement.OperatingProfit = financeMoneyPointer(profit)
}

func financeCURSourceStatus(cur financeCURCostView) (string, string) {
	switch cur.Status {
	case "verified":
		if cur.ExactCost == nil {
			return "incomplete", "CUR 产物状态不一致；AWS 成本保持空值，不按 0 计算"
		}
		return "verified", fmt.Sprintf("CUR 产物已核验，当前区间 AWS 成本 %s", cur.ExactCost.Display)
	case "draft":
		amount := "当前区间没有完整自然日"
		if cur.KnownCost.MicroUSD != "" {
			amount = "当前区间已知成本 " + cur.KnownCost.Display
		}
		rounding := ""
		if cur.MonthRoundingMicro != "" || cur.DayRoundingMicro != "" || cur.ProductRoundingMicro != "" {
			rounding = "；明细舍入尾差已确定性归入最后完整期间"
		}
		blockers := make([]string, 0, 2)
		if !cur.AllocationComplete {
			blockers = append(blockers, fmt.Sprintf("归属未闭合（%d 条，%s）", cur.IssueCount, cur.UnallocatedCost.Display))
		}
		if !cur.BillingFinalized {
			blockers = append(blockers, "AWS 当期账单未封账")
		}
		if len(blockers) == 0 {
			blockers = append(blockers, "CUR 产物仍为草稿")
		}
		return "incomplete", fmt.Sprintf("%s；归属覆盖 %.4f%%；%s，草稿不参与正式利润%s", amount, float64(cur.CoveragePPM)/10_000, strings.Join(blockers, "；"), rounding)
	case "error":
		return "incomplete", cur.Error + "；AWS 成本保持空值，不按 0 计算"
	default:
		return "not_connected", "未启用本地 CUR 2.0 核算产物；AWS 成本不按 0 计算"
	}
}
