package monitor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"strconv"
	"strings"

	"github.com/yl0711-coder/newapi-monitor/internal/financecur"
)

const maxFinanceCURSourceBytes = 16 << 20

type financeCURSource struct {
	legacy        *financecur.Artifact
	projector     *financecur.SourceProjector
	review        financecur.SourceSetReview
	monthly       *financecur.CEMonthlyEvidence
	billingMonth  bool
	bundleVersion string
}

type financeCURProductProjection struct {
	code string
	cost financeCURCostView
}

// One bounded byte snapshot is fully read into memory before choosing its strict
// decoder. No directory scan, file references, AWS call or database mutation.
func readFinanceCURSource(reader io.Reader) (*financeCURSource, error) {
	if reader == nil {
		return nil, fmt.Errorf("AWS 成本来源为空")
	}
	data, err := io.ReadAll(io.LimitReader(reader, maxFinanceCURSourceBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxFinanceCURSourceBytes {
		return nil, fmt.Errorf("AWS 成本来源文件过大")
	}
	var header struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return nil, err
	}
	if header.Kind == "" {
		a, err := financecur.ReadArtifact(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		return &financeCURSource{legacy: &a}, nil
	}
	if header.Kind == "aws_cost_evidence_bundle" {
		return readFinanceAWSEvidenceBundle(data)
	}
	if header.Kind != "cur_source_set" {
		return nil, fmt.Errorf("不支持的 AWS 成本来源格式")
	}
	set, err := financecur.ReadSourceSet(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	review, err := financecur.ReviewSourceSet(set)
	if err != nil {
		return nil, err
	}
	if review.TimeZone != "Asia/Shanghai" {
		return nil, fmt.Errorf("AWS 成本核算时区不是 Asia/Shanghai")
	}
	p, err := financecur.NewSourceProjector(set)
	if err != nil {
		return nil, err
	}
	return &financeCURSource{projector: p, review: review}, nil
}

// A failed refresh must not leave last generation's verified AWS cost/profit
// attached to an otherwise reusable report. User/upstream evidence is kept.
func clearFinanceCURReport(report *financeOperatingReport) {
	clear := func(s *financeStatementView) {
		s.AWSInfrastructureCost = nil
		s.KnownAWSInfrastructureCost = channelEconomicsMoneyView{}
		s.OperatingProfit = nil
		s.KnownOperatingProfit = channelEconomicsMoneyView{}
	}
	report.CURCost = financeCURCostView{}
	report.CURProducts = nil
	report.AWSMonthlyBills = nil
	report.BillingResult = nil
	clear(&report.Statement)
	for i := range report.Periods {
		report.Periods[i].BillingResult = nil
		clear(&report.Periods[i].Statement)
	}
	for i := range report.Days {
		clear(&report.Days[i].Statement)
	}
}

func (s *financeCURSource) version() string {
	if s.bundleVersion != "" {
		return s.bundleVersion
	}
	if s.legacy != nil {
		return "statement:" + s.legacy.Statement.StatementID
	}
	return "source-set-v1:" + s.review.SourceSetID
}

func (m *Monitor) loadFinanceCURSource() (*financeCURSource, financeCURCostView) {
	if !m.cfg.FinanceCURArtifactEnabled {
		return nil, financeCURCostView{Status: "disabled"}
	}
	v := financeCURCostView{Enabled: true, Status: "error"}
	path := strings.TrimSpace(m.cfg.FinanceCURArtifactPath)
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		v.Error = "无法打开本地核算产物"
		return nil, v
	}
	f, err := os.Open(path)
	if err != nil {
		v.Error = "无法打开本地核算产物"
		return nil, v
	}
	defer f.Close()
	source, err := readFinanceCURSource(f)
	if err != nil {
		v.Error = "本地核算产物完整性校验失败"
		return nil, v
	}
	v.Loaded = true
	v.Status = "draft"
	return source, v
}

func (s *financeCURSource) project(from, to int64) (financeCURCostView, error) {
	if s.legacy != nil {
		return projectVerifiedFinanceCURCost(*s.legacy, from, to)
	}
	r, err := s.projector.Query(from, to)
	if err != nil {
		return financeCURCostView{}, err
	}
	v := financeCURCostView{Enabled: true, Loaded: true, Status: "draft", StatementID: r.SourceSetID,
		IncludedDays: len(r.Days), KnownNanoUSD: strconv.FormatInt(r.Known.NexusAPINanoUSD, 10),
		UnallocatedNanoUSD: strconv.FormatInt(r.Known.UnallocatedNanoUSD, 10), ConflictNanoUSD: strconv.FormatInt(r.Known.ConflictNanoUSD, 10),
		AllocationComplete: r.Known.Complete, BillingFinalized: true, RangeComplete: r.RangeComplete,
		MissingBillingPeriods: r.MissingBillingPeriods, PartialBoundaryDays: r.PartialBoundaryDays, UnverifiedSourceCount: len(r.UnverifiedStatementIDs)}
	for _, statement := range s.review.Statements {
		if statement.BillingPeriodEnd <= from || statement.BillingPeriodStart >= to {
			continue
		}
		v.SourceCount++
		if v.BillingPeriodStart == 0 || statement.BillingPeriodStart < v.BillingPeriodStart {
			v.BillingPeriodStart = statement.BillingPeriodStart
		}
		v.BillingPeriodEnd = max(v.BillingPeriodEnd, statement.BillingPeriodEnd)
		if v.DataFrom == 0 || statement.FirstUsageUnix < v.DataFrom {
			v.DataFrom = statement.FirstUsageUnix
		}
		v.DataThrough = max(v.DataThrough, statement.LastUsageThroughUnix)
		v.BillingFinalized = v.BillingFinalized && statement.BillingFinalized
		v.AllocationComplete = v.AllocationComplete && statement.AllocationComplete
		if err := addFinanceCURInt64(&v.IssueCount, statement.IssueCount); err != nil {
			return financeCURCostView{}, err
		}
	}
	if v.SourceCount == 0 {
		v.BillingFinalized = false
		v.AllocationComplete = false
	}
	if r.Known.TotalAbsNanoUSD > 0 {
		n := new(big.Int).Mul(new(big.Int).SetUint64(r.Known.AllocatedAbsNanoUSD), big.NewInt(1_000_000))
		v.CoveragePPM = n.Quo(n, new(big.Int).SetUint64(r.Known.TotalAbsNanoUSD)).Int64()
	} else if len(r.Days) > 0 {
		v.CoveragePPM = 1_000_000
	}
	for _, pair := range []struct {
		nano   int64
		target *channelEconomicsMoneyView
	}{
		{r.Known.UnallocatedNanoUSD, &v.UnallocatedCost}, {r.Known.ConflictNanoUSD, &v.ConflictCost},
	} {
		micro, err := nanoUSDToRoundedMicroUSD(pair.nano)
		if err != nil {
			return financeCURCostView{}, err
		}
		*pair.target = economicsMoney(micro)
	}
	if len(r.Days) > 0 {
		micro, err := nanoUSDToRoundedMicroUSD(r.Known.NexusAPINanoUSD)
		if err != nil {
			return financeCURCostView{}, err
		}
		v.KnownCost = economicsMoney(micro)
	}
	if r.RangeComplete {
		v.Status = "verified"
		v.ExactCost = financeMoneyPointer(v.KnownCost)
	}
	return v, nil
}

func (s *financeCURSource) products(from, to int64, total financeCURCostView) ([]financeCURProductProjection, error) {
	result := []financeCURProductProjection{}
	if s.legacy != nil {
		for _, p := range s.legacy.Timeline.Products {
			v, err := projectFinanceCURBuckets(s.legacy.Statement, p.Days, from, to)
			if err != nil {
				return nil, err
			}
			if v.IncludedDays > 0 {
				result = append(result, financeCURProductProjection{p.ProductCode, v})
			}
		}
		return result, nil
	}
	r, err := s.projector.Query(from, to)
	if err != nil {
		return nil, err
	}
	for _, p := range r.Products {
		var nano int64
		for _, d := range p.Days {
			if err := addFinanceCURInt64(&nano, d.NexusAPINanoUSD); err != nil {
				return nil, err
			}
		}
		micro, err := nanoUSDToRoundedMicroUSD(nano)
		if err != nil {
			return nil, err
		}
		v := financeCURCostView{IncludedDays: len(p.Days), KnownNanoUSD: strconv.FormatInt(nano, 10), KnownCost: economicsMoney(micro)}
		if total.RangeComplete {
			v.ExactCost = financeMoneyPointer(v.KnownCost)
		}
		result = append(result, financeCURProductProjection{p.ProductCode, v})
	}
	return result, nil
}
