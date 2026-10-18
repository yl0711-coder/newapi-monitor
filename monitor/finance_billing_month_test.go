package monitor

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func billingTestReport(t *testing.T, fromMonth, toMonth time.Month) *financeOperatingReport {
	t.Helper()
	loc, _ := time.LoadLocation("Asia/Shanghai")
	r := financeCURRangeReport(t, time.Date(2026, fromMonth, 1, 0, 0, 0, 0, loc).Unix(), time.Date(2026, toMonth, 1, 0, 0, 0, 0, loc).Unix())
	for i := range r.Periods {
		r.Periods[i].Statement.OperatingRevenue = financeMoneyPointer(economicsMoney(100_000_000))
		r.Periods[i].Statement.RawCorrectedUpstreamCost = financeMoneyPointer(economicsMoney(40_000_000))
		r.Periods[i].Statement.CorrectedUpstreamCost = financeMoneyPointer(economicsMoney(30_000_000))
		r.Periods[i].Statement.InternalTestUpstreamCost = financeMoneyPointer(economicsMoney(10_000_000))
	}
	return r
}

func billingTestSource(t *testing.T) *financeCURSource {
	t.Helper()
	b := awsBundleFixture(t)
	b.AccountingBasis = financeAWSBillingMonthBasis
	s, err := readFinanceCURSource(bytes.NewReader(awsBundleRaw(t, b)))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestFinanceBillingMonthCompleteAndIndependent(t *testing.T) {
	s := billingTestSource(t)
	r := billingTestReport(t, 8, 10)
	before := awsBundleRaw(t, r)
	s.applyBillingMonths(r)
	if r.Periods[0].BillingResult.Profit.MicroUSD != "52000000" || *r.Periods[0].BillingResult.MarginPercent != "52.00" || r.Periods[0].BillingResult.AWSSource != "cost_explorer" {
		t.Fatal("CE month wrongly aligned or credit counted twice", r.Periods[0].BillingResult)
	}
	if r.Periods[1].BillingResult.AWSCost.MicroUSD != "720000" || r.Periods[1].BillingResult.Profit.MicroUSD != "59280000" {
		t.Fatal("CUR billing month not used whole", r.Periods[1].BillingResult)
	}
	if r.BillingResult.Profit.MicroUSD != "111280000" || *r.BillingResult.MarginPercent != "55.64" {
		t.Fatal("aggregate margin is not weighted by revenue", r.BillingResult)
	}
	r.Periods[1].Statement.OperatingRevenue = financeMoneyPointer(economicsMoney(200_000_000))
	s.applyBillingMonths(r)
	if r.BillingResult.Profit.MicroUSD != "211280000" || *r.BillingResult.MarginPercent != "70.43" {
		t.Fatal("monthly percentages were averaged instead of weighted")
	}
	var cached financeOperatingReport
	if err := json.Unmarshal(awsBundleRaw(t, r), &cached); err != nil || !reflect.DeepEqual(cached.BillingResult, r.BillingResult) || !reflect.DeepEqual(cached.Periods, r.Periods) {
		t.Fatal("monthly accounting lost in cache serialization", err)
	}
	var original financeOperatingReport
	if err := json.Unmarshal(before, &original); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original.Statement, r.Statement) || !reflect.DeepEqual(original.Days, r.Days) {
		t.Fatal("monthly booking changed time-window facts")
	}
	r.Periods[1].Statement.RawCorrectedUpstreamCost = nil
	s.applyBillingMonths(r)
	if r.Periods[0].BillingResult.Profit == nil || r.Periods[1].BillingResult.Profit != nil || r.BillingResult.Profit != nil {
		t.Fatal("missing month blocked good month or invented full total")
	}
	// Recompute must clear an earlier published monthly profit.
	r.Periods[0].Statement.OperatingRevenue = nil
	s.applyBillingMonths(r)
	if r.Periods[0].BillingResult.Profit != nil {
		t.Fatal("stale monthly profit survived invalidation")
	}
}

func TestFinanceBillingMonthPartialRangesAndMissingSource(t *testing.T) {
	s := billingTestSource(t)
	for _, mutate := range []func(*financeOperatingReport){
		func(r *financeOperatingReport) { r.Periods[0].From += 3600; r.From += 3600 },
		func(r *financeOperatingReport) { r.Periods[0].To -= 3600; r.To -= 3600 },
		func(r *financeOperatingReport) { r.Periods[0].To = r.Periods[0].From + 86400; r.To = r.Periods[0].To },
	} {
		r := billingTestReport(t, 8, 9)
		mutate(r)
		s.applyBillingMonths(r)
		if r.BillingResult.Profit != nil || r.Periods[0].BillingResult.AWSCost != nil || r.Periods[0].BillingResult.Status != "partial_period" {
			t.Fatal("full bill charged to partial range")
		}
		if r.Periods[0].BillingResult.Revenue == nil || r.Periods[0].BillingResult.Revenue.MicroUSD != "100000000" {
			t.Fatal("partial month lost available same-range revenue")
		}
	}
	r := billingTestReport(t, 7, 9)
	s.applyBillingMonths(r)
	if r.Periods[0].BillingResult.Profit != nil || r.Periods[1].BillingResult.Profit == nil || r.BillingResult.Profit != nil {
		t.Fatal("missing July charged as zero or blocked August")
	}
	r = billingTestReport(t, 8, 10)
	r.Periods = append(r.Periods, r.Periods[0])
	s.applyBillingMonths(r)
	if r.BillingResult.Profit != nil {
		t.Fatal("duplicate month counted")
	}
	r = billingTestReport(t, 8, 9)
	r.Periods[0].Period = "2026-07"
	s.applyBillingMonths(r)
	if r.BillingResult.Profit != nil {
		t.Fatal("mislabeled month accepted")
	}
}

func TestFinanceBillingMonthSignedPrecisionAndMargins(t *testing.T) {
	for raw, want := range map[string]string{"197.1008700236": "197100870", "0.0000005": "1", "-0.0000005": "-1", "-20": "-20000000", "9.9999999968": "10000000", "0": "0"} {
		got, err := financeBillingMoney(raw)
		if err != nil || got.MicroUSD != want {
			t.Fatal(raw, got, err)
		}
	}
	for _, raw := range []string{"NaN", "1/2", "1e9", "+1", "10000000000000", "0.0000000000000000001"} {
		if _, err := financeBillingMoney(raw); err == nil {
			t.Fatal("invalid cost accepted", raw)
		}
	}
	s := billingTestSource(t)
	s.monthly.Months[0].NetUSD = "-20"
	r := billingTestReport(t, 8, 9)
	s.applyBillingMonths(r)
	if r.BillingResult.Profit.MicroUSD != "80000000" || *r.BillingResult.MarginPercent != "80.00" {
		t.Fatal("signed credit wrongly deducted")
	}
	for _, rev := range []int64{0, -1} {
		r.Periods[0].Statement.OperatingRevenue = financeMoneyPointer(economicsMoney(rev))
		s.applyBillingMonths(r)
		if r.BillingResult.Profit == nil || r.BillingResult.MarginPercent != nil {
			t.Fatal("nonpositive revenue margin should be undefined")
		}
	}
	r = billingTestReport(t, 8, 9)
	r.Periods[0].Statement.RawCorrectedUpstreamCost = financeMoneyPointer(economicsMoney(200_000_000))
	s.applyBillingMonths(r)
	if *r.BillingResult.MarginPercent != "-80.00" {
		t.Fatal("loss hidden")
	}
	r = billingTestReport(t, 8, 9)
	r.Periods[0].Statement.OperatingRevenue = financeMoneyPointer(economicsMoney(9223372036854775807))
	r.Periods[0].Statement.RawCorrectedUpstreamCost = financeMoneyPointer(economicsMoney(-1))
	s.applyBillingMonths(r)
	if r.BillingResult.Profit != nil {
		t.Fatal("overflow profit published")
	}
}

func TestFinanceBillingMonthProvisionalAndRollback(t *testing.T) {
	s := billingTestSource(t)
	s.monthly.Months[0].Estimated = true
	r := billingTestReport(t, 8, 9)
	s.applyBillingMonths(r)
	if r.BillingResult.Status != "provisional" || r.BillingResult.Profit == nil {
		t.Fatal("estimated bill unlabelled or unusable")
	}
	s = billingTestSource(t)
	s.review.Statements[0].BillingFinalized = false
	r = billingTestReport(t, 9, 10)
	s.applyBillingMonths(r)
	if r.BillingResult.Status != "provisional" {
		t.Fatal("unfinalized CUR published as reconciled")
	}
	s.review.Statements[0].LastUsageThroughUnix -= 86400
	s.applyBillingMonths(r)
	if r.BillingResult.Profit != nil {
		t.Fatal("partial source passed as full bill")
	}
	s = billingTestSource(t)
	s.review.Statements[0].ExcludedNanoUSD = 1
	s.applyBillingMonths(r)
	if r.BillingResult.Profit != nil {
		t.Fatal("excluded account costs silently accepted")
	}
	path := filepath.Join(t.TempDir(), "bundle.json")
	b := awsBundleFixture(t)
	b.AccountingBasis = financeAWSBillingMonthBasis
	newSource, err := readFinanceCURSource(bytes.NewReader(awsBundleRaw(t, b)))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, awsBundleRaw(t, b), 0600); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{cfg: Settings{FinanceCURArtifactEnabled: true, FinanceCURArtifactPath: path}}
	r = billingTestReport(t, 8, 9)
	m.applyFinanceCURArtifactToReport(r)
	if r.BillingResult == nil || r.BillingResult.Profit == nil {
		t.Fatal("new mode not connected")
	}
	b.AccountingBasis = ""
	oldSource, err := readFinanceCURSource(bytes.NewReader(awsBundleRaw(t, b)))
	if err != nil || oldSource.version() == newSource.version() {
		t.Fatal("accounting mode did not invalidate source cache", err)
	}
	if err := os.WriteFile(path, awsBundleRaw(t, b), 0600); err != nil {
		t.Fatal(err)
	}
	m.applyFinanceCURArtifactToReport(r)
	if r.BillingResult != nil || r.Periods[0].BillingResult != nil {
		t.Fatal("rollback kept stale monthly result")
	}
	b.AccountingBasis = "unknown"
	if _, err := readFinanceCURSource(bytes.NewReader(awsBundleRaw(t, b))); err == nil {
		t.Fatal("unrecognized accounting rule accepted")
	}
}
