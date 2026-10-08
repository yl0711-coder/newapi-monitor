package monitor

import (
	"math"
	"testing"
)

func TestModelAndStabilityTTFTP95UseSameDistribution(t *testing.T) {
	model := modelStatisticsTTFTAggregate{
		ttft500: 4, ttft1k: 5, ttft2k: 6, ttft5k: 80, ttft10k: 4, ttftInf: 1,
		observed: 100, over3s: 8, maxMs: 12000,
	}
	_, modelP50, modelP95, modelMax, modelOver, _ := modelStatisticsTTFTView(model)
	metrics := (stabilityCounts{
		Ttft500: 4, Ttft1k: 5, Ttft2k: 6, Ttft5k: 80, Ttft10k: 4, TtftInf: 1,
		TtftObserved: 100, TtftOver3s: 8, TtftMaxMs: 12000,
	}).metrics()
	if math.Abs(modelP50-metrics.TTFTP50Ms) > 1e-9 || math.Abs(modelP95-metrics.TTFTP95Ms) > 1e-9 {
		t.Fatalf("模型统计与稳定性 P50/P95 口径不一致: model=(%v,%v) stability=(%v,%v)",
			modelP50, modelP95, metrics.TTFTP50Ms, metrics.TTFTP95Ms)
	}
	if modelMax != metrics.TTFTMaxMs || modelOver != metrics.TTFTOver3s {
		t.Fatalf("模型统计与稳定性 TTFT 精确计数不一致: model=(max=%d,over=%d) stability=(max=%d,over=%d)",
			modelMax, modelOver, metrics.TTFTMaxMs, metrics.TTFTOver3s)
	}
}

func TestModelAndStabilityTTFTP99UseSameDistribution(t *testing.T) {
	model := modelStatisticsTTFTAggregate{
		ttft500: 4, ttft1k: 5, ttft2k: 6, ttft5k: 80, ttft10k: 4, ttftInf: 1,
		observed: 100, over3s: 8, maxMs: 12000,
	}
	_, _, _, modelP99, _, _, _ := modelStatisticsTTFTViewDetailed(model)
	metrics := (stabilityCounts{
		Ttft500: 4, Ttft1k: 5, Ttft2k: 6, Ttft5k: 80, Ttft10k: 4, TtftInf: 1,
		TtftObserved: 100, TtftOver3s: 8, TtftMaxMs: 12000,
	}).metrics()
	if math.Abs(modelP99-metrics.TTFTP99Ms) > 1e-9 {
		t.Fatalf("模型统计与稳定性 P99 口径不一致: model=%v stability=%v", modelP99, metrics.TTFTP99Ms)
	}
}

func TestTTFTLegacyHistogramWithoutObservedHasUnknownPercentiles(t *testing.T) {
	view := computeTTFTMetricView([6]int64{0, 0, 0, 4, 0, 0}, 0, 0, 4900)
	if view.HistValid || view.P50Ms != 0 || view.P95Ms != 0 || view.MaxMs != 0 || view.Observed != 0 {
		t.Fatalf("observed=0 的旧直方图必须没有分位数: %+v", view)
	}
	observed, p50, p95, maxMs, over, overPct := modelStatisticsTTFTView(modelStatisticsTTFTAggregate{
		ttft5k: 4, maxMs: 4900,
	})
	if observed != 0 || p50 != 0 || p95 != 0 || maxMs != 0 || over != 0 || overPct != 0 {
		t.Fatalf("模型统计 observed=0 时所有 TTFT 数值必须未知: observed=%d p50=%v p95=%v max=%d over=%d pct=%v",
			observed, p50, p95, maxMs, over, overPct)
	}
}

func TestTTFTInconsistentCountersAreUnknown(t *testing.T) {
	for name, args := range map[string]struct {
		hist     [6]int64
		observed int64
		over3s   int64
		maxMs    int64
	}{
		"over exceeds observed": {[6]int64{1, 0, 0, 0, 0, 0}, 1, 2, 500},
		"negative observed":     {[6]int64{1, 0, 0, 0, 0, 0}, -1, 0, 500},
		"negative bucket":       {[6]int64{-1, 0, 0, 0, 0, 0}, 0, 0, 0},
	} {
		t.Run(name, func(t *testing.T) {
			view := computeTTFTMetricView(args.hist, args.observed, args.over3s, args.maxMs)
			if view.HistValid || view.Observed != 0 || view.Over3s != 0 || view.P50Ms != 0 || view.P95Ms != 0 || view.P99Ms != 0 || view.MaxMs != 0 {
				t.Fatalf("不一致 TTFT 计数必须返回未知视图: %+v", view)
			}
		})
	}
}

func TestTTFTHistogramMismatchIsEntirelyUnknown(t *testing.T) {
	cases := []struct {
		name     string
		hist     [6]int64
		observed int64
		over3s   int64
		maxMs    int64
	}{
		{"histogram total differs", [6]int64{1, 0, 0, 0, 0, 0}, 2, 0, 500},
		{"over 3s cannot be split", [6]int64{0, 0, 0, 0, 1, 0}, 1, 0, 6000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			view := computeTTFTMetricView(tc.hist, tc.observed, tc.over3s, tc.maxMs)
			if view != (ttftMetricView{}) {
				t.Fatalf("直方图校验失败必须整组未知: %+v", view)
			}
		})
	}
}

func TestTTFTMaxMustBelongToHighestNonZeroBucket(t *testing.T) {
	for name, tc := range map[string]struct {
		hist [6]int64
		max  int64
	}{
		"max above highest bucket":   {hist: [6]int64{1, 0, 0, 0, 0, 0}, max: 501},
		"max below highest bucket":   {hist: [6]int64{0, 0, 0, 1, 0, 0}, max: 2000},
		"infinite bucket needs >10s": {hist: [6]int64{0, 0, 0, 0, 0, 1}, max: 10000},
	} {
		t.Run(name, func(t *testing.T) {
			view := computeTTFTMetricView(tc.hist, 1, 0, tc.max)
			if view != (ttftMetricView{}) {
				t.Fatalf("最高非零桶与最大值不一致时必须未知: %+v", view)
			}
		})
	}
	view := computeTTFTMetricView([6]int64{0, 0, 0, 1, 0, 0}, 1, 1, 5000)
	if !view.HistValid || view.MaxMs != 5000 {
		t.Fatalf("最高非零桶内的最大值应保持有效: %+v", view)
	}
}
