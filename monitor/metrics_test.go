package monitor

import (
	"math"
	"testing"
)

// metrics_test.go:核心数值逻辑的单测——成功率、健康判定、异常成簇、直方图近似分位。
// 这些是最容易藏 bug 的纯函数,既验证正确性,也作为行为文档与回归保护。

func approx(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func TestRate(t *testing.T) {
	cases := []struct {
		s, total int64
		want     float64
	}{
		{0, 0, 0}, // 除零保护
		{50, 100, 50},
		{99, 100, 99},
		{1, 3, 100.0 / 3},
		{3, 3, 100},
	}
	for _, c := range cases {
		if got := rate(c.s, c.total); !approx(got, c.want) {
			t.Errorf("rate(%d,%d)=%v want %v", c.s, c.total, got, c.want)
		}
	}
}

func TestHealth(t *testing.T) {
	cases := []struct {
		total int64
		r     float64
		want  string
	}{
		{19, 100, "nosample"}, // 样本不足(< minSample),不判色
		{20, 100, "good"},
		{20, 99, "good"},   // 边界:>=99 = good
		{20, 98.9, "warn"}, // 99 下方掉到 warn
		{20, 95, "warn"},   // 边界:>=95 = warn
		{20, 94.99, "bad"}, // 95 下方掉到 bad
		{1000, 50, "bad"},
	}
	for _, c := range cases {
		if got := health(c.total, c.r); got != c.want {
			t.Errorf("health(%d,%v)=%q want %q", c.total, c.r, got, c.want)
		}
	}
}

func TestAnomalyBurst(t *testing.T) {
	mk := func(vals ...int64) []TimePoint {
		out := make([]TimePoint, len(vals))
		for i, v := range vals {
			out[i] = TimePoint{Anomaly: v}
		}
		return out
	}
	cases := []struct {
		name  string
		spark []TimePoint
		n     int
		want  bool
	}{
		{"empty", nil, 3, false},
		{"three-consecutive", mk(1, 1, 1), 3, true},
		{"two-consecutive-not-enough", mk(1, 1), 3, false},
		{"scattered-no-run", mk(1, 0, 1, 0, 1), 3, false}, // 零散抖动不算
		{"run-at-tail", mk(1, 1, 0, 1, 1, 1), 3, true},    // 末尾连续 3 桶
		{"n-zero-defaults-to-3", mk(1, 1, 1), 0, true},    // n<1 兜底为 3
		{"n-one-single-anomaly", mk(0, 1), 1, true},
	}
	for _, c := range cases {
		if got := anomalyBurst(c.spark, c.n); got != c.want {
			t.Errorf("%s: anomalyBurst=%v want %v", c.name, got, c.want)
		}
	}
}

func TestPercentile(t *testing.T) {
	h := func(v ...int64) []int64 { return v } // 7 档:(0,1] (1,2] (2,5] (5,10] (10,30] (30,60] (60,∞)
	cases := []struct {
		name   string
		hist   []int64
		maxVal int
		p      float64
		want   float64
	}{
		{"empty-returns-0", h(0, 0, 0, 0, 0, 0, 0), 0, 50, 0},
		{"single-bucket-p50", h(10, 0, 0, 0, 0, 0, 0), 1, 50, 0.5}, // 桶内线性插值
		{"single-bucket-p95", h(10, 0, 0, 0, 0, 0, 0), 1, 95, 0.95},
		{"two-bucket-p50", h(5, 5, 0, 0, 0, 0, 0), 2, 50, 1.0}, // 恰在桶边界
		{"two-bucket-p95", h(5, 5, 0, 0, 0, 0, 0), 2, 95, 1.9},
		{"tail-bucket-uses-maxval", h(0, 0, 0, 0, 0, 0, 10), 120, 50, 90}, // 末档以 maxVal 收尾
	}
	for _, c := range cases {
		if got := percentile(c.hist, latEdges, c.maxVal, c.p); !approx(got, c.want) {
			t.Errorf("%s: percentile=%v want %v", c.name, got, c.want)
		}
	}
}
