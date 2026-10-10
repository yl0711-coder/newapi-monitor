package monitor

import "math"

// histogramQuantileRange describes the containing bucket, not a confidence
// interval. Lower is exclusive and Upper is inclusive. OpenTail means that
// Upper comes from the observed maximum, not a configured finite bucket edge.
type histogramQuantileRange struct {
	Lower    float64 `json:"lower"`
	Upper    float64 `json:"upper"`
	OpenTail bool    `json:"open_tail"`
	Valid    bool    `json:"valid"`
}

type histogramQuantileBounds struct {
	P50 histogramQuantileRange `json:"p50"`
	P95 histogramQuantileRange `json:"p95"`
	P99 histogramQuantileRange `json:"p99"`
}

// These bounds add read-only metadata to legacy approximate numeric fields.
// No stored histogram or financial/request counter is rewritten.
func quantileBounds(hist []int64, edges []int, maximum int) histogramQuantileBounds {
	return histogramQuantileBounds{
		P50: quantileBucket(hist, edges, maximum, 50),
		P95: quantileBucket(hist, edges, maximum, 95),
		P99: quantileBucket(hist, edges, maximum, 99),
	}
}

func quantileBucket(hist []int64, edges []int, maximum int, p float64) histogramQuantileRange {
	if len(hist) != len(edges)+1 || maximum <= 0 || math.IsNaN(p) || p <= 0 || p > 100 {
		return histogramQuantileRange{}
	}
	var total int64
	highest := -1
	for i, count := range hist {
		if count < 0 || count > math.MaxInt64-total {
			return histogramQuantileRange{}
		}
		total += count
		if count > 0 {
			highest = i
		}
	}
	for i, edge := range edges {
		if edge <= 0 || (i > 0 && edge <= edges[i-1]) {
			return histogramQuantileRange{}
		}
	}
	if highest < 0 || (highest > 0 && maximum <= edges[highest-1]) || (highest < len(edges) && maximum > edges[highest]) {
		return histogramQuantileRange{}
	}
	target := p / 100 * float64(total)
	var cumulative int64
	for i, count := range hist {
		cumulative += count
		if count == 0 || float64(cumulative) < target {
			continue
		}
		lower := 0
		if i > 0 {
			lower = edges[i-1]
		}
		upper := maximum
		if i < len(edges) {
			upper = min(upper, edges[i])
		}
		return histogramQuantileRange{Lower: float64(lower), Upper: float64(upper), OpenTail: i == len(edges), Valid: true}
	}
	return histogramQuantileRange{}
}

// stabilityFRTQuantileBounds adds display metadata only to stability reports.
// Validation and numeric percentiles remain owned by computeTTFTMetricView;
// model monitoring and alert payloads do not use or depend on these bounds.
func stabilityFRTQuantileBounds(c stabilityCounts, view ttftMetricView) histogramQuantileBounds {
	if !view.HistValid {
		return histogramQuantileBounds{}
	}
	threeToFive := view.Over3s - c.Ttft10k - c.TtftInf
	return quantileBounds(
		[]int64{c.Ttft500, c.Ttft1k, c.Ttft2k, c.Ttft5k - threeToFive, threeToFive, c.Ttft10k, c.TtftInf},
		[]int{500, 1000, 2000, 3000, 5000, 10000}, int(view.MaxMs),
	)
}
