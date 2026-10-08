package monitor

// ttftMetricView is the common, conservative view of the six TTFT histogram
// buckets.  The source histogram keeps (2s,5s] as one bucket, so the exact
// over-3s counter is used to split it into (2s,3s] and (3s,5s].
//
// Percentiles are valid only when the observed denominator and the complete
// histogram agree.  This is intentional: a row written before the exact TTFT
// projection may have non-zero legacy buckets but observed=0, and a partially
// upgraded aggregate must not look like a fast request.
type ttftMetricView struct {
	Observed  int64
	Over3s    int64
	Over3sPct float64
	P50Ms     float64
	P95Ms     float64
	P99Ms     float64
	MaxMs     int64
	HistValid bool
}

func computeTTFTMetricView(hist [6]int64, observed, over3s, maxMs int64) ttftMetricView {
	// Do not repair contradictory counters.  A negative value or an over-3s
	// count larger than the observed denominator indicates a corrupt/partial
	// projection; clamping it would make bad data look valid in the UI.
	if observed < 0 || over3s < 0 || maxMs < 0 || over3s > observed {
		return ttftMetricView{}
	}
	for _, bucket := range hist {
		if bucket < 0 {
			return ttftMetricView{}
		}
	}
	view := ttftMetricView{Observed: observed, Over3s: over3s, MaxMs: maxMs}
	view.Over3sPct = rate(view.Over3s, view.Observed)
	if view.Observed <= 0 {
		// A max value without an observed denominator is an orphaned legacy
		// field, not an independently meaningful sample.
		view.MaxMs = 0
		return view
	}

	var total int64
	for i := range hist {
		total += hist[i]
	}
	// A valid distribution must account for every exact observation and have a
	// positive observed maximum.  Any mismatch is a coverage/schema boundary,
	// not evidence that the requests were fast.  Do not retain a partial
	// denominator/max value here: callers must render the whole projection as
	// unknown instead of combining a trustworthy-looking count with invalid
	// percentiles.
	if total != view.Observed || view.MaxMs <= 0 {
		return ttftMetricView{}
	}
	// The maximum must fall inside the highest non-empty bucket.  Comparing
	// only the aggregate count and max is not enough: a corrupt projection can
	// otherwise report a plausible maximum while all non-zero samples belong to
	// a lower bucket.  Keep the same strict bucket boundaries used by the
	// sampler (the upper edge is inclusive).
	highest := len(hist) - 1
	for highest >= 0 && hist[highest] == 0 {
		highest--
	}
	validMax := false
	switch highest {
	case 0:
		validMax = view.MaxMs > 0 && view.MaxMs <= 500
	case 1:
		validMax = view.MaxMs > 500 && view.MaxMs <= 1000
	case 2:
		validMax = view.MaxMs > 1000 && view.MaxMs <= 2000
	case 3:
		validMax = view.MaxMs > 2000 && view.MaxMs <= 5000
	case 4:
		validMax = view.MaxMs > 5000 && view.MaxMs <= 10000
	case 5:
		validMax = view.MaxMs > 10000
	}
	if !validMax {
		return ttftMetricView{}
	}

	overFive := hist[4] + hist[5]
	threeToFive := view.Over3s - overFive
	if threeToFive < 0 || threeToFive > hist[3] {
		return ttftMetricView{}
	}
	twoToThree := hist[3] - threeToFive
	refined := []int64{hist[0], hist[1], hist[2], twoToThree, threeToFive, hist[4], hist[5]}
	edges := []int{500, 1000, 2000, 3000, 5000, 10000}
	view.P50Ms = percentile(refined, edges, int(view.MaxMs), 50)
	view.P95Ms = percentile(refined, edges, int(view.MaxMs), 95)
	view.P99Ms = percentile(refined, edges, int(view.MaxMs), 99)
	view.HistValid = true
	return view
}
