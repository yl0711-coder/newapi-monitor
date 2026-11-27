package monitor

import "strings"

type usageLogPlanningKey struct {
	kind   int
	id, at int64
}

// Planning-only summaries reconstructed from the single-row probe protocol.
// Counts here choose plans, never become final totals.
type usageLogPlanningSummary struct {
	kind, n       int
	low, high, at int64
}

func usageLogsSummaryDecision(f usageLogFilter, summaries []usageLogPlanningSummary) int {
	var n [8]int
	var low, high int64
	var first, last usageLogPlanningKey
	for _, s := range summaries {
		if s.kind < 1 || s.kind > 7 || s.n < 0 || s.n > 257 || s.low < 0 || s.high < s.low {
			return usageLogPlanNative
		}
		n[s.kind] = s.n
		switch s.kind {
		case 5:
			low, high = s.low, s.high
		case 6:
			first = usageLogPlanningKey{s.kind, s.low, s.at}
		case 7:
			last = usageLogPlanningKey{s.kind, s.low, s.at}
		}
	}
	return usageLogsPlanningCountsDecision(f, n, low, high, first, last)
}

const (
	usageLogPlanSpan = iota
	usageLogPlanNative
	usageLogPlanTime
)

func usageLogsPlanningCountsDecision(f usageLogFilter, n [8]int, low, high int64, first, last usageLogPlanningKey) int {
	if strings.Contains(f.ModelName, "%") || strings.Contains(f.Username, "%") {
		return usageLogPlanNative
	}
	for _, s := range []struct {
		kind    int
		enabled bool
	}{{1, f.Username != ""}, {2, f.Channel != 0}, {3, f.ModelName != ""}, {4, f.Group != ""}} {
		if s.enabled && n[s.kind] <= 256 {
			return usageLogPlanNative
		}
	}
	if n[6] != 1 || n[7] != 1 || first.at <= 0 || last.at <= 0 {
		return usageLogPlanNative
	}
	if n[5] != 64 || high < low || uint64(high)-uint64(low) > uint64(2*n[5]-1) || f.Type != 0 && f.Type != 2 {
		return usageLogPlanTime
	}
	inside := func(at int64) bool { return at >= f.Start && at <= f.End }
	if f.Username == "" && f.Channel == 0 && f.ModelName == "" && f.Group == "" && inside(first.at) && inside(last.at) {
		return usageLogPlanNative
	}
	if inside(last.at) && last.id >= low && last.id-low >= usageLogsQuotaSpanMaxRows {
		return usageLogPlanNative
	}
	return usageLogPlanSpan
}
