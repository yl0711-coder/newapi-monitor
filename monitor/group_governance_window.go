package monitor

import (
	"fmt"
	"sort"
	"strings"
)

// Both populations come from the same bounded local read. Selection is per
// group/channel; same-model comparisons always use the selected window too.
func ggBuildMetrics(rows []modelFRTRow, errors []GroupGovernanceErrorMinute, channels []ChannelSnap, end int64, coverage map[int]ggWindowCoverage, p ggPolicy) []ggMetrics {
	short := ggBuildWindowMetrics(rows, errors, channels, end, coverage, p, p.WindowMinutes)
	long := ggBuildWindowMetrics(rows, errors, channels, end, coverage, p, p.ExpandedMinutes)
	type samples struct{ requests, observed int64 }
	counts := map[ggMetricKey]samples{}
	for _, v := range short {
		k := ggMetricKey{Group: v.Group, Channel: v.ChannelID}
		x := counts[k]
		x.requests += v.Requests
		if v.FRT.Observed != nil {
			x.observed += *v.FRT.Observed
		}
		counts[k] = x
	}
	pool := make([]*ggMetrics, 0, len(short)+len(long))
	for i := range short {
		pool = append(pool, &short[i])
	}
	for i := range long {
		pool = append(pool, &long[i])
	}
	selected := make([]ggMetrics, 0, len(short))
	for _, v := range pool {
		x := counts[ggMetricKey{Group: v.Group, Channel: v.ChannelID}]
		var gaps []string
		if x.requests < int64(p.MinSamples) {
			gaps = append(gaps, fmt.Sprintf("请求尝试 %d 次", x.requests))
		}
		if x.observed < int64(p.MinSamples) {
			gaps = append(gaps, fmt.Sprintf("有效首字样本 %d 个", x.observed))
		}
		n := p.WindowMinutes
		if len(gaps) > 0 {
			n = p.ExpandedMinutes
		}
		if v.WindowMinutes != n {
			continue
		}
		copy := *v
		if len(gaps) > 0 {
			copy.WindowReason = fmt.Sprintf("近 %d 分钟样本不足（%s，门槛各 %d），已采用近 %d 分钟数据", p.WindowMinutes, strings.Join(gaps, "、"), p.MinSamples, p.ExpandedMinutes)
		}
		copy.windowMetrics = pool
		selected = append(selected, copy)
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Key < selected[j].Key })
	return selected
}

type ggWindowGroup struct {
	group, model string
	from, to     int64
}

func ggWindowGroupKey(v *ggMetrics) ggWindowGroup {
	return ggWindowGroup{v.Group, v.Model, v.From, v.To}
}
func ggMemoryKey(v *ggMetrics) string { return fmt.Sprintf("%s/window:%d", v.Key, v.WindowMinutes) }

// Prefer the selected row, whose runtime freshness/config checks have already
// been applied. Context rows inherit exactly those checks; they never bypass
// a source failure or turn a stale configuration into executable advice.
func ggComparisonMetrics(report *ggRealtimeReport) []*ggMetrics {
	all := map[string]*ggMetrics{}
	selected := map[string]*ggMetrics{}
	for i := range report.Metrics {
		v := &report.Metrics[i]
		all[ggMemoryKey(v)] = v
		selected[v.Key] = v
	}
	for i := range report.Metrics {
		for _, v := range report.Metrics[i].windowMetrics {
			key := ggMemoryKey(v)
			if all[key] != nil {
				continue
			}
			if owner := selected[v.Key]; owner != nil {
				v.Stale = owner.Stale
				v.ConfigCurrent = v.ConfigCurrent && owner.ConfigCurrent
			}
			all[key] = v
		}
		// Every row shares this pool; collect it once.
		if len(report.Metrics[i].windowMetrics) > 0 {
			break
		}
	}
	keys := make([]string, 0, len(all))
	for key := range all {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]*ggMetrics, 0, len(keys))
	for _, key := range keys {
		out = append(out, all[key])
	}
	return out
}

func ggWindowGroups(all []*ggMetrics) map[ggWindowGroup][]*ggMetrics {
	groups := map[ggWindowGroup][]*ggMetrics{}
	for _, v := range all {
		key := ggWindowGroupKey(v)
		groups[key] = append(groups[key], v)
	}
	return groups
}
