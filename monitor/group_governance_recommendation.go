package monitor

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

type ggRuleResult struct {
	ID             string   `json:"id"`
	Eligible       bool     `json:"eligible"`
	Hit            bool     `json:"hit"`
	Active         bool     `json:"active"`
	Recovering     bool     `json:"recovering"`
	Hits           int      `json:"hits"`
	Recoveries     int      `json:"recoveries"`
	Actual         *float64 `json:"actual"`
	Threshold      *float64 `json:"threshold"`
	Reason         string   `json:"reason"`
	Comparison     *float64 `json:"comparison"`
	ComparisonKind string   `json:"comparison_kind"`
	DeviationPct   *float64 `json:"deviation_pct"`
	Unit           string   `json:"unit"`
	Evaluations    int      `json:"evaluations"`
	Immediate      bool     `json:"immediate"`
	Description    string   `json:"description"`
	recovered      bool
}
type ggAlternative struct {
	ChannelID int      `json:"channel_id"`
	Name      string   `json:"name"`
	MeanMs    *float64 `json:"mean_ms"`
	Success   int64    `json:"success"`
	RPM       float64  `json:"rpm"`
	ProvenRPM float64  `json:"proven_rpm"`
	Headroom  float64  `json:"headroom_rpm"`
	Basis     string   `json:"basis"`
}
type ggAction struct {
	ID        string            `json:"id"`
	Kind      string            `json:"kind"`
	Subtype   string            `json:"subtype"`
	ChannelID int               `json:"channel_id,omitempty"`
	Group     string            `json:"group,omitempty"`
	Model     string            `json:"model,omitempty"`
	Models    []string          `json:"models"`
	Scope     []string          `json:"scope"`
	Reason    string            `json:"reason"`
	Execution string            `json:"execution"`
	Subtypes  []string          `json:"subtypes"`
	Reasons   []string          `json:"reasons"`
	Triggers  []ggActionTrigger `json:"triggers"`
}

type ggActionTrigger struct {
	MetricKey   string       `json:"metric_key"`
	ChannelID   int          `json:"channel_id"`
	ChannelName string       `json:"channel_name"`
	Group       string       `json:"group"`
	Model       string       `json:"model"`
	From        int64        `json:"from_ts"`
	To          int64        `json:"to_ts"`
	Rule        ggRuleResult `json:"rule"`
}
type ggHit struct {
	At  int64
	Hit bool
}
type ggRuleMemory struct {
	Last       int64
	History    []ggHit
	Active     bool
	Recoveries int
	// Re-evaluate late evidence against the state before this minute, rather
	// than treating another publication of this minute as another observation.
	BeforeActive     bool
	BeforeRecoveries int
}
type ggMemory struct {
	Config       string
	Last         int64
	Normal       []ggPoint
	Deferred     []ggPoint
	Rules        map[string]ggRuleMemory
	Frozen       bool
	FreezeReason string
}
type ggControl struct {
	Version  string
	Metrics  map[string]ggMemory
	Channels map[string]ggMemory
}

// Linear interpolation (R-7) for every cross-channel quantile. A channel is
// exactly one sample; request-level quantiles remain histogram estimates.
func ggQuantile(values []float64, q float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	v := append([]float64(nil), values...)
	sort.Float64s(v)
	h := float64(len(v)-1) * q
	i := int(h)
	x := v[i]
	if i+1 < len(v) {
		x += (v[i+1] - v[i]) * (h - float64(i))
	}
	return &x
}
func ggRatioPct(value, base *float64) *float64 {
	if value == nil || base == nil || *base <= 0 {
		return nil
	}
	x := (*value / *base - 1) * 100
	return &x
}
func ggFloat(v float64) *float64 { return &v }
func ggHasString(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}
func ggEligibleFRT(v *ggMetrics, p ggPolicy) bool {
	return !v.Stale && v.ChannelStatus == 1 && v.ConfigCurrent && v.FRT.MeanCoverageComplete && v.FRT.MeanMs != nil && v.FRT.Observed != nil && *v.FRT.Observed >= int64(p.MinSamples)
}

func ggUpdateRule(old ggRuleMemory, result ggRuleResult, at int64, p ggPolicy, immediate bool) (ggRuleMemory, ggRuleResult) {
	if at > old.Last {
		old.BeforeActive, old.BeforeRecoveries = old.Active, old.Recoveries
		if old.Last > 0 && at-old.Last > 60 {
			old.BeforeRecoveries = 0
		}
		old.Last = at
		trim := old.History[:0]
		for _, h := range old.History {
			if h.At > at-int64(p.HitMinutes)*60 {
				trim = append(trim, h)
			}
		}
		old.History = trim
		// Ineligible evidence is neither a hit nor a recovery.
		old.History = append(old.History, ggHit{at, result.Eligible && result.Hit})
	} else if at == old.Last {
		// Replace this minute's observation without adding a hit/recovery minute.
		for i := range old.History {
			if old.History[i].At == at {
				old.History[i].Hit = result.Eligible && result.Hit
			}
		}
	}
	for _, h := range old.History {
		if h.At > at-int64(p.HitMinutes)*60 {
			result.Evaluations++
		}
		if h.Hit && h.At > at-int64(p.HitMinutes)*60 {
			result.Hits++
		}
	}
	if at == old.Last {
		old.Recoveries = 0
		if result.Eligible {
			old.Active = old.BeforeActive || (result.Hit && (immediate || result.Hits >= p.HitRequired))
			if result.recovered {
				old.Recoveries = old.BeforeRecoveries + 1
			}
			if old.Recoveries >= p.RecoveryMinutes {
				old.Active = false
			}
		}
		// Missing evidence suspends executable advice via Eligible. It cannot
		// erase a known fault or count as a recovery, even on repeated refresh.
	}
	result.Active = old.Active
	result.Immediate = immediate
	result.Recoveries = old.Recoveries
	result.Recovering = old.Active && old.Recoveries > 0
	return old, result
}

func ggBaselineView(points []ggPoint, frozen bool, p ggPolicy) ggBaseline {
	b := ggBaseline{Minutes: len(points), Frozen: frozen, Source: "本地完整分钟；排除当前窗口；仅纳入无已知故障且通过延迟校验的分钟"}
	if len(points) == 0 {
		return b
	}
	b.From = points[0].At
	b.To = points[len(points)-1].At + 60
	if len(points) < p.BaselineMinutes {
		return b
	}
	var sum, observed, requests, peak int64
	for _, v := range points {
		sum += v.SumMs
		observed += v.Observed
		requests += v.Requests
		peak = max(peak, v.Requests)
	}
	if observed >= int64(p.MinSamples) && sum > 0 {
		b.MeanMs = ggFloat(float64(sum) / float64(observed))
	}
	b.RPM = ggFloat(float64(requests) / float64(len(points)))
	b.PeakRPM = ggFloat(float64(peak))
	return b
}

func ggPrepareBaseline(v *ggMetrics, mem *ggMemory, p ggPolicy) {
	// Never include minutes from the observation window in its normal baseline.
	prior := mem.Normal[:0]
	for _, pt := range mem.Normal {
		valid := true
		for _, current := range v.minutes {
			if current.At == pt.At && (!current.Complete || current.Errors > 0 || current.Pending > 0) {
				valid = false
			}
		}
		if pt.At < v.From && valid {
			prior = append(prior, pt)
		}
	}
	mem.Normal = prior
	// Bootstrap only from the thirty minutes preceding the current window.
	// Error coverage is required, not merely a zero coarse error count. A robust
	// median gate prevents an isolated slow minute becoming normal history.
	if mem.Last == 0 {
		var means []float64
		for _, pt := range v.minutes {
			if pt.At < v.From && pt.At >= v.From-int64(p.BaselineMinutes)*60 && pt.Complete && pt.Errors == 0 && pt.Pending == 0 && pt.MeanMs != nil {
				means = append(means, *pt.MeanMs)
			}
		}
		med := ggQuantile(means, .5)
		for _, pt := range v.minutes {
			if pt.At < v.From && pt.At >= v.From-int64(p.BaselineMinutes)*60 && pt.Complete && pt.Errors == 0 && pt.Pending == 0 && (pt.Requests == 0 || (pt.MeanMs != nil && med != nil && *pt.MeanMs <= *med*p.FRTAttention)) {
				mem.Normal = append(mem.Normal, pt)
			}
		}
	}
	if !mem.Frozen {
		keep := mem.Deferred[:0]
		for _, pt := range mem.Deferred {
			if pt.At < v.From {
				if len(mem.Normal) == 0 || pt.At > mem.Normal[len(mem.Normal)-1].At {
					mem.Normal = append(mem.Normal, pt)
				}
			} else {
				keep = append(keep, pt)
			}
		}
		mem.Deferred = keep
		if len(mem.Normal) > p.BaselineMinutes {
			mem.Normal = mem.Normal[len(mem.Normal)-p.BaselineMinutes:]
		}
	}
	v.Baseline = ggBaselineView(mem.Normal, mem.Frozen, p)
	v.Baseline.FreezeReason = mem.FreezeReason
	v.normal = mem.Normal
	v.FRTChangePct = ggRatioPct(v.FRT.MeanMs, v.Baseline.MeanMs)
	v.GrowthPct = ggRatioPct(&v.RPM, v.Baseline.RPM)
	if v.Baseline.RPM == nil {
		v.Missing = append(v.Missing, fmt.Sprintf("近期正常基准 %d/%d 分钟；增长率暂不可计算", v.Baseline.Minutes, p.BaselineMinutes))
	}
}

func ggCompare(metrics []*ggMetrics, p ggPolicy) {
	var means, rates, bases []float64
	var total int64
	for _, v := range metrics {
		total += v.Requests
		if ggEligibleFRT(v, p) {
			means = append(means, *v.FRT.MeanMs)
			if v.Baseline.MeanMs != nil {
				bases = append(bases, *v.Baseline.MeanMs)
			}
		}
		if !v.Stale && v.ChannelStatus == 1 && v.FaultRate != nil && v.Requests >= int64(p.MinSamples) {
			rates = append(rates, *v.FaultRate)
		}
	}
	median := ggQuantile(means, .5)
	baseMedian := ggQuantile(bases, .5)
	for _, v := range metrics {
		v.Comparison = ggComparison{Count: len(means)}
		var peers []float64
		for _, other := range metrics {
			if other.ChannelID != v.ChannelID && ggEligibleFRT(other, p) {
				peers = append(peers, *other.FRT.MeanMs)
			}
		}
		v.Comparison.PeerMedian = ggQuantile(peers, .5)
		v.Comparison.P50 = median
		v.Comparison.Method = "少渠道：与其他可比渠道的平均 FRT 中位数比较实际差距，并结合自身正常基准；R-7"
		if len(means) >= p.PercentileChannels {
			v.Comparison.P80 = ggQuantile(means, .8)
			v.Comparison.P90 = ggQuantile(means, .9)
			v.Comparison.Method = "各可比渠道平均 FRT，每渠道一个值，R-7 线性插值"
		}
		if len(means) < 2 {
			v.Comparison.Method = "同类渠道不足；仅使用自身正常基准"
		}
		if v.FRT.MeanMs != nil {
			for _, n := range means {
				if n <= *v.FRT.MeanMs {
					v.Comparison.Position++
				}
			}
			v.Comparison.AboveMedianPct = ggRatioPct(v.FRT.MeanMs, median)
		}
		v.FaultPeers = len(rates)
		v.FaultP90 = nil
		v.FaultP50 = ggQuantile(rates, .5)
		if len(rates) >= p.PercentileChannels {
			v.FaultP90 = ggQuantile(rates, .9)
		}
		if total > 0 {
			v.SharePct = ggFloat(float64(v.Requests) * 100 / float64(total))
		}
		// Compare the same channel population; incomplete baseline populations
		// cannot claim the entire group has deteriorated.
		v.GroupSlow = median != nil && baseMedian != nil && len(bases) == len(means) && *median > *baseMedian*p.GroupSlow
		v.groupMean, v.groupBase = median, nil
		if len(bases) == len(means) {
			v.groupBase = baseMedian
		}
		slowShare := v.Over5sPct
		tailReference := median
		if len(means) < p.PercentileChannels {
			tailReference = v.Comparison.PeerMedian
		}
		if len(means) < 2 {
			tailReference = v.Baseline.MeanMs
		}
		if tailReference != nil && *tailReference >= 5000 {
			slowShare = v.Over10sPct
		}
		v.LongTail = tailReference != nil && v.FRT.P50Ms != nil && *v.FRT.P50Ms <= *tailReference*p.FRTAttention && slowShare != nil && *slowShare < p.MinSlowShare
		v.SharedFailure = false
		if v.ErrorsComplete && v.Errors >= int64(p.MinErrors) && v.channel.BaseHost != "" {
			for _, other := range metrics {
				if other.ChannelID == v.ChannelID || !other.ErrorsComplete || other.Errors < int64(p.MinErrors) || other.channel.BaseHost != v.channel.BaseHost {
					continue
				}
				for _, reason := range v.Reasons {
					for _, r := range other.Reasons {
						if reason.Attributed && r.Attributed && reason.RuleID != "" && reason.RuleID == r.RuleID {
							v.SharedFailure = true
						}
					}
				}
			}
		}
	}
}

func ggEvaluate(v *ggMetrics, p ggPolicy) []ggRuleResult {
	v.Unavailable, v.CapacityLimited = false, false
	base := v.Comparison.P50
	if v.Comparison.Count >= 2 && v.Comparison.Count < p.PercentileChannels {
		base = v.Comparison.PeerMedian
	}
	if v.Comparison.Count < 2 {
		base = v.Baseline.MeanMs
	}
	eligible := ggEligibleFRT(v, p) && base != nil && *base > 0
	attention, severe := (*float64)(nil), (*float64)(nil)
	if base != nil {
		attention = ggFloat(*base * p.FRTAttention)
		severe = ggFloat(*base * p.FRTSevere)
	}
	if v.Comparison.P80 != nil {
		attention = ggFloat(math.Max(*attention, *v.Comparison.P80))
	}
	if v.Comparison.P90 != nil {
		severe = ggFloat(math.Max(*severe, *v.Comparison.P90))
	}
	reason := "缺少有效 FRT 或同类/自身基准，无法作速度判断"
	if eligible {
		reason = fmt.Sprintf("平均首字 %.2f 秒；比较基准 %.2f 秒，相对变化 %+.1f%%", *v.FRT.MeanMs/1000, *base/1000, (*v.FRT.MeanMs / *base - 1)*100)
		if v.Comparison.P90 != nil {
			reason += fmt.Sprintf("；同组同模型 P90 %.2f 秒", *v.Comparison.P90/1000)
		}
	}
	recovered := eligible && *v.FRT.MeanMs < *base*p.RecoveryRatio
	rules := []ggRuleResult{
		{ID: "frt_attention", Eligible: eligible, Hit: eligible && *v.FRT.MeanMs > *attention, Actual: v.FRT.MeanMs, Threshold: attention, Reason: reason, recovered: recovered},
		{ID: "frt_severe", Eligible: eligible, Hit: eligible && *v.FRT.MeanMs > *severe, Actual: v.FRT.MeanMs, Threshold: severe, Reason: reason, recovered: recovered},
	}
	if v.Comparison.Count > 1 && v.Comparison.Count < p.PercentileChannels && v.Baseline.MeanMs == nil {
		rules[1].Eligible = false
		rules[1].Reason += "；少渠道且自身正常基准不足，暂不形成暂停条件"
	}
	if v.LongTail {
		rules[1].Hit = false
		rules[1].Reason += "；请求 P50 与慢请求占比表明可能为偶发长尾，不凭均值暂停"
	}
	unavailableDistinct := v.FailureRun.Distinct
	for _, r := range v.Reasons {
		if r.Capacity && r.Count > 0 {
			v.CapacityLimited = true
		}
		ids := map[string]bool{}
		for _, s := range r.Samples {
			if s.RequestID != "" {
				ids[s.RequestID] = true
			}
		}
		// All attempts in the window failed with this one concrete cause, so
		// successful attempts or retries cannot masquerade as consecutive failures.
		if r.Unavailable && len(ids) >= 2 && v.Success == 0 && v.Pending == 0 && r.Count == v.Requests && v.Requests > 0 {
			v.Unavailable = true
			unavailableDistinct = max(unavailableDistinct, len(ids))
		}
	}
	if v.FailureRun.Attempts >= 2 && v.FailureRun.Distinct >= 2 {
		v.Unavailable = true
	}
	faultEligible := !v.Stale && v.ErrorsComplete && v.Requests >= int64(p.MinSamples) && v.FaultRate != nil && v.FaultP50 != nil && v.FaultPeers >= 2
	faultThreshold := (*float64)(nil)
	if v.FaultP50 != nil {
		faultThreshold = ggFloat(*v.FaultP50 + p.FaultDelta)
		if v.FaultP90 != nil {
			faultThreshold = ggFloat(math.Max(*faultThreshold, *v.FaultP90))
		}
	}
	rules = append(rules, ggRuleResult{ID: "fault_attention", Eligible: faultEligible, Hit: faultEligible && v.Errors >= int64(p.MinErrors) && *v.FaultRate >= *v.FaultP50+p.FaultDelta && (v.FaultP90 == nil || *v.FaultRate > *v.FaultP90), Actual: v.FaultRate, Threshold: faultThreshold, Reason: fmt.Sprintf("已归责 %d 次 / %d 请求尝试；待判 %d 次；至少 %d 次，且高于同类中位数至少 %.0f 个百分点及可用 P90", v.Errors, v.Requests, v.Pending, p.MinErrors, p.FaultDelta), recovered: faultEligible && *v.FaultRate < *v.FaultP50+p.RecoveryFaultDelta})
	pressureEligible := !v.Stale && v.RequestsComplete && v.Baseline.RPM != nil && *v.Baseline.RPM > 0
	pressureThreshold := (*float64)(nil)
	if pressureEligible {
		pressureThreshold = ggFloat(*v.Baseline.RPM * p.Pressure)
	}
	rules = append(rules, ggRuleResult{ID: "pressure", Eligible: pressureEligible, Hit: pressureEligible && v.RPM > *pressureThreshold, Actual: &v.RPM, Threshold: pressureThreshold, Reason: fmt.Sprintf("当前平均每分钟 %.2f 次；仅在此前正常每分钟请求数大于零时计算增长", v.RPM), recovered: pressureEligible && v.RPM < *v.Baseline.RPM*p.RecoveryRatio})
	unavailableReason := "不同 Request ID 连续出现同一明确凭据/账号不可用错误；连续区间须完整且无成功或其他原因打断；立即提示"
	if v.FailureRun.Attempts > 0 {
		unavailableReason += fmt.Sprintf("；尾部连续 %d 次尝试，已核验 %d 个不同 Request ID", v.FailureRun.Attempts, v.FailureRun.Distinct)
	}
	rules = append(rules, ggRuleResult{ID: "unavailable", Eligible: !v.Stale && v.ErrorsComplete, Hit: v.Unavailable, Actual: ggFloat(float64(unavailableDistinct)), Threshold: ggFloat(2), Reason: unavailableReason, recovered: !v.Unavailable && v.ErrorsComplete && v.Pending == 0 && v.Errors == 0 && v.Success > 0})
	rules = append(rules, ggRuleResult{ID: "capacity_limit", Eligible: !v.Stale && v.ErrorsComplete, Hit: v.CapacityLimited, Reason: "排障规则已确认上游账号并发、RPM、TPM 或额度容量限制；客户自身限流不计入", recovered: !v.CapacityLimited && v.ErrorsComplete && v.Pending == 0 && v.Success > 0})
	groupEligible := !v.Stale && eligible && v.Baseline.MeanMs != nil
	rules = append(rules, ggRuleResult{ID: "group_slow", Eligible: groupEligible, Hit: v.GroupSlow, Reason: "同组同模型中位首字超过同一渠道集合此前正常基准的 1.5 倍；先排查公共故障或容量", recovered: groupEligible && !v.GroupSlow && recovered})
	rules = append(rules, ggRuleResult{ID: "shared_failure", Eligible: !v.Stale && v.ErrorsComplete && v.Requests > 0, Hit: v.SharedFailure, Reason: "多个渠道在同一已记录上游主机出现同一明确归责错误；相同依赖不能作为互相替代的证明", recovered: !v.SharedFailure && v.ErrorsComplete && v.Errors == 0 && v.Pending == 0 && v.Success > 0})
	return rules
}
func ggActive(v *ggMetrics, id string) bool {
	for _, r := range v.Rules {
		if r.ID == id {
			return r.Active && r.Eligible
		}
	}
	return false
}
func ggConcern(v *ggMetrics) bool {
	return ggActive(v, "frt_attention") || ggActive(v, "frt_severe") || ggActive(v, "fault_attention") || ggActive(v, "unavailable") || ggActive(v, "group_slow") || ggActive(v, "capacity_limit") || ggActive(v, "shared_failure")
}

func ggRecommend(report *ggRealtimeReport, control *ggControl, configKey string, p ggPolicy) {
	report.Policy = p
	if control.Version != p.Version || control.Metrics == nil {
		*control = ggControl{Version: p.Version, Metrics: map[string]ggMemory{}}
	}
	control.Version = p.Version
	groups := map[[2]string][]*ggMetrics{}
	memories := map[string]ggMemory{}
	for i := range report.Metrics {
		v := &report.Metrics[i]
		groups[[2]string{v.Group, v.Model}] = append(groups[[2]string{v.Group, v.Model}], v)
	}
	report.comparisonMetrics = ggComparisonMetrics(report)
	for _, v := range report.comparisonMetrics {
		v.Rules = []ggRuleResult{}
		mem := control.Metrics[ggMemoryKey(v)]
		config := fmt.Sprintf("%s/%d/%s/%s/%d/%s", configKey, v.ChannelStatus, v.channel.Groups, v.channel.Models, v.channel.DeletedAt, v.channel.BaseHost)
		if mem.Config != config {
			mem = ggMemory{Config: config, Rules: map[string]ggRuleMemory{}}
		}
		ggPrepareBaseline(v, &mem, p)
		memories[ggMemoryKey(v)] = mem
	}
	for _, vs := range ggWindowGroups(report.comparisonMetrics) {
		ggCompare(vs, p)
	}
	for _, v := range report.comparisonMetrics {
		mem := memories[ggMemoryKey(v)]
		var recentAttempts int64
		for _, pt := range v.minutes {
			recentAttempts += pt.Requests
		}
		if recentAttempts == 0 && len(control.Metrics[ggMemoryKey(v)].Rules) == 0 {
			// Idle configured models stay visible, but no empty minute arrays
			// or seven identical rule histories need to be persisted for each one.
			v.Trend = nil
			v.Rules = []ggRuleResult{}
			v.Baseline.Source = "此前完整窗口无请求；零基准不计算增长百分比"
			delete(control.Metrics, ggMemoryKey(v))
			continue
		}
		for _, evaluation := range ggEvaluate(v, p) {
			next, result := ggUpdateRule(mem.Rules[evaluation.ID], evaluation, v.To, p, evaluation.ID == "unavailable")
			ggDescribeRule(v, &result, p)
			mem.Rules[evaluation.ID] = next
			v.Rules = append(v.Rules, result)
		}
		v.Quality, mem.FreezeReason = ggQuality(v)
		freeze := v.Quality != "normal"
		ggRememberQuality(v, &mem, p)
		if ggConcern(v) && !v.Stale {
			v.Status = "attention"
		} else if freeze && !v.Stale && v.Status == "unchanged" {
			v.Status = "pending"
		}
		if !v.ConfigCurrent && !v.Stale {
			v.Status = "pending"
		}
		control.Metrics[ggMemoryKey(v)] = mem
	}
	// Prune only obsolete derived control keys, never request facts.
	for key := range control.Metrics {
		if _, ok := memories[key]; !ok {
			delete(control.Metrics, key)
		}
	}
	ggPlanActions(report, groups, p)
	for i := range report.Metrics {
		ggExplainMetric(&report.Metrics[i], report, p)
	}
	ggSummarizeChannels(report, control, configKey, p)
}

func ggPlanActions(report *ggRealtimeReport, groups map[[2]string][]*ggMetrics, p ggPolicy) {
	all := report.comparisonMetrics
	if len(all) == 0 {
		for i := range report.Metrics {
			all = append(all, &report.Metrics[i])
		}
	}
	windows := ggWindowGroups(all)
	selected := map[*ggMetrics]bool{}
	for i := range report.Metrics {
		selected[&report.Metrics[i]] = true
	}
	report.Actions = []ggAction{}
	report.PauseCount = 0
	report.IncreaseCount = 0
	report.AdjustmentCount = 0
	for i := range report.Metrics {
		report.Metrics[i].Actions = []string{}
		report.Metrics[i].Alternatives = []ggAlternative{}
		report.Metrics[i].Checks = []string{}
	}
	actions := map[string]ggAction{}
	add := func(v *ggMetrics, kind, subtype, reason string, ruleIDs ...string) {
		if !selected[v] || v.Stale || !v.ConfigCurrent {
			return
		}
		// Adding one channel to a group can cover several affected models.
		// Merge that operation, retaining every capability and trigger to check.
		id := kind + ":" + ggKey(ggMetricKey{v.Group, "", 0})
		if kind == "pause" {
			id = fmt.Sprintf("pause:%d", v.ChannelID)
		}
		if _, ok := actions[id]; !ok {
			actions[id] = ggAction{ID: id, Kind: kind, Subtype: subtype, ChannelID: v.ChannelID, Group: v.Group, Model: v.Model, Scope: []string{v.Group + " / " + v.Model}, Reason: reason, Execution: "由管理员在中转站手动操作；执行前刷新并核对配置与负载"}
		}
		a := actions[id]
		if !ggHasString(a.Models, v.Model) {
			a.Models = append(a.Models, v.Model)
		}
		if scope := v.Group + " / " + v.Model; !ggHasString(a.Scope, scope) {
			a.Scope = append(a.Scope, scope)
		}
		if !ggHasString(a.Subtypes, subtype) {
			a.Subtypes = append(a.Subtypes, subtype)
		}
		if !ggHasString(a.Reasons, reason) {
			a.Reasons = append(a.Reasons, reason)
		}
		if len(a.Subtypes) > 1 {
			a.Subtype = "combined"
		}
		for _, rule := range v.Rules {
			if !ggHasString(ruleIDs, rule.ID) || !rule.Active || !rule.Eligible {
				continue
			}
			found := false
			for _, t := range a.Triggers {
				if t.MetricKey == v.Key && t.From == v.From && t.To == v.To && t.Rule.ID == rule.ID {
					found = true
				}
			}
			if !found {
				a.Triggers = append(a.Triggers, ggActionTrigger{v.Key, v.ChannelID, v.ChannelName, v.Group, v.Model, v.From, v.To, rule})
			}
		}
		if subtype == "backup" && v.ChannelStatus != 1 && len(ruleIDs) == 0 {
			a.Triggers = append(a.Triggers, ggActionTrigger{v.Key, v.ChannelID, v.ChannelName, v.Group, v.Model, v.From, v.To, ggRuleResult{ID: "primary_exited", Eligible: true, Hit: true, Active: true, Actual: ggFloat(float64(v.Requests)), Unit: "attempts", Reason: "近期主力渠道已停用，独立备用不足", Description: fmt.Sprintf("#%d 在此窗口承担 %d 次请求尝试，当前已停用；近期可靠独立备用不足。", v.ChannelID, v.Requests)}})
		}
		a.Reason = strings.Join(a.Reasons, "；")
		actions[id] = a
		if !ggHasString(v.Actions, id) {
			v.Actions = append(v.Actions, id)
		}
	}
	byChannel := map[int][]*ggMetrics{}
	candidates := map[int]bool{}
	for i := range report.Metrics {
		v := &report.Metrics[i]
		byChannel[v.ChannelID] = append(byChannel[v.ChannelID], v)
	}
	for _, vs := range windows {
		slow := 0
		pressure := false
		capacity := false
		common := false
		var anchor *ggMetrics
		reliableHosts := map[string]bool{}
		for _, v := range vs {
			if ggActive(v, "frt_attention") || ggActive(v, "frt_severe") || (ggActive(v, "group_slow") && v.FRTChangePct != nil && *v.FRTChangePct > (p.GroupSlow-1)*100) {
				slow++
			}
			pressure = pressure || ggActive(v, "pressure")
			capacity = capacity || ggActive(v, "capacity_limit")
			common = common || ggActive(v, "group_slow") || ggActive(v, "shared_failure")
			if anchor == nil || v.Requests > anchor.Requests {
				anchor = v
			}
			if ggEligibleFRT(v, p) && v.Success > 0 && v.ErrorsComplete && v.Errors == 0 && v.Pending == 0 && !ggConcern(v) && v.channel.BaseHost != "" {
				reliableHosts[v.channel.BaseHost] = true
			}
		}
		if capacity {
			for _, v := range vs {
				if ggActive(v, "capacity_limit") {
					add(v, "increase", "capacity", "已有上游容量限制，需要为该分组和模型补充容量", "capacity_limit")
				}
			}
		}
		if pressure && slow >= 2 {
			selectedTrigger := false
			for _, v := range vs {
				if ggActive(v, "pressure") || ggActive(v, "frt_attention") || ggActive(v, "frt_severe") || (ggActive(v, "group_slow") && v.FRTChangePct != nil && *v.FRTChangePct > (p.GroupSlow-1)*100) {
					add(v, "increase", "capacity", "请求压力增长，同时多条渠道持续变慢，需要为该分组和模型补充容量", "pressure", "frt_attention", "frt_severe", "group_slow")
					selectedTrigger = selectedTrigger || (selected[v] && !v.Stale && v.ConfigCurrent)
				}
			}
			// Peers may normally display a different window. Retain the exact
			// same-window support for this group operation without changing their
			// selected totals or assigning an unrelated action to those channels.
			id := "increase:" + ggKey(ggMetricKey{vs[0].Group, "", 0})
			if a, ok := actions[id]; ok && selectedTrigger {
				for _, v := range vs {
					if selected[v] {
						continue
					}
					for _, rule := range v.Rules {
						if rule.Active && rule.Eligible && ggHasString([]string{"pressure", "frt_attention", "frt_severe", "group_slow"}, rule.ID) {
							a.Triggers = append(a.Triggers, ggActionTrigger{v.Key, v.ChannelID, v.ChannelName, v.Group, v.Model, v.From, v.To, rule})
						}
					}
				}
				actions[id] = a
			}
		}
		if anchor != nil && !anchor.Stale && anchor.ConfigCurrent && anchor.ChannelStatus != 1 && anchor.Requests > 0 && len(reliableHosts) < 2 {
			add(anchor, "increase", "backup", "近期承担最多请求的渠道已退出，具备近期成功且无已知故障的独立备用不足")
		}
		for _, v := range vs {
			if !selected[v] {
				continue
			}
			if v.ChannelStatus != 1 {
				v.Checks = append(v.Checks, "当前渠道已停用/删除，保留历史请求；不重复建议暂停")
				continue
			}
			if v.Stale || !v.ConfigCurrent {
				v.Checks = append(v.Checks, "数据或当前渠道配置未就绪，暂不形成操作建议")
				continue
			}
			if ggActive(v, "shared_failure") {
				add(v, "increase", "backup", "多条渠道在同一上游主机出现同一明确错误，现有渠道不能证明故障来源独立，建议补独立备用", "shared_failure")
			}
			bad := ggActive(v, "unavailable") || (ggActive(v, "frt_severe") && !v.LongTail) || ggActive(v, "fault_attention")
			if common && !ggActive(v, "unavailable") {
				v.Checks = append(v.Checks, "整组共同恶化：先排查公共依赖和容量，不按相对排名逐个暂停")
				if slow >= 2 && !pressure && !capacity {
					if ggConcern(v) {
						add(v, "increase", "backup", "同组多渠道共同恶化，需补充独立故障来源的备用", "group_slow", "shared_failure", "frt_attention", "frt_severe")
					}
				}
				continue
			}
			if !bad {
				continue
			}
			candidates[v.ChannelID] = true
		}
	}
	// Reserve successful alternatives once across ALL simultaneous proposals.
	// Never use another pause candidate as an alternative. This conservative
	// exclusion prevents cycles (A replaces B while B replaces A).
	reserve := map[int]float64{}
	// Shared accounts/channels cannot claim the same capacity separately for
	// each group/model. Use concurrently observed aggregate minute load, not
	// the sum of peaks that may have occurred at different times.
	type loadKey struct {
		id       int
		from, to int64
	}
	loadsByWindow := map[loadKey][]*ggMetrics{}
	for _, v := range all {
		key := loadKey{v.ChannelID, v.From, v.To}
		loadsByWindow[key] = append(loadsByWindow[key], v)
	}
	channelRPM, channelProven := map[loadKey]float64{}, map[loadKey]float64{}
	for id, leaves := range loadsByWindow {
		counts := map[int64]int{}
		loads := map[int64]int64{}
		for _, v := range leaves {
			channelRPM[id] += v.RPM
			for _, pt := range v.normal {
				counts[pt.At]++
				loads[pt.At] += pt.Requests
			}
		}
		valid := 0
		peak := int64(0)
		for at, n := range counts {
			if n == len(leaves) {
				valid++
				peak = max(peak, loads[at])
			}
		}
		if valid >= p.BaselineMinutes {
			channelProven[id] = float64(peak) * p.CapacityReserve
		}
	}
	ids := make([]int, 0, len(candidates))
	for id := range candidates {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		leaves := byChannel[id]
		safe := len(leaves) > 0
		allocations := map[int]float64{}
		scopes := []string{}
		for _, v := range leaves {
			scopes = append(scopes, v.Group+" / "+v.Model)
			if v.Stale || !v.ConfigCurrent || !v.RequestsComplete || !v.ErrorsComplete {
				safe = false
				v.Checks = append(v.Checks, "跨组/模型影响检查未完成")
				continue
			}
			need := v.RPM
			if need <= 0 {
				need = 1
			} // Even idle configured models require a tested fallback.
			for _, alt := range windows[ggWindowGroupKey(v)] {
				if alt.ChannelID == id || candidates[alt.ChannelID] || !ggEligibleFRT(alt, p) || !alt.ErrorsComplete || alt.Pending > 0 || alt.Errors > 0 || alt.Success == 0 || ggConcern(alt) || alt.Baseline.PeakRPM == nil {
					continue
				}
				// Same hostname is not evidence of an independent fallback. Unknown
				// hosts cannot be silently treated as independent either.
				if v.channel.BaseHost == "" || alt.channel.BaseHost == "" || strings.EqualFold(v.channel.BaseHost, alt.channel.BaseHost) {
					continue
				}
				load := loadKey{alt.ChannelID, v.From, v.To}
				proven := channelProven[load]
				headroom := math.Max(0, proven-channelRPM[load]-reserve[alt.ChannelID]-allocations[alt.ChannelID])
				headroom = math.Min(headroom, math.Max(0, *alt.Baseline.PeakRPM*p.CapacityReserve-alt.RPM))
				if headroom <= 0 {
					continue
				}
				v.Alternatives = append(v.Alternatives, ggAlternative{alt.ChannelID, alt.ChannelName, alt.FRT.MeanMs, alt.Success, channelRPM[load], proven, headroom, fmt.Sprintf("同一 %d 分钟窗口的真实成功和当前负载 + 渠道所有分组/模型同一分钟已承受总 RPM 的 80%% 保守余量；不同已记录主机；不保证供应商实际物理独立", v.WindowMinutes)})
				used := math.Min(need, headroom)
				allocations[alt.ChannelID] += used
				need -= used
				if need <= 0 {
					break
				}
			}
			if need > 0 {
				safe = false
				v.Checks = append(v.Checks, fmt.Sprintf("替代渠道的近期成功、模型能力、独立来源或承接余量不足（尚缺每分钟 %.2f 次），先补备用", need))
			} else {
				v.Checks = append(v.Checks, "替代渠道有近期成功、同模型记录及保守负载余量；已扣除其他暂停事项预留流量")
			}
		}
		if safe {
			for key, n := range allocations {
				reserve[key] += n
			}
			for _, v := range leaves {
				add(v, "pause", "channel", ggConfirmedConcern(leaves)+"；整个渠道的跨组影响和合并承接检查已通过", "unavailable", "frt_severe", "fault_attention")
				v.Status = "pause"
				v.Checks = append(v.Checks, "中转站操作范围为整个渠道，不能只禁用一个模型")
			}
			key := fmt.Sprintf("pause:%d", id)
			a := actions[key]
			a.Scope = scopes
			a.Group = ""
			a.Model = ""
			actions[key] = a
		} else {
			for _, v := range leaves {
				if ggActive(v, "unavailable") || ggActive(v, "frt_severe") || ggActive(v, "fault_attention") {
					add(v, "increase", "backup", ggConfirmedConcern([]*ggMetrics{v})+"；整个渠道的承接能力未确认，先补独立备用", "unavailable", "frt_severe", "fault_attention")
					v.Status = "increase"
				}
			}
		}
	}
	for _, a := range actions {
		sort.Strings(a.Models)
		sort.Strings(a.Scope)
		sort.Strings(a.Subtypes)
		sort.Strings(a.Reasons)
		a.Reason = strings.Join(a.Reasons, "；")
		sort.Slice(a.Triggers, func(i, j int) bool {
			x, y := a.Triggers[i], a.Triggers[j]
			if x.MetricKey+":"+x.Rule.ID == y.MetricKey+":"+y.Rule.ID {
				return x.From < y.From
			}
			return x.MetricKey+":"+x.Rule.ID < y.MetricKey+":"+y.Rule.ID
		})
		if a.Kind == "increase" {
			a.ChannelID = 0
			a.Model = ""
		} // A group/model operation has no arbitrary anchor channel.
		report.Actions = append(report.Actions, a)
		if a.Kind == "pause" {
			report.PauseCount++
		} else {
			report.IncreaseCount++
		}
	}
	sort.Slice(report.Actions, func(i, j int) bool { return report.Actions[i].ID < report.Actions[j].ID })
	report.AdjustmentCount = len(report.Actions)
	for i := range report.Metrics {
		v := &report.Metrics[i]
		if len(v.Actions) > 0 && v.Status != "pause" {
			v.Status = "increase"
		}
	}
}
