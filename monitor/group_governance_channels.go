package monitor

import (
	"fmt"
	"sort"
	"strings"
)

// Only one public row per group/channel. Model comparisons remain evidence,
// rather than a second set of displayed totals or executable operations.
type ggChannelMetrics struct {
	ggMetrics
	Models          []string            `json:"models"`
	Evidence        []ggChannelEvidence `json:"evidence"`
	ComparedSamples int64               `json:"compared_samples"`
	ComparedMeanMs  *float64            `json:"compared_mean_ms"`
	ReferenceMeanMs *float64            `json:"reference_mean_ms"`
	ReferenceChange *float64            `json:"reference_change_pct"`
	SpeedState      string              `json:"speed_state"`
	SpeedMissing    []string            `json:"speed_missing"`
}

type ggChannelEvidence struct {
	Key          string          `json:"key"`
	Model        string          `json:"model"`
	From         int64           `json:"from_ts"`
	To           int64           `json:"to_ts"`
	MeanMs       *float64        `json:"mean_ms"`
	Observed     *int64          `json:"observed"`
	Comparison   ggComparison    `json:"comparison"`
	Baseline     ggBaseline      `json:"baseline"`
	Rules        []ggRuleResult  `json:"rules"`
	Alternatives []ggAlternative `json:"alternatives"`
	Checks       []string        `json:"checks"`
}

func ggAppendUnique(values []string, value string) []string {
	if value != "" && !ggHasString(values, value) {
		return append(values, value)
	}
	return values
}

func ggConfirmedConcern(leaves []*ggMetrics) string {
	var reasons []string
	for _, v := range leaves {
		if ggActive(v, "unavailable") {
			found := false
			for _, why := range v.Reasons {
				if why.Attributed && why.Unavailable {
					reasons = ggAppendUnique(reasons, "连续出现"+why.Reason)
					found = true
				}
			}
			if !found {
				reasons = ggAppendUnique(reasons, "不同请求连续出现已确认的不可用错误")
			}
		}
		if ggActive(v, "frt_severe") {
			reasons = ggAppendUnique(reasons, "同模型比较持续显著偏慢")
		}
		if ggActive(v, "fault_attention") {
			reasons = ggAppendUnique(reasons, "已确认渠道或上游故障率持续偏高")
		}
	}
	sort.Strings(reasons)
	return strings.Join(reasons, "，")
}

func ggRememberQuality(v *ggMetrics, mem *ggMemory, p ggPolicy) {
	freeze := v.Quality != "normal"
	if freeze {
		mem.Frozen, mem.Deferred = true, nil
	}
	if v.To > mem.Last {
		mem.Frozen = freeze
		if !freeze && v.Pending == 0 {
			for _, pt := range v.Trend {
				if pt.At >= mem.Last && pt.Complete && pt.Errors == 0 && pt.Pending == 0 && (pt.Requests == 0 || pt.MeanMs != nil) {
					mem.Deferred = append(mem.Deferred, pt)
				}
			}
		}
		if len(mem.Deferred) > p.ExpandedMinutes+p.BaselineMinutes {
			mem.Deferred = mem.Deferred[len(mem.Deferred)-(p.ExpandedMinutes+p.BaselineMinutes):]
		}
		mem.Last = v.To
	}
	v.Baseline.Frozen, v.Baseline.FreezeReason = mem.Frozen, mem.FreezeReason
	if mem.Frozen {
		v.Baseline.ResumeCondition = "覆盖完整、速度可判断且错误恢复后继续更新；已触发故障需连续三个新分钟恢复。单纯流量增长不冻结正常基准。"
	}
}

func ggSummarizeChannels(report *ggRealtimeReport, control *ggControl, configKey string, p ggPolicy) {
	type channelWindow struct {
		ggMetricKey
		minutes int
	}
	by := map[channelWindow][]*ggMetrics{}
	selectedWindows := map[ggMetricKey]int{}
	for i := range report.Metrics {
		v := &report.Metrics[i]
		selectedWindows[ggMetricKey{Group: v.Group, Channel: v.ChannelID}] = v.WindowMinutes
	}
	all := report.comparisonMetrics
	if len(all) == 0 {
		for i := range report.Metrics {
			all = append(all, &report.Metrics[i])
		}
	}
	for _, v := range all {
		k := channelWindow{ggMetricKey{Group: v.Group, Channel: v.ChannelID}, v.WindowMinutes}
		by[k] = append(by[k], v)
	}
	if control.Channels == nil {
		control.Channels = map[string]ggMemory{}
	}
	report.Channels = []ggChannelMetrics{}
	live := map[string]bool{}
	for k, leaves := range by {
		first := leaves[0]
		c := ggChannelMetrics{ggMetrics: ggMetrics{Key: ggKey(k.ggMetricKey), Group: k.Group, ChannelID: k.Channel, ChannelName: first.ChannelName, ChannelStatus: first.ChannelStatus, From: first.From, To: first.To, WindowMinutes: first.WindowMinutes, WindowReason: first.WindowReason, RequestsComplete: true, ErrorsComplete: true, FRTSourceComplete: true, ConfigCurrent: true, ConfigAt: first.ConfigAt, channel: first.channel, Rules: []ggRuleResult{}, Actions: []string{}, Reasons: []ggErrorReason{}, Missing: []string{}}, Models: []string{}, Evidence: []ggChannelEvidence{}, SpeedState: "normal"}
		v := &c.ggMetrics
		points := map[int64]ggPoint{}
		reasons := map[string]*ggErrorReason{}
		quality := "normal"
		var actual, reference float64
		for _, m := range leaves {
			// All leaves are built from the same channel window. Refuse a partial
			// sum if a future producer violates that contract.
			if m.From != v.From || m.To != v.To {
				v.RequestsComplete, v.Stale = false, true
				v.Missing = ggAppendUnique(v.Missing, "模型窗口未对齐，渠道汇总暂不可判断")
				continue
			}
			c.Models = ggAppendUnique(c.Models, m.Model)
			v.Requests += m.Requests
			v.Success += m.Success
			v.Errors += m.Errors
			v.Pending += m.Pending
			v.KnownOther += m.KnownOther
			v.RequestsComplete = v.RequestsComplete && m.RequestsComplete
			v.ErrorsComplete = v.ErrorsComplete && m.ErrorsComplete
			v.FRTSourceComplete = v.FRTSourceComplete && m.FRTSourceComplete
			v.ConfigCurrent = v.ConfigCurrent && m.ConfigCurrent
			v.Stale = v.Stale || m.Stale
			v.ErrorCapturedAt = max(v.ErrorCapturedAt, m.ErrorCapturedAt)
			v.frtAggregate.merge(m.frtAggregate)
			for _, a := range m.Actions {
				v.Actions = ggAppendUnique(v.Actions, a)
			}
			if m.Requests > 0 && m.Quality != "normal" {
				if m.Quality == "abnormal" {
					quality = "abnormal"
				} else if quality != "abnormal" {
					quality = "unknown"
				}
			}
			for _, pt := range m.minutes {
				sum, ok := points[pt.At]
				if !ok {
					sum = ggPoint{At: pt.At, Complete: true}
				}
				sum.Requests += pt.Requests
				sum.Success += pt.Success
				sum.SumMs += pt.SumMs
				sum.Observed += pt.Observed
				sum.Errors += pt.Errors
				sum.Pending += pt.Pending
				sum.Complete = sum.Complete && pt.Complete && (pt.Requests == 0 || pt.MeanMs != nil)
				points[pt.At] = sum
			}
			for _, why := range m.Reasons {
				id := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%t\x00%t", why.RuleID, why.Code, why.Reason, why.Responsibility, why.Attributed, why.Pending)
				if reasons[id] == nil {
					x := why
					x.Count = 0
					x.Samples = nil
					reasons[id] = &x
				}
				r := reasons[id]
				r.Count += why.Count
				for _, sample := range why.Samples {
					if len(r.Samples) >= 6 {
						break
					}
					sample.Model = m.Model
					duplicate := false
					for _, old := range r.Samples {
						if sample.LogID == old.LogID && sample.RequestID == old.RequestID {
							duplicate = true
						}
					}
					if !duplicate {
						r.Samples = append(r.Samples, sample)
					}
				}
			}
			if m.Requests > 0 || len(m.Actions) > 0 {
				c.Evidence = append(c.Evidence, ggChannelEvidence{Key: m.Key, Model: m.Model, From: m.From, To: m.To, MeanMs: m.FRT.MeanMs, Observed: m.FRT.Observed, Comparison: m.Comparison, Baseline: m.Baseline, Rules: m.Rules, Alternatives: m.Alternatives, Checks: m.Checks})
			}
			if m.Requests > 0 {
				r := ggFindRule(m, "frt_attention")
				if r == nil || !r.Eligible {
					switch {
					case m.FRT.MeanMs == nil:
						c.SpeedMissing = ggAppendUnique(c.SpeedMissing, "部分模型"+ggFRTMissing(m.FRT.MeanUnavailableReason))
					case m.FRT.Observed == nil || *m.FRT.Observed < int64(p.MinSamples):
						c.SpeedMissing = ggAppendUnique(c.SpeedMissing, fmt.Sprintf("部分模型有效首字样本不足 %d 个，暂不能比较速度", p.MinSamples))
					case !m.FRTSourceComplete:
						c.SpeedMissing = ggAppendUnique(c.SpeedMissing, "首字耗时分钟覆盖未完整")
					case m.ChannelStatus != 1:
						c.SpeedMissing = ggAppendUnique(c.SpeedMissing, "已停用渠道保留实际耗时，不参与当前速度排名")
					case m.Comparison.Count < 2 && m.Baseline.MeanMs == nil:
						c.SpeedMissing = ggAppendUnique(c.SpeedMissing, "同模型可比较渠道不足，且自身正常耗时基准尚未建立")
					}
				}
				if r != nil && r.Eligible && r.Comparison != nil && m.FRT.MeanMs != nil && m.FRT.Observed != nil {
					n := *m.FRT.Observed
					c.ComparedSamples += n
					actual += *m.FRT.MeanMs * float64(n)
					reference += *r.Comparison * float64(n)
				}
				if ggActive(m, "frt_attention") || ggActive(m, "frt_severe") || ggActive(m, "group_slow") {
					c.SpeedState = "slow"
				} else if c.SpeedState != "slow" && (r == nil || !r.Eligible) {
					c.SpeedState = "unknown"
				} else if c.SpeedState == "normal" && r.Hit {
					c.SpeedState = "observing"
				}
			}
		}
		sort.Strings(c.Models)
		v.FRT = v.frtAggregate.view(v.FRTSourceComplete)
		if v.FRT.Observed != nil && *v.FRT.Observed > 0 {
			n5, n10 := v.frtAggregate.hist.ttft10k+v.frtAggregate.hist.ttftInf, v.frtAggregate.hist.ttftInf
			v.Over5s, v.Over10s = &n5, &n10
			v.Over5sPct, v.Over10sPct = ggFloat(float64(n5)*100/float64(*v.FRT.Observed)), ggFloat(float64(n10)*100/float64(*v.FRT.Observed))
		}
		for at := v.To - int64(p.ExpandedMinutes+p.BaselineMinutes)*60; at < v.To; at += 60 {
			pt := points[at]
			pt.At = at
			if pt.Observed > 0 && pt.SumMs > 0 {
				pt.MeanMs = ggFloat(float64(pt.SumMs) / float64(pt.Observed))
			}
			v.minutes = append(v.minutes, pt)
			if at >= v.From {
				v.Trend = append(v.Trend, pt)
				v.PeakRPM = max(v.PeakRPM, pt.Requests)
			}
		}
		v.RPM = float64(v.Requests) / float64(v.WindowMinutes)
		if v.RequestsComplete {
			v.RequestsThrough = v.To
		}
		if v.FRTSourceComplete {
			v.FRTThrough = v.To
		}
		if v.ErrorsComplete {
			v.ErrorsThrough = v.To
			if v.Requests > 0 {
				v.FaultRate = ggFloat(float64(v.Errors) * 100 / float64(v.Requests))
			}
		}
		for _, r := range reasons {
			v.Reasons = append(v.Reasons, *r)
		}
		sort.Slice(v.Reasons, func(i, j int) bool {
			if v.Reasons[i].Count != v.Reasons[j].Count {
				return v.Reasons[i].Count > v.Reasons[j].Count
			}
			return v.Reasons[i].Reason < v.Reasons[j].Reason
		})
		if c.ComparedSamples > 0 {
			c.ComparedMeanMs = ggFloat(actual / float64(c.ComparedSamples))
			c.ReferenceMeanMs = ggFloat(reference / float64(c.ComparedSamples))
			c.ReferenceChange = ggRatioPct(c.ComparedMeanMs, c.ReferenceMeanMs)
		}
		if v.FRT.MeanMs == nil {
			c.SpeedState = "unknown"
		}
		memoryKey := ggMemoryKey(v)
		mem := control.Channels[memoryKey]
		config := fmt.Sprintf("%s/%d/%s/%s/%d/%s", configKey, v.ChannelStatus, v.channel.Groups, v.channel.Models, v.channel.DeletedAt, v.channel.BaseHost)
		if mem.Config != config {
			mem = ggMemory{Config: config, Rules: map[string]ggRuleMemory{}}
		}
		ggPrepareBaseline(v, &mem, p)
		v.Quality = quality
		if v.Stale || !v.RequestsComplete {
			v.Quality = "unknown"
			mem.FreezeReason = "采集落后或请求窗口覆盖不完整"
		} else if v.Errors > 0 {
			v.Quality = "abnormal"
			mem.FreezeReason = fmt.Sprintf("已确认 %d 次渠道或上游错误", v.Errors)
		} else if quality == "abnormal" {
			mem.FreezeReason = "同模型速度规则尚未完成连续恢复"
		} else if !v.ErrorsComplete || v.Pending > 0 {
			v.Quality = "unknown"
			mem.FreezeReason = "错误归责尚未完成"
		} else if !v.FRTSourceComplete {
			v.Quality = "unknown"
			mem.FreezeReason = "首字耗时分钟覆盖尚未完整"
		} else if v.FRT.MeanMs == nil {
			v.Quality = "unknown"
			mem.FreezeReason = ggFRTMissing(v.FRT.MeanUnavailableReason)
		} else if quality == "unknown" {
			mem.FreezeReason = strings.Join(c.SpeedMissing, "；")
		} else {
			mem.FreezeReason = ""
		}
		ggRememberQuality(v, &mem, p)
		control.Channels[memoryKey] = mem
		live[memoryKey] = true
		ggExplainChannel(&c, report)
		if selectedWindows[k.ggMetricKey] == v.WindowMinutes {
			report.Channels = append(report.Channels, c)
		}
	}
	for key := range control.Channels {
		if !live[key] {
			delete(control.Channels, key)
		}
	}
	sort.Slice(report.Channels, func(i, j int) bool { return report.Channels[i].Key < report.Channels[j].Key })
	type shareKey struct {
		group    string
		from, to int64
	}
	totals := map[shareKey]int64{}
	for _, v := range all {
		totals[shareKey{v.Group, v.From, v.To}] += v.Requests
	}
	for i := range report.Channels {
		c := &report.Channels[i]
		total := totals[shareKey{c.Group, c.From, c.To}]
		if total > 0 {
			c.SharePct = ggFloat(float64(c.Requests) * 100 / float64(total))
		}
	}
}

func ggExplainChannel(c *ggChannelMetrics, report *ggRealtimeReport) {
	v := &c.ggMetrics
	d := ggDecision{Gaps: []string{}}
	if v.Requests > 0 && v.Requests < int64(report.Policy.MinSamples) {
		d.Gaps = append(d.Gaps, fmt.Sprintf("最近 %d 分钟仅 %d 次请求尝试，请求样本不足 %d 次；已有错误仍展示", v.WindowMinutes, v.Requests, report.Policy.MinSamples))
	}
	if v.Stale || !v.RequestsComplete {
		d.Gaps = append(d.Gaps, "采集落后或请求窗口覆盖不完整，已确认错误仍保留")
	}
	if !v.ConfigCurrent {
		d.Gaps = append(d.Gaps, "渠道配置尚未更新，暂不能确认调整范围")
	}
	if !v.FRTSourceComplete {
		d.Gaps = append(d.Gaps, "首字耗时分钟覆盖尚未完整")
	}
	if v.FRT.MeanMs == nil {
		d.FRT = ggFRTMissing(v.FRT.MeanUnavailableReason)
		if v.Requests > 0 {
			d.Gaps = append(d.Gaps, d.FRT)
		}
	} else if c.ReferenceChange == nil || v.FRT.Observed == nil || c.ComparedSamples < *v.FRT.Observed {
		d.FRT = fmt.Sprintf("其中 %d 个有效样本具有同模型参照，暂不能判断渠道总体速度", c.ComparedSamples)
		if len(c.SpeedMissing) > 0 {
			d.FRT = strings.Join(c.SpeedMissing, "；")
		}
		d.Gaps = append(d.Gaps, c.SpeedMissing...)
	} else {
		direction := "快"
		delta := -*c.ReferenceChange
		if *c.ReferenceChange >= 0 {
			direction = "慢"
			delta = *c.ReferenceChange
		}
		d.FRT = fmt.Sprintf("按当前模型构成对齐参照 %.2f 秒，相比%s %.1f%%", *c.ReferenceMeanMs/1000, direction, delta)
		switch c.SpeedState {
		case "slow":
			d.FRT += "；部分模型判慢规则已持续满足"
		case "observing":
			d.FRT += "；尚未持续满足判慢条件"
		default:
			d.FRT += "；未达到持续判慢条件"
		}
	}
	d.Errors = "已确认错误的具体原因见详细依据"
	if v.Errors == 0 {
		d.Errors = "未发现已确认的渠道或上游错误"
	}
	for _, r := range v.Reasons {
		if r.Attributed {
			d.Errors = "主要原因：" + r.Reason + "（" + r.Responsibility + "）"
			break
		}
	}
	d.Errors += fmt.Sprintf("；待判 %d 次", v.Pending)
	if !v.ErrorsComplete {
		d.Gaps = append(d.Gaps, "错误归责覆盖未完整，已发现错误不能冒充完整错误率")
	}
	if v.Pending > 0 {
		d.Gaps = append(d.Gaps, fmt.Sprintf("仍有 %d 次错误待判", v.Pending))
	}
	if v.GrowthPct == nil {
		d.Traffic = "尚无足够正常数据，暂不能计算增长"
		if v.Baseline.RPM != nil && *v.Baseline.RPM == 0 {
			d.Traffic = "近期正常请求量为零，暂不能计算增长百分比"
		}
		if v.Requests > 0 {
			d.Gaps = append(d.Gaps, d.Traffic)
		}
	} else {
		direction := "增加"
		delta := *v.GrowthPct
		if delta < 0 {
			direction = "减少"
			delta = -delta
		}
		d.Traffic = fmt.Sprintf("比此前正常水平%s %.1f%%（每分钟 %.2f 次）", direction, delta, *v.Baseline.RPM)
	}
	var pause, increase []ggAction
	for _, a := range report.Actions {
		if ggHasString(v.Actions, a.ID) {
			if a.Kind == "pause" {
				pause = append(pause, a)
			} else {
				increase = append(increase, a)
			}
		}
	}
	v.Status = "attention"
	d.Label = "继续观察"
	switch {
	case v.Stale || !v.RequestsComplete:
		v.Status = "stale"
		d.Label = "暂无法判断"
		d.Reason = "当前请求窗口覆盖尚未完整，暂不调整渠道"
	case !v.ConfigCurrent:
		v.Status = "pending"
		d.Label = "暂无法判断"
		d.Reason = "渠道配置尚未更新，暂不能确认调整范围"
	case len(pause) > 0:
		v.Status = "pause"
		d.Label = "建议暂停"
		d.Reason = fmt.Sprintf("暂停整个渠道 #%d：%s", v.ChannelID, pause[0].Reason)
	case len(increase) > 0:
		v.Status = "increase"
		d.Label = "建议增加渠道"
		var reasons []string
		for _, a := range increase {
			reasons = ggAppendUnique(reasons, a.Reason)
		}
		d.Reason = "为“" + v.Group + "”分组增加渠道：" + strings.Join(reasons, "；")
	case v.Requests == 0:
		v.Status = "no_requests"
		d.Label = "近期无请求"
		d.Reason = fmt.Sprintf("最近 %d 分钟无请求，保留当前配置", v.WindowMinutes)
	case v.Errors > 0:
		d.Reason = "已确认渠道或上游错误，先处理下方具体故障；尚未满足安全调整条件"
		for _, why := range v.Reasons {
			if why.Attributed {
				d.Reason = "已确认“" + why.Reason + "”；尚未满足安全调整条件，先处理该故障"
				break
			}
		}
	case v.Quality == "normal":
		v.Status = "unchanged"
		d.Label = "不用调整"
		d.Reason = "速度和错误情况正常，维持现状"
		if v.GrowthPct != nil && *v.GrowthPct > 0 {
			d.Reason = "请求量增长，但速度和错误情况正常，维持现状"
		}
	case v.Quality == "unknown":
		v.Status = "pending"
		d.Label = "暂无法判断"
		d.Reason = memReason(v)
	default:
		d.Reason = "同模型速度已出现恶化，继续核对持续性和替代渠道承接能力"
	}
	if v.ChannelStatus != 1 {
		d.Reason += "；该渠道已停用，不重复暂停"
	}
	v.Decision = d
}

func memReason(v *ggMetrics) string {
	if v.Baseline.FreezeReason != "" {
		return v.Baseline.FreezeReason + "，暂不调整渠道"
	}
	return "判断依据尚未完整，暂不调整渠道"
}
