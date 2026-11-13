package monitor

import (
	"fmt"
	"strings"
)

// These explanations travel with the exact rule evaluation. The browser only
// formats values and renders this result; it must not invent another classifier.
type ggDecision struct {
	Label     string   `json:"label"`
	Reason    string   `json:"reason"`
	FRT       string   `json:"frt"`
	FRTRuleID string   `json:"frt_rule_id"`
	Errors    string   `json:"errors"`
	Traffic   string   `json:"traffic"`
	Gaps      []string `json:"gaps"`
}

func ggFindRule(v *ggMetrics, id string) *ggRuleResult {
	for i := range v.Rules {
		if v.Rules[i].ID == id {
			return &v.Rules[i]
		}
	}
	return nil
}

func ggQuality(v *ggMetrics) (string, string) {
	if v.Stale || !v.RequestsComplete {
		return "unknown", "采集落后或请求窗口覆盖不完整"
	}
	// Known faults remain known even when no request emitted a first byte.
	if v.Errors > 0 {
		return "abnormal", fmt.Sprintf("已确认 %d 次渠道或上游错误，暂不纳入正常分钟", v.Errors)
	}
	for _, r := range v.Rules {
		if r.ID != "pressure" && ((r.Eligible && r.Hit) || r.Active) {
			return "abnormal", "延迟恶化或明确故障尚未完成连续恢复"
		}
	}
	if !v.ErrorsComplete || v.Pending > 0 {
		return "unknown", "错误归责尚未完整或仍有待判错误，服务质量尚不能确认"
	}
	if !v.FRTSourceComplete {
		return "unknown", "首字耗时采集覆盖尚未完整"
	}
	if v.FRT.MeanMs == nil {
		return "unknown", ggFRTMissing(v.FRT.MeanUnavailableReason)
	}
	if v.Requests == 0 {
		return "unknown", "近期没有请求，尚无本分钟质量样本"
	}
	r := ggFindRule(v, "frt_attention")
	if r == nil || !r.Eligible {
		return "unknown", "有效样本、可比较渠道或自身正常基准不足，首字耗时质量无法判断"
	}
	return "normal", ""
}

func ggFRTMissing(reason string) string {
	switch reason {
	case "no_valid_samples", "":
		return "无有效首字耗时样本，暂不能比较速度"
	case "historical_sum_unavailable", "historical_unsupported":
		return "已采集记录缺少可用的首字耗时总和或计量版本，暂不能比较速度"
	default:
		return "首字耗时记录校验未通过，暂不能比较速度（" + reason + "）"
	}
}

func ggRuleValue(n *float64, unit string) string {
	if n == nil {
		return "未取得"
	}
	switch unit {
	case "ms":
		return fmt.Sprintf("%.2f 秒", *n/1000)
	case "percent":
		return fmt.Sprintf("%.2f%%", *n)
	case "rpm":
		return fmt.Sprintf("每分钟 %.2f 次", *n)
	default:
		return fmt.Sprintf("%.0f 次", *n)
	}
}

func ggDescribeRule(v *ggMetrics, r *ggRuleResult, p ggPolicy) {
	switch r.ID {
	case "frt_attention", "frt_severe":
		r.Unit = "ms"
		r.Comparison = v.Comparison.P50
		r.ComparisonKind = "同组同模型可比渠道的平均首字耗时中位数"
		if v.Comparison.Count < 2 {
			r.Comparison = v.Baseline.MeanMs
			r.ComparisonKind = "自身此前有效正常分钟的平均首字耗时"
		} else if v.Comparison.Count < p.PercentileChannels {
			r.Comparison = v.Comparison.PeerMedian
			r.ComparisonKind = "同组同模型其他可比渠道的平均首字耗时中位数"
		}
	case "fault_attention":
		r.Unit = "percent"
		r.Comparison = v.FaultP50
		r.ComparisonKind = "同组同模型可比渠道的故障率中位数"
	case "pressure":
		r.Unit = "rpm"
		r.Comparison = v.Baseline.RPM
		r.ComparisonKind = "此前正常请求强度"
	case "group_slow":
		r.Unit = "ms"
		r.Actual = v.groupMean
		r.Comparison = v.groupBase
		r.ComparisonKind = "同一渠道集合此前正常首字耗时的中位数"
		if v.groupBase != nil {
			r.Threshold = ggFloat(*v.groupBase * p.GroupSlow)
		}
	case "capacity_limit":
		r.Unit = "attempts"
		var n int64
		for _, e := range v.Reasons {
			if e.Capacity {
				n += e.Count
			}
		}
		r.Actual = ggFloat(float64(n))
		r.Threshold = ggFloat(1)
	case "shared_failure":
		r.Unit = "attempts"
		r.Actual = ggFloat(float64(v.Errors))
		r.Threshold = ggFloat(float64(p.MinErrors))
	default:
		r.Unit = "attempts"
	}
	r.DeviationPct = ggRatioPct(r.Actual, r.Comparison)
	r.Description = r.Reason + "。"
	if r.Actual != nil && r.Threshold != nil {
		r.Description = fmt.Sprintf("当前 %s", ggRuleValue(r.Actual, r.Unit))
		if r.Comparison != nil {
			r.Description += fmt.Sprintf("，参照%s为 %s", r.ComparisonKind, ggRuleValue(r.Comparison, r.Unit))
		}
		r.Description += fmt.Sprintf("，本规则触发线为 %s。%s。", ggRuleValue(r.Threshold, r.Unit), r.Reason)
	}
	if !r.Eligible {
		r.Description += "当前所需证据不完整，暂不能判断是否达到本规则。"
	} else if r.Hit {
		r.Description += "本分钟满足条件。"
	} else {
		r.Description += "本分钟未满足条件。"
	}
	if r.Immediate {
		r.Description += "明确不可用按不同请求的连续错误判断，不等待普通样本门槛。"
	} else {
		r.Description += fmt.Sprintf("最近 %d 个新分钟中已评估 %d 个，%d 次满足条件，至少需 %d 次；重复刷新不增加次数。", p.HitMinutes, r.Evaluations, r.Hits, p.HitRequired)
	}
	if r.Active {
		r.Description += fmt.Sprintf("持续条件已成立；当前连续恢复 %d 个新分钟，需 %d 个才撤销。", r.Recoveries, p.RecoveryMinutes)
	}
}

func ggExplainMetric(v *ggMetrics, report *ggRealtimeReport, p ggPolicy) {
	d := ggDecision{Gaps: []string{}}
	if v.Stale || !v.RequestsComplete {
		d.Gaps = append(d.Gaps, "采集落后：当前窗口尚未完整，不能据此确认无请求或正常")
	}
	if !v.FRTSourceComplete {
		d.Gaps = append(d.Gaps, "首字耗时分钟覆盖尚未完整")
	}
	if !v.ConfigCurrent {
		d.Gaps = append(d.Gaps, "配置待更新：渠道配置尚未更新，暂不能确认调整范围")
	}
	if v.RequestsComplete && v.Requests == 0 {
		d.Gaps = append(d.Gaps, fmt.Sprintf("最近 %d 分钟无请求", v.WindowMinutes))
	}
	if v.Requests > 0 && (v.FRT.Observed == nil || *v.FRT.Observed < int64(p.MinSamples)) {
		d.Gaps = append(d.Gaps, fmt.Sprintf("有效样本不足：首字耗时普通比较至少需要 %d 个有效样本", p.MinSamples))
	}
	if v.Comparison.Count < 2 {
		d.Gaps = append(d.Gaps, "可比较渠道不足：不使用横向排名，需自身正常基准")
	}
	if v.Baseline.MeanMs == nil || v.Baseline.RPM == nil {
		d.Gaps = append(d.Gaps, fmt.Sprintf("正常基准不足：已取得 %d / %d 个正常分钟", v.Baseline.Minutes, p.BaselineMinutes))
	}
	if !v.ErrorsComplete || v.Pending > 0 {
		d.Gaps = append(d.Gaps, "错误尚有待判或覆盖缺口，不能当作没有渠道故障")
	}
	for _, gap := range v.Missing {
		if !ggHasString(d.Gaps, gap) {
			d.Gaps = append(d.Gaps, gap)
		}
	}
	frt := ggFindRule(v, "frt_attention")
	if severe := ggFindRule(v, "frt_severe"); severe != nil && severe.Eligible && (severe.Hit || severe.Active) {
		frt = severe
	}
	if v.FRT.MeanMs == nil {
		d.FRT = ggFRTMissing(v.FRT.MeanUnavailableReason)
	} else if frt != nil {
		d.FRTRuleID = frt.ID
		d.FRT = frt.Description
	} else {
		d.FRT = "尚无足够有效首字耗时样本，本轮不能判断是否偏慢。"
	}
	if v.LongTail {
		d.FRT += "单次请求的中位耗时与慢请求占比提示可能是偶发长尾，不能只凭均值建议暂停。"
	}
	d.Errors = fmt.Sprintf("同窗共有 %d 次请求尝试，已确认渠道或上游错误 %d 次", v.Requests, v.Errors)
	if v.FaultRate != nil {
		d.Errors += fmt.Sprintf("，占 %.2f%%", *v.FaultRate)
	} else {
		d.Errors += "；归责覆盖未完成或无请求，故障率不可计算"
	}
	d.Errors += fmt.Sprintf("；待判错误 %d 次，其他已明确责任错误 %d 次。", v.Pending, v.KnownOther)
	for i, r := range v.Reasons {
		if i >= 3 {
			break
		}
		d.Errors += fmt.Sprintf("%s：%d 次，责任方%s。", r.Reason, r.Count, r.Responsibility)
	}
	if r := ggFindRule(v, "fault_attention"); r != nil && (r.Hit || r.Active) {
		d.Errors += r.Description
	}
	d.Traffic = fmt.Sprintf("窗口内 %d 次请求尝试，平均每分钟 %.2f 次，峰值每分钟 %d 次；包含失败重试尝试，不是去重后的客户请求数。", v.Requests, v.RPM, v.PeakRPM)
	if v.GrowthPct == nil || v.Baseline.RPM == nil || *v.Baseline.RPM <= 0 {
		d.Traffic += "尚无足够正常数据或正常基准为零，暂不能计算增长。"
	} else {
		d.Traffic += fmt.Sprintf("此前正常基准为每分钟 %.2f 次，变化 %+.1f%%。", *v.Baseline.RPM, *v.GrowthPct)
	}
	if v.Quality == "normal" {
		d.Traffic += "首字耗时和错误情况未恶化，单纯流量增加不需要补渠道。"
	} else if ggActive(v, "capacity_limit") {
		d.Traffic += "同时出现持续上游容量限制，增加渠道的依据见实际触发渠道。"
	} else {
		d.Traffic += "是否需要补渠道还须结合持续延迟和明确容量限制，不能仅按流量增长判断。"
	}
	summary := fmt.Sprintf("最近 %d 分钟有 %d 次请求尝试，平均首字耗时 %s，已确认渠道或上游错误 %d 次。", v.WindowMinutes, v.Requests, ggRuleValue(v.FRT.MeanMs, "ms"), v.Errors)
	d.Label = "继续观察"
	d.Reason = summary
	var actions []ggAction
	for _, a := range report.Actions {
		if ggHasString(v.Actions, a.ID) {
			actions = append(actions, a)
		}
	}
	switch {
	case v.Stale || !v.RequestsComplete:
		d.Label = "暂无法判断"
		d.Reason += "采集落后或窗口覆盖不足，保留已取得的指标。"
	case !v.ConfigCurrent:
		d.Label = "暂无法判断"
		d.Reason += "渠道配置尚未更新，暂不能确认调整范围。"
	case len(actions) > 0:
		d.Label = "建议增加"
		if v.Status == "pause" {
			d.Label = "建议暂停"
		}
		for _, a := range actions {
			d.Reason += a.Reason + "。调整范围：" + strings.Join(a.Scope, "、")
			seen := map[string]bool{}
			var names []string
			for _, t := range a.Triggers {
				name := fmt.Sprintf("#%d %s（%s）", t.ChannelID, t.ChannelName, t.Model)
				if !seen[name] {
					names = append(names, name)
					seen[name] = true
				}
			}
			if len(names) > 0 {
				d.Reason += "；触发依据来自 " + strings.Join(names, "、")
			}
			d.Reason += "。"
		}
	case v.Requests == 0:
		d.Label = "继续观察"
		d.Reason = fmt.Sprintf("最近 %d 分钟无请求，保留当前配置，尚无近期表现可供判断。", v.WindowMinutes)
	case v.Quality == "normal":
		d.Label = "维持现状"
		d.Reason += "首字耗时和错误情况均未恶化，暂不需要增加或暂停渠道。"
		if v.GrowthPct != nil && *v.GrowthPct > 0 {
			d.Reason = fmt.Sprintf("请求量比近期正常水平增加 %.1f%%，但首字耗时和错误情况均未恶化，暂不需要增加或暂停渠道。", *v.GrowthPct)
		}
	case v.Quality == "unknown":
		d.Label = "暂无法判断"
		if v.Baseline.FreezeReason != "" {
			d.Reason += v.Baseline.FreezeReason + "。"
		}
	default:
		for _, r := range v.Rules {
			if r.ID != "pressure" && (r.Hit || r.Active) {
				d.Reason += r.Description
				break
			}
		}
		d.Reason += "仍须核对持续性和可靠承接能力，尚未形成可执行调整事项。"
	}
	if v.ChannelStatus != 1 {
		d.Reason += "当前渠道已停用，保留窗口内历史表现，不重复建议暂停。"
	}
	v.Decision = d
}
