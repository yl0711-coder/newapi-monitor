package monitor

// 客户维护的归因层：把公司今天命中的问题归到 上游 / 我方 / 客户 三档。
//
// ★ 复用既有判据，不另立一套 ★
// 责任方取值用 logchain_fault.go 的 faultUpstream/faultOurs/faultDownstream/
// faultUnknown；判据用同两张表：
//   logChainFaultByStatus       按 HTTP 状态码
//   logChainFaultMessageRules   按错误原文措辞（含 requireUpstream 来源门）
//
// 为什么能直接复用：StabilityProblemSample.Code 就是从原文里提取的
// status_code（见 stabilityProblemCode），Message 是脱敏后的错误原文。
// Code 非空 ⟺ 原文带 status_code= 前缀 ⟺ 这句话是上游说的，
// 这正是 logChainFaultMessageRules 里 requireUpstream 想区分的东西。
//
// 归因粒度的诚实说明：StabilityProblemSample 没有 user_id，所以本页给出的是
// “这家公司失败记录命中的渠道＋模型＋分组上主要在出什么问题”，不是
// “这家公司的这条请求是谁的责任”。
// 置信度因此一律降一档（见 customerHealthDowngradeConfidence）。

import (
	"sort"
	"strconv"
	"strings"
)

// customerHealthFaultVerdict 一次归因结论。
type customerHealthFaultVerdict struct {
	fault      string
	confidence string
	reason     string
}

// customerHealthAttributeProblem 对单条问题签名归因。
//
// 判据顺序与客户排障一致：先按原文措辞（更具体），再按状态码（更泛）。
// 反过来会让"上游说它没有可用渠道"这类明确措辞被 503 那条泛规则吃掉。
func customerHealthAttributeProblem(p customerHealthProblem) customerHealthFaultVerdict {
	fromUpstream := p.code != ""
	for _, rule := range logChainFaultMessageRules {
		if !rule.pattern.MatchString(p.message) {
			continue
		}
		if rule.requireUpstream != nil && *rule.requireUpstream != fromUpstream {
			continue
		}
		return customerHealthFaultVerdict{fault: rule.fault, confidence: rule.confidence, reason: rule.why}
	}
	if status, err := strconv.Atoi(p.code); err == nil {
		if rule, ok := logChainFaultByStatus[status]; ok {
			return customerHealthFaultVerdict{fault: rule.fault, confidence: rule.confidence, reason: rule.why}
		}
	}
	// 认不出来必须落 unknown，不能默认归给任何一方。
	// 默认归我方会让人白查我们自己，默认归上游会让人错误投诉上游。
	return customerHealthFaultVerdict{
		fault: faultUnknown, confidence: faultConfNone,
		reason: "错误原文不匹配任何既有判据，需到「客户排障」看原文",
	}
}

// customerHealthDowngradeConfidence 把逐条判据的置信度降一档。
//
// 原因：那些 confidence 值是为"看着一条请求的原文"设计的。本页看到的是
// 渠道级聚合，无法确认这家公司的失败请求就是这条签名——同渠道上可能同时
// 存在多类问题。声称 high 会让人过度相信一个推断出来的结论。
func customerHealthDowngradeConfidence(in string) string {
	switch in {
	case faultConfHigh:
		return faultConfMid
	case faultConfMid:
		return faultConfLow
	default:
		return in
	}
}

// customerHealthPrimaryModels always returns the most-used model. If two models
// each account for strictly more than 40% of the company's records, it returns
// both. Exact 40% does not qualify; ties fall back to model name for a stable
// single winner when no two models cross the threshold.
func customerHealthPrimaryModels(counts map[string]int64, total int64) []CustomerHealthPrimaryModel {
	if total <= 0 {
		return []CustomerHealthPrimaryModel{}
	}
	normalized := make(map[string]int64, len(counts))
	for name, count := range counts {
		if count <= 0 {
			continue
		}
		name = strings.TrimSpace(name)
		if name == "" {
			name = "未标注模型"
		}
		normalized[name] += count
	}
	all := make([]CustomerHealthPrimaryModel, 0, len(normalized))
	for name, count := range normalized {
		all = append(all, CustomerHealthPrimaryModel{
			Name: name, Requests: count, SharePct: float64(count) * 100 / float64(total),
		})
	}
	if len(all) == 0 {
		return []CustomerHealthPrimaryModel{}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Requests != all[j].Requests {
			return all[i].Requests > all[j].Requests
		}
		return all[i].Name < all[j].Name
	})
	if len(all) >= 2 && all[0].SharePct > 40 && all[1].SharePct > 40 {
		return all[:2]
	}
	return all[:1]
}

// customerHealthPickFault 从公司命中的多条签名里选出主要原因。
//
// 选法：按命中次数加权，取次数最多的那一档责任方；同档内取次数最多的签名
// 作为说明。不按"最严重"选——严重程度是人的判断，次数是事实。
//
// unknown 参与计票但**不作为胜者**（除非只有 unknown）：这一档的存在意义是
// "别瞎猜"，若让它凭数量胜出，页面就永远显示"需人工判断"，等于没归因。
func customerHealthPickFault(problems []customerHealthProblem) customerHealthFaultVerdict {
	if len(problems) == 0 {
		return customerHealthFaultVerdict{fault: "", confidence: "", reason: ""}
	}
	type bucket struct {
		count   int64
		best    customerHealthFaultVerdict
		bestCnt int64
	}
	buckets := map[string]*bucket{}
	for _, p := range problems {
		verdict := customerHealthAttributeProblem(p)
		b := buckets[verdict.fault]
		if b == nil {
			b = &bucket{}
			buckets[verdict.fault] = b
		}
		b.count += p.count
		if p.count > b.bestCnt {
			b.bestCnt, b.best = p.count, verdict
		}
	}
	// 先在确定档里选，unknown 只作兜底。
	ordered := make([]string, 0, len(buckets))
	for fault := range buckets {
		if fault != faultUnknown {
			ordered = append(ordered, fault)
		}
	}
	if len(ordered) == 0 {
		b := buckets[faultUnknown]
		return customerHealthFaultVerdict{
			fault: faultUnknown, confidence: faultConfNone, reason: b.best.reason,
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		a, b := buckets[ordered[i]], buckets[ordered[j]]
		if a.count != b.count {
			return a.count > b.count
		}
		return ordered[i] < ordered[j]
	})
	winner := buckets[ordered[0]]
	return customerHealthFaultVerdict{
		fault:      winner.best.fault,
		confidence: customerHealthDowngradeConfidence(winner.best.confidence),
		reason:     winner.best.reason,
	}
}

// customerHealthChannelScope 比较当前公司和同渠道其他账号的失败记录占比。
//
// 只看该公司出问题最多的那个渠道：多渠道混着比会得出"有的有有的没有"这种
// 无法行动的结论。聚焦单一渠道才能回答"该换渠道还是查这个客户"。
//
// 当前公司失败率比其他账号合计失败率高至少 7 个百分点，才提示问题集中在当前
// 公司；不足 7 个百分点则提示更像渠道共同问题。这样双方稳定率都很高且相近时，
// 不会仅因其他账号失败率未超过某个绝对阈值就误判为当前公司问题。
func customerHealthChannelScope(channelID int, usage *customerHealthUsageIndex, memberIDs map[int64]struct{}) (string, string) {
	users := usage.channelUsers[channelID]
	if len(users) == 0 {
		return customerHealthScopeUnknown, "本地事实里没有该渠道今天的用量，无法比对"
	}
	var mineFailed, mineTotal, others, othersFailed, othersTotal int64
	for userID := range users {
		facts := usage.byUser[userID]
		if facts == nil {
			continue
		}
		ch := facts.channels[channelID]
		if _, mine := memberIDs[userID]; mine {
			mineFailed += ch.healthAnomaly + ch.healthFailed
			mineTotal += ch.success + ch.anomaly + ch.failed
			continue
		}
		others++
		othersFailed += ch.healthAnomaly + ch.healthFailed
		othersTotal += ch.success + ch.anomaly + ch.failed
	}
	if mineTotal == 0 {
		return customerHealthScopeUnknown, "当前公司在该渠道没有可比的日志记录"
	}
	if others == 0 {
		return customerHealthScopeSoleUser, "该渠道今天只有这家公司在用，无法判断是渠道问题还是客户问题"
	}
	if othersTotal == 0 {
		return customerHealthScopeUnknown, "同渠道其他账号今天没有可比的日志记录"
	}
	mineRate := float64(mineFailed) * 100 / float64(mineTotal)
	othersRate := float64(othersFailed) * 100 / float64(othersTotal)
	gap := mineRate - othersRate
	notePrefix := "本公司失败记录占比 " + strconv.FormatFloat(mineRate, 'f', 1, 64) + "%；同渠道另有 " +
		strconv.FormatInt(others, 10) + " 个账号，合计为 " + strconv.FormatFloat(othersRate, 'f', 1, 64) + "%"
	if gap >= customerHealthScopeGapThresholdPct {
		return customerHealthScopeOnlyThis,
			notePrefix + "，本公司高出 " + strconv.FormatFloat(gap, 'f', 1, 64) + " 个百分点——更像该公司问题"
	}
	comparison := "本公司未高于其他账号"
	if gap > 0 {
		comparison = "本公司仅高出 " + strconv.FormatFloat(gap, 'f', 1, 64) + " 个百分点"
	} else if gap < 0 {
		comparison = "本公司低 " + strconv.FormatFloat(-gap, 'f', 1, 64) + " 个百分点"
	}
	return customerHealthScopeShared,
		notePrefix + "，" + comparison + "，差值不足 " +
			strconv.FormatFloat(customerHealthScopeGapThresholdPct, 'f', -1, 64) + " 个百分点——更像渠道问题"
}

// buildCustomerHealthRow 汇总一家公司今天的稳定性与归因。
func buildCustomerHealthRow(company customerHealthCompany, usage *customerHealthUsageIndex,
	problems *customerHealthProblemIndex) CustomerHealthRow {
	row := CustomerHealthRow{
		GroupID: company.groupID, Company: company.name, Members: company.members,
		MetricsReady: true,
	}
	memberIDs := make(map[int64]struct{}, len(company.members))
	// worstChannel 记该公司“计入稳定性的问题”最多的渠道，用于同渠道横向比较。
	var worstChannel int
	var worstBad, worstHealthAnomaly, worstHealthFailed int64
	badByChannel := map[int]int64{}
	modelCounts := map[string]int64{}
	for _, member := range company.members {
		memberIDs[member.UserID] = struct{}{}
		facts := usage.byUser[member.UserID]
		if facts == nil {
			continue
		}
		row.Success += facts.success
		row.Anomaly += facts.anomaly
		row.Failed += facts.failed
		row.StabilityAnomaly += facts.healthAnomaly
		row.StabilityFailed += facts.healthFailed
		for model, count := range facts.models {
			modelCounts[model] += count
		}
		for channelID, ch := range facts.channels {
			badByChannel[channelID] += ch.healthAnomaly + ch.healthFailed
		}
	}
	for channelID, bad := range badByChannel {
		if bad > worstBad || (bad == worstBad && bad > 0 && (worstChannel == 0 || channelID < worstChannel)) {
			worstBad, worstChannel = bad, channelID
		}
	}
	row.Total = row.Success + row.Anomaly + row.Failed
	row.StabilityPct = customerHealthStability(row.Total, row.StabilityAnomaly, row.StabilityFailed)
	row.PrimaryModels = customerHealthPrimaryModels(modelCounts, row.Total)

	// 今天没有任何请求：不给归因也不给横向比较。
	// 这一档必须与"有请求且全部成功"区分开——前者是没数据，后者是真健康。
	if row.Total == 0 {
		row.Fault, row.FaultConfidence = "", ""
		row.Reason = "今天还没有已进入渠道的请求"
		row.ChannelScope, row.ScopeNote = customerHealthScopeUnknown, "无请求，无可比对"
		return row
	}
	if worstBad == 0 {
		row.Fault, row.FaultConfidence = "", ""
		row.Reason = "今天没有计入稳定性的异常或错误"
		row.ChannelScope, row.ScopeNote = customerHealthScopeHealthy, "该公司今天没有影响稳定性的责任问题，无需比对"
		return row
	}
	for _, member := range company.members {
		if facts := usage.byUser[member.UserID]; facts != nil {
			ch := facts.channels[worstChannel]
			worstHealthAnomaly += ch.healthAnomaly
			worstHealthFailed += ch.healthFailed
		}
	}
	// 只接受该公司在最差渠道上真实出现过 type=5 的模型＋分组。若只有异常
	// type=2 而没有错误原文，保持待判；绝不借同渠道其它客户的问题签名定责。
	failedScopes := map[customerHealthProblemScope]struct{}{}
	for _, member := range company.members {
		if facts := usage.byUser[member.UserID]; facts != nil {
			for scope, count := range facts.channels[worstChannel].failedScopes {
				if count > 0 {
					failedScopes[scope] = struct{}{}
				}
			}
		}
	}
	matched := make([]customerHealthProblem, 0)
	for _, problem := range problems.byChannel[worstChannel] {
		if _, ok := failedScopes[customerHealthProblemScope{modelName: problem.modelName, grp: problem.grp}]; ok &&
			customerHealthAttributeProblem(problem).fault != faultDownstream {
			matched = append(matched, problem)
		}
	}
	verdict := customerHealthPickFault(matched)
	if worstHealthFailed == 0 && worstHealthAnomaly > 0 {
		// The only anomaly class admitted by the new stability policy is a real
		// stream failure, whose shared logchain rule attributes it upstream.
		verdict = customerHealthFaultVerdict{
			fault: faultUpstream, confidence: faultConfLow,
			reason: "流传输异常结束，按客户排障同一判据归为上游问题",
		}
	}
	if verdict.fault == "" {
		// 有失败计数但本地问题签名里没有对应记录。
		// 常见原因：错误采集（StabilityProblemSample）未开启或落后于用量采集。
		// 这种情况必须说清是"证据缺失"，不能显示成"没问题"。
		row.Fault, row.FaultConfidence = faultUnknown, faultConfNone
		row.Reason = "有计入稳定性的异常或错误，但未采到该公司同渠道＋模型＋分组的对应错误原文，暂不定责"
	} else {
		row.Fault, row.FaultConfidence, row.Reason = verdict.fault, verdict.confidence, verdict.reason
	}
	row.ChannelScope, row.ScopeNote = customerHealthChannelScope(worstChannel, usage, memberIDs)
	return row
}
