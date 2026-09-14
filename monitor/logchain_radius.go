package monitor

// logchain_radius.go：影响面判别（「看范围」层）。
//
// 出处：docs/Monitor｜稳定性与上游观察.md §管理端应重点查看什么 要求三步走——
//   先看新鲜度和健康 → **再看范围** → 最后看证据
// 其中「看范围」定义为：「通过时间、分组、模型和渠道逐层缩小异常面，
// 区分单模型、单渠道、单上游域名和全局问题」。
//
// 逐行归因（logchain_fault.go）做的是第三步「看证据」：它只看本行的状态码、
// 原文语义、首字延迟等**本行自带的事实**。本文件做第二步「看范围」：
// 它要跨行统计，回答「这批问题集中在谁身上」。
//
// ★ 为什么不把影响面折进逐行 fault ★
//
// 逐行 fault 是纯函数，同一条请求在任何页面、任何筛选下结论都一样。
// 若把「同渠道涉及几个客户」折进去，同一条请求会因为翻页位置或筛选条件不同
// 而得到不同责任方——那种不稳定的结论比没有结论更糟。
// 因此影响面单独成一个汇总信号，且**必须标明它的统计范围只是当前这一页**。

import (
	"sort"
	"strconv"
	"strings"
)

// 本文件大量拼接依据文本，包内没有现成的短别名，就地定义两个，
// 避免每处都写 strconv.FormatInt(x, 10) 把判读逻辑淹在噪声里。
func lcNum(v int) string     { return strconv.Itoa(v) }
func lcNum64(v int64) string { return strconv.FormatInt(v, 10) }

// logChainRadiusMaxItems 每个维度最多返回几项。前端只用来看集中度，
// 给太多反而看不出形状；超出的部分以 OtherItems / OtherCount 汇总告知，
// 不静默丢弃——见 logChainRadiusDim 的说明。
const logChainRadiusMaxItems = 5

// logChainRadiusItem 一个维度上的一项及其覆盖面。
type logChainRadiusItem struct {
	Key   string `json:"key"`   // 渠道 ID+名 / 客户 / 上游域名 / 模型
	Count int    `json:"count"` // 该项的问题行数
	// Spread 是「另一维度」的去重数量，用来判读形状：
	//   按渠道看时 = 受影响的客户数（多客户 → 指向上游）
	//   按客户看时 = 涉及的渠道数（跨多渠道 → 指向该客户侧）
	Spread int `json:"spread"`
}

// logChainRadiusDim 一个维度的 Top-N 及被截断部分的汇总。
//
// ★ 为什么不能只给 Top-N ★
//
// 只给前 5 项时，页面上那张表看起来就像「问题只涉及这 5 个渠道」。
// 而形状判读用的是**全量** map（chanCount / userCount 都是 len(m)，不受截断影响），
// 于是会出现「结论说分散在 12 个渠道，表里只有 5 行」这种对不上的场面。
// 与 usage.go 的 ByModelTruncated 同一原则：截断必须显式标记。
// 这里比 bool 多给两个数，因为「其余 7 项共 9 条」和「其余 7 项共 300 条」
// 对判读的意义完全不同——后者说明 Top-N 根本没覆盖住主体。
type logChainRadiusDim struct {
	Items []logChainRadiusItem `json:"items,omitempty"`
	// OtherItems 被截断掉的项数。0 表示没有截断。
	OtherItems int `json:"other_items,omitempty"`
	// OtherCount 被截断那些项的问题行数合计。
	// 注意没有对应的 Spread 汇总：Spread 是去重计数，跨项相加会重复计数，
	// 给一个偏大的假数字比不给更糟。
	OtherCount int `json:"other_count,omitempty"`
}

// logChainReasonItem 是当前页一个互斥主原因。每条问题只进入一个原因，
// 因此所有原因 Count（含 other_count）之和必须等于 blast_radius.rows。
type logChainReasonItem struct {
	Reason    string `json:"reason"`
	Count     int    `json:"count"`
	Customers int    `json:"customers"`
	Channels  int    `json:"channels"`
}

type logChainReasonSummary struct {
	Items      []logChainReasonItem `json:"items,omitempty"`
	OtherItems int                  `json:"other_items,omitempty"`
	OtherCount int                  `json:"other_count,omitempty"`
}

// logChainFaultCount 聚合已有逐行责任推断。Fault=unknown 包含无依据和待判；
// 页面必须继续写“疑似责任方”，不能把该分布当成事实。
type logChainFaultCount struct {
	Fault string `json:"fault"`
	Count int    `json:"count"`
}

// logChainBlastRadius 影响面/筛选原因汇总。
type logChainBlastRadius struct {
	// Mode 由后端根据已校验的实际 scope 推导：overview 看跨维度集中度，
	// focused 看筛选结果的原因构成。前端不得自行声明模式。
	Mode string `json:"mode"`
	// Rows 是本次统计覆盖的问题行数。它等于当前页问题行数，不是窗口内总数。
	Rows int `json:"rows"`
	// PageHasMore=true 表示筛选范围内还有未进入本页的记录；此时任何原因结论
	// 都只能描述当前页，不能冒充整个筛选范围。
	PageHasMore bool `json:"page_has_more"`

	ByChannel  logChainRadiusDim     `json:"by_channel"`
	ByCustomer logChainRadiusDim     `json:"by_customer"`
	ByDomain   logChainRadiusDim     `json:"by_domain"`
	ByModel    logChainRadiusDim     `json:"by_model"`
	Reasons    logChainReasonSummary `json:"reasons"`
	Faults     []logChainFaultCount  `json:"faults,omitempty"`

	// Shape / ShapeWhy 只供 overview 的通用影响面判读。
	Shape    string `json:"shape"`
	ShapeWhy string `json:"shape_why"`
	// ReasonShape / ReasonWhy 只供 focused 的筛选原因判读。它描述当前返回记录
	// 的表现，不把日志现象自动升级为已经证实的根因。
	ReasonShape string `json:"reason_shape,omitempty"`
	ReasonWhy   string `json:"reason_why,omitempty"`
}

const (
	logChainRadiusModeOverview = "overview"
	logChainRadiusModeFocused  = "focused"
)

// logChainRadiusShape 取值。
const (
	radiusSingleChannel  = "single_channel"
	radiusSingleCustomer = "single_customer"
	radiusSingleDomain   = "single_domain"
	radiusSingleModel    = "single_model"
	radiusWidespread     = "widespread"
	radiusInsufficient   = "insufficient"

	reasonShapeSmall       = "small_sample"
	reasonShapeDominant    = "dominant"
	reasonShapeDual        = "dual"
	reasonShapeDistributed = "distributed"
)

// logChainRadiusMinRows 少于这个行数不做形状判读：两三条数据上的「集中度」
// 没有意义，硬给结论会把偶发当成规律。
const logChainRadiusMinRows = 5

// logChainRadiusDominantPct 单项占比达到此值才算「集中」。
// 取 70% 而非 50%：过半只说明它最多，谈不上集中。
const logChainRadiusDominantPct = 70

// logChainPrimaryReason 为每条问题生成唯一主原因，保证原因分布互斥且可加总。
// 错误摘要复用稳定性问题签名的脱敏/规范化边界，不把凭据或长标识扩散进汇总。
func logChainPrimaryReason(r LogChainRow) string {
	if r.Type == 5 {
		code := strings.TrimSpace(r.UpstreamErrorCode)
		if code == "" && r.UpstreamStatusCode > 0 {
			code = "HTTP " + strconv.Itoa(r.UpstreamStatusCode)
		}
		if code == "" {
			code = stabilityProblemCode(r.Content)
			if code != "" {
				code = "HTTP " + code
			}
		}
		if code == "" {
			code = "无明确错误码"
		}
		message, _ := stabilityProblemText(r.Content)
		if message == "" {
			message = "无错误原文"
		}
		return code + " · " + message
	}
	labels := make([]string, 0, len(r.AnomalyTags))
	for _, tag := range r.AnomalyTags {
		switch tag {
		case "stream":
			value := strings.TrimSpace(r.EndReason)
			if value == "" {
				value = "error_count>0"
			}
			labels = append(labels, "流故障("+value+")")
		case logChainClientGoneEndReason:
			labels = append(labels, "客户端断连")
		case "billing_unpaid":
			labels = append(labels, "扣费未交付")
		case anomalyUndeliveredUnbilled:
			labels = append(labels, "未交付·未扣费")
		case "billing_free":
			labels = append(labels, "交付未扣费")
		default:
			labels = append(labels, tag)
		}
	}
	if len(labels) == 0 {
		return "未分类问题"
	}
	return strings.Join(labels, " + ")
}

// computeLogChainBlastRadius 保留既有默认行为，供纯计算测试和其它包内调用使用。
// HTTP handler 会走 computeLogChainBlastRadiusForScope，由实际生效的筛选决定模式。
func computeLogChainBlastRadius(rows []LogChainRow) logChainBlastRadius {
	return computeLogChainBlastRadiusMode(rows, logChainRadiusModeOverview, false)
}

func computeLogChainBlastRadiusForScope(rows []LogChainRow, scope logChainScope, hasMore bool) logChainBlastRadius {
	mode := logChainRadiusModeOverview
	if logChainScopeIsFocused(scope) {
		mode = logChainRadiusModeFocused
	}
	return computeLogChainBlastRadiusMode(rows, mode, hasMore)
}

// computeLogChainBlastRadiusMode 只统计有问题的行（type=5 错误，或带异常标签的消费行）。
// 把正常请求算进去会稀释集中度和原因占比。
func computeLogChainBlastRadiusMode(rows []LogChainRow, mode string, hasMore bool) logChainBlastRadius {
	type bucket struct {
		count  int
		spread map[string]struct{}
	}
	type reasonBucket struct {
		count     int
		customers map[string]struct{}
		channels  map[string]struct{}
	}
	chans := map[string]*bucket{}
	users := map[string]*bucket{}
	domains := map[string]*bucket{}
	models := map[string]*bucket{}
	reasons := map[string]*reasonBucket{}
	faults := map[string]int{}

	add := func(m map[string]*bucket, key, spreadKey string) {
		if key == "" {
			return
		}
		b := m[key]
		if b == nil {
			b = &bucket{spread: map[string]struct{}{}}
			m[key] = b
		}
		b.count++
		if spreadKey != "" {
			b.spread[spreadKey] = struct{}{}
		}
	}

	problem := 0
	for _, r := range rows {
		if r.Type != 5 && len(r.AnomalyTags) == 0 {
			continue // 正常消费行不参与影响面统计
		}
		problem++
		chanKey := logChainRadiusChannelKey(r)
		userKey := logChainRadiusCustomerKey(r)
		add(chans, chanKey, userKey)
		add(users, userKey, chanKey)
		add(domains, r.UpstreamDomain, userKey)
		add(models, r.ModelName, userKey)

		reason := logChainPrimaryReason(r)
		rb := reasons[reason]
		if rb == nil {
			rb = &reasonBucket{customers: map[string]struct{}{}, channels: map[string]struct{}{}}
			reasons[reason] = rb
		}
		rb.count++
		if userKey != "" {
			rb.customers[userKey] = struct{}{}
		}
		if chanKey != "" {
			rb.channels[chanKey] = struct{}{}
		}
		fault := r.Fault
		switch fault {
		case faultUpstream, faultOurs, faultDownstream, faultUnknown:
		default:
			fault = faultUnknown
		}
		faults[fault]++
	}

	top := func(m map[string]*bucket) logChainRadiusDim {
		out := make([]logChainRadiusItem, 0, len(m))
		for k, b := range m {
			out = append(out, logChainRadiusItem{Key: k, Count: b.count, Spread: len(b.spread)})
		}
		// 按数量降序；数量相同按 key 升序，保证输出稳定可测。
		sort.Slice(out, func(i, j int) bool {
			if out[i].Count != out[j].Count {
				return out[i].Count > out[j].Count
			}
			return out[i].Key < out[j].Key
		})
		dim := logChainRadiusDim{Items: out}
		if len(out) > logChainRadiusMaxItems {
			// 先把要丢的那批数清出来，再截断。顺序反了就数不到了。
			for _, it := range out[logChainRadiusMaxItems:] {
				dim.OtherCount += it.Count
			}
			dim.OtherItems = len(out) - logChainRadiusMaxItems
			dim.Items = out[:logChainRadiusMaxItems]
		}
		return dim
	}

	reasonItems := make([]logChainReasonItem, 0, len(reasons))
	for reason, b := range reasons {
		reasonItems = append(reasonItems, logChainReasonItem{
			Reason: reason, Count: b.count, Customers: len(b.customers), Channels: len(b.channels),
		})
	}
	sort.Slice(reasonItems, func(i, j int) bool {
		if reasonItems[i].Count != reasonItems[j].Count {
			return reasonItems[i].Count > reasonItems[j].Count
		}
		return reasonItems[i].Reason < reasonItems[j].Reason
	})
	reasonSummary := logChainReasonSummary{Items: reasonItems}
	if len(reasonItems) > logChainRadiusMaxItems {
		for _, it := range reasonItems[logChainRadiusMaxItems:] {
			reasonSummary.OtherCount += it.Count
		}
		reasonSummary.OtherItems = len(reasonItems) - logChainRadiusMaxItems
		reasonSummary.Items = reasonItems[:logChainRadiusMaxItems]
	}
	faultSummary := make([]logChainFaultCount, 0, 4)
	for _, fault := range []string{faultUpstream, faultOurs, faultDownstream, faultUnknown} {
		if count := faults[fault]; count > 0 {
			faultSummary = append(faultSummary, logChainFaultCount{Fault: fault, Count: count})
		}
	}

	br := logChainBlastRadius{
		Mode:        mode,
		Rows:        problem,
		PageHasMore: hasMore,
		ByChannel:   top(chans),
		ByCustomer:  top(users),
		ByDomain:    top(domains),
		ByModel:     top(models),
		Reasons:     reasonSummary,
		Faults:      faultSummary,
	}
	if mode == logChainRadiusModeFocused {
		br.ReasonShape, br.ReasonWhy = logChainFocusedReasonOf(problem, reasonItems, hasMore)
	} else {
		br.Shape, br.ShapeWhy = logChainRadiusShapeOf(problem, len(chans), len(users), br)
	}
	return br
}

// logChainFocusedReasonOf 分析已经被筛选条件缩小后的问题原因构成。
// 这里分析的是本次查询已经返回的记录，不额外查询生产库；hasMore 为真时必须
// 明说后面还有记录，不能把当前页比例冒充整个筛选范围。
func logChainFocusedReasonOf(rows int, reasons []logChainReasonItem, hasMore bool) (string, string) {
	coverage := "当前筛选结果已全部返回"
	if hasMore {
		coverage = "仅分析当前页 " + lcNum(rows) + " 条问题，筛选结果仍有更多记录"
	}
	if rows == 0 || len(reasons) == 0 {
		return reasonShapeSmall, "当前返回结果中没有可归类的问题记录；" + coverage
	}
	pct := func(n int) int { return n * 100 / rows }
	first := reasons[0]
	firstText := first.Reason + "（" + lcNum(first.Count) + " 条，占 " + lcNum(pct(first.Count)) + "%）"
	if rows < logChainRadiusMinRows {
		return reasonShapeSmall, "当前记录较少，主要表现为 " + firstText +
			"；这里只描述已返回记录，不外推长期主因；" + coverage
	}
	if first.Count*100 >= rows*50 {
		return reasonShapeDominant, "当前返回问题以 " + firstText +
			" 为主要表现；这是日志原因构成，不代表根因已经证实；" + coverage
	}
	if len(reasons) >= 2 {
		second := reasons[1]
		combined := first.Count + second.Count
		if combined*100 >= rows*70 {
			return reasonShapeDual, "当前返回问题主要由两类表现构成：" + firstText + "；" +
				second.Reason + "（" + lcNum(second.Count) + " 条，占 " + lcNum(pct(second.Count)) + "%）" +
				"；两类合计占 " + lcNum(pct(combined)) + "%；这不是已经证实的根因；" + coverage
		}
	}
	return reasonShapeDistributed, "当前返回问题的原因较分散，最高项为 " + firstText +
		"，未形成明确主因；请结合下方原因分布和逐行证据复核；" + coverage
}

// logChainRadiusChannelKey 渠道标识。带上名字便于人直接读懂，
// 但以 ID 打头保证唯一（渠道可以同名，历史上出现过带 Tab 的名字）。
func logChainRadiusChannelKey(r LogChainRow) string {
	if r.ChannelID == 0 {
		return "" // 未打到渠道的行不计入渠道维度
	}
	k := "#" + lcNum64(r.ChannelID)
	if r.ChannelName != "" {
		k += " " + r.ChannelName
	}
	return k
}

// logChainRadiusCustomerKey 客户标识。优先用户名，回落到 user_id——
// 排障要能一眼认出是谁，纯数字 ID 认不出来。
func logChainRadiusCustomerKey(r LogChainRow) string {
	if r.Member != "" {
		return r.Member
	}
	if r.UserID != 0 {
		return "ID " + lcNum64(r.UserID)
	}
	return ""
}

// logChainRadiusShapeOf 判读形状。
//
// 判读顺序即优先级，不可随意调换：
//  1. 样本不足 → 不判（少数几条上的集中度没有意义）
//  2. 单渠道集中且跨多客户 → 该渠道/上游的问题（最有行动价值的结论）
//  3. 单客户集中且跨多渠道 → 该客户侧的问题
//  4. 单上游域名集中 → 该上游整站问题
//  5. 都不集中 → 全局
//
// 第 2、3 条的**关键是 Spread**：只看「哪个渠道条数最多」会被长尾渠道误导——
// 某渠道只有一个客户在用时，它的错误天然全部来自那一个客户，
// 形状上无法区分「渠道坏了」与「那个客户在做异常请求」。
// 因此单渠道集中必须同时满足「跨多个客户」才判为渠道问题。
func logChainRadiusShapeOf(rows, chanCount, userCount int, br logChainBlastRadius) (string, string) {
	if rows < logChainRadiusMinRows {
		return radiusInsufficient,
			"本页仅 " + lcNum(rows) + " 条问题记录，不足以判读集中度（阈值 " + lcNum(logChainRadiusMinRows) + " 条）"
	}
	pct := func(n int) int { return n * 100 / rows }

	// 单渠道集中 + 跨多客户 → 渠道/上游问题。
	if len(br.ByChannel.Items) > 0 {
		c := br.ByChannel.Items[0]
		if pct(c.Count) >= logChainRadiusDominantPct {
			if c.Spread >= 2 {
				return radiusSingleChannel,
					"本页 " + lcNum(pct(c.Count)) + "% 的问题集中在渠道 " + c.Key +
						"，且影响 " + lcNum(c.Spread) + " 个客户——多客户同时中招，指向该渠道或其上游"
			}
			// 只有一个客户在用：形状无判别力，如实说明而不是硬给结论。
			return radiusInsufficient,
				"问题集中在渠道 " + c.Key + "，但该渠道本页只有 1 个客户在用（长尾渠道）" +
					"，无法区分渠道故障与该客户的异常请求——请看逐行的责任方与错误原文"
		}
	}

	// 单客户集中 + 跨多渠道 → 客户侧问题。
	if len(br.ByCustomer.Items) > 0 {
		u := br.ByCustomer.Items[0]
		if pct(u.Count) >= logChainRadiusDominantPct && u.Spread >= 2 {
			return radiusSingleCustomer,
				"本页 " + lcNum(pct(u.Count)) + "% 的问题集中在客户 " + u.Key +
					"，且跨 " + lcNum(u.Spread) + " 个渠道都失败——换渠道仍失败，指向该客户侧"
		}
	}

	// 单上游域名集中 → 该上游整站问题。
	if len(br.ByDomain.Items) > 0 {
		d := br.ByDomain.Items[0]
		if pct(d.Count) >= logChainRadiusDominantPct && chanCount >= 2 {
			return radiusSingleDomain,
				"本页 " + lcNum(pct(d.Count)) + "% 的问题集中在上游 " + d.Key +
					"，跨该域名下 " + lcNum(chanCount) + " 个渠道——指向上游整站而非单个账号"
		}
	}

	// 单模型集中 → 该模型的问题（上游不支持、或我方映射配置错）。
	if len(br.ByModel.Items) > 0 {
		mo := br.ByModel.Items[0]
		if pct(mo.Count) >= logChainRadiusDominantPct && chanCount >= 2 {
			return radiusSingleModel,
				"本页 " + lcNum(pct(mo.Count)) + "% 的问题集中在模型 " + mo.Key +
					"，跨 " + lcNum(chanCount) + " 个渠道——指向该模型的上游支持或我方模型映射"
		}
	}

	return radiusWidespread,
		"问题分散在 " + lcNum(chanCount) + " 个渠道、" + lcNum(userCount) +
			" 个客户上，无单点集中——可能是全局问题，也可能本页混了多个互不相关的故障"
}
