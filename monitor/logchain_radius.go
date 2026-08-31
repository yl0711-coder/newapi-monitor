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

// logChainBlastRadius 影响面汇总。
type logChainBlastRadius struct {
	// Rows 是本次统计覆盖的行数。**它等于当前页行数，不是窗口内总数**——
	// 拿不到真总数（那需要额外一次 COUNT，会再占一次生产库查询与闸门）。
	Rows int `json:"rows"`

	ByChannel  logChainRadiusDim `json:"by_channel"`
	ByCustomer logChainRadiusDim `json:"by_customer"`
	ByDomain   logChainRadiusDim `json:"by_domain"`
	ByModel    logChainRadiusDim `json:"by_model"`

	// Shape 是形状判读结论：single_channel / single_customer / single_domain /
	// single_model / widespread / insufficient。
	Shape string `json:"shape"`
	// ShapeWhy 是判读依据，必须能让人复核。与 fault_why 同一原则：
	// 只给结论不给依据，人就无法判断该不该相信它。
	ShapeWhy string `json:"shape_why"`
}

// logChainRadiusShape 取值。
const (
	radiusSingleChannel  = "single_channel"
	radiusSingleCustomer = "single_customer"
	radiusSingleDomain   = "single_domain"
	radiusSingleModel    = "single_model"
	radiusWidespread     = "widespread"
	radiusInsufficient   = "insufficient"
)

// logChainRadiusMinRows 少于这个行数不做形状判读：两三条数据上的「集中度」
// 没有意义，硬给结论会把偶发当成规律。
const logChainRadiusMinRows = 5

// logChainRadiusDominantPct 单项占比达到此值才算「集中」。
// 取 70% 而非 50%：过半只说明它最多，谈不上集中。
const logChainRadiusDominantPct = 70

// computeLogChainBlastRadius 统计影响面。
//
// 只统计**有问题的行**（type=5 错误，或带异常标签的消费行）：把正常请求
// 算进去会稀释集中度，让所有形状都看起来像 widespread。
func computeLogChainBlastRadius(rows []LogChainRow) logChainBlastRadius {
	type bucket struct {
		count  int
		spread map[string]struct{}
	}
	chans := map[string]*bucket{}
	users := map[string]*bucket{}
	domains := map[string]*bucket{}
	models := map[string]*bucket{}

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

	br := logChainBlastRadius{
		Rows:       problem,
		ByChannel:  top(chans),
		ByCustomer: top(users),
		ByDomain:   top(domains),
		ByModel:    top(models),
	}
	br.Shape, br.ShapeWhy = logChainRadiusShapeOf(problem, len(chans), len(users), br)
	return br
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
