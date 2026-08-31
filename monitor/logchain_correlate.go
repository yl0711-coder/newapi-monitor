package monitor

// logchain_correlate.go：把我方错误行与上游错误日志串起来。
//
// ★★ 它输出的是「证据」还是「推断」，取决于用了哪条路径 ★★
//
// 两条路径的证据强度差一个量级，**页面上绝不能显示成一样**：
//
//	exact     双方 content 里嵌的同一个模型商 id 相等 → 同一请求，铁证
//	probable  上游渠道 + 状态码 + 时间窗内唯一 → 高置信推断，不是铁证
//	ambiguous 回退键落在多义桶里 → 只列候选，不下结论
//
// 判成 exact 而实际不是同一请求，会让人拿着别的请求的上游日志解释眼前的故障——
// 那比不给结论更糟。所以 exact 只在键完全相等时给，没有任何模糊匹配。
//
// ★ 为什么串联键是「双方嵌的 id」而不是上游的 request_id ★
//
// 2026-08-28 实测（kpzhu.com，我方渠道 #66，跨 4 天）：
//
//	上游 request_id 字段 ↔ 我方嵌的 id →   1 / 486 命中（≈0）
//	上游嵌的 id         ↔ 我方嵌的 id → 152 命中
//
// 错误体逐层透传：模型商生成 P → 上游记自己的 request_id K 但 content 里带 P
// → 我方记自己的 O 但 content 里还是 P。能对上的是 P ↔ P。
// 详见 ChannelUpstreamErrorLog.JoinKey 的注释。

import (
	"context"
	"log/slog"
	"sort"
	"strconv"
	"strings"
)

// 关联置信度取值。闭集。
const (
	correlateExact     = "exact"     // 串联键相等，同一请求
	correlateProbable  = "probable"  // 回退键在窗口内唯一
	correlateAmbiguous = "ambiguous" // 回退键有多个候选，不下结论
	correlateNone      = "none"      // 找不到对应
	// correlateNotApplicable 上游**本来就不会有**对应记录，不是采集缺失。
	//
	// ★ 为什么必须与 none 分开 ★
	// none 是「上游应该有、但我们没找到」——可能是采集漏了、可能是时钟偏差。
	// 这一档是「上游自身压根没记这次失败」，改任何关联逻辑都不会有结果。
	// 两者都显示成"无对应上游日志"会让人反复去查采集，而那是白费功夫。
	correlateNotApplicable = "not_applicable"
)

// correlateFallbackWindow 回退匹配的时间窗（秒）。
//
// 取 10 秒而不是 1 秒：两侧是不同机器，时钟必有偏差，1 秒会把本该匹配的错开。
// 也不敢取更大：实测「上游渠道+状态码」在 1 秒窗内 90% 唯一、10 秒窗 82% 唯一，
// 再放宽唯一性会继续掉，而唯一性一掉，probable 就名不副实了。
const correlateFallbackWindow = 10

// LogChainUpstreamMatch 一条我方错误行的上游对应。
type LogChainUpstreamMatch struct {
	// Confidence 是 correlateExact / Probable / Ambiguous / None 之一。
	// **前端必须按它分档显示**，不能把 probable 画成 exact。
	Confidence string `json:"confidence"`
	// Why 说明凭什么这么判，供人复核。
	Why string `json:"why"`

	// 命中时的上游侧事实（只在 exact / probable 时有值）。
	UpstreamChannelName string `json:"upstream_channel_name,omitempty"`
	UpstreamStatusCode  int64  `json:"upstream_status_code,omitempty"`
	UpstreamErrorCode   string `json:"upstream_error_code,omitempty"`
	UpstreamErrorType   string `json:"upstream_error_type,omitempty"`
	UpstreamContent     string `json:"upstream_content,omitempty"`
	UpstreamCreatedAt   int64  `json:"upstream_created_at,omitempty"`
	UpstreamUseTime     int64  `json:"upstream_use_time,omitempty"`

	// CandidateCount 在 ambiguous 时给出候选条数，让人知道模糊到什么程度。
	CandidateCount int `json:"candidate_count,omitempty"`
	// CandidateChannels 在 ambiguous 时列出候选涉及的上游渠道（去重、逗号分隔）。
	//
	// ★ 这是 ambiguous 档唯一有信息量的东西 ★
	// 具体是哪一条不能给（给了就是猜），但「这批候选集中在上游同一个渠道」
	// 与「散在多个渠道」含义完全不同：前者指向那个渠道在批量出错，
	// 后者指向上游整体。只报条数会把这个区别丢掉。
	CandidateChannels string `json:"candidate_channels,omitempty"`
}

// correlateUpstreamErrors 给一页我方错误行批量找上游对应。
//
// ★ 批量而不是逐行查 ★
// 一页最多 200 行，逐行两次查询就是 400 次往返。这里先按串联键批量捞一次，
// 再按回退键批量捞一次，全程 2 次查询。上游日志表在本地 SQLite，
// 但本函数与排障主查询同在一个请求里，多一次往返都会加到用户等待上。
//
// 只处理 type=5：正常消费行没有"上游错误"可对。
// domain 为空的行跳过——没有上游域名就无从查起（渠道快照缺失时会这样）。
func (m *Monitor) correlateUpstreamErrors(ctx context.Context, rows []LogChainRow) map[int64]LogChainUpstreamMatch {
	out := map[int64]LogChainUpstreamMatch{}
	if len(rows) == 0 || m.storeDB == nil {
		return out
	}

	// 收集待匹配的行：有串联键的走精确路径，其余留给回退路径。
	keyed := map[string][]int64{}
	var fallback []LogChainRow
	for _, r := range rows {
		if r.Type != 5 || r.UpstreamDomain == "" {
			continue
		}
		if k := logChainJoinKeyFrom(r.Content); k != "" {
			keyed[k] = append(keyed[k], r.ID)
			continue
		}
		fallback = append(fallback, r)
	}

	// 第一趟：串联键精确匹配。
	if len(keyed) > 0 {
		m.matchByJoinKey(ctx, keyed, out)
	}
	// 第二趟：串联键没命中的，走回退键。
	var leftover []LogChainRow
	for _, r := range fallback {
		if _, done := out[r.ID]; !done {
			leftover = append(leftover, r)
		}
	}
	if len(leftover) > 0 {
		m.matchByFallback(ctx, leftover, out)
	}
	// 兜底：两趟都没给结论的行也要有档位。
	//
	// ★ 不补这一步会留下静默空白 ★
	// 带串联键但上游没有对应记录的行，matchByJoinKey 里是 continue 跳过的，
	// 又不在 fallback 列表里（它们进了 keyed），于是 out 里根本没有条目，
	// 前端什么都不显示——与「采集没开」表现一致，无法区分。
	for _, r := range rows {
		if r.Type != 5 || r.UpstreamDomain == "" {
			continue
		}
		if _, done := out[r.ID]; !done {
			out[r.ID] = noMatchFor(r)
		}
	}
	return out
}

// logChainCDNStatusCodes 由上游**前置 CDN** 产生、上游自身不会记录的状态码。
//
// ★ 判据来源：2026-08-28 生产实测 ★
// 我方一条 08-26 23:52 的 524（渠道 #66 kpzhu）在上游日志里找不到对应。
// 查上游那 507 条日志的状态码分布：503×542、404×230、429×74、500×48、
// 502×44、403×20、504×10、400×4 —— **524 一条都没有**。
//
// 原因：524 是 Cloudflare 的「源站未及时响应」，由 CDN 在上游应用**之前**
// 就返回给我方了。上游的 new-api 从未看到这次请求完成，自然没有日志。
//
// 5xx 里只列 Cloudflare 专有段（520~526）：502/503/504 是标准 HTTP，
// 上游自己也会产生并记录（实测它记了 503×542、502×44、504×10），
// 把那些列进来会把「上游确实记了、只是我们没找到」误判成「本来就没有」。
var logChainCDNStatusCodes = map[int]bool{
	520: true, // Web Server Returned an Unknown Error
	521: true, // Web Server Is Down
	522: true, // Connection Timed Out
	523: true, // Origin Is Unreachable
	524: true, // A Timeout Occurred —— 实测遇到的就是这个
	525: true, // SSL Handshake Failed
	526: true, // Invalid SSL Certificate
}

// matchByJoinKey 精确路径：串联键相等即同一请求。
//
// 不限定域名：串联键是最深层模型商生成的，全局唯一，加域名条件只会在
// 渠道快照与上游域名对不上时误杀。键相等本身就是足够强的证据。
func (m *Monitor) matchByJoinKey(ctx context.Context, keyed map[string][]int64, out map[int64]LogChainUpstreamMatch) {
	keys := make([]string, 0, len(keyed))
	for k := range keyed {
		keys = append(keys, k)
	}
	var hits []ChannelUpstreamErrorLog
	if err := m.storeDB.WithContext(ctx).
		Where("join_key IN ?", keys).
		Find(&hits).Error; err != nil {
		// 关联失败不能拖垮排障主流程：明细已经查到了，少一列关联信息
		// 仍然可用。记日志、返回空，让前端显示"未关联"。
		slog.Warn("上游错误日志关联失败，排障明细不受影响", "err", err)
		return
	}
	byKey := map[string]ChannelUpstreamErrorLog{}
	for _, h := range hits {
		// 同一个键理论上只有一条。真出现多条时取先到的那条并不重要——
		// 它们描述的是同一个模型商请求，上游侧重复记录不影响我方判读。
		if _, ok := byKey[h.JoinKey]; !ok {
			byKey[h.JoinKey] = h
		}
	}
	for k, ids := range keyed {
		h, ok := byKey[k]
		if !ok {
			continue
		}
		for _, id := range ids {
			out[id] = LogChainUpstreamMatch{
				Confidence:          correlateExact,
				Why:                 "双方错误原文里嵌着同一个模型商 request id，是同一请求",
				UpstreamChannelName: h.UpstreamChannelName,
				UpstreamStatusCode:  h.StatusCode,
				UpstreamErrorCode:   h.ErrorCode,
				UpstreamErrorType:   h.ErrorType,
				UpstreamContent:     h.Content,
				UpstreamCreatedAt:   h.CreatedAt,
				UpstreamUseTime:     h.UseTime,
			}
		}
	}
}

// distinctUpstreamChannels 取候选里去重后的上游渠道名，排序后逗号分隔。
//
// 排序是为了输出稳定可测——不排的话同一批候选每次顺序都可能不同，
// 页面上那行字会无端变化，看的人会以为数据在变。
//
// 上限 5 个：超过就说明散得很开，列全了也没有判读价值，反而挤爆那一行。
func distinctUpstreamChannels(cands []ChannelUpstreamErrorLog) string {
	seen := map[string]bool{}
	var names []string
	for _, c := range cands {
		n := strings.TrimSpace(c.UpstreamChannelName)
		if n == "" || seen[n] {
			continue
		}
		seen[n] = true
		names = append(names, n)
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	const max = 5
	more := ""
	if len(names) > max {
		more = " 等 " + strconv.Itoa(len(names)-max) + " 个"
		names = names[:max]
	}
	return strings.Join(names, ", ") + more
}

// noMatchFor 给「没找到对应」的行判一个档：是真没找到，还是本来就不会有。
//
// ★ 这个区分决定人要不要去查采集 ★
// 标 none 会让人怀疑采集漏了、时钟偏了，跑去核对采集状态；
// 而 CDN 系状态码是**上游自身没有记录**，那趟核对必然白跑。
func noMatchFor(r LogChainRow) LogChainUpstreamMatch {
	if logChainCDNStatusCodes[r.UpstreamStatusCode] {
		return LogChainUpstreamMatch{
			Confidence: correlateNotApplicable,
			Why: "HTTP " + strconv.Itoa(r.UpstreamStatusCode) +
				" 由上游前置 CDN 产生，请求未到达上游应用，故上游自身无对应记录" +
				"——这不是采集缺失，改关联逻辑也不会有结果",
		}
	}
	return LogChainUpstreamMatch{
		Confidence: correlateNone,
		Why:        "上游日志里没有相近时刻的同模型同状态码错误",
	}
}

// matchByFallback 回退路径：上游渠道名 + 状态码 + 时间窗。
//
// ★ 这条路径给的是推断，不是证据 ★
// 判据是「同一上游、同一状态码、时间相近」，而同一秒内同渠道同状态码可能有多条。
// 实测唯一性：1 秒窗 90%、10 秒窗 82%。落在多义桶里的一律标 ambiguous，
// 只报候选条数、不给具体那条——报了就是在猜。
//
// ★ 为什么用上游渠道名而不是域名 ★
// 我方 LogChainRow 只有上游**域名**（渠道快照反查得来），而上游日志里记的是
// 它自己的渠道名。两者对不上，所以这里改用「我方模型名 + 状态码」做键：
// 模型名两侧一致（都来自同一次请求），状态码也一致（上游返回什么我方就记什么）。
//
// 这一层比原设想弱：原以为能用 other.channel_name 对，但那是**上游自己的**
// 渠道名，我方无从得知。所以退到模型名——判别力低一些，但至少两侧同义。
func (m *Monitor) matchByFallback(ctx context.Context, rows []LogChainRow, out map[int64]LogChainUpstreamMatch) {
	if len(rows) == 0 {
		return
	}
	// 一次捞全窗口，避免逐行查。
	minTs, maxTs := rows[0].CreatedAt, rows[0].CreatedAt
	for _, r := range rows {
		if r.CreatedAt < minTs {
			minTs = r.CreatedAt
		}
		if r.CreatedAt > maxTs {
			maxTs = r.CreatedAt
		}
	}
	var pool []ChannelUpstreamErrorLog
	if err := m.storeDB.WithContext(ctx).
		Where("created_at BETWEEN ? AND ?",
			minTs-correlateFallbackWindow, maxTs+correlateFallbackWindow).
		Find(&pool).Error; err != nil {
		slog.Warn("上游错误日志回退关联失败，排障明细不受影响", "err", err)
		return
	}
	if len(pool) == 0 {
		return
	}

	for _, r := range rows {
		var cands []ChannelUpstreamErrorLog
		for _, h := range pool {
			if h.ModelName != r.ModelName {
				continue
			}
			// 状态码两侧应一致：上游返回什么，我方就记什么。
			// 我方状态码为 0 表示 other 里没有（旧行或非错误行），
			// 那种情况不比对状态码——少一个维度会让唯一性下降，
			// 但强行要求相等会把这些行全判成 none，那是更糟的错。
			if r.UpstreamStatusCode > 0 && h.StatusCode > 0 &&
				int64(r.UpstreamStatusCode) != h.StatusCode {
				continue
			}
			if h.CreatedAt < r.CreatedAt-correlateFallbackWindow ||
				h.CreatedAt > r.CreatedAt+correlateFallbackWindow {
				continue
			}
			cands = append(cands, h)
		}
		switch len(cands) {
		case 0:
			out[r.ID] = noMatchFor(r)
			continue
		case 1:
			// 唯一，落到下面给 probable
		default:
			out[r.ID] = LogChainUpstreamMatch{
				Confidence:        correlateAmbiguous,
				Why:               "上游在相近时刻有多条同模型同状态码的错误，无法唯一对应",
				CandidateCount:    len(cands),
				CandidateChannels: distinctUpstreamChannels(cands),
			}
			continue
		}
		h := cands[0]
		out[r.ID] = LogChainUpstreamMatch{
			Confidence:          correlateProbable,
			Why:                 "按模型名 + 状态码 + 时间窗匹配（非精确键，仅高置信推断）",
			UpstreamChannelName: h.UpstreamChannelName,
			UpstreamStatusCode:  h.StatusCode,
			UpstreamErrorCode:   h.ErrorCode,
			UpstreamErrorType:   h.ErrorType,
			UpstreamContent:     h.Content,
			UpstreamCreatedAt:   h.CreatedAt,
			UpstreamUseTime:     h.UseTime,
		}
	}
}
