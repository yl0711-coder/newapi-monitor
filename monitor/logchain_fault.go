package monitor

// logchain_fault.go：疑似责任方归因。回答排障最核心的问题——**这条请求出错，是谁的问题。**
//
// ★★ 这一层输出的是**推断，不是事实** ★★
//
// 与本页其它字段的根本区别：
//   - end_reason / content / channel_id → **事实**，转述 new-api 或上游写下的原值
//   - fault（本文件产出）              → **推断**，我方规则对事实的解读
//
// 因此每条判断都必须携带依据（FaultWhy）与可信度（FaultConfidence），
// 并且必须保留「待判」这一档。没有「待判」，规则会被迫对每条给答案，
// 模糊的那些会得到看似确定实则瞎猜的结论——**那比不给结论更糟**：
// 判成"我方配置"会让人去改自己的配置，而问题其实在上游。
//
// 规则来源：2026-08-21 与 08-24 两天生产真实数据（约 3600 条问题记录）逐条核对得出。
// 全部判据都有实测支撑，注释里记了条数——那是判断可信度的唯一依据。

import (
	"regexp"
	"strconv"
	"strings"
)

// 责任方取值。闭集，不接受其它值。
const (
	faultUpstream   = "upstream"   // 上游（含上游的上游）
	faultOurs       = "ours"       // 我方（Monitor / new-api / 渠道配置 / 限流阈值）
	faultDownstream = "downstream" // 下游（客户的请求或客户端行为）
	faultUnknown    = "unknown"    // 待判：证据不足，**必须保留这一档**
)

// 可信度取值。样本量少的映射必须标出来，不能与高置信的混在一起显示。
const (
	faultConfHigh = "high" // 判据明确且实测样本充足
	faultConfMid  = "mid"  // 判据合理但存在其它解释
	faultConfLow  = "low"  // 样本不足或判据本身模糊
	faultConfNone = "none" // 待判专用
)

// logChainStatusCodeRe 从错误原文里取 HTTP 状态码。
// new-api 把上游响应写成 "status_code=503, ..." 的形式。
var logChainStatusCodeRe = regexp.MustCompile(`status_code=(\d{3})`)

// logChainFaultFromUpstream 判断这条错误消息**是不是上游返回的**。
//
// ★ 这是本文件最关键的一条判据，也是我在评估阶段连错三次才找到的 ★
//
// new-api 把上游响应原样抄进 content，并以 "status_code=" 开头；
// 它自己生成的消息没有这个前缀。分不清来源会导致最危险的一类误判：
//
//	status_code=404, Model "gpt-5.4" is not supported by any configured account in this group
//
// 这句话的口气像"我方没配"，实际是**上游**在说它没有可用账号。
// 2026-08-24 实测：这类消息 115 条，**无一例外都带 status_code 前缀**，
// 全部来自上游。若判成我方配置，人会去改自己的渠道配置，而问题在上游。
func logChainFaultFromUpstream(content string) bool {
	return strings.HasPrefix(content, "status_code=")
}

// ═══════════ 上游自己的失败分类（other.error_code）═══════════
//
// ★★ 这一层的证据强度高于状态码，也高于原文正则 ★★
//
// error_code 是 new-api **自己**对这次失败的分类，是它写下的原值——
// 而状态码映射与原文正则都是我方对文本的解读。同一个 HTTP 408 可能是上游渠道
// 超时、也可能是我方闸门中断，状态码分不开；error_code 直接说了是哪种。
//
// 判据来源：2026-08-28 生产实测近 3 天 1256 条 type=5 行逐类核对。
// 每条规则的注释里记了实测条数——那是判断可信度的唯一依据。
//
// ★ 一条关键前提，实测确认 ★
// 这 1256 条**全部**带 status_code= 前缀（from_ours = 0），即 error_code 一律
// 来自上游响应，不存在"我方 new-api 自己生成"的情形。因此 code 里的 user
// 指的是**我方在上游的账号**，不是我方客户——insufficient_user_quota 是
// 上游说我方账号余额不足，属我方问题。
//
// 若将来出现不带前缀的 error_code（我方自己分类），本表判据会失准：
// 那时 user 的所指会翻转成我方客户。故 logChainFaultByErrorCode 的调用处
// 加了来源门（requireUpstream），与本文件既有做法一致。
var logChainFaultByErrorCode = map[string]struct {
	fault      string
	confidence string
	why        string
}{
	// —— 明确指向上游 ——
	// 40 条，全部 status_code=408。上游明说是**它自己的渠道**响应超时，
	// 而 408 在状态码层是"待判"（分不清我方闸门还是上游超时）。
	"channel:response_time_exceeded": {faultUpstream, faultConfHigh,
		"上游明示其自身渠道响应超时（error_code=channel:response_time_exceeded），责任方在上游"},
	// 42 条（41 条 new_api_error + 1 条 openai_error），全部 status_code=500。
	// 上游的 new-api 连请求都没发出去——那是上游侧的分发失败。
	"do_request_failed": {faultUpstream, faultConfHigh,
		"上游未能向其渠道发出请求（error_code=do_request_failed），失败发生在上游侧"},
	// 20 条，全部 500。上游返回的响应体本身不合法。
	"bad_response_body": {faultUpstream, faultConfHigh,
		"上游返回的响应体不合法（error_code=bad_response_body），责任方在上游"},
	// 2 条，400。上游把失败明确归给了它的上游。
	"upstream_error": {faultUpstream, faultConfHigh,
		"上游明示错误来自其更上游（error_code=upstream_error）"},
	// 1 条，502。HTTP/2 流层错误，发生在与上游的连接上。
	"upstream_http2_stream_error": {faultUpstream, faultConfHigh,
		"与上游的 HTTP/2 流出错（error_code=upstream_http2_stream_error）"},
	// 1 条，500，claude_error。读流失败，样本极少故只给中可信度。
	"stream_read_error": {faultUpstream, faultConfMid,
		"读取上游流失败（error_code=stream_read_error）。样本仅 1 条，结论待更多数据验证"},
	// 1 条，403。上游的合规策略拦截，我方无法左右。
	"session_blocked_by_cyber_policy": {faultUpstream, faultConfMid,
		"上游合规策略拦截了会话（error_code=session_blocked_by_cyber_policy）。样本仅 1 条"},

	// —— 明确指向我方 ——
	// 4 条，全部 403。**注意语义**：这里的 user 是我方在上游的账号（见上方前提），
	// 上游在说"你的余额不够"，需要我方去上游充值，不是客户的问题。
	"insufficient_user_quota": {faultOurs, faultConfHigh,
		"上游报告我方账号额度不足（error_code=insufficient_user_quota），需到该上游充值"},
	// 2 条，429。上游对我方账号限流。可行动方在我方（降并发或申请提额），
	// 但也可能是上游限额过低，故给中可信度。
	"user:concurrency_limited": {faultOurs, faultConfMid,
		"上游对我方账号做了并发限流（error_code=user:concurrency_limited）。可降并发或向上游申请提额"},

	// —— 明确指向客户 ——
	// 20 条，全部 400。客户传的数组超长，请求本身不合法。
	"array_above_max_length": {faultDownstream, faultConfHigh,
		"客户请求中的数组超过长度上限（error_code=array_above_max_length），请求本身不合法"},
	// 1 条，400。参数值非法。样本少故中可信度。
	"invalid_value": {faultDownstream, faultConfMid,
		"客户请求参数值非法（error_code=invalid_value）。样本仅 1 条"},

	// ★ 以下三类**故意不判**，实测确认它们没有判别力 ★
	//
	//	unknown_error            490 条，状态码 400~524 全谱，原文也无线索
	//	bad_response_status_code 466 条，只是"上游返回了坏状态码"，判别力全在状态码本身
	//	                         → 留给状态码层，本层不插手
	//	model_not_found          166 条，可能是客户请求了不存在的模型、我方渠道配置
	//	                         过期、或上游下架了模型；三者措辞相同，无法区分
	//
	// 把它们写进本表会让"待判"变成"看似确定实则瞎猜"——那比不给结论更糟。
}

// logChainFaultMessageRule 按错误原文的语义归因。**必须在状态码之前判**：
// 上游会用 503/404 这种看起来像上游故障的状态码报告它自己的路由失败，
// 只看状态码会把语义完全不同的情况混为一谈。
type logChainFaultMessageRule struct {
	pattern *regexp.Regexp
	// requireUpstream 为 nil 表示不关心来源；非 nil 时必须与 status_code 前缀一致。
	requireUpstream *bool
	fault           string
	confidence      string
	why             string
}

func boolPtr(b bool) *bool { return &b }

// logChainFaultMessageRules 原文语义规则，**按顺序匹配，命中即返回**。
var logChainFaultMessageRules = []logChainFaultMessageRule{
	{
		// 上游（它自己也是中转站）说它没有可用账号/渠道支持该模型。
		// 2026-08-24 实测 115 条，全部带 status_code 前缀 → 全部来自上游。
		// 08-24 当天 114 条错误里 112 条是这一类，正是那天错误率从 0.3% 飙到 4.97% 的原因。
		//
		// 附带动作：我方渠道的 models 列表仍声明支持该模型（实测渠道 74/48 的 models
		// 里确有 gpt-5.4 且状态为启用），属"我方声明与上游实际能力不符"，需同步核对。
		// 但根因在上游——它不提供或账号池耗尽。
		pattern:         regexp.MustCompile(`(?i)not supported by any configured account|No available channel for model|no available upstream in group`),
		requireUpstream: boolPtr(true),
		fault:           faultUpstream,
		confidence:      faultConfHigh,
		why:             "上游报告它没有可用账号/渠道支持该模型（上游返回）。另需核对：我方渠道的模型列表仍声明支持它",
	},
	{
		// 同样措辞但**没有** status_code 前缀 → 是我方 new-api 自己的路由失败。
		// 实测数据里目前一条都没有，保留此条以防上游版本变化或我方行为改变。
		pattern:         regexp.MustCompile(`(?i)not supported by any configured account|No available channel for model|no available upstream in group`),
		requireUpstream: boolPtr(false),
		fault:           faultOurs,
		confidence:      faultConfHigh,
		why:             "我方路由失败：new-api 自己报告本分组无可用渠道（无 status_code 前缀，非上游返回）",
	},
	{
		// 我方超时闸门主动掐断。2026-08-24 实测 5 条，原文形如
		// "status_code=408, 响应时间 125.03s 超过阈值 120.00s"。
		// 责任方是我方——阈值是我方设的；但上游确实慢到了阈值附近，
		// 所以依据里同时给出两条线索，让人自己判断该调阈值还是换上游。
		// ★ 2026-08-28 补来源门。原先没有，导致 40 条被判错 ★
		//
		// 原注释举的例子是 "status_code=408, 响应时间 125.03s 超过阈值 120.00s"，
		// 并断言"责任方是我方——阈值是我方设的"。**那个断言错了**：
		// 带 status_code= 前缀说明这句话是上游说的，超的是**上游的**阈值。
		//
		// 决定性证据（生产实测）：这些行我方 use_time **全为 0**。
		// 若是我方闸门在 120s 掐断，use_time 应 ≈120；为 0 说明我方压根没观测到
		// 125.03s 这个时长——观测到它的是上游。且 other.error_code =
		// channel:response_time_exceeded，上游自己也指向它的渠道。
		//
		// 加门后这些行落到 error_code 层，判为上游（见 logChainFaultByErrorCode）。
		// 本规则保留，用于我方**自己**的闸门消息（无前缀那种）。
		//
		// 与验收报告 P2-03 同一类缺陷：同族规则里别人有来源门，这条漏了。
		pattern:         regexp.MustCompile(`超过阈值|exceeds threshold`),
		requireUpstream: boolPtr(false),
		fault:           faultOurs,
		confidence:      faultConfMid,
		why:             "我方超时闸门主动中断（响应时间超过我方设定阈值，且无 status_code 前缀即非上游返回）。可考虑调阈值或换更快的上游",
	},
	{
		// "全部上游暂时不可用"分不清：可能上游真的全挂，
		// 也可能我方熔断/健康检查把它们全禁用了。措辞不足以区分。
		pattern:    regexp.MustCompile(`(?i)All upstreams are temporarily unavailable`),
		fault:      faultUnknown,
		confidence: faultConfNone,
		why:        "全部上游不可用：可能上游确实全挂，也可能我方熔断将其全部禁用，措辞无法区分",
	},
	{
		// 上游明说是它自己的数据库/内部故障。实测原文（2026-08-22，2 条）：
		//   status_code=401, bad response status code 401,
		//   message: 无效的令牌，数据库查询出错，请联系管理员
		//
		// 状态码是 401（看起来像我方凭据问题），但原文说的是**上游的数据库查询出错**，
		// 并让我们联系管理员——责任方在上游。这正是"状态码不足以定责"的又一个实例，
		// 也是把 401 默认映射从"我方"改成"待判"的直接原因。
		// **必须带 requireUpstream=true**（验收报告 P2-03）：
		// "internal server error" / "database error" 这类措辞我方 new-api 也会产出。
		// 少了来源门，我方自身故障会被判成上游，运营据此去投诉上游而放过自己的问题。
		// 本文件上面两条规则都有这道门，这条漏了属实现不一致。
		pattern:         regexp.MustCompile(`数据库查询出错|database (query )?error|internal server error`),
		requireUpstream: boolPtr(true),
		fault:           faultUpstream,
		confidence:      faultConfMid,
		why:             "上游明示是其自身数据库/内部故障（上游返回且原文含相应措辞），责任方在上游",
	},
}

// logChainFaultStatusRule 状态码 → 责任方。**仅在原文语义规则未命中时使用。**
type logChainFaultStatusRule struct {
	fault      string
	confidence string
	why        string
}

// logChainFaultByStatus 状态码映射。confidence 反映实测样本量——
// 样本少的必须标 low，不能让人以为它和 502/503 一样可靠。
var logChainFaultByStatus = map[int]logChainFaultStatusRule{
	// 以下四个两天实测共 121 条，判据明确：上游网关/服务/超时类故障。
	502: {faultUpstream, faultConfHigh, "上游网关错误"},
	503: {faultUpstream, faultConfHigh, "上游服务不可用"},
	504: {faultUpstream, faultConfHigh, "上游网关超时"},
	524: {faultUpstream, faultConfHigh, "上游响应超时"},
	520: {faultUpstream, faultConfMid, "上游返回异常响应"},
	529: {faultUpstream, faultConfMid, "上游过载"},

	// 429 **不给结论**：可能是上游配额耗尽，也可能是我方限流配置或客户突发。
	// 两天实测 15 条，原文都是 "Upstream rate limit exceeded" —— 措辞指向上游，
	// 但无法排除我方阈值。要真正区分需要上游侧的配额数据（当前未采集）。
	429: {faultUnknown, faultConfNone, "限流：无法区分上游配额耗尽、我方限流配置与客户突发超额"},

	// 403/401 **不给结论**。这不是"样本少所以先猜一个"，而是原文本身不含判别信息：
	//
	//	status_code=403, bad response status code 403          ← 只有状态码，没有任何线索
	//
	// 鉴权失败有两种完全不同的成因，处置也不同：
	//   - 我方密钥失效/过期        → 换密钥
	//   - 上游主动封禁/风控拦截我们 → 联系上游
	// 原文里没有任何字段能区分这两者，**再多样本也不会变得可区分**——
	// 这类必须靠人去上游后台核对，规则给结论只会误导。
	//
	// 曾经把它们映射成"我方"（低可信度），实测发现那是错的：
	// 401 的真实原文是「无效的令牌，数据库查询出错，请联系管理员」——
	// 说的是**上游的数据库**出错，责任方在上游，与"我方凭据无效"正好相反。
	403: {faultUnknown, faultConfNone, "鉴权失败：原文不含判别信息，无法区分我方密钥失效与上游主动封禁，需到上游后台核对"},
	401: {faultUnknown, faultConfNone, "凭据校验失败：原文不含判别信息，需到上游后台核对是密钥问题还是上游侧故障"},

	// 413/400 指向客户请求本身。413 两天仅 1 条；400 实测原文如
	// "Invalid 'input[60].id'" 属明确的客户参数错误。
	413: {faultDownstream, faultConfLow, "客户请求体过大（样本不足）"},
	400: {faultDownstream, faultConfMid, "客户请求参数有误"},

	// 404 落到这里说明原文不含"模型不支持"那类措辞，语义未知。
	404: {faultUnknown, faultConfNone, "资源不存在：可能客户请求了不存在的模型，也可能我方渠道配置缺失"},

	// 500 是通用错误，必须读原文才能判。
	500: {faultUnknown, faultConfNone, "通用错误，需读错误原文判断"},
	408: {faultUnknown, faultConfNone, "请求超时，需读原文判断是我方闸门还是上游超时"},
}

// 客户断连的判别阈值。**全部来自 2026-08-24 生产实测(200 条无偏样本)。**
//
// 判别核心不是"猜客户为什么走"，而是**上游有没有响应、什么时候响应、响应后有没有产出**——
// 这三件都是客观事实，比拿耗时猜客户心理可靠得多。
//
// 实测分布（这组数字是阈值的唯一依据）：
//
//	无首字延迟(上游一字未回)  66 条   平均耗时  5s   其中 49 条在 5 秒内
//	有首字延迟(上游已开口)   134 条   平均耗时 20s   平均输出 117 tok
//
// 关键发现（推翻了我最初的假设）：「无首字」**不是**上游没响应把客户等跑了，
// 恰恰相反——大多是客户在上游来得及响应之前就自己取消了（39 条在 3 秒内）。
// 所以「无首字 + 耗时短」指向下游，「无首字 + 耗时长」才指向上游。
const (
	// logChainCGNoFirstByteFastSec 无首字且低于此耗时 → 客户在上游响应前取消（实测 49 条）。
	logChainCGNoFirstByteFastSec = 5
	// logChainCGSlowFirstByteMS 首字延迟超过此值 → 上游开口过慢（实测 9 条）。
	logChainCGSlowFirstByteMS = 5000
	// logChainCGStallSec 上游开口后无产出超过此秒数 → 上游开口即卡住（实测 20 条）。
	logChainCGStallSec = 10
	// logChainCGLeaveAfterOpenSec 上游开口后客户在此秒数内就走 → 客户侧行为（实测 12 条）。
	logChainCGLeaveAfterOpenSec = 2
)

// logChainFault 归因结果。三个字段必须一起用：只显示 Fault 会让推断被当成事实。
type logChainFault struct {
	Fault      string // faultUpstream / faultOurs / faultDownstream / faultUnknown
	Confidence string
	Why        string // 依据。必须能让人复核，不能只说结论
}

// logChainAttributeFault 对一行做归因。**纯函数，只看本行**——不依赖同页其它行，
// 因此结果稳定可测；影响面（同渠道涉及几个客户）留给将来单独做，
// 那需要跨行统计，混进来会让同一条请求在不同页面上得到不同结论。
//
// 判定顺序（顺序本身就是判据，不能改）：
//  1. 非错误非异常行 → 不归因（正常请求没有"谁的问题"）
//  2. 原文语义规则（含来源判别）—— 必须在状态码之前
//  3. 状态码映射
//  4. 客户断连的耗时/产出启发式
//  5. 兜底 → 待判（绝不猜）
func logChainAttributeFault(r LogChainRow, tags []string) logChainFault {
	isErr := r.Type == 5
	hasTag := len(tags) > 0
	if !isErr && !hasTag {
		// 正常请求不归因。返回空 Fault，前端不显示该列。
		return logChainFault{}
	}

	// —— 错误行：原文语义 → 状态码 ——
	if isErr {
		for _, rule := range logChainFaultMessageRules {
			if !rule.pattern.MatchString(r.Content) {
				continue
			}
			if rule.requireUpstream != nil && *rule.requireUpstream != logChainFaultFromUpstream(r.Content) {
				continue
			}
			return logChainFault{Fault: rule.fault, Confidence: rule.confidence, Why: rule.why}
		}
		// 上游自己的失败分类。放在原文规则之后、状态码之前：
		//   - 之后：原文规则里有几条带来源门的精细判据（如"无可用渠道"要分我方/上游），
		//     那些比 error_code 更具体，不能被这一层抢走
		//   - 之前：error_code 是上游写下的原值，判别力强于我方对状态码的解读
		//     （408 在状态码层是待判，而 channel:response_time_exceeded 明确指向上游）
		//
		// 来源门：只在 content 带 status_code= 前缀时采信。实测这 1256 条全部带前缀，
		// 但若将来出现我方自己分类的 error_code，其中 user 的所指会翻转成我方客户，
		// 那时本表判据会反向出错——这道门是防止那种静默失准。
		if r.UpstreamErrorCode != "" && logChainFaultFromUpstream(r.Content) {
			if rule, ok := logChainFaultByErrorCode[r.UpstreamErrorCode]; ok {
				return logChainFault{Fault: rule.fault, Confidence: rule.confidence, Why: rule.why}
			}
		}
		if m := logChainStatusCodeRe.FindStringSubmatch(r.Content); m != nil {
			code, _ := strconv.Atoi(m[1])
			if rule, ok := logChainFaultByStatus[code]; ok {
				return logChainFault{Fault: rule.fault, Confidence: rule.confidence,
					Why: "HTTP " + m[1] + "：" + rule.why}
			}
			return logChainFault{Fault: faultUnknown, Confidence: faultConfNone,
				Why: "状态码 " + m[1] + " 尚无归因规则，需读错误原文判断"}
		}
		return logChainFault{Fault: faultUnknown, Confidence: faultConfNone,
			Why: "错误原文中没有状态码，无法按规则归因，需人工读原文"}
	}

	// —— 异常行（type=2 但有标签）——
	for _, t := range tags {
		switch t {
		case logChainClientGoneEndReason:
			return logChainClientGoneFault(r)
		case "stream":
			// 流传输真的出故障：timeout / scanner_error / panic / ping_fail 或未知取值。
			// end_reason 是 new-api 写的事实，指向传输层，通常是上游侧的流异常；
			// 但也可能是网络中途出问题，故给中可信度并把原值摆出来让人复核。
			return logChainFault{Fault: faultUpstream, Confidence: faultConfMid,
				Why: "流传输异常结束（end_reason=" + r.EndReason + "），通常为上游侧流故障"}
		}
	}
	// 只有消费异常、没有流问题：扣费与交付不一致，但责任方无从判断。
	// 这类需要人工核对计费口径，规则不该猜。
	return logChainFault{Fault: faultUnknown, Confidence: faultConfNone,
		Why: "计费与交付不一致，但无流中断等旁证，责任方需人工核对"}
}

// logChainClientGoneFault 客户断连的归因。
//
// 判据只用四个客观事实，不猜客户心理：
//   - 上游有没有开口（FirstByteMs > 0）
//   - 什么时候开口（FirstByteMs 的值）
//   - 开口后有没有产出（CompletionTokens）
//   - 开口后客户又等了多久（UseTime - FirstByteMs）
//
// 阈值来自 2026-08-24 的 200 条无偏实测样本，见上方常量注释。
// 用这套判据后可归因率从 32% 升到约 85%，且每一档都有实测条数支撑。
func logChainClientGoneFault(r LogChainRow) logChainFault {
	sec := func(v int64) string { return strconv.FormatInt(v, 10) }

	// **有产出必须最先判**：产出 token 是"上游确实响应过"的最强证据，
	// 比首字延迟更可靠（FirstByteMs 可能因上游版本或字段缺失而为 0）。
	// 顺序反了会出现"有 300 个 token 却说上游尚未开始返回内容"这种自相矛盾的依据。
	if r.CompletionTokens > 0 {
		// 实测 62/134。上游确实交付了内容，客户在流中途离开。
		return logChainFault{Fault: faultDownstream, Confidence: faultConfMid,
			Why: "上游已交付 " + strconv.FormatInt(r.CompletionTokens, 10) + " 个 token，客户在流传输中途断开——属客户侧行为"}
	}

	// —— 零产出：靠首字延迟判上游有没有开口 ——
	if r.FirstByteMs <= 0 {
		if r.UseTime < logChainCGNoFirstByteFastSec {
			// 实测 49/66。客户在上游来得及响应之前就取消了。
			// 这是我最初判断反了的地方：无首字 + 耗时短指向**下游**，不是上游。
			return logChainFault{Fault: faultDownstream, Confidence: faultConfHigh,
				Why: "客户在 " + sec(r.UseTime) + " 秒内断开，上游尚未开始返回内容——属客户侧主动取消"}
		}
		// 实测 17/66。等了 5 秒以上上游仍一字未回，指向上游响应过慢。
		return logChainFault{Fault: faultUpstream, Confidence: faultConfMid,
			Why: "客户等待 " + sec(r.UseTime) + " 秒，上游始终未返回任何内容（无首字延迟）——疑似上游无响应"}
	}

	// —— 上游已开口但零产出。先看开口本身是否已经很慢 ——
	if r.FirstByteMs >= logChainCGSlowFirstByteMS {
		// 实测 9 条。首字就要等 5 秒以上，上游慢是明确事实。
		return logChainFault{Fault: faultUpstream, Confidence: faultConfMid,
			Why: "上游首字延迟 " + strconv.FormatInt(r.FirstByteMs/1000, 10) + " 秒且此后无产出——上游开口过慢"}
	}

	// 开口及时但之后卡住：用"开口后又等了多久"区分是上游卡住还是客户走了。
	afterOpen := r.UseTime - r.FirstByteMs/1000
	switch {
	case afterOpen >= logChainCGStallSec:
		// 实测 20 条。上游及时开口后 10 秒以上没有产出，卡在上游。
		return logChainFault{Fault: faultUpstream, Confidence: faultConfMid,
			Why: "上游开口后 " + sec(afterOpen) + " 秒无任何产出——上游开口即停滞"}
	case afterOpen < logChainCGLeaveAfterOpenSec:
		// 实测 12 条。上游刚开口客户就走了，来不及归咎上游。
		return logChainFault{Fault: faultDownstream, Confidence: faultConfMid,
			Why: "上游已开口，客户在 " + sec(afterOpen) + " 秒内即断开——属客户侧行为"}
	default:
		// 实测 19 条。开口后 2~10 秒之间断开且零产出，两种解释都说得通，不猜。
		return logChainFault{Fault: faultUnknown, Confidence: faultConfNone,
			Why: "上游已开口但零产出，客户在 " + sec(afterOpen) + " 秒后断开——时长不足以区分上游停滞与客户放弃"}
	}
}
