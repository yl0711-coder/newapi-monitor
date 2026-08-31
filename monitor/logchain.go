package monitor

// 客户排障链路（P1）。回答"某客户某条请求走了哪个上游、上游返回了什么"。
//
// 与客户自助面（portal.go / LogRow）的根本区别：
//   - 本文件产出渠道 ID / 渠道名 / 上游主域名，属经营内部信息，仅 view 组（管理员及以上）可读，
//     绝不可挂到 portal 或 public 面。
//   - content 原文直出，不过 scrubContent。scrubContent 见 content 含"渠道"二字即整段清空，
//     而 new-api 的上游错误原文常形如"渠道 xxx (#12) 返回错误：..."——排障恰恰要看这句。
//
// 数据来源：生产 logs 表（含 type=5 错误），不走本地事实表——事实表口径是 type IN (2,6)，
// 排除了错误日志。channel_id → base_domain 的补全查本地 channel_snaps（生产库与本地库
// 是两个连接，无法在单条 SQL 里 join，因此分两步）。
//
// 已知盲区（见 serveLogChainRequests 返回的 blind_spots，不要在 UI 上假装没有）：
//  1. 未打到渠道即被拒的请求（限流/无可用渠道/分组无权限）不在 logs 里，
//     只在 stability_reject_hours 的小时聚合中，且该表无 user_id，定位不到具体客户。
//  2. new-api 换渠道重试会落多条 type=5，本层无法把它们归并成一次客户请求。

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	logChainDefaultLimit   = 50
	logChainMaxLimit       = 200
	logChainMaxDays        = 31   // 单次查询最大跨度，防全表扫
	logChainQueryTimeoutMS = 8000 // 生产库 MAX_EXECUTION_TIME 上限
	logChainMaxDomainChans = 500  // 域名反查渠道 ID 的条数上限，防 IN 列表爆炸

	// logChainGateTimeout 必须 <= 既有 detail 泳道调用方的超时(countGroupLogs /
	// queryGroupLogs 均为 15s)。该泳道容量 1，客户 Portal 查自己日志走的是同一条。
	// 排障是内部功能，不得比客户功能占用更久。改大这个值即为回归。
	logChainGateTimeout = 15 * time.Second
)

// ★★ 关键词搜索必须限定错误行（type=5）——验收报告 P2-02 的解法 ★★
//
// 关键词搜的是 `content LIKE '%kw%'`（见 logChainWhere）——前导通配，用不上索引，
// 只能逐行读 content 做模式匹配。而 usageDetailGate 容量只有 1，
// 客户 Portal 查自己日志走同一条泳道（客户翻页自己的 SQL 预算只有 3 秒，
// 见 usage.go queryGroupLogs）——排障占满 8 秒，就是让客户被一个比自己重
// 两倍多的查询挡在门外。这与「新增功能不得妨碍既有功能」直接冲突。
//
// ★ 曾以为收窄跨度就能解决，生产实测推翻了 ★
//
// 2026-08-26 在 nexus_ro 只读隧道上实测（MAX_EXECUTION_TIME 8000，应用同形 SQL）：
//
//	口径             跨度        耗时      结果
//	type=5          31 天      5.0s      过
//	type=5           3 天      1.7s      过
//	type IN (2,5)    3 天      9.6s      ★ 跑满预算被掐断
//	type IN (2,5)    1 天      2.1s      过（余量很小）
//
// **成本不由跨度决定，由「需要读 content 的行数」决定。** 同一 3 天窗口内：
//
//	type    行数        content 为空    平均长度
//	2       105,414     90,458          1 字符（最长 31）
//	5       883         0               95 字符（最长 506）
//
// 消费行的 content 几乎全是空的，最长 31 字符——**里面根本没有可搜的东西**。
// 为搜那 883 行有内容的错误，却要扫 105,414 行（119 倍），9 万多行扫了个空。
// 所以对症的不是砍跨度，而是限定口径：限定 type=5 后 31 天也只要 5.0s。
//
// 语义上也一致：这个输入框在界面上就叫「错误原文关键词」。
//
// ★ 限定必须显式告知 ★
// 静默把口径改小，人会以为"消费行里没有匹配的"，而实际是压根没查。
// 因此 scope 回显里带 keyword_scoped_to_errors，前端必须显示。
//
// keywordScopedReason 是告知文案里的原因，集中一处避免前后端各写一份。
const keywordScopedReason = "关键词按全文匹配、用不上索引；消费行的错误原文为空，" +
	"纳入搜索只会拖慢查询并挤占客户查自己日志的通道"

// logChainWideSpanMinDays 「多日 + 完全无筛选」的拦截门槛（自然日计）。
//
// ★ 这道闸门止损的是一个尚未修的既有缺陷，不是它的修复 ★
//
// 2026-08-26 生产实测：默认口径、不加任何筛选条件时——
//
//	days=1  → 200, 1.8s      days=3  → ★ 500, 8.4s
//	days=2  → 200, 1.1s      days=7/14/31 → ★ 500, 8.4s
//
// 根因是 queryLogChain 的 FROM 没有 FORCE INDEX：测试流量排除条件要读 content 等
// 非索引列，覆盖索引失效后优化器改判全表扫（EXPLAIN 实证 type=ALL, key=NULL,
// rows=1,070,910），一旦走 filesort，ORDER BY created_at DESC 必须排完所有匹配行，
// LIMIT 51 完全无法短路。客户 Portal 的 logSourceClause 早已用 FORCE INDEX 解决，
// 排障这条路径没继承——那是下一轮的独立设计，见开发说明书 18.7。
//
// ★ 为什么只拦「完全无筛选」 ★
//
// 带筛选的多日查法实测大部分是好的：channel_id 3.5s、user_id=130 1.1s、
// model 6.3s、group 6.7s 都在预算内。**拦宽了就会砍掉现在能用的查法**，
// 那正是「新增功能不得妨碍既有功能」要防的。只有「无筛选」是确定性失败。
//
// ★★ 这个门槛会随日增量漂，不是稳定常数；改动前必须重测 ★★
//
// 它与 logChainSourceClause 的强制索引是**配套的两层**：
// 强制索引把能救的跨度救回来，本闸门拦住连强制索引也救不动的部分。
// **只改一层会失配**——若把门槛压到强制索引的能力之下，那些本已能查的跨度
// 会在进 SQL 前被 400 掉，等于白做了索引那层。
//
// 2026-08-27 10:32 生产实测（表 1,262,215 行，无筛选，LIMIT 101）：
//
//	跨度   优化器自选        强制 created_at_type   窗口内行数
//	1 天   3143ms ✓          2587ms ✓                40,232
//	2 天   9519ms ★超时      4988ms ✓               179,670
//	3 天   9479ms ★超时      5551ms ✓               204,645
//	5 天   9480ms ★超时      5984ms ✓               228,273
//	7 天   9457ms ★超时      7675ms ✓               307,477
//	10 天  9544ms ★超时      9488ms ★超时           585,094
//	14/21/31 天  全超时       全超时                 70 万～110 万
//
// 取 6（放行到 5 天）而不是 8（放行到 7 天）：7 天那格 7675ms 距 8000ms 预算
// 只剩 325ms（4% 余量），而当日上午 10:32 已积 40,232 行、按前一日走势整日会到
// 13 万——那一格很快会翻。5 天及以内每格都有 2 秒以上余量。
//
// 历史：初版取 3（依据 08-26 18:26「2 天过 1.08s、3 天挂」），当晚 20:00 复测
// 2 天已稳定超时，改为 2；加强制索引后重测，改为 6。**同一个常数三天内改了三次**，
// 这本身就说明固定天数只是止损、不是解法——悬崖由「要读多少行 content」决定。
//
// 代价：`user_id=1`（高频账号）与 `token_name` 单独用在长跨度上仍会超时。
// 那两格带筛选、走 ref 索引，是命中后回表行数太多，索引选择层面无解，本闸门不管。
const logChainWideSpanMinDays = 6

// 异常筛选取值。不认识的值直接 400，不静默忽略——参数拼错会得到"全部请求"，
// 而人会以为自己在看异常清单。
const (
	// anomalyStream 流传输真的出了问题：timeout / scanner_error / panic / ping_fail
	// 以及任何没见过的新取值。**不含 client_gone**——见 anomalyClientGone。
	anomalyStream = "stream"

	// anomalyClientGone 下游客户端主动断连，单独一档。
	//
	// 为什么从流异常里分出来：2026-08-24 生产实测当天 1594 条 client_gone 里
	// **92%（1465 条）已经真交付了内容**（平均 324 输出 token）——客户拿到部分回答
	// 后自己断开，这是下游行为，多数不是故障。把它和 timeout/panic 混在一档，
	// 真正的流故障会被它淹掉（1594 : 25 的量级差）。
	//
	// 但它也不能直接丢掉：耗时长的 client_gone 可能是上游拖慢把客户等跑了，
	// 那种根因在上游。数据上无法区分主动取消与被拖走，只能并排给出耗时让人判断。
	anomalyClientGone = "client_gone"

	anomalyBilling       = "billing"        // 消费异常（两个方向）
	anomalyBillingUnpaid = "billing_unpaid" // 扣费未交付（客户亏）
	anomalyBillingFree   = "billing_free"   // 交付未扣费（我方亏）
	// anomalyAll 全部异常：流故障 + 客户断连 + 消费异常。分档后它仍是三者的并集，
	// 否则"全部异常"会漏掉刚拆出去的 client_gone。
	anomalyAll = "all"
	// anomalyErrAnom = 错误(type=5) + 流异常 + 消费异常，即本页能查到的全部问题。
	// 这是唯一跨 type 的取值，所以它不受"异常判据限定 type=2"那条冲突校验约束。
	//
	// 为什么放在后端而不是前端滤：前端过滤会让分页失准——后端按 limit 返 100 行、
	// 前端滤掉其中正常的消费请求，has_more 与计数就都对不上，"加载更多"行为诡异。
	anomalyErrAnom = "err_anom"
)

// 排障页的异常判据。**不复用 expandAnomalyPredicates**：那套服务稳定性报表，
// 故意排除了 client_gone（客户断连不算我方故障，否则客户关标签页会拉低渠道评分）。
// 排障页要的恰恰是 client_gone——客户的实际体验是"回答没出来"。
// 改那套会让历史稳定性数据的判定标准变化，属破坏既有功能。
//
// 但**复用它的取值 SQL**（anomalyEndReasonSQL / anomalyErrCountSQL），
// 那两个常量已处理三个踩过的坑：JSON_VALID 兜底、COALESCE 防 NULL 传染、
// 用 REPLACE(CAST(...)) 而非 MySQL 专有的 JSON_UNQUOTE（保持本地 SQLite 假库也能跑）。
// logChainNormalEndReasons 表示"流正常结束"的 end_reason 取值。**SQL 侧与 Go 侧的唯一事实源。**
//
// 判定用**排除法**：不在本名单里的取值一律算异常。枚举法（只列已知故障值）会把
// new-api 新增的取值静默吞掉，而排障最怕"没见过的情况被藏起来"。
//
// 名单成员及其来源：
//   - ""     非流式请求：other 里根本没有 stream_status，取不到值
//   - "eof"  正常结束（占绝大多数）
//   - "done" 正常结束的另一种标记。**2026-08-21 在生产真实数据上发现**：
//     当天 20 条 done 全部真交付（平均 741 输出 token、31 秒），与 eof 无实质差别，
//     此前被误判成"流未正常结束"。这条只能靠真数据发现——代码和文档里都没有它。
//
// 收成一份列表而不是在两处各写一遍：曾经 SQL 侧与 Go 侧各硬编码一份，
// 加取值时漏改一处就会出现"筛出来了但没标签"的矛盾结果，只能靠测试事后发现。
var logChainNormalEndReasons = []string{"", "eof", "done"}

// logChainNormalEndReasonSQL 把名单渲染成 SQL 的 IN 列表字面量：
// 每个取值加单引号后用逗号连接（空串成员渲染为一对单引号）。
// 取值都是编译期常量、无用户输入，无注入面。
func logChainNormalEndReasonSQL() string {
	quoted := make([]string, 0, len(logChainNormalEndReasons))
	for _, v := range logChainNormalEndReasons {
		quoted = append(quoted, "'"+v+"'")
	}
	return strings.Join(quoted, ",")
}

// logChainIsNormalEndReason Go 侧的同一判定。与 SQL 侧共用 logChainNormalEndReasons。
func logChainIsNormalEndReason(endReason string) bool {
	for _, v := range logChainNormalEndReasons {
		if endReason == v {
			return true
		}
	}
	return false
}

// logChainClientGoneEndReason 下游客户端主动断连的 end_reason 取值。
//
// 只有这一个值。单独列成常量而不是散在各处写字面量：SQL 侧、标签侧、
// 排除逻辑三处都要用它，散写会漂移。
const logChainClientGoneEndReason = "client_gone"

// logChainClientGoneSQL 客户断连判据。独立一档，不混进流故障。
//
// 注意它**不看 error_count**：客户断连时流内可能没有任何错误，
// 而 error_count > 0 属于真的流故障，归 logChainStreamAnomalySQL。
func logChainClientGoneSQL() string {
	return "(" + anomalyEndReasonSQL + " = '" + logChainClientGoneEndReason + "')"
}

// logChainStreamAnomalySQL 流**真的出故障**的判据：timeout / scanner_error / panic /
// ping_fail，以及任何没见过的新取值；或流内出错计数 > 0。
//
// 仍用**排除法**：正常名单 + client_gone 之外的一律算故障。
// 排除 client_gone 是因为它已独立成档（92% 实际已交付内容，多数不是故障），
// 混在一起会让 25 条真故障被 1594 条客户断连淹掉。
//
// **排除法本身没有被削弱**：新增的未知取值仍会落到这里，不会被静默吞掉。
func logChainStreamAnomalySQL() string {
	excluded := logChainNormalEndReasonSQL() + ",'" + logChainClientGoneEndReason + "'"
	return "(" + anomalyEndReasonSQL + " NOT IN (" + excluded + ") OR " +
		anomalyErrCountSQL + " > 0)"
}

// logChainTextCompletionPaths 会产生文本 completion token 的 API 端点白名单。
//
// ★★ 这是「扣费未交付」的主判据，模型名关键词只作兜底 ★★
//
// 为什么改成端点白名单（2026-08-25 验收报告 RB-02）：
// 原实现只按模型名关键词排除，漏掉 dall-e / sora / veo / kling / wan / vidu /
// flux / stable-diffusion 以及语音转录等一整批模态，把它们的成功请求误判为
// "客户付了钱没拿到内容"。运营可能据此错误赔付、下架渠道或向上游投诉。
//
// 端点比模型名可靠：2026-08-25 生产实测近 5 天 197371 行 type=2，
// other.request_path 填充率 **100%**（无空值），取值只有
// /v1/responses、/v1/chat/completions、/v1/messages、/pg/chat/completions。
// 非文本端点（图片/视频/音频）的 completion_tokens=0 属正常。
//
// 模型名关键词退为兜底：仅当端点在白名单内时才参与，用于挡住
// 文本端点上确实不产出 token 的模型（如某些 embedding 走 chat/completions）。
var logChainTextCompletionPaths = []string{
	"/v1/chat/completions",
	"/v1/responses",
	"/v1/messages",
	"/v1/completions",
}

// logChainNoOutputModelKeywords 文本端点上仍可能不产出 token 的模型关键词。
// **仅作兜底**：主判据是上面的端点白名单。
var logChainNoOutputModelKeywords = []string{
	"embed", "rerank", "bge-", "m3e", "image", "seedream", "seedance",
}

// logChainTextCompletionPathSQL 端点白名单的 SQL 形式。
// 用等值 OR 而非 LIKE：端点是闭集且实测只有四种取值，等值比模式匹配更精确，
// 不会因为 /v1/completions 是 /v1/chat/completions 的子串而误命中。
func logChainTextCompletionPathSQL() string {
	expr := channelTestJSONEnumSQL("$.request_path")
	parts := make([]string, 0, len(logChainTextCompletionPaths))
	for _, p := range logChainTextCompletionPaths {
		parts = append(parts, expr+" = '"+p+"'")
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

// logChainIsTextCompletionPath Go 侧的同一判定，与 SQL 侧共用白名单。
// 空路径按**不在白名单**处理：宁可漏报也不误报——把未知端点当文本端点，
// 就会重新引入 RB-02 那类"图片请求被判扣费未交付"的误报。
func logChainIsTextCompletionPath(path string) bool {
	p := strings.ToLower(strings.TrimSpace(path))
	if p == "" {
		return false
	}
	for _, want := range logChainTextCompletionPaths {
		if p == want {
			return true
		}
	}
	return false
}

// logChainNoOutputModelSQL 排除天然无输出模型，否则 embedding 类会被整类误判成"扣费未交付"。
//
// 用 LOWER(...) NOT LIKE 链而非 MySQL 专有的 NOT REGEXP：后者在 SQLite 假生产源上直接报语法错，
// 导致这批判据无法用真执行的测试覆盖（只能做字符串断言，而排序 bug 正是字符串断言漏掉的）。
// 两种写法都用不上索引，性能无差别；LOWER 显式声明大小写不敏感，也与 Go 侧的 strings.ToLower 对齐，
// 不再依赖 MySQL 的 collation 恰好是 _ci。
func logChainNoOutputModelSQL() string {
	parts := make([]string, 0, len(logChainNoOutputModelKeywords))
	for _, kw := range logChainNoOutputModelKeywords {
		parts = append(parts, "LOWER(COALESCE(model_name,'')) NOT LIKE '%"+kw+"%'")
	}
	return "(" + strings.Join(parts, " AND ") + ")"
}

// logChainBillingUnpaidSQL 扣费未交付：客户付了钱，一个 token 都没拿到。
// 判"是否真交付"只能用 completion_tokens——frt 只证明上游开口（任何 data: 行都置位），
// prompt_tokens 也不行（上游不返 usage 时 new-api 本地估算并照此扣费）。
func logChainBillingUnpaidSQL() string {
	// 端点白名单是主判据（RB-02）：非文本端点的 completion_tokens=0 属正常，
	// 图片/视频/音频请求本来就不产出文本 token，绝不能判为未交付。
	return "(type = 2 AND quota > 0 AND completion_tokens = 0 AND " +
		logChainTextCompletionPathSQL() + " AND " +
		logChainNoOutputModelSQL() + ")"
}

// logChainBillingFreeSQL 交付未扣费：内容给了，钱没收。方向相反，亏的是我方。
//
// 写成函数而非常量：它要调 channelTestJSONEnumSQL 取 other.billing_source，
// 函数调用不是编译期常量。用那个 helper 而不是自己拼，是为了与既有取值写法一致
// （MySQL 与本地 SQLite 假库都能执行）。
//
// 必须排除订阅计费：billing_source='subscription' 时走订阅额度而非钱包 quota，
// quota 自然为 0，属正常。不排除会把所有订阅客户的请求整批误报成漏计费。
func logChainBillingFreeSQL() string {
	return "(type = 2 AND quota = 0 AND completion_tokens > 0 AND " +
		logChainNoOutputModelSQL() + " AND " +
		channelTestJSONEnumSQL("$.billing_source") + " <> 'subscription')"
}

// logChainAnomalyTags 标注这一行为什么被判为异常，可同时命中多类
// （如 client_gone 且扣费未交付）。让每行自证，而不是让人对着结果猜口径。
//
// 判据必须与 SQL 侧保持一致，否则会出现"筛出来了但没标签"或反之的矛盾结果。
// 两处各写一份是有意的：SQL 在库里筛（不能把全部行捞回来再过滤），
// 这里给已捞回的行打标签。改动时必须同改两处——测试会钉住一致性。
//
// **不读 EndError**：它是自由文本，可能含 "panic" 等词，参与判定会误命中。
func logChainAnomalyTags(r LogChainRow, quota int64) []string {
	var tags []string
	if r.Type == 2 {
		switch {
		// 客户断连单独一档。判定顺序：先认 client_gone，再判流故障——
		// 否则它会被下面那条排除法当成"未知取值"重新算进 stream，等于没拆。
		//
		// error_count > 0 时按流故障处理：那说明流内真的出过错，
		// 不只是客户走了（一行可同时是断连与故障，此时以故障为准更要紧）。
		case r.EndReason == logChainClientGoneEndReason && r.StreamErrorCount == 0:
			tags = append(tags, logChainClientGoneEndReason)
		// 流真的出故障：正常名单与 client_gone 之外的取值，或流内有错误计数。
		// 排除法保持不变——没见过的新取值仍落在这里。
		case !logChainIsNormalEndReason(r.EndReason) || r.StreamErrorCount > 0:
			tags = append(tags, "stream")
		}
	}
	// 消费异常只在**文本端点**上判（RB-02）：图片/视频/音频请求的 completion_tokens=0
	// 属正常，按旧的"模型名关键词"判会把它们整批误报成"客户付钱没拿到内容"。
	// 端点白名单是主判据，模型名关键词退为白名单内的兜底。
	if r.Type == 2 && !logChainNoOutputModel(r.ModelName) {
		switch {
		// 扣费未交付**只在文本端点上判**（RB-02）：图片/视频/音频的 completion_tokens=0
		// 属正常，旧判据（仅按模型名关键词）会把它们整批误报成"客户付钱没拿到内容"，
		// 运营可能据此错误赔付或投诉上游。此处的路径条件必须与
		// logChainBillingUnpaidSQL 完全一致，否则会出现"筛出来了但没标签"。
		//
		// billing_free 分支**不加**路径条件：它要求 completion_tokens > 0，
		// 即已产出文本，端点必然是文本端点，加了是冗余且会与 SQL 侧不一致。
		case quota > 0 && r.CompletionTokens == 0 && logChainIsTextCompletionPath(r.RequestPath):
			tags = append(tags, "billing_unpaid") // 客户付了钱没拿到内容
		case quota == 0 && r.CompletionTokens > 0:
			// 订阅计费的 quota 恒为 0，属正常，不算漏计费。
			// 这里无法从 LogChainRow 读 billing_source，故由调用方保证：
			// 该字段已在 SQL 侧排除；标签侧同样跳过订阅来源的行。
			if r.BillingSource != "subscription" {
				tags = append(tags, "billing_free") // 我方漏收钱
			}
		}
	}
	return tags
}

// logChainNoOutputModel 天然无输出 token 的模型。**与 logChainNoOutputModelSQL 共用
// logChainNoOutputModelKeywords 这一份名单**，不再各写一份——两侧名单曾是硬编码副本，
// 一旦漂移就会出现"SQL 筛出来的行在标签侧被判为不是异常"这种自相矛盾的结果。
func logChainNoOutputModel(model string) bool {
	m := strings.ToLower(model)
	for _, kw := range logChainNoOutputModelKeywords {
		if strings.Contains(m, kw) {
			return true
		}
	}
	return false
}

// logChainAnomalySQL 按筛选类型返回判据片段。
// kind 只来自 parseLogChainScope 校验过的闭集，不接受任意用户输入，无注入面。
//
// 流状态异常额外限定 type=2：流结束状态只在消费日志上有意义，
// 不限定会把 type=5 错误日志里恰好带 stream_status 的行也算进"流异常"，
// 与「错误」筛选重叠、双重计数。
func logChainAnomalySQL(kind string) string {
	streamWithType := "(type = 2 AND " + logChainStreamAnomalySQL() + ")"
	// 客户断连同样限定 type=2：流结束状态只在消费日志上有意义。
	// 且必须排除 error_count>0 的行——那些归流故障，否则两档会重叠、双重计数。
	clientGoneWithType := "(type = 2 AND " + logChainClientGoneSQL() +
		" AND " + anomalyErrCountSQL + " = 0)"
	switch kind {
	case anomalyStream:
		return streamWithType
	case anomalyClientGone:
		return clientGoneWithType
	case anomalyBillingUnpaid:
		return logChainBillingUnpaidSQL()
	case anomalyBillingFree:
		return logChainBillingFreeSQL()
	case anomalyBilling:
		return "(" + logChainBillingUnpaidSQL() + " OR " + logChainBillingFreeSQL() + ")"
	case anomalyAll:
		// 拆档后 all 必须显式含 client_gone，否则"全部异常"会漏掉刚分出去的那一档。
		return "(" + streamWithType + " OR " + clientGoneWithType + " OR " +
			logChainBillingUnpaidSQL() + " OR " + logChainBillingFreeSQL() + ")"
	case anomalyErrAnom:
		// 唯一跨 type 的取值：错误(type=5) + 全部异常(type=2 里的问题请求)。
		// 在 SQL 层做而非前端滤，否则 limit/has_more/计数三者会全部失准。
		return "(type = 5 OR " + streamWithType + " OR " + clientGoneWithType + " OR " +
			logChainBillingUnpaidSQL() + " OR " + logChainBillingFreeSQL() + ")"
	}
	// 走不到：kind 已在解析阶段校验。返回恒假而不是恒真——
	// 万一将来有人绕过校验调进来，宁可查不到也不要把全部请求当异常吐出去。
	return "1 = 0"
}

// LogChainRow 一条请求的排障视图。含渠道与上游主域名——仅管理员面可见。
type LogChainRow struct {
	ID        int64  `json:"id"`
	CreatedAt int64  `json:"created_at"`
	Type      int    `json:"type"`
	TypeName  string `json:"type_name"`
	RequestID string `json:"request_id,omitempty"`

	// 客户侧
	UserID    int64  `json:"user_id"`
	Member    string `json:"member"`
	Group     string `json:"group"`
	TokenName string `json:"token_name"`

	// 上游侧：channel_id 来自 logs，其余由本地 channel_snaps 补全
	ChannelID         int64  `json:"channel_id"`
	ChannelName       string `json:"channel_name,omitempty"`
	ChannelVendor     string `json:"channel_vendor,omitempty"`
	UpstreamDomain    string `json:"upstream_domain,omitempty"`
	ChannelStatus     int    `json:"channel_status,omitempty"`
	ChannelDeleted    bool   `json:"channel_deleted,omitempty"`
	ChannelUnresolved bool   `json:"channel_unresolved,omitempty"` // 快照查不到该渠道

	// 请求侧
	ModelName         string `json:"model_name"`
	UpstreamModelName string `json:"upstream_model_name,omitempty"` // 模型映射后上游实际收到的名字
	IsModelMapped     bool   `json:"is_model_mapped,omitempty"`
	PromptTokens      int64  `json:"prompt_tokens"`
	CompletionTokens  int64  `json:"completion_tokens"`
	CacheReadTokens   int64  `json:"cache_read_tokens,omitempty"`
	UseTime           int64  `json:"use_time"`
	IsStream          bool   `json:"is_stream"`
	FirstByteMs       int64  `json:"first_byte_ms,omitempty"`
	RequestPath       string `json:"request_path,omitempty"`

	// CostUSD 仅消费(type=2)有意义，同 LogRow 口径：其它类型 quota 恒为 0，折美元会误导。
	CostUSD float64 `json:"cost_usd"`

	// Content 是 logs.content 原文，未经 scrubContent。错误(type=5)的上游返回全在这里。
	Content string `json:"content,omitempty"`

	// —— 上游自己的失败分类（事实，取自 other，非我方推断）——
	//
	// 2026-08-28 生产实测：type=5 行的 other 顶层固定含 error_type / error_code /
	// status_code。它们比 HTTP 状态码有判别力得多——例如 408 只说"超时"，而
	// error_code=channel:response_time_exceeded 明说是上游渠道超时。
	//
	// 归因层优先用它们（见 logchain_fault.go），因为**它们是上游写下的原值**，
	// 可信度高于我方对状态码与原文的解读。
	UpstreamErrorType string `json:"upstream_error_type,omitempty"`
	UpstreamErrorCode string `json:"upstream_error_code,omitempty"`
	// UpstreamStatusCode 与 content 里的 "status_code=" 是同一个值。
	// 两者都留：前者省掉正则，后者在 other 缺失时兜底。
	UpstreamStatusCode int `json:"upstream_status_code,omitempty"`

	// UpstreamMatch 是与上游错误日志的关联结果（见 logchain_correlate.go）。
	//
	// 只在采集开启且找到对应时有值。**前端必须按 Confidence 分档显示**：
	// exact 是铁证，probable 是推断，两者混在一起显示会让人拿推断当证据。
	UpstreamMatch *LogChainUpstreamMatch `json:"upstream_match,omitempty"`

	// 流结束状态。EndReason 直出原值（eof / client_gone / timeout / ...），
	// 不归类不翻译：new-api 升级新增取值时，归类写法会把它静默吞掉。
	EndReason string `json:"end_reason,omitempty"`
	// EndError 是自由文本（如 "context canceled"）。仅展示，不参与任何判定。
	EndError         string `json:"end_error,omitempty"`
	StreamErrorCount int    `json:"stream_error_count,omitempty"`

	// BillingSource = "subscription" 表示本次走订阅额度而非钱包 quota，
	// 此时 quota 恒为 0 属正常，不能算漏计费。判"交付未扣费"必须据此排除。
	BillingSource string `json:"billing_source,omitempty"`

	// AnomalyTags 说明这一行"为什么被判为异常"，可同时命中多类
	// （如 client_gone 且扣费未交付）。让每行自证，而不是让人对着结果猜口径。
	AnomalyTags []string `json:"anomaly_tags,omitempty"`

	// —— 以下三个是**推断，不是事实**（见 logchain_fault.go 的文件头）——
	//
	// Fault 疑似责任方：upstream / ours / downstream / unknown。
	// FaultWhy 是判断依据，FaultConfidence 是可信度。
	// **三者必须一起显示**：只给 Fault 会让人把推断当成事实，
	// 而归因判错的代价是去找错人（判成我方会让人去改自己的配置，而问题在上游）。
	Fault           string `json:"fault,omitempty"`
	FaultConfidence string `json:"fault_confidence,omitempty"`
	FaultWhy        string `json:"fault_why,omitempty"`

	// EdgeEvidence 是 Nginx 入口层的短期旁路事实。它只通过本行已经存在的
	// NewAPI Request ID 在内存中做 HMAC 后查询，不保存或再次回传任何 HMAC。
	// pilot 只供核对，不参与上面的责任归因；verified 才表示关联已验收。
	EdgeEvidence         *LogChainEdgeEvidence `json:"edge_evidence,omitempty"`
	EdgeEvidenceMode     string                `json:"edge_evidence_mode,omitempty"`
	EdgeEvidenceVerified bool                  `json:"edge_evidence_verified,omitempty"`
}

type LogChainEdgeEvidence struct {
	EventMS          int64  `json:"event_ms"`
	Node             string `json:"node"`
	Route            string `json:"route"`
	Status           int    `json:"status"`
	UpstreamStatus   int    `json:"upstream_status"`
	UpstreamAttempts int    `json:"upstream_attempts"`
	UpstreamStatuses string `json:"upstream_statuses,omitempty"`
	RequestMS        int64  `json:"request_ms"`
	UpstreamMS       int64  `json:"upstream_ms"`
	UpstreamPresent  bool   `json:"upstream_present"`
	ConnectMS        int64  `json:"connect_ms"`
	HeaderMS         int64  `json:"header_ms"`
	BytesSent        int64  `json:"bytes_sent"`
	Completion       string `json:"completion"`
}

// logChainScope 已校验的查询范围。所有字段都来自用户输入但已收敛到安全区间。
type logChainScope struct {
	FromTs    int64
	ToTs      int64
	UserID    int64
	ChannelID int64
	Domain    string
	Model     string
	Group     string
	TokenName string
	RequestID string
	Keyword   string
	ErrorOnly bool
	// Anomaly 异常筛选，取值见 anomalyStream 等常量；空=不按异常筛。
	// 与 ErrorOnly 互斥：错误是 type=5，异常是 type=2 里的问题请求，两者交集为空。
	// 同时传必然返回空集，所以在解析阶段就拒掉，而不是让人查出 0 行再怀疑功能坏了。
	Anomaly string
	LogType int
	// 复合游标 (created_at, id)。排序按 created_at 而非 id：
	// new-api 在请求**完成时**写日志，耗时长的请求会比后发起、快速失败的请求更晚写入，
	// 因此 id 序并不等于发生时间序。排障要的是"几点几分发生的"，必须按 created_at。
	// 单用 created_at 做游标会在同秒多条时漏行或重复，故带上 id 破平。
	BeforeTs int64
	BeforeID int64
	// Asc=true 为时间正序（最早在上），false 为倒序（最新在上，默认）。
	// 排序方向一变，游标比较方向必须跟着翻转，否则"加载更多"会往反方向取、
	// 翻出已经看过的行。两者由 logChainOrderBySQL / logChainWhere 统一处理。
	Asc   bool
	Limit int
	// SpanCap 记录跨度被收窄的全过程，空 Reasons 表示按用户要求原样生效。
	SpanCap logChainSpanCap
	// KeywordScopedToErrors 记录「因关键词而把口径收窄到 type=5」这件事（P2-02）。
	//
	// 只在用户**没有**自己指定错误口径时置位：他已经勾了「只看错误」时再告知一遍
	// 是噪声。用途是回显给前端，静默收窄会让人以为"消费行里没有匹配的"，
	// 而实际是压根没查。
	KeywordScopedToErrors bool
}

// logChainSpanCap 跨度收窄的记录。
//
// 目前只有 logChainMaxDays 一条收窄规则（曾另有一条按关键词收窄跨度的规则，
// 被生产实测推翻并撤销——成本由「要读多少行 content」决定，不由跨度决定，
// 详见文件头 keywordScopedReason 上方那段）。
//
// ★ 仍然记成「可累积的链条」而不是单个标记 ★
//
// 将来若再加一条收窄规则，两条叠加时若各自回显，页面会同时出现
// 「238 → 31」和「31 → N」两条提示，读的人得自己拼出「我要的 238 天只剩 N 天」；
// 而只回显后一条会显示「31 → N」——**用户根本没要过 31 天**。
// 统一记「用户要多少、实际给多少、被哪几条规则砍过」可以从结构上避免这种表达。
type logChainSpanCap struct {
	// RequestedDays 用户原本要查的天数。**第一个收窄点写入后不再改写**——
	// 那才是用户的真实意图，后续规则看到的已经是被砍过的值。
	RequestedDays int
	// Reasons 依次触发的收窄原因，供人复核「为什么只剩这么多」。
	Reasons []string
}

// noteSpanCap 记一次收窄。requested 只在首次收窄时写入。
func (sc *logChainSpanCap) noteSpanCap(requestedDays int, reason string) {
	if sc.RequestedDays == 0 {
		sc.RequestedDays = requestedDays
	}
	sc.Reasons = append(sc.Reasons, reason)
}

// capped 是否发生过收窄。
func (sc logChainSpanCap) capped() bool { return len(sc.Reasons) > 0 }

// parseLogChainScope 解析并收敛查询参数。时间窗按 CST 自然日左闭右开，与事实层口径一致。
// 任何越界值都收敛而非报错，只有语义矛盾（from/to 只给一个、日期格式错）才拒绝。
func parseLogChainScope(c *gin.Context, now time.Time) (logChainScope, error) {
	now = now.In(cstLocation)
	s := logChainScope{Limit: logChainDefaultLimit}

	days, _ := strconv.Atoi(c.DefaultQuery("days", "1"))
	if days < 1 {
		days = 1
	}
	if days > logChainMaxDays {
		// 收窄必须告知：不说的话，查 90 天只回来 31 天的数据，
		// 人会以为那 59 天真的没有请求。
		s.SpanCap.noteSpanCap(days, "单次查询跨度上限 "+strconv.Itoa(logChainMaxDays)+" 天（防全表扫）")
		days = logChainMaxDays
	}
	from := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, cstLocation).AddDate(0, 0, -days+1)
	to := now.Add(time.Second) // 含当前秒，避免刚发生的请求查不到

	fromText, toText := strings.TrimSpace(c.Query("from")), strings.TrimSpace(c.Query("to"))
	if fromText != "" || toText != "" {
		if fromText == "" || toText == "" {
			return logChainScope{}, errors.New("from 和 to 必须同时提供")
		}
		f, err := time.ParseInLocation("2006-01-02", fromText, cstLocation)
		if err != nil {
			return logChainScope{}, errors.New("from 日期格式应为 YYYY-MM-DD")
		}
		t, err := time.ParseInLocation("2006-01-02", toText, cstLocation)
		if err != nil {
			return logChainScope{}, errors.New("to 日期格式应为 YYYY-MM-DD")
		}
		t = t.AddDate(0, 0, 1) // to 当天整日纳入
		if t.Before(f) {
			return logChainScope{}, errors.New("to 不能早于 from")
		}
		// 跨度硬上限：宁可截断也不放行全表扫。
		if span := t.Sub(f); span > time.Duration(logChainMaxDays)*24*time.Hour {
			// 天数向上取整：查 30 天零 1 小时时说"31 天"比说"30 天"更贴近他的输入。
			s.SpanCap.noteSpanCap(int((span+24*time.Hour-time.Second)/(24*time.Hour)),
				"单次查询跨度上限 "+strconv.Itoa(logChainMaxDays)+" 天（防全表扫）")
			f = t.AddDate(0, 0, -logChainMaxDays)
		}
		from, to = f, t
	}
	s.FromTs, s.ToTs = from.Unix(), to.Unix()

	s.UserID, _ = strconv.ParseInt(strings.TrimSpace(c.Query("user_id")), 10, 64)
	s.ChannelID, _ = strconv.ParseInt(strings.TrimSpace(c.Query("channel_id")), 10, 64)
	s.Domain = strings.ToLower(strings.TrimSpace(c.Query("domain")))
	s.Model = strings.TrimSpace(c.Query("model"))
	s.Group = strings.TrimSpace(c.Query("group"))
	s.TokenName = strings.TrimSpace(c.Query("token_name"))
	s.RequestID = strings.TrimSpace(c.Query("request_id"))
	s.Keyword = strings.TrimSpace(c.Query("keyword"))
	s.ErrorOnly = c.Query("error_only") == "true"
	if t, err := strconv.Atoi(strings.TrimSpace(c.Query("type"))); err == nil && t >= 1 && t <= 6 {
		s.LogType = t
	}
	if s.ErrorOnly && s.LogType != 0 && s.LogType != 5 {
		return logChainScope{}, errors.New("error_only 与 type 冲突：error_only=true 时 type 只能为 5")
	}
	// 异常筛选。三类冲突全部显式拒绝，不静默忽略——静默会让人拿着错的结果当真：
	//   与 error_only 同传：交集为空，必然 0 行
	//   与 type≠2 同传：异常判据全部限定 type=2
	//   取值拼错：会退化成"全部请求"，而人以为在看异常清单
	if a := strings.TrimSpace(c.Query("anomaly")); a != "" {
		switch a {
		case anomalyStream, anomalyClientGone, anomalyBilling, anomalyBillingUnpaid,
			anomalyBillingFree, anomalyAll, anomalyErrAnom:
			s.Anomaly = a
		default:
			return logChainScope{}, fmt.Errorf("anomaly 取值无效：只支持 %s / %s / %s / %s / %s / %s / %s",
				anomalyStream, anomalyClientGone, anomalyBilling, anomalyBillingUnpaid,
				anomalyBillingFree, anomalyAll, anomalyErrAnom)
		}
		if s.ErrorOnly {
			return logChainScope{}, errors.New("anomaly 与 error_only 互斥：错误是 type=5，异常是 type=2 里的问题请求，交集为空")
		}
		// err_anom 本身就含 type=5，是唯一跨 type 的取值，故不受 type=2 约束。
		if s.Anomaly != anomalyErrAnom && s.LogType != 0 && s.LogType != 2 {
			return logChainScope{}, errors.New("anomaly 与 type 冲突：异常判据限定 type=2")
		}
		if s.Anomaly == anomalyErrAnom && s.LogType != 0 {
			return logChainScope{}, errors.New("anomaly=err_anom 已含错误与异常两类，不能再指定 type")
		}
	}
	// 游标必须成对提供：只给一个无法定位 (created_at,id) 的位置，
	// 静默忽略会让"加载更多"从头再来、出现重复行，所以显式拒绝。
	beforeTsText := strings.TrimSpace(c.Query("before_ts"))
	beforeIDText := strings.TrimSpace(c.Query("before_id"))
	if (beforeTsText == "") != (beforeIDText == "") {
		return logChainScope{}, errors.New("before_ts 与 before_id 必须同时提供")
	}
	if beforeTsText != "" {
		bt, err1 := strconv.ParseInt(beforeTsText, 10, 64)
		bi, err2 := strconv.ParseInt(beforeIDText, 10, 64)
		if err1 != nil || err2 != nil || bt <= 0 || bi <= 0 {
			return logChainScope{}, errors.New("before_ts / before_id 必须为正整数")
		}
		s.BeforeTs, s.BeforeID = bt, bi
	}
	// 排序方向。只认 "asc"，其余一律按默认倒序——排障最常看"刚刚发生了什么"。
	// 不把用户字符串带进 SQL，只转成 bool，无注入面。
	s.Asc = strings.EqualFold(strings.TrimSpace(c.Query("order")), "asc")
	if l, err := strconv.Atoi(strings.TrimSpace(c.Query("limit"))); err == nil && l > 0 {
		s.Limit = l
	}
	if s.Limit > logChainMaxLimit {
		s.Limit = logChainMaxLimit
	}
	// 关键词限定错误行（P2-02）。必须放在最后：它要看 Keyword、LogType、Anomaly
	// 三者的最终值，而三者分散在上面各处解析。
	if err := s.scopeKeywordToErrors(); err != nil {
		return logChainScope{}, err
	}
	// 多日 + 完全无筛选的止损闸门。必须在 scopeKeywordToErrors 之后：
	// 带关键词的查询已被前者强制为 error_only，算「有筛选」，不该被这道门拦。
	if err := s.guardWideSpanWithoutFilter(); err != nil {
		return logChainScope{}, err
	}
	return s, nil
}

// logChainDaysSpanned 时间窗覆盖几个 CST 自然日。
//
// 按自然日而非秒数算：用户想的是「查几天」，报错文案里也得说自然日。
// 用 ToTs-1 秒定右端，因为时间窗是左闭右开——to 落在次日 00:00 时
// 覆盖的是前一天，不该算成两天。
func logChainDaysSpanned(fromTs, toTs int64) int {
	if toTs <= fromTs {
		return 0
	}
	day := func(ts int64) time.Time {
		t := time.Unix(ts, 0).In(cstLocation)
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, cstLocation)
	}
	return int(day(toTs-1).Sub(day(fromTs)).Hours()/24) + 1
}

// guardWideSpanWithoutFilter 拦下「跨度 >= logChainWideSpanMinDays 且完全无筛选」的查询。
//
// 为什么显式拒绝而不是静默收窄跨度：这不是「范围被改小」，而是「这个查法当前不可用」。
// 静默给 2 天的数据，用户拿不到他要的 31 天，也不知道该怎么才能拿到。
// 400 直说「加一个筛选条件」——那是他真正需要知道的信息。
//
// 与其他拒绝分支同一原则（见 anomaly 与 error_only 的三类冲突）：
// 宁可明确拒绝，不要让人拿着错的或空的结果当真。
func (s *logChainScope) guardWideSpanWithoutFilter() error {
	days := logChainDaysSpanned(s.FromTs, s.ToTs)
	if days < logChainWideSpanMinDays {
		return nil
	}
	// 任一筛选条件都算：实测带筛选的多日查法大部分在预算内，不该拦。
	// 判据与 logChainSourceClause 共用同一函数——两处若各写一份，
	// 迟早出现「闸门放行但 FROM 去强制索引」这种错配。
	// 解析阶段还没做域名反查，故 domainChans 传 nil；s.Domain 非空本身已算收窄。
	if logChainHasNarrowingFilter(*s, nil) {
		return nil
	}
	// 可加项里**不含令牌名**：它是前导通配 LIKE，不通过索引收窄，加了也一样超时
	// （见 logChainHasNarrowingFilter 的说明）。列一个照做也没用的办法比不列更糟。
	return fmt.Errorf("跨 %d 天且未加可收窄索引的筛选条件，该查询当前不可用"+
		"（已知缺陷，见开发说明书 18.7）：会让生产库放弃索引改走全表扫，"+
		"跑满 %d 秒预算后失败，期间还占用与客户日志查询共用的通道。"+
		"请任选一项后重试：只看错误 / 指定 type / 指定异常档 / 关键词 / "+
		"渠道 / 客户 ID / 模型 / 分组 / Request ID / 上游域名；"+
		"或把跨度缩到 %d 天以内",
		days, logChainQueryTimeoutMS/1000, logChainWideSpanMinDays)
}

// scopeKeywordToErrors 带关键词时把口径限定到 type=5，并拒绝语义矛盾的组合。
//
// 为什么矛盾要拒绝而不是静默改：本文件既有做法一致（见 anomaly 与 error_only 的
// 三类冲突）——静默忽略会让人拿着错的结果当真。这里更要拒绝，因为用户显式传了
// type=2 却查到 type=5，属于"页面答的不是我问的"。
func (s *logChainScope) scopeKeywordToErrors() error {
	if s.Keyword == "" {
		return nil
	}
	// 显式指定了非错误 type：矛盾，拒绝。
	if s.LogType != 0 && s.LogType != 5 {
		return fmt.Errorf("keyword 与 type=%d 冲突：关键词搜索限定错误行（type=5），"+
			"因为%s。要查 type=%d 请去掉关键词", s.LogType, keywordScopedReason, s.LogType)
	}
	// anomaly 各档都落在 type=2（err_anom 跨 type，但它的 type=2 那半同样 content 为空）。
	// 与关键词同传时，能匹配的只有 type=5 那部分，等于关键词自己就能给出的结果，
	// 而 SQL 却要额外跑一遍异常判据。显式拒绝，让人改查法。
	if s.Anomaly != "" {
		return fmt.Errorf("keyword 与 anomaly=%s 冲突：异常档位于消费行（type=2），"+
			"而消费行的错误原文为空、搜不到内容。请二选一", s.Anomaly)
	}
	// 走到这里只剩两种情况：未指定 type，或已经是 type=5 / error_only。
	// 前者需要收窄并告知；后者本来就是错误行，不必再告知（用户已经自己勾了）。
	if !s.ErrorOnly && s.LogType != 5 {
		s.KeywordScopedToErrors = true
	}
	s.ErrorOnly = true
	return nil
}

// resolveDomainChannelIDs 把上游主域名反查成渠道 ID 列表。base_domain 是 channel_snaps
// 的索引列，且渠道删除后快照保留，因此历史请求也能按域名筛到。
// 命中数超过上限时返回 truncated=true，由调用方明确告知前端结果不完整——不静默截断。
func (m *Monitor) resolveDomainChannelIDs(ctx context.Context, domain string) (ids []int64, truncated bool, err error) {
	if domain == "" {
		return nil, false, nil
	}
	var rows []struct{ ID int64 }
	q := m.storeDB.WithContext(ctx).Raw(
		"SELECT id FROM channel_snaps WHERE base_domain = ? ORDER BY id LIMIT ?",
		domain, logChainMaxDomainChans+1)
	if err := q.Scan(&rows).Error; err != nil {
		return nil, false, fmt.Errorf("按域名反查渠道失败: %w", err)
	}
	if len(rows) > logChainMaxDomainChans {
		rows = rows[:logChainMaxDomainChans]
		truncated = true
	}
	ids = make([]int64, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.ID)
	}
	return ids, truncated, nil
}

// logChainWhere 拼生产库 WHERE。全部用户可控值参数化，无拼接注入。
// domainChans 为 nil 表示未按域名筛；非 nil 且为空表示该域名无对应渠道（调用方应直接返回空集）。
func logChainWhere(s logChainScope, domainChans []int64) (string, []any) {
	where := "created_at >= ? AND created_at < ?"
	args := []any{s.FromTs, s.ToTs}

	// 与既有口径一致：排除渠道测试流量，只看真实客户请求。
	where += " AND NOT (" + channelTestLogPredicateSQL() + ")"

	switch {
	case s.Anomaly != "":
		// 异常判据自带 type=2（见各 SQL 常量），这里不再另加 type 条件，
		// 否则会出现 "type = 2 AND (type = 2 AND ...)" 这种重复。
		where += " AND " + logChainAnomalySQL(s.Anomaly)
	case s.ErrorOnly:
		where += " AND type = 5"
	case s.LogType > 0:
		where += " AND type = ?"
		args = append(args, s.LogType)
	default:
		// 排障默认只看消费与错误：充值/管理/系统日志与"请求走了哪个上游"无关，
		// 混进来会把错误行挤出首页。要看它们请显式传 type。
		where += " AND type IN (2,5)"
	}
	if s.UserID > 0 {
		where += " AND user_id = ?"
		args = append(args, s.UserID)
	}
	if s.ChannelID > 0 {
		where += " AND channel_id = ?"
		args = append(args, s.ChannelID)
	}
	if len(domainChans) > 0 {
		inSQL, inArgs := usageIn("channel_id", domainChans)
		where += " AND " + inSQL
		args = append(args, inArgs...)
	}
	if s.Model != "" {
		where += " AND model_name = ?"
		args = append(args, s.Model)
	}
	if s.Group != "" {
		where += " AND `group` = ?"
		args = append(args, s.Group)
	}
	if s.TokenName != "" {
		where += " AND token_name LIKE ? ESCAPE '!'"
		args = append(args, "%"+escapeLike(s.TokenName)+"%")
	}
	if s.RequestID != "" { // logs.request_id 有独立索引 idx_logs_request_id
		where += " AND request_id = ?"
		args = append(args, s.RequestID)
	}
	if s.Keyword != "" {
		where += " AND content LIKE ? ESCAPE '!'"
		args = append(args, "%"+escapeLike(s.Keyword)+"%")
	}
	// 复合游标：沿排序方向取"下一批"，不用深 OFFSET。
	// 写成 created_at ? ? OR (created_at = ? AND id ? ?) 而非行值比较
	// ((created_at,id) < (?,?))：前者能用上 created_at 索引，后者在 MySQL 上
	// 未必走索引。同秒多条时用 id 破平，避免漏行或重复。
	//
	// 比较方向必须跟随排序方向：倒序时取更早的（<），正序时取更晚的（>）。
	// 方向写死会让"加载更多"在正序下往回翻，重复吐出已看过的行。
	if s.BeforeTs > 0 && s.BeforeID > 0 {
		cmp := "<"
		if s.Asc {
			cmp = ">"
		}
		where += " AND (created_at " + cmp + " ? OR (created_at = ? AND id " + cmp + " ?))"
		args = append(args, s.BeforeTs, s.BeforeTs, s.BeforeID)
	}
	return where, args
}

// logChainOrderBySQL 排序子句。抽成函数是为了让实现与测试共用同一份字面量，
// 避免"改了 SQL 但测试还在断言旧写法"的漂移。
//
// 按发生时间倒序，不按 id：new-api 在请求**完成时**写日志，一个耗时 60s 的超时请求
// 会比后发起、快速失败的请求更晚写入，故 id 序 ≠ 发生时间序。排障看的是"几点几分
// 发生的"，用户也明确要求按发生时间排列。同秒多条时用 id 破平以保证顺序稳定
// （否则复合游标翻页可能漏行或重复）。
// asc=true 为时间正序（最早在上），false 为倒序（最新在上，默认）。
// 方向只在这一处拼进 SQL，且只取自 bool——不接受用户传入的排序字符串，无注入面。
func logChainOrderBySQL(asc bool) string {
	if asc {
		return "ORDER BY created_at ASC, id ASC"
	}
	return "ORDER BY created_at DESC, id DESC"
}

// logChainSourceClause 决定 FROM 子句：只在「完全无筛选」时强制 idx_created_at_type，
// 其余一律裸 logs、让优化器自选。
//
// ★★ 为什么只在无筛选时强制，而不照抄客户侧的分派表 ★★
//
// 客户侧 mysqlLogSourceClause 按 group / user_id 分派多个 FORCE INDEX。
// 排障这边**不能照抄**——2026-08-26 生产实测（31 天跨度，同一条 SQL 两种索引选择）：
//
//	筛选条件          优化器自选                      强制 created_at_type
//	无筛选            9571ms ★超时 ALL/none           9826ms ★超时
//	channel_id=32     4481ms ✓ ref/idx_logs_channel_id   9409ms ★超时
//	user_id=130       2634ms ✓ ref/idx_user_id_id        11149ms ★超时
//	model=gpt-5.4     7218ms ✓ ref/idx_logs_model_name    9414ms ★超时
//	group=default     7890ms ✓ ref/idx_logs_group         9432ms ★超时
//	request_id=...    1344ms ✓ ref/idx_logs_request_id    9417ms ★超时
//
// **优化器在带筛选时选得更好**（全是精准等值索引），无条件强制会把这 5 种
// 现在好用的查法弄成超时。它只在「完全无筛选」这一格判错——那时它翻成
// ALL/none 全表扫，而 ORDER BY created_at DESC 一旦走 filesort，
// LIMIT 就完全无法短路（必须排完所有匹配行）。
//
// 无筛选那一格加强制后的实测（08-26 整日 139,439 行，页面形状 LIMIT 101）：
//
//	优化器自选           9475ms ★超时  ALL/none/1093096
//	FORCE created_at_type 4720ms 101行  range/idx_created_at_type/294354
//
// 那一格现在是**确定性失败**的，所以强制它不存在「弄坏」，风险面为零。
//
// 不用 idx_created_at_id：它的最左列是 id 而非 created_at，实测优化器直接忽略、
// 仍走全表扫（9449ms 超时）。索引名里有 created_at 不代表它能服务 created_at 范围。
func (m *Monitor) logChainSourceClause(s logChainScope, domainChans []int64) string {
	if m.prodDB == nil {
		return "logs"
	}
	// ★ SQLite 不认 FORCE INDEX ★
	// 单元测试的假生产库（newFakeProdDB）是 SQLite，直接拼进去会让所有涉及
	// 假生产库的用例一起报语法错。与客户侧 logSourceClause 同一道门。
	if !strings.Contains(strings.ToLower(fmt.Sprintf("%T", m.prodDB.Driver())), "mysql") {
		return "logs"
	}
	return mysqlLogChainSourceClause(s, domainChans)
}

// mysqlLogChainSourceClause 是上面那个方法的纯函数部分（已确认驱动是 MySQL）。
// 拆出来是为了能直接单测判定逻辑而不必造假驱动——与客户侧
// mysqlLogSourceClause 同一做法（见 usage_test.go 对它的表驱动测试）。
func mysqlLogChainSourceClause(s logChainScope, domainChans []int64) string {
	if logChainHasNarrowingFilter(s, domainChans) {
		return "logs" // 让优化器自选——实测它在带筛选时选得更好
	}
	return "logs FORCE INDEX (idx_created_at_type)"
}

// logChainHasNarrowingFilter 是否带了**能通过索引收窄**的筛选条件。
//
// 与 guardWideSpanWithoutFilter 共用同一判据：两处必须一致，否则会出现
// 「闸门认为有筛选所以放行，FROM 却认为无筛选去强制索引」这种错配。
// 因此抽成一个函数，不各写一份。
//
// ★ 为什么 TokenName 不算 ★
//
// 它拼的是 `token_name LIKE '%kw%'`（前导通配），用不上 idx_logs_token_name——
// **它根本不收窄，只是多一个逐行判断**。2026-08-27 实测它单独用时的行为
// 与「完全无筛选」一模一样：
//
//	跨度   优化器自选              强制 created_at_type
//	1 天   1992ms  ✓ range         1627ms ✓
//	2 天   9469ms ★超时 ALL/none    4531ms ✓
//	5 天   9484ms ★超时 ALL/none    5395ms ✓
//	7 天   9459ms ★超时 ALL/none    7250ms ✓
//	14 天  9467ms ★超时 ALL/none    9486ms ★超时
//
// 把它算成「有筛选」会同时坏两件事：FROM 不强制索引（于是 2 天起全表扫），
// 闸门也放行（于是撞到 8 秒超时）。改判后 2/3/5 天由强制索引接住、6 天起被闸门拦，
// **严格优于此前「2 天起一律 500」**。
//
// Keyword 同样是前导通配，但它已被 scopeKeywordToErrors 强制加上 type=5
// （错误行只有几百条，量级差两个数量级），故仍算收窄。
//
// ★ UserID 为什么必须算，即使它有时也超时 ★
//
// 实测 `user_id=1`（高频账号）31 天超时，但 `user_id=130` 31 天只要 1.10 秒——
// **同一个参数、不同取值，行为差一个数量级**，而解析阶段无从知道哪个是高频的。
// 按跨度拦会把 user_id=130 这类好用的查法一起砍掉，属于「妨碍既有功能」。
// 所以这一格不拦，留作已知缺口（见开发说明书 18.7）。
func logChainHasNarrowingFilter(s logChainScope, domainChans []int64) bool {
	return s.ErrorOnly || s.LogType > 0 || s.Anomaly != "" || s.Keyword != "" ||
		s.UserID > 0 || s.ChannelID > 0 || s.Domain != "" ||
		s.Model != "" || s.Group != "" || s.RequestID != "" ||
		len(domainChans) > 0
}

// queryLogChain 查生产 logs 取一页排障明细。多取一行判断 has_more，不做 COUNT(*)。
func (m *Monitor) queryLogChain(ctx context.Context, s logChainScope, domainChans []int64) ([]LogChainRow, bool, error) {
	if m.prodDB == nil {
		return nil, false, errors.New("生产库未连接：本地快照只读模式无法查询请求明细")
	}
	// 超时必须 <= 既有调用方,不得放长。usageDetailGate 容量为 1,与客户 Portal 的
	// 日志计数/分页(countGroupLogs / queryGroupLogs,均用 15s)是同一条泳道:
	// 本接口多占 1 秒,就是让客户查自己日志时多排队 1 秒。新功能不得挤占既有功能。
	cctx, cancel := context.WithTimeout(ctx, logChainGateTimeout)
	defer cancel()
	if err := m.acquireInteractiveUsageDetailGate(cctx); err != nil {
		return nil, false, fmt.Errorf("等待日志查询槽位失败: %w", err)
	}
	defer m.releaseUsageDetailGate()

	where, args := logChainWhere(s, domainChans)
	// COALESCE 全列：历史版本与迁移数据可能留 NULL，直接 Scan 进 int64 会让整页返回 500。
	q := "SELECT /*+ MAX_EXECUTION_TIME(" + strconv.Itoa(logChainQueryTimeoutMS) + ") */" +
		" id, created_at, COALESCE(type,0), COALESCE(user_id,0), COALESCE(username,'')," +
		" COALESCE(`group`,''), COALESCE(token_name,''), COALESCE(channel_id,0)," +
		" COALESCE(model_name,''), COALESCE(prompt_tokens,0), COALESCE(completion_tokens,0)," +
		" COALESCE(use_time,0), COALESCE(is_stream,0), COALESCE(quota,0)," +
		" COALESCE(content,''), COALESCE(other,''), COALESCE(request_id,'')" +
		" FROM " + m.logChainSourceClause(s, domainChans) + " WHERE " + where +
		" " + logChainOrderBySQL(s.Asc) + " LIMIT " + strconv.Itoa(s.Limit+1)

	rows, err := m.prodDB.QueryContext(cctx, q, args...)
	if err != nil {
		return nil, false, fmt.Errorf("排障日志查询失败: %w", err)
	}
	defer rows.Close()

	out := make([]LogChainRow, 0, s.Limit+1)
	for rows.Next() {
		var r LogChainRow
		var quota int64
		var isStream int
		var content, other string
		if err := rows.Scan(&r.ID, &r.CreatedAt, &r.Type, &r.UserID, &r.Member,
			&r.Group, &r.TokenName, &r.ChannelID, &r.ModelName, &r.PromptTokens,
			&r.CompletionTokens, &r.UseTime, &isStream, &quota,
			&content, &other, &r.RequestID); err != nil {
			return nil, false, err
		}
		r.TypeName = logTypeName(r.Type)
		r.IsStream = isStream != 0
		// 同 LogRow：非消费类型 quota 恒为 0，折美元会得 $0.00 误导对账。
		if r.Type == 2 {
			r.CostUSD = float64(quota) / quotaPerUSD
		}
		// 关键差异：不过 scrubContent。管理员面要看含"渠道 xxx"字样的上游错误原文。
		r.Content = content
		if o := parseLogOther(other); o != nil {
			if r.IsStream && o.FRT > 0 {
				r.FirstByteMs = int64(o.FRT)
			}
			r.CacheReadTokens = int64(o.CacheTokens)
			r.RequestPath = o.RequestPath
			// 上游的失败分类只在错误行上有意义；消费行的 other 里没有这些键，
			// 读了也是空。限定 type=5 是为了让"空值"只有一种含义：
			// 上游这次没给分类，而不是"这行本来就不该有"。
			if r.Type == 5 {
				r.UpstreamErrorType = o.ErrorType
				r.UpstreamErrorCode = o.ErrorCode
				r.UpstreamStatusCode = o.UpstreamStatusCode
			}
			if o.IsModelMapped && o.UpstreamModelName != "" {
				r.IsModelMapped = true
				r.UpstreamModelName = o.UpstreamModelName
			}
			// 流结束状态原值直出，不归类不翻译：new-api 将来新增取值时，
			// 归类写法会把它静默吞掉，而排障最怕"没见过的情况被吞了"。
			r.EndReason = o.StreamStatus.EndReason
			r.EndError = o.StreamStatus.EndError
			r.StreamErrorCount = o.StreamStatus.ErrorCount
			r.BillingSource = o.BillingSource
		}
		r.AnomalyTags = logChainAnomalyTags(r, quota)
		// 归因必须在标签算完之后：异常行的责任方依赖标签种类
		// （client_gone 与 stream 的归因逻辑完全不同）。
		if f := logChainAttributeFault(r, r.AnomalyTags); f.Fault != "" {
			r.Fault, r.FaultConfidence, r.FaultWhy = f.Fault, f.Confidence, f.Why
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	hasMore := len(out) > s.Limit
	if hasMore {
		out = out[:s.Limit]
	}
	return out, hasMore, nil
}

// attachChannelSnaps 用本地 channel_snaps 补全渠道名/厂商/上游主域名。
// 生产库与本地库是两个连接，不能在一条 SQL 里 join，故分两步。
//
// 查不到快照时标 ChannelUnresolved 而不是留空：留空会被读成"没有上游域名"，
// 而真实含义是"我们的快照没覆盖到这个渠道"，两者排障动作完全不同。
func (m *Monitor) attachChannelSnaps(ctx context.Context, rows []LogChainRow) error {
	if len(rows) == 0 {
		return nil
	}
	seen := make(map[int64]struct{}, len(rows))
	ids := make([]int64, 0, len(rows))
	for _, r := range rows {
		if r.ChannelID <= 0 {
			continue // channel_id=0：未打到渠道即失败，无快照可补
		}
		if _, ok := seen[r.ChannelID]; ok {
			continue
		}
		seen[r.ChannelID] = struct{}{}
		ids = append(ids, r.ChannelID)
	}
	if len(ids) == 0 {
		return nil
	}
	var snaps []ChannelSnap
	if err := m.storeDB.WithContext(ctx).
		Select("id", "name", "vendor", "base_domain", "status", "deleted_at").
		Where("id IN ?", ids).Find(&snaps).Error; err != nil {
		return fmt.Errorf("读取渠道快照失败: %w", err)
	}
	byID := make(map[int64]ChannelSnap, len(snaps))
	for _, s := range snaps {
		byID[int64(s.ID)] = s
	}
	for i := range rows {
		if rows[i].ChannelID <= 0 {
			continue
		}
		s, ok := byID[rows[i].ChannelID]
		if !ok {
			rows[i].ChannelUnresolved = true
			continue
		}
		rows[i].ChannelName = s.Name
		rows[i].ChannelVendor = s.Vendor
		rows[i].UpstreamDomain = s.BaseDomain
		rows[i].ChannelStatus = s.Status
		rows[i].ChannelDeleted = s.DeletedAt > 0
	}
	return nil
}

// attachNginxEvidence performs one bounded local lookup for the whole page.
// It never adds production-DB work and never turns missing optional evidence
// into a customer-troubleshooting failure.
func (m *Monitor) attachNginxEvidence(ctx context.Context, rows []LogChainRow) error {
	mode := nginxEvidenceMode(m.cfg.NginxEvidenceMode)
	if mode == "off" || m.nginxEvidenceDB == nil || len(rows) == 0 {
		return nil
	}
	type keySet struct {
		id     string
		key    string
		hashes []string
	}
	sets := []keySet{{id: m.cfg.NginxEvidenceHMACKeyID, key: m.cfg.NginxEvidenceHMACKey}}
	if m.cfg.NginxEvidencePreviousHMACKey != "" {
		sets = append(sets, keySet{id: m.cfg.NginxEvidencePreviousHMACKeyID, key: m.cfg.NginxEvidencePreviousHMACKey})
	}
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if row.RequestID == "" {
			continue
		}
		for i := range sets {
			h := nginxEvidenceIDHMAC(sets[i].key, "oneapi-request-id", row.RequestID)
			if _, ok := seen[sets[i].id+"\x00"+h]; ok {
				continue
			}
			seen[sets[i].id+"\x00"+h] = struct{}{}
			sets[i].hashes = append(sets[i].hashes, h)
		}
	}
	qctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	query := m.nginxEvidenceDB.WithContext(qctx).Model(&NginxRequestEvidence{})
	hasClause := false
	for _, set := range sets {
		if len(set.hashes) == 0 {
			continue
		}
		clause := "hmac_key_id = ? AND oneapi_id_hmac IN ?"
		if !hasClause {
			query = query.Where(clause, set.id, set.hashes)
			hasClause = true
		} else {
			query = query.Or(clause, set.id, set.hashes)
		}
	}
	if !hasClause {
		return nil
	}
	var evidence []NginxRequestEvidence
	if err := query.Order("event_ms DESC").Limit(len(rows) * len(sets) * 2).Find(&evidence).Error; err != nil {
		return err
	}
	byKey := make(map[string]NginxRequestEvidence, len(evidence))
	for _, item := range evidence {
		key := item.HMACKeyID + "\x00" + item.OneAPIIDHMAC
		if _, exists := byKey[key]; !exists {
			byKey[key] = item
		}
	}
	for i := range rows {
		if rows[i].RequestID == "" {
			continue
		}
		for _, set := range sets {
			h := nginxEvidenceIDHMAC(set.key, "oneapi-request-id", rows[i].RequestID)
			item, ok := byKey[set.id+"\x00"+h]
			if !ok {
				continue
			}
			rows[i].EdgeEvidenceMode = mode
			rows[i].EdgeEvidenceVerified = mode == "verified"
			rows[i].EdgeEvidence = &LogChainEdgeEvidence{EventMS: item.EventMS, Node: item.Node, Route: item.Route, Status: item.Status,
				UpstreamStatus: item.UpstreamStatus, UpstreamAttempts: item.UpstreamAttempts, UpstreamStatuses: item.UpstreamStatuses,
				RequestMS: item.RequestMS, UpstreamMS: item.UpstreamMS, UpstreamPresent: item.UpstreamPresent,
				ConnectMS: item.ConnectMS, HeaderMS: item.HeaderMS, BytesSent: item.BytesSent, Completion: item.Completion}
			break
		}
	}
	return nil
}

// serveLogChainFilters GET /logchain/filters
// 供筛选下拉取值：服务分组 / 上游主域名 / 渠道。**只读本地 channel_snaps，不碰生产库。**
// 单独一个接口而不是塞进 requests 响应：下拉选项与所选日期无关，
// 换一天不该重新算一遍，也不该因为当天没有错误就让下拉变空。
func (m *Monitor) serveLogChainFilters(c *gin.Context) {
	var snaps []ChannelSnap
	if err := m.storeDB.WithContext(c.Request.Context()).
		Select("id", "name", "vendor", "base_domain", "groups", "status", "deleted_at").
		Find(&snaps).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取渠道快照失败: " + err.Error()})
		return
	}
	groupSet := map[string]struct{}{}
	domainSet := map[string]struct{}{}
	type chanOpt struct {
		ID      int64  `json:"id"`
		Name    string `json:"name"`
		Domain  string `json:"domain"`
		Deleted bool   `json:"deleted,omitempty"`
	}
	chans := make([]chanOpt, 0, len(snaps))
	for _, s := range snaps {
		// 服务分组是逗号分隔的多值列，拆开去重。
		for _, g := range strings.Split(s.Groups, ",") {
			if g = strings.TrimSpace(g); g != "" {
				groupSet[g] = struct{}{}
			}
		}
		if s.BaseDomain != "" {
			domainSet[s.BaseDomain] = struct{}{}
		}
		// 已删除渠道也列出：历史请求仍要能按它筛，与 base_domain 保留快照同理。
		chans = append(chans, chanOpt{ID: int64(s.ID), Name: s.Name, Domain: s.BaseDomain, Deleted: s.DeletedAt > 0})
	}
	sortedKeys := func(m map[string]struct{}) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	sort.Slice(chans, func(i, j int) bool {
		if chans[i].Domain != chans[j].Domain {
			return chans[i].Domain < chans[j].Domain
		}
		return chans[i].Name < chans[j].Name
	})
	c.JSON(http.StatusOK, gin.H{
		"ok":       true,
		"groups":   sortedKeys(groupSet),
		"domains":  sortedKeys(domainSet),
		"channels": chans,
	})
}

// logChainBlindSpots 是本接口结构性答不了的问题。随响应一起返回，让前端必须显式面对：
// 排障工具最危险的失效方式是"查不到"被读成"没发生过"。
func logChainBlindSpots() []string {
	return []string{
		"未打到渠道即被拒的请求（限流/无可用渠道/分组无权限）不在本结果内：这类记录不写 logs，" +
			"只在 stability_reject_hours 的小时聚合里，且该表无 user_id，无法定位到具体客户。" +
			"客户报“请求根本发不出去”时，本接口查不到属预期，不代表没发生。",
		"new-api 换渠道重试会落多条 type=5：本接口按条列出，但无法归并成一次客户请求，" +
			"看到 N 条错误不等于失败 N 次。",
		// 原第三条"从不采集请求/响应正文"已删：加入 end_reason / end_error 后，
		// "回答只出一半就断了"这类已能回答（看 client_gone + 耗时）。
		// 剩下真正答不了的是"内容写得不对"，那属内容审查、不是排障范畴，写在这里是跑题。
	}
}

// serveLogChainRequests GET /logchain/requests
// 管理员排障：按客户/渠道/上游域名/模型筛请求，看上游返回的错误原文。
func (m *Monitor) serveLogChainRequests(c *gin.Context) {
	scope, err := parseLogChainScope(c, time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ctx := c.Request.Context()

	// 按域名筛：先本地反查渠道 ID。域名无对应渠道时直接返回空集，不去打生产库。
	var domainChans []int64
	domainTruncated := false
	if scope.Domain != "" {
		domainChans, domainTruncated, err = m.resolveDomainChannelIDs(ctx, scope.Domain)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if len(domainChans) == 0 {
			c.JSON(http.StatusOK, gin.H{
				"ok": true, "rows": []LogChainRow{}, "has_more": false,
				"scope": logChainScopeEcho(scope), "blind_spots": logChainBlindSpots(),
				"note": "该上游主域名在本地渠道快照中没有对应渠道",
			})
			return
		}
	}

	rows, hasMore, err := m.queryLogChain(ctx, scope, domainChans)
	if err != nil {
		if abortCanceledUsageRequest(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	channelEnrichErr := m.attachChannelSnaps(ctx, rows)
	edgeEvidenceErr := m.attachNginxEvidence(ctx, rows)
	var upstreamCorrelationErr error
	if channelEnrichErr == nil && m.cfg.UpstreamErrorLogSyncEnabled {
		var matches map[int64]LogChainUpstreamMatch
		matches, upstreamCorrelationErr = m.correlateUpstreamErrors(ctx, rows)
		if upstreamCorrelationErr == nil {
			for i := range rows {
				if match, ok := matches[rows[i].ID]; ok {
					rows[i].UpstreamMatch = &match
				}
			}
		}
	}
	resp := gin.H{
		"ok": true, "rows": rows, "has_more": hasMore,
		"scope": logChainScopeEcho(scope), "blind_spots": logChainBlindSpots(),
		"nginx_evidence_mode":     nginxEvidenceMode(m.cfg.NginxEvidenceMode),
		"nginx_evidence_verified": nginxEvidenceMode(m.cfg.NginxEvidenceMode) == "verified",
	}
	// 两类本地补全都只能降级，不能吞掉已经从生产 logs 取回的明细。
	if channelEnrichErr != nil {
		resp["channel_enrich_error"] = channelEnrichErr.Error()
	}
	if edgeEvidenceErr != nil {
		resp["edge_evidence_error"] = edgeEvidenceErr.Error()
	}
	if upstreamCorrelationErr != nil {
		resp["upstream_correlation_error"] = upstreamCorrelationErr.Error()
	}
	// 影响面只描述当前页，不额外查生产库。渠道补全失败时
	// 维度不可信，宁可不给结论，也不返回假的 blast radius。
	if channelEnrichErr == nil {
		resp["blast_radius"] = computeLogChainBlastRadius(rows)
	}
	if hasMore && len(rows) > 0 {
		// 游标必须成对返回：排序键是 (created_at, id)，只给 id 无法定位续查位置。
		last := rows[len(rows)-1]
		resp["next_before_ts"] = last.CreatedAt
		resp["next_before_id"] = last.ID
	}
	if domainTruncated {
		resp["domain_channels_truncated"] = true
		resp["note"] = fmt.Sprintf("该域名下渠道数超过 %d，仅取前 %d 个渠道的请求，结果不完整",
			logChainMaxDomainChans, logChainMaxDomainChans)
	}
	c.JSON(http.StatusOK, resp)
}

// logChainScopeEcho 回显生效范围。用户传的值可能被收敛过（跨度截断、limit 上限），
// 不回显的话前端会以为筛选条件按原样生效了。
func logChainScopeEcho(s logChainScope) gin.H {
	h := gin.H{
		"from_ts": s.FromTs,
		"to_ts":   s.ToTs,
		"from":    time.Unix(s.FromTs, 0).In(cstLocation).Format("2006-01-02 15:04:05"),
		"to":      time.Unix(s.ToTs, 0).In(cstLocation).Format("2006-01-02 15:04:05"),
		"limit":   s.Limit,
	}
	if s.UserID > 0 {
		h["user_id"] = s.UserID
	}
	if s.ChannelID > 0 {
		h["channel_id"] = s.ChannelID
	}
	if s.Domain != "" {
		h["domain"] = s.Domain
	}
	if s.Model != "" {
		h["model"] = s.Model
	}
	if s.Group != "" {
		h["group"] = s.Group
	}
	if s.TokenName != "" {
		h["token_name"] = s.TokenName
	}
	if s.RequestID != "" {
		h["request_id"] = s.RequestID
	}
	if s.Keyword != "" {
		h["keyword"] = s.Keyword
	}
	// 跨度收窄必须回显。静默收窄会让人以为"这段时间里就这些"，据此得出错误结论——
	// 与「缺失绝不显示为零」同一条原则：范围被改小了就必须让人知道。
	//
	// effective 由 from/to 算出而非套用常量：两条收窄叠加时，最终跨度由后一条决定，
	// 写死任一个常量都会在叠加场景下报错数。
	if s.SpanCap.capped() {
		effective := 0
		if s.ToTs > s.FromTs {
			effective = int((s.ToTs - s.FromTs + 86399) / 86400)
		}
		h["span_capped"] = gin.H{
			"requested_days": s.SpanCap.RequestedDays,
			"effective_days": effective,
			"reasons":        s.SpanCap.Reasons,
		}
	}
	// 关键词把口径收窄到错误行也必须回显（P2-02）：不说的话，
	// 人会以为"消费行里没有匹配的"，而实际是压根没查那一部分。
	if s.KeywordScopedToErrors {
		h["keyword_scoped_to_errors"] = gin.H{"reason": keywordScopedReason}
	}
	if s.ErrorOnly {
		h["error_only"] = true
	}
	if s.LogType > 0 {
		h["type"] = s.LogType
	}
	// 回显生效的排序方向：前端据此高亮对应按钮，避免按钮状态与实际结果不一致。
	if s.Asc {
		h["order"] = "asc"
	} else {
		h["order"] = "desc"
	}
	return h
}
