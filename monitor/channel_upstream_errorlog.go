package monitor

// channel_upstream_errorlog.go：上游错误日志采集（NewAPI 系）。
//
// ★ 它与渠道管理的用量同步是两回事，别混 ★
//
// 用量同步（channel_upstream_usage.go）拉 /api/log/self?type=2，拿到逐条日志后
// **立刻折进小时桶**（ChannelUpstreamUsageHour：requests/tokens/quota/cost_usd），
// 每条的时间、模型、原文全部在聚合那一步丢掉——那是刻意设计，它只回答
// 「这个上游今天花了多少钱」。
//
// 本文件要的是相反的东西：**保留逐条明细**，用于排障时把我方 type=5 与上游
// type=5 并排看。所以不能复用那条聚合路径，得单独一张表。
//
// ★ 为什么只做 NewAPI ★
//
// 三种 provider 的上游端点语义不同（2026-08-27 读码核实）：
//
//	newapi      /api/log/self?type=N     日志接口，逐条，type 可选 → 能拿错误
//	sub2api     /api/v1/usage            聚合查询，返回 total_requests/total_tokens
//	                                     /total_actual_cost，**连逐条都没有**
//	aicodewith  /api/v1/usage/details    计价明细，我方 decoder 只读 5 个字段，
//	                                     响应全字段未抓过 → 未知
//
// 所以本文件只实现 NewAPI；另两家由调用方落 upstreamStatusUnsupported 状态，
// 页面明确显示「该上游无日志接口」——**不能让人以为没记录等于没出错**。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm/clause"
)

// upstreamErrorLogType 上游 NewAPI 的错误日志 type。与我方 logs 表同一口径：
// 2=消费 5=错误。这里只拉 5。
const upstreamErrorLogType = 5

// upstreamErrorLogContentMax 单条错误原文入库上限。
//
// 生产实测我方 type=5 的 content 平均 95 字符、最长 506（见开发说明书 18.5），
// 上游同量级。取 2000 留足余量，同时防止上游异常返回超长文本把库撑爆——
// 截断时保留**前缀**：错误原文的判别信息（status_code=、错误类型）都在开头。
const upstreamErrorLogContentMax = 2000

// ChannelUpstreamErrorLog 上游错误日志的逐条明细。
//
// ★ 主键选择 ★
// (domain, upstream_id) —— upstream_id 是上游那条日志自己的 id。
// 用它做主键使重复拉取天然幂等：同一条拉两次是 upsert，不会翻倍。
// 不用自增 id：那样重跑窗口就会产生重复行，而这张表要能被反复回填。
type ChannelUpstreamErrorLog struct {
	Domain     string `gorm:"primaryKey;size:253;column:domain"`
	UpstreamID int64  `gorm:"primaryKey;column:upstream_id"`

	// CreatedAt 是上游那条日志的发生时间（秒）。带索引：排障按时间窗查。
	CreatedAt int64  `gorm:"column:created_at;index"`
	ModelName string `gorm:"size:128;column:model_name;index"`

	// Content 是上游的错误原文。**这是本表存在的理由**——
	// 没有它就只知道「上游报错了」，不知道报的是什么。
	Content string `gorm:"type:text;column:content"`

	// UpstreamRequestID 是上游那条日志里的 request_id（它自己的），
	// UpstreamUpstreamRequestID 是上游记录的**它的**上游的 request_id。
	// 两者都可能为空，不作为主键，只作为对账线索。
	UpstreamRequestID         string `gorm:"size:128;column:upstream_request_id;index"`
	UpstreamUpstreamRequestID string `gorm:"size:128;column:upstream_upstream_request_id"`

	// TokenName / GroupName 是上游侧的令牌名与分组，用于判断是我方哪个渠道打过去的。
	TokenName string `gorm:"size:128;column:token_name"`
	GroupName string `gorm:"size:128;column:group_name"`

	// UseTime 上游侧这条请求的耗时（秒）。顶层字段，2026-08-28 实测可用。
	// 区分「上游超时」与「上游立即拒绝」——两者根因不同。
	UseTime int64 `gorm:"column:use_time"`

	// ★★ 以下五个字段来自 other 嵌套 JSON，是拉上游日志的核心价值 ★★
	//
	// 2026-08-28 在我方生产 963 条真实 type=5 行上实测，other 顶层键固定为：
	//   admin_info, channel_id, error_code, error_type, status_code,
	//   channel_name, channel_type, request_path
	// 我方主站与上游同为 new-api，结构一致；用户也已用真实上游响应确认过
	// content 字段与「顶层 channel_name 为空、真名在 other 内」这两点。
	//
	// **这些是我方 logs 表完全没有的信息**：我方只知道"打某个上游失败了"，
	// 而这里能知道"上游用它自己的哪个渠道去打、对方返回什么状态码和错误类型"。
	// 没有它们，这张表的价值只剩一份错误原文。

	// UpstreamChannelName 上游自己用的渠道名，取自 other.channel_name。
	// 注意**不是** other.admin_info.channel_name——后者实测恒为 NULL。
	UpstreamChannelName string `gorm:"size:128;column:upstream_channel_name;index"`
	// UpstreamChannelID 上游自己的渠道 ID，取自 other.channel_id。
	UpstreamChannelID int64 `gorm:"column:upstream_channel_id"`
	// StatusCode 上游拿到的 HTTP 状态码，取自 other.status_code。
	StatusCode int64 `gorm:"column:status_code;index"`
	// ErrorCode / ErrorType 上游侧的错误分类，取自 other.error_code / other.error_type。
	ErrorCode string `gorm:"size:128;column:error_code"`
	ErrorType string `gorm:"size:128;column:error_type"`
	// RequestPath 上游侧的请求路径，取自 other.request_path。
	// 用于区分是哪类接口失败（chat/completions vs messages 等）。
	RequestPath string `gorm:"size:256;column:request_path"`

	// ★★ JoinKey 是与我方日志串联的唯一可用键 ★★
	//
	// 它是 content 里嵌的 "(request id: X)" 中的 X，即**最深层模型商**生成的 id。
	//
	// ★ 为什么不是 UpstreamRequestID ★
	// 2026-08-28 实测（kpzhu.com，我方渠道 #66，跨 4 天）：
	//
	//	上游 request_id 字段 ↔ 我方嵌的 id →   1 / 486 命中（≈0）
	//	上游嵌的 id         ↔ 我方嵌的 id → 152 命中（我方带 key 的行几乎全中）
	//
	// 原因是错误体逐层透传：模型商生成 P → 上游记自己的 request_id K、content 里带 P
	// → 我方记自己的 O、content 里还是 P。**能对上的是 P ↔ P**。
	// K 是上游自己的流水号，我方永远看不到它，用它做键必然落空。
	//
	// 单独成列而不是查询时正则：没有索引的 REGEXP 会让每次串联退化成全表扫，
	// 而这张表会持续增长。
	JoinKey string `gorm:"size:128;column:join_key;index"`

	// FetchedAt 本地抓取时刻，用于判断数据新鲜度与做保留期清理。
	FetchedAt int64 `gorm:"column:fetched_at;index"`

	// RawJSON 是上游那条日志的原始 JSON。
	//
	// ★ 字段名已核实（2026-08-28）★
	// 用户用后台 JWT 调真实上游 /api/log/self?type=5 确认：
	//   content              ← 错误原文，猜对了
	//   use_time             ← 顶层可用
	//   顶层 channel_name    ← **空字符串**，真名在 other 内（见上面那五个字段）
	// 其余（id / created_at / model_name / group / token_id / quota /
	// prompt_tokens / completion_tokens / other / user_id）由契约 fixture、
	// 生产计价路径与开发说明书 3.3.1 共同证实。
	//
	// ★ 仍然留原文，理由变了但没消失 ★
	// 不再是「怕字段名猜错」，而是：
	//  1. other 里还有 admin_info / channel_type 等我们当前没建列的字段，
	//     将来要用时可以就地重解，不必重新向上游拉——**上游日志有保留期，
	//     过期了再也拿不回来**；
	//  2. 上游改版加字段时，原文是唯一能事后发现的凭据。
	// 代价是磁盘：单条按 2KB 估、一天几百条错误，量级可忽略。
	RawJSON string `gorm:"type:text;column:raw_json"`
}

// upstreamErrorLogItem 是从上游响应解出的一条错误日志。
type upstreamErrorLogItem struct {
	ID                        int64
	CreatedAt                 int64
	ModelName                 string
	Content                   string
	TokenName                 string
	GroupName                 string
	UpstreamRequestID         string
	UpstreamUpstreamRequestID string
	// JoinKey 见 ChannelUpstreamErrorLog.JoinKey：从 content 抠出的串联键。
	JoinKey string
	UseTime int64
	// 以下五个来自 other 嵌套 JSON，见表结构里的说明。
	UpstreamChannelName string
	UpstreamChannelID   int64
	StatusCode          int64
	ErrorCode           string
	ErrorType           string
	RequestPath         string
	// Raw 是原始 JSON，见 ChannelUpstreamErrorLog.RawJSON 的说明。
	Raw string
	// UnresolvedFields 列出**一个候选名都没命中**的字段。
	// 它不是错误：缺 token_name 不影响排障。但必须能被观测到——
	// 若上线后发现 content 长期在这个列表里，说明字段名猜错了，
	// 那时可以照 RawJSON 就地重解，不用重新拉。
	UnresolvedFields []string
}

// decodeUpstreamErrorLogItem 解一条上游错误日志。
//
// ★ 容错原则与 decodeNewAPIUsageItem 不同 ★
//
// 计价那条路径对缺字段是**硬失败**（少一个 quota 就整条报错），因为算钱错不得。
// 错误日志是排障线索，**缺字段不该丢整条**：只要有 id 和 created_at 就值得存，
// 其余字段空着也比没有这条记录好。所以这里只对那两个字段硬校验。
//
// 反过来，id 必须严格：它是主键，错了会让两条不同的日志互相覆盖。
// ★ 为什么解成 map 再挑键，而不是定型结构体 ★
//
// 定型结构体对未知字段是**静默丢弃**——字段名猜错时表现为「解出来是空串」，
// 而看不出到底是上游没这个字段、还是我们名字写错了。
// 解成 map 就能：① 一个字段试多个候选名；② 记下哪些字段一个都没命中。
// aicodewith 的 decoder 已是这个做法（firstRawField），此处照它。
func decodeUpstreamErrorLogItem(itemJSON json.RawMessage) (upstreamErrorLogItem, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(itemJSON, &fields); err != nil {
		return upstreamErrorLogItem{}, fmt.Errorf("上游错误日志条目无效: %w", err)
	}

	// id 与 created_at 是硬要求：前者是主键（错了会互相覆盖），后者是时间轴锚点。
	// 这两个已由契约 fixture 与生产计价路径证实，不需要候选名。
	id, err := rawJSONInt64Exact(fields["id"])
	if err != nil || id <= 0 {
		return upstreamErrorLogItem{}, fmt.Errorf("上游错误日志缺少有效 id")
	}
	created, err := rawJSONNumber(fields["created_at"])
	if err != nil || created <= 0 || created != math.Trunc(created) || created > math.MaxInt64 {
		return upstreamErrorLogItem{}, fmt.Errorf("上游错误日志缺少有效 created_at")
	}

	item := upstreamErrorLogItem{ID: id, CreatedAt: int64(created), Raw: string(itemJSON)}
	// pick 取第一个命中的候选名；一个都没命中时登记到 UnresolvedFields。
	pick := func(label string, maxBytes int, names ...string) string {
		for _, n := range names {
			if raw, ok := fields[n]; ok {
				if s := jsonRawToString(raw); s != "" {
					return boundedUpstreamErrorField(s, maxBytes)
				}
			}
		}
		item.UnresolvedFields = append(item.UnresolvedFields, label)
		return ""
	}

	// 候选名按「最可能」在前。已证实的（model_name / group）也走 pick，
	// 这样上游改版换名时能自动兼容而不是静默变空。
	item.ModelName = pick("model_name", 128, "model_name", "modelName", "model")
	item.Content = pick("content", upstreamErrorLogContentMax,
		"content", "message", "error", "detail", "remark")
	item.TokenName = pick("token_name", 128, "token_name", "tokenName")
	item.GroupName = pick("group", 128, "group", "group_name", "groupName")
	item.UpstreamRequestID = pick("request_id", 128, "request_id", "requestId", "request_id_str")
	item.UpstreamUpstreamRequestID = pick("upstream_request_id", 128,
		"upstream_request_id", "upstreamRequestId")
	// use_time 顶层可用（2026-08-28 实测）。非整数或缺失时留 0——
	// 0 与「真的用了 0 秒」不可区分，但那种请求本来也没有耗时信息可言。
	if v, err := rawJSONNumber(fields["use_time"]); err == nil && v >= 0 {
		item.UseTime = int64(v)
	}
	item.applyUpstreamErrorOther(fields["other"])
	// 串联键从 content 抠。抠不到很正常（实测 43% 有）——超时类错误
	// 上游还没拿到模型商的响应体，自然没有那个 id。
	item.JoinKey = logChainJoinKeyFrom(item.Content)
	return item, nil
}

// logChainJoinKeyFrom 从错误原文里抠出上下游串联键。
//
// ★ 两种形态都必须认 ★
// 2026-08-28 生产实测两种并存：
//
//	(request id: 202608280208089288070118268d9d6WhLijC4B)   ← new-api 系，空格
//	(request_id: req_1787652519221_1a03fa99)                ← 另一种上游，下划线
//
// 只认前者会漏掉后者那批，而后者恰是 bad_response_status_code 的主体——
// 那正是最需要串联的一类（状态码层判不出责任方）。
// 实测：408 类里空格形态 0 条、下划线形态 49 条（53%），只认一种会把整类判成不可串联。
var logChainJoinKeyRe = regexp.MustCompile(`\(request[ _]id:\s*([0-9A-Za-z_-]+)\)`)

func logChainJoinKeyFrom(content string) string {
	m := logChainJoinKeyRe.FindStringSubmatch(content)
	if m == nil {
		return ""
	}
	return boundedUpstreamErrorField(m[1], 128)
}

// applyUpstreamErrorOther 解 other 嵌套 JSON，取出上游侧的渠道与错误分类。
//
// ★ other 有两种形态，必须都认 ★
// 契约 fixture（channel_upstream_pricing_ledger_test.go）里它是**转义的 JSON
// 字符串**：`"other":"{\"model_ratio\":2.5,...}"`；而我方生产库里是 JSON 对象。
// 只认一种，另一种会静默解不出——而这些字段正是本表的核心价值。
func (item *upstreamErrorLogItem) applyUpstreamErrorOther(raw json.RawMessage) {
	if len(raw) == 0 {
		item.UnresolvedFields = append(item.UnresolvedFields, "other")
		return
	}
	// 先试对象；不是对象就试「字符串里套 JSON」。
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		var inner string
		if err2 := json.Unmarshal(raw, &inner); err2 != nil || inner == "" {
			item.UnresolvedFields = append(item.UnresolvedFields, "other")
			return
		}
		if err3 := json.Unmarshal([]byte(inner), &fields); err3 != nil {
			item.UnresolvedFields = append(item.UnresolvedFields, "other")
			return
		}
	}

	str := func(label string, maxBytes int, names ...string) string {
		for _, n := range names {
			if v, ok := fields[n]; ok {
				if s := jsonRawToString(v); s != "" {
					return boundedUpstreamErrorField(s, maxBytes)
				}
			}
		}
		item.UnresolvedFields = append(item.UnresolvedFields, "other."+label)
		return ""
	}
	num := func(label string, names ...string) int64 {
		for _, n := range names {
			if v, ok := fields[n]; ok {
				if f, err := rawJSONNumber(v); err == nil {
					return int64(f)
				}
			}
		}
		item.UnresolvedFields = append(item.UnresolvedFields, "other."+label)
		return 0
	}

	// channel_name 只找 other 顶层：other.admin_info.channel_name 实测恒为 NULL，
	// 把它列进候选会让「找到了但是空」和「没找到」混淆。
	item.UpstreamChannelName = str("channel_name", 128, "channel_name", "channelName")
	item.UpstreamChannelID = num("channel_id", "channel_id", "channelId")
	item.StatusCode = num("status_code", "status_code", "statusCode")
	item.ErrorCode = str("error_code", 128, "error_code", "errorCode")
	item.ErrorType = str("error_type", 128, "error_type", "errorType")
	item.RequestPath = str("request_path", 256, "request_path", "requestPath")
}

// jsonRawToString 把一个 JSON 值取成字符串。
//
// 上游可能把同一个字段发成字符串或数字（契约 fixture 里 other 就是转义字符串，
// quota 有时是字符串有时是数字）。只认字符串会让数字型字段静默变空。
func jsonRawToString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	// 不是字符串：原样返回字面量（数字/布尔）。null 与空对象视为空。
	lit := strings.TrimSpace(string(raw))
	if lit == "null" || lit == "{}" || lit == "[]" {
		return ""
	}
	if strings.HasPrefix(lit, "{") || strings.HasPrefix(lit, "[") {
		return "" // 结构化值不当标量用，避免把整个对象塞进 model_name 这类列
	}
	return lit
}

// fetchUpstreamErrorLogPage 取一页上游错误日志。
//
// ★ 复用 fetchNewAPIUsagePageWithDecoder，不另写一套 ★
// 那个泛型函数已经带了：pacer 限速、Bearer + New-Api-User 认证、
// 半开区间 [from,to) 的 end_timestamp-1 处理、total 校验、页指纹（防上游翻页
// 不稳定导致漏页/重页）、401/403 转 upstreamAuthError。这些全都必须要，
// 重写一份只会漏掉其中几条。
//
// 唯一的差别是 type：那个函数写死了 "2"，故本文件用带 logType 参数的变体。
func fetchUpstreamErrorLogPage(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred newAPICredential, from, to int64, pageNumber int, pacer *upstreamUsageRequestPacer) (newAPIUsagePage[upstreamErrorLogItem], error) {
	return fetchNewAPILogPageWithType(ctx, client, row, cred, from, to, pageNumber, pacer,
		upstreamErrorLogType, decodeUpstreamErrorLogItem)
}

// UpstreamErrorLogSyncState 每个上游的错误日志采集调度状态。
//
// ★ 为什么另开一张表，不往 ChannelUpstreamAccount 加列 ★
// 那张表已被余额、用量、计价台账、AICodeWith 多路共用，字段已经很多；
// 再塞四列会让本功能的失败状态与别人的混在一行里，回滚也更难拆。
// 与 AICodeWithKeySyncState 同一做法：调度状态独立成表，凭据仍留在账户表。
type UpstreamErrorLogSyncState struct {
	Domain string `gorm:"primaryKey;size:253;column:domain"`

	// NextSyncAt 下次可以同步的时刻。失败时按退避推后，成功时按正常间隔推后。
	NextSyncAt    int64 `gorm:"column:next_sync_at;index"`
	LastAttemptAt int64 `gorm:"column:last_attempt_at"`
	LastSuccessAt int64 `gorm:"column:last_success_at"`

	// SyncedUntil 已采集到的时间水位（上游日志的 created_at）。
	// 下一轮从这里往后拉，避免每次都从当天 0 点重扫。
	SyncedUntil int64 `gorm:"column:synced_until"`

	ConsecutiveFails int    `gorm:"column:consecutive_fails"`
	LastError        string `gorm:"size:512;column:last_error"`
	Status           string `gorm:"size:24;column:status;index"`

	// RowsTotal 累计入库条数，仅用于运营端判断「到底有没有拉到东西」。
	RowsTotal int64 `gorm:"column:rows_total"`

	// UnresolvedFields 最近一轮未命中候选名的字段，JSON 对象文本。
	//
	// **这是字段名未核实的观测出口**（见 RawJSON 的说明）。落在库里而不只打日志：
	// 运营端要能直接看到「content 一直没命中」，而不是让人去翻容器日志。
	UnresolvedFields string `gorm:"size:512;column:unresolved_fields"`

	UpdatedAt int64 `gorm:"column:updated_at;index"`
}

// upstreamErrorLogResult 一次窗口同步的结果。
type upstreamErrorLogResult struct {
	Rows      []ChannelUpstreamErrorLog
	Total     int64 // 上游报告的窗口内总条数
	PagesRead int
	// Truncated 为真表示达到单轮请求预算就停了，窗口没读完。
	// **必须回显**：不然运营会以为「上游那段时间只有这些错误」。
	Truncated bool
	// UnresolvedFields 统计各字段「一个候选名都没命中」的条数。
	//
	// 这是本轮字段名未核实的观测出口：若上线后 content 长期出现在这里，
	// 说明名字猜错了——此时不必重新向上游拉（上游有保留期，过期就没了），
	// 照 ChannelUpstreamErrorLog.RawJSON 就地重解即可。
	UnresolvedFields map[string]int
}

// syncUpstreamErrorLogWindow 拉一个时间窗的上游错误日志。
//
// 与用量同步的关键差别：**不聚合**。每条都进 Rows，供落库保留明细。
//
// 单轮请求预算由 pacer 控制，达到上限时返回已读到的部分并置 Truncated，
// 而不是报错丢弃——排障宁可拿到一半也比拿不到好，但必须知道是一半。
func syncUpstreamErrorLogWindow(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, cred newAPICredential, from, to int64, pacer *upstreamUsageRequestPacer, now int64) (upstreamErrorLogResult, error) {
	if row.Provider != upstreamProviderNewAPI {
		return upstreamErrorLogResult{}, fmt.Errorf("%s 无日志接口，无法读取上游错误日志", upstreamProviderName(row.Provider))
	}
	if to <= from {
		return upstreamErrorLogResult{}, fmt.Errorf("上游错误日志窗口无效")
	}
	var out upstreamErrorLogResult
	seen := map[int64]struct{}{}
	for page := 1; ; page++ {
		got, err := fetchUpstreamErrorLogPage(ctx, client, row, cred, from, to, page, pacer)
		if err != nil {
			// 请求预算耗尽不是失败：返回已读部分并标明未读完。
			var budget *upstreamUsageRunBudgetExhausted
			if errors.As(err, &budget) {
				out.Truncated = true
				return out, nil
			}
			return out, err
		}
		out.Total = got.Total
		out.PagesRead = page
		if len(got.Items) == 0 {
			return out, nil
		}
		for _, item := range got.Items {
			// 上游翻页期间有新日志写入时，同一条可能在两页都出现。
			// 主键能兜住重复写，但这里先去重可以少一次无用的 upsert。
			if _, dup := seen[item.ID]; dup {
				continue
			}
			seen[item.ID] = struct{}{}
			out.Rows = append(out.Rows, ChannelUpstreamErrorLog{
				Domain:                    row.Domain,
				UpstreamID:                item.ID,
				CreatedAt:                 item.CreatedAt,
				ModelName:                 item.ModelName,
				Content:                   item.Content,
				TokenName:                 item.TokenName,
				GroupName:                 item.GroupName,
				UpstreamRequestID:         item.UpstreamRequestID,
				UpstreamUpstreamRequestID: item.UpstreamUpstreamRequestID,
				JoinKey:                   item.JoinKey,
				UseTime:                   item.UseTime,
				UpstreamChannelName:       item.UpstreamChannelName,
				UpstreamChannelID:         item.UpstreamChannelID,
				StatusCode:                item.StatusCode,
				ErrorCode:                 item.ErrorCode,
				ErrorType:                 item.ErrorType,
				RequestPath:               item.RequestPath,
				FetchedAt:                 now,
				RawJSON:                   item.Raw,
			})
			// 汇总未命中的字段名，供调用方回显。一条一条报会淹掉日志，
			// 而「content 在整个窗口里都没命中」才是真正需要知道的信号。
			for _, f := range item.UnresolvedFields {
				if out.UnresolvedFields == nil {
					out.UnresolvedFields = map[string]int{}
				}
				out.UnresolvedFields[f]++
			}
		}
		// 读满 total 即止。用 >= 而非 ==：上游 total 可能在翻页期间变大，
		// 死等相等会多转一圈甚至转不完。
		if int64(len(seen)) >= got.Total {
			return out, nil
		}
	}
}

// persistUpstreamErrorLogs 落库。按 (domain, upstream_id) upsert，重复拉取幂等。
//
// 分批写：单条 SQL 塞几千行会撞 SQLite 的变量上限，也会让一次事务过长
// 阻塞其它写入。批大小取 200，与本仓库其它批写保持一致的量级。
func (m *Monitor) persistUpstreamErrorLogs(ctx context.Context, rows []ChannelUpstreamErrorLog) error {
	if len(rows) == 0 {
		return nil
	}
	const batch = 200
	for start := 0; start < len(rows); start += batch {
		end := start + batch
		if end > len(rows) {
			end = len(rows)
		}
		if err := m.storeDB.WithContext(ctx).
			Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "domain"}, {Name: "upstream_id"}},
				// 只更新可能变化的字段。upstream_id / domain 是主键不动；
				// created_at 理论上不会变，但上游若修正过时间戳，以最新为准。
				DoUpdates: clause.AssignmentColumns([]string{
					"created_at", "model_name", "content", "token_name", "group_name",
					"upstream_request_id", "upstream_upstream_request_id", "fetched_at",
					"raw_json", "use_time", "upstream_channel_name", "upstream_channel_id",
					"status_code", "error_code", "error_type", "request_path", "join_key",
				}),
			}).
			Create(rows[start:end]).Error; err != nil {
			return fmt.Errorf("保存上游错误日志失败: %w", err)
		}
	}
	return nil
}

// pruneUpstreamErrorLogs 按保留期清理。
//
// 必须有：这张表按条存，不清理会无界增长。保留期与排障实际用法对齐——
// 用户明确说过「日志大多数情况下只是当天有用，过了今天再看意义不大，
// 统计时才需要」，故默认 31 天与我方排障的跨度上限一致。
func (m *Monitor) pruneUpstreamErrorLogs(ctx context.Context, before int64) error {
	if before <= 0 {
		return nil
	}
	if err := m.storeDB.WithContext(ctx).
		Where("created_at < ?", before).
		Delete(&ChannelUpstreamErrorLog{}).Error; err != nil {
		return fmt.Errorf("清理上游错误日志失败: %w", err)
	}
	return nil
}

// 调度参数。
const (
	// upstreamErrorLogSyncInterval 正常间隔。错误日志一天几百条，
	// 5 分钟一轮足够排障用（客户反馈到我方查看通常隔几分钟以上），
	// 又不至于给上游添压。
	upstreamErrorLogSyncInterval = 5 * time.Minute

	// upstreamErrorLogBackoffBase 失败退避基数，按连续失败次数指数增长、
	// 上限 upstreamErrorLogBackoffMax。上游挂了不该每 5 分钟去撞一次。
	upstreamErrorLogBackoffBase = 2 * time.Minute
	upstreamErrorLogBackoffMax  = 2 * time.Hour

	// upstreamErrorLogMaxRequestsPerRun 单账户单轮请求上限。
	// 与用量同步的预算机制同源：宁可标 Truncated 也不无限翻页。
	upstreamErrorLogMaxRequestsPerRun = 6

	// upstreamErrorLogLookbackOnFirstRun 首轮回看多久。
	// 没有水位时从这里起拉，不从「上游有史以来」起——那会翻很多页。
	upstreamErrorLogLookbackOnFirstRun = 6 * time.Hour

	// upstreamErrorLogOverlap 每轮从水位往前回退一点，防止边界漏行：
	// 上游写日志有延迟，严格从水位往后拉会漏掉「水位之前才落库」的那些。
	// 主键幂等，重叠部分重复拉不会产生重复行。
	upstreamErrorLogOverlap = 2 * time.Minute

	// upstreamErrorLogRetentionDays 本地保留期。
	// 用户口径：日志主要当天有用，隔日看得少，统计时才回溯。
	// 取 31 天与我方排障的跨度上限一致。
	upstreamErrorLogRetentionDays = 31
)

// syncDueUpstreamErrorLogs 是后台调度的入口：扫一遍到期的账户，逐个采集。
//
// 串行而非并发：与用量同步同一原则——上游速率限制是按账号算的，
// 并发只会更快撞限流；而错误日志量小，串行完全够。
func (m *Monitor) syncDueUpstreamErrorLogs(ctx context.Context) {
	if !m.cfg.UpstreamErrorLogSyncEnabled {
		return
	}
	now := time.Now().Unix()
	var rows []ChannelUpstreamAccount
	// 只取启用了日志同步授权的账户。UsageSyncEnabled 是管理员对
	// 「允许读这个上游的日志」的授权，错误日志同属日志，不绕过它。
	if err := m.storeDB.WithContext(ctx).
		Where("enabled = ? AND usage_sync_enabled = ?", true, true).
		Find(&rows).Error; err != nil {
		slog.Warn("读取上游账户失败，本轮错误日志采集跳过", "err", err)
		return
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return
		}
		m.syncOneUpstreamErrorLog(ctx, row, now)
	}
	// 顺带清理过期数据。放在同一轮里而不另起 goroutine：
	// 这张表增长慢，没必要为它多一个后台任务。
	cutoff := now - int64(upstreamErrorLogRetentionDays)*86400
	if err := m.pruneUpstreamErrorLogs(ctx, cutoff); err != nil {
		slog.Warn("清理上游错误日志失败", "err", err)
	}
}

// syncOneUpstreamErrorLog 采集单个账户。
//
// 不支持的 provider 落 unsupported 状态并**永不重试**（NextSyncAt=0 表示不排期）——
// sub2api/aicodewith 的端点是聚合/计价语义，重试一万次也不会变出日志接口。
// 但状态必须落库：页面要能区分「该上游无日志接口」与「该上游没有错误」。
func (m *Monitor) syncOneUpstreamErrorLog(ctx context.Context, row ChannelUpstreamAccount, now int64) {
	state := m.loadUpstreamErrorLogState(ctx, row.Domain)

	if row.Provider != upstreamProviderNewAPI {
		// 已经标过就不再重复写，省掉每轮一次无意义的更新。
		if state.Status == upstreamStatusUnsupported {
			return
		}
		state.Status = upstreamStatusUnsupported
		state.LastAttemptAt = now
		state.NextSyncAt = 0
		state.LastError = upstreamProviderName(row.Provider) + " 无日志接口（其端点为聚合/计价语义），无法采集上游错误日志"
		m.saveUpstreamErrorLogState(ctx, &state, now)
		return
	}
	if state.NextSyncAt > now {
		return
	}

	cred, err := m.credentialForAccount(row)
	if err != nil {
		m.failUpstreamErrorLogState(ctx, &state, now, fmt.Errorf("凭据不可用: %w", err))
		return
	}
	newCred, ok := cred.(newAPICredential)
	if !ok {
		m.failUpstreamErrorLogState(ctx, &state, now, fmt.Errorf("凭据类型与供应商不匹配"))
		return
	}

	from := state.SyncedUntil - int64(upstreamErrorLogOverlap.Seconds())
	if state.SyncedUntil <= 0 {
		from = now - int64(upstreamErrorLogLookbackOnFirstRun.Seconds())
	}
	to := now + 1 // 含当前秒，避免刚写入的日志被半开区间排除

	// 复用用量同步的请求间隔常量：同一个上游、同一个端点，
	// 节流口径没有理由不一致。
	pacer := newUpstreamUsageRequestPacer(upstreamErrorLogMaxRequestsPerRun,
		upstreamUsageRequestInterval)
	result, err := syncUpstreamErrorLogWindow(ctx, m.channelUpstreamHTTPClient(),
		row, newCred, from, to, pacer, now)
	if err != nil {
		m.failUpstreamErrorLogState(ctx, &state, now, err)
		return
	}
	if err := m.persistUpstreamErrorLogs(ctx, result.Rows); err != nil {
		m.failUpstreamErrorLogState(ctx, &state, now, err)
		return
	}

	state.Status = upstreamStatusOK
	state.LastAttemptAt = now
	state.LastSuccessAt = now
	state.ConsecutiveFails = 0
	state.LastError = ""
	state.RowsTotal += int64(len(result.Rows))
	// 水位只在窗口读完时才推进。被 Truncated 就停在原地，
	// 下一轮从同一处继续——否则未读完的那段会被永久跳过。
	if !result.Truncated {
		state.SyncedUntil = to
	}
	state.UnresolvedFields = encodeUnresolvedFields(result.UnresolvedFields)
	state.NextSyncAt = now + int64(upstreamErrorLogSyncInterval.Seconds())
	m.saveUpstreamErrorLogState(ctx, &state, now)

	if result.Truncated {
		slog.Info("上游错误日志窗口未读完，下轮从同一水位继续",
			"domain", row.Domain, "rows", len(result.Rows), "total", result.Total)
	}
	if len(result.UnresolvedFields) > 0 {
		// 字段名可能猜错——这是唯一的观测出口，必须显式告警而不是静默。
		slog.Warn("上游错误日志有字段未命中任何候选名，原文已留存可就地重解",
			"domain", row.Domain, "unresolved", state.UnresolvedFields)
	}
}

// encodeUnresolvedFields 把未命中统计编成紧凑 JSON 文本，供落库与页面展示。
func encodeUnresolvedFields(counts map[string]int) string {
	if len(counts) == 0 {
		return ""
	}
	// 排序保证同样的输入得到同样的文本，避免 map 遍历顺序让这一列每轮都"变化"。
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ordered := make([][2]any, 0, len(keys))
	for _, k := range keys {
		ordered = append(ordered, [2]any{k, counts[k]})
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, kv := range ordered {
		if i > 0 {
			b.WriteByte(',')
		}
		name, _ := json.Marshal(kv[0])
		b.Write(name)
		b.WriteByte(':')
		fmt.Fprintf(&b, "%d", kv[1])
	}
	b.WriteByte('}')
	return boundedUpstreamErrorField(b.String(), 512)
}

func (m *Monitor) loadUpstreamErrorLogState(ctx context.Context, domain string) UpstreamErrorLogSyncState {
	var state UpstreamErrorLogSyncState
	if err := m.storeDB.WithContext(ctx).First(&state, "domain = ?", domain).Error; err != nil {
		// 找不到就是首轮，返回零值（NextSyncAt=0 → 立即可同步）。
		return UpstreamErrorLogSyncState{Domain: domain}
	}
	return state
}

func (m *Monitor) saveUpstreamErrorLogState(ctx context.Context, state *UpstreamErrorLogSyncState, now int64) {
	state.UpdatedAt = now
	if err := m.storeDB.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "domain"}},
			UpdateAll: true,
		}).
		Create(state).Error; err != nil {
		slog.Warn("保存上游错误日志采集状态失败", "domain", state.Domain, "err", err)
	}
}

// failUpstreamErrorLogState 记一次失败并按连续失败次数指数退避。
//
// **不推进水位**：失败那段必须留给下一轮重试，否则会永久漏掉。
func (m *Monitor) failUpstreamErrorLogState(ctx context.Context, state *UpstreamErrorLogSyncState, now int64, cause error) {
	state.Status = upstreamStatusError
	state.LastAttemptAt = now
	state.ConsecutiveFails++
	state.LastError = boundedUpstreamErrorField(cause.Error(), 512)
	backoff := upstreamErrorLogBackoffBase << min(state.ConsecutiveFails-1, 6)
	if backoff > upstreamErrorLogBackoffMax {
		backoff = upstreamErrorLogBackoffMax
	}
	state.NextSyncAt = now + int64(backoff.Seconds())
	m.saveUpstreamErrorLogState(ctx, state, now)
	slog.Warn("上游错误日志采集失败", "domain", state.Domain,
		"fails", state.ConsecutiveFails, "retry_after_s", int64(backoff.Seconds()), "err", cause)
}

// boundedUpstreamErrorField 截断到列宽并去掉首尾空白。
//
// 按**字节**截而不是按 rune：列宽是字节数，按 rune 截仍可能超。
// 但要避免把一个 UTF-8 字符切成两半（那会产生非法编码），所以退到最近的
// 字符边界。中文错误原文很常见，这个细节不能省。
func boundedUpstreamErrorField(s string, maxBytes int) string {
	s = strings.TrimSpace(s)
	if len(s) <= maxBytes {
		return s
	}
	cut := maxBytes
	// UTF-8 续字节形如 10xxxxxx；往前退到首字节为止。
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}
	return s[:cut]
}
