package monitor

// 入口协议派生。
//
// 为什么叫 ingress_protocol 而不是 protocol：这个值只说明**客户端打进来的端点形状**，
// 它不证明上游用了同一协议，也不证明协议调用成功。字段名自带边界，比靠注释约束可靠。
// 01.3 §8 聚合键里的 protocol 维度由本字段承担，页面与接口一律标注「入口协议（派生）」。
//
// 数据来源：生产 logs 表没有原生 protocol 字段，只有 other.request_path 与 is_stream。
// logchain.go:280-293 记录的 2026-08-25 实测（197371 行 type=2）显示 request_path
// 填充率 100%，取值只有四种。**但那次实测只覆盖 type=2**；type=5 错误行的填充率
// 截至 2026-10-09 未实测（用户明确不授权查生产库），所以本派生不得宣称完整覆盖。
// 正因如此，判定顺序是**先异常后正常**：缺失走 ingressUnknown，绝不兜底成某个具体协议。
//
// 派生成功不提高 evidence_level：路径字段齐全只说明入口可识别，与上游证据强度无关。
// 由 TestIngressProtocolNeverRaisesEvidenceLevel 钉死。

// IngressProtocol 是入口协议派生结果。
//
// 五个正常值对应 01.3 §7 的五类协议；三个异常值不可被折叠进正常值——
// 「没采到」「出现新端点」「数据自相矛盾」是三种不同的事实，处置也不同。
type IngressProtocol string

const (
	IngressChatNonStream   IngressProtocol = "chat_nonstream"
	IngressChatSSE         IngressProtocol = "chat_sse"
	IngressResponses       IngressProtocol = "responses"
	IngressClaudeNonStream IngressProtocol = "claude_nonstream"
	IngressClaudeSSE       IngressProtocol = "claude_sse"

	// ingressUnknown：other 为 NULL、非法 JSON，或 request_path 缺失/空。
	// 「没采到」——不参与协议健康判断，无观察点不等于正常。
	IngressUnknown IngressProtocol = "ingress_unknown"

	// ingressUnrecognized：路径有值但不在白名单。
	// 「出现了新端点」——这是需要人去看的信号，不能和「没采到」混在一起。
	IngressUnrecognized IngressProtocol = "ingress_unrecognized"

	// ingressConflict：数据自相矛盾（如 Chat 路径但 is_stream 未知，
	// 无法区分 SSE 与非流式）。
	IngressConflict IngressProtocol = "ingress_conflict"
)

// IngressProtocolRuleVersion 是派生规则版本。
//
// 规则变更时递增（v1 → v2）。历史事故保留当时版本，不被追溯改写。
// **不进聚合键**——否则改一次规则会让全部在途事故重新建单；
// 由 TestIngressProtocolRuleVersionNotInDedupKey 钉住。
const IngressProtocolRuleVersion = "ingress-proto-v1"

// 四条白名单路径。/pg/chat/completions 是 playground 前缀，协议形状与
// /v1/chat/completions 相同，故归同一类；原值由 ingress_route_raw 另存，
// 聚合时合并、追溯时可分。
const (
	ingressRouteChatV1    = "/v1/chat/completions"
	ingressRouteChatPG    = "/pg/chat/completions"
	ingressRouteResponses = "/v1/responses"
	ingressRouteMessages  = "/v1/messages"
)

// IngressProtocolInput 是派生输入，全部为源数据原值。
//
// RouteRaw 为 request_path 原值；OtherUsable 表示 other 列可解析
// （非 NULL 且 JSON_VALID）；Stream 为 is_stream 原值，nil 表示 NULL。
// 用 *bool 而不是 bool：NULL 必须保持未知，不能写成 false（宪法：缺失绝不显示为零）。
type IngressProtocolInput struct {
	RouteRaw    string
	OtherUsable bool
	Stream      *bool
}

// DeriveIngressProtocol 按「先异常后正常」顺序派生入口协议。
//
// 顺序是刻意的：若先匹配白名单再处理异常，缺失值会在某个分支被静默兜底成
// chat_nonstream（最常见的路径），那正是用户明确禁止的行为。
func DeriveIngressProtocol(in IngressProtocolInput) IngressProtocol {
	// 1. other 不可解析，或路径缺失/空 → 没采到。
	if !in.OtherUsable {
		return IngressUnknown
	}
	route := in.RouteRaw
	if route == "" || route == "null" {
		return IngressUnknown
	}

	// 2. 路径不在白名单 → 出现新端点，与「没采到」区分开。
	//    放在流式判断之前：路径本身就不认识时，再谈流式状态没有意义。
	switch route {
	case ingressRouteChatV1, ingressRouteChatPG, ingressRouteResponses, ingressRouteMessages:
	default:
		return IngressUnrecognized
	}

	// 3. Responses 不依赖 is_stream：01.3 §7 的五类里 Responses 只有一类，
	//    没有拆流式与非流式。所以即使 is_stream 未知，这一类仍可确定。
	if route == ingressRouteResponses {
		return IngressResponses
	}

	// 4. Chat 与 Claude 必须靠 is_stream 区分 SSE 与非流式。
	//    is_stream 未知时无法区分 → 数据矛盾，不猜其中一个。
	if in.Stream == nil {
		return IngressConflict
	}

	switch route {
	case ingressRouteChatV1, ingressRouteChatPG:
		if *in.Stream {
			return IngressChatSSE
		}
		return IngressChatNonStream
	case ingressRouteMessages:
		if *in.Stream {
			return IngressClaudeSSE
		}
		return IngressClaudeNonStream
	}

	// 不可达：上面的白名单校验已穷尽所有分支。保留以防白名单新增时漏改。
	return IngressUnrecognized
}

// IsDerived 区分「派生出了真实协议」与「派生失败/数据异常」。
//
// 调用方据此决定是否参与协议健康判断：三个异常值是合法的聚合键取值
// （不丢数据、照常建事故），但不能参与「协议是否健康」的结论。
func (p IngressProtocol) IsDerived() bool {
	switch p {
	case IngressChatNonStream, IngressChatSSE, IngressResponses,
		IngressClaudeNonStream, IngressClaudeSSE:
		return true
	}
	return false
}

// Valid 校验取值在八个枚举之内。
func (p IngressProtocol) Valid() bool {
	switch p {
	case IngressChatNonStream, IngressChatSSE, IngressResponses,
		IngressClaudeNonStream, IngressClaudeSSE,
		IngressUnknown, IngressUnrecognized, IngressConflict:
		return true
	}
	return false
}

// DisplayName 返回页面文案。三个异常值不显示成某个具体协议。
func (p IngressProtocol) DisplayName() string {
	switch p {
	case IngressChatNonStream:
		return "Chat 非流式"
	case IngressChatSSE:
		return "Chat SSE"
	case IngressResponses:
		return "Responses"
	case IngressClaudeNonStream:
		return "Claude 非流式"
	case IngressClaudeSSE:
		return "Claude SSE"
	case IngressUnknown:
		return "入口协议未知"
	case IngressUnrecognized:
		return "入口协议未识别"
	case IngressConflict:
		return "入口协议数据矛盾"
	}
	return "入口协议未知"
}
