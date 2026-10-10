package monitor

// Incident 聚合键与去重。
//
// 按 01.3 §8：primary_fault_class + channel + model + user_group + protocol + route，
// 外加 config_change_ref 分段。**滚动时间窗口绝不进键**——否则窗口一滚动就重复建单，
// 这是 MON-003 要解决的核心问题。
//
// 去重靠数据库唯一约束（dedup_key UNIQUE + ON CONFLICT DO UPDATE），
// 不靠应用层先查后写：并发下先查后写必然产生重复行。

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
)

// IncidentChannelScope 区分三种渠道状态。
//
// 为什么不用 channel_id = 0：既有代码里 0 同时承担「未命中渠道」和「渠道未知」
// 两种含义，而这两件事必须分开——「确知没有渠道」是事实，「不知道有没有渠道」是缺口。
// 用户明确要求 route_no_channel 不得与「渠道信息缺失」混为一谈。
type IncidentChannelScope string

const (
	// 正常命中渠道，ChannelID 非 nil，可归责该渠道。
	ChannelScopeAssigned IncidentChannelScope = "assigned"
	// 路由前被拒，确实没有分配渠道（route_no_channel）。ChannelID 为 nil。
	// 这是**已确知的事实**，不是缺失。
	ChannelScopeNotAssigned IncidentChannelScope = "not_assigned"
	// 采集缺口，渠道信息没采到。ChannelID 为 nil。
	ChannelScopeUnknown IncidentChannelScope = "unknown"
)

// Valid 校验取值。
func (s IncidentChannelScope) Valid() bool {
	switch s {
	case ChannelScopeAssigned, ChannelScopeNotAssigned, ChannelScopeUnknown:
		return true
	}
	return false
}

// DisplayName 返回页面文案。绝不显示「渠道 0」。
func (s IncidentChannelScope) DisplayName() string {
	switch s {
	case ChannelScopeAssigned:
		return "已分配渠道"
	case ChannelScopeNotAssigned:
		return "未分配渠道"
	case ChannelScopeUnknown:
		return "渠道信息缺失"
	}
	return "渠道信息缺失"
}

// IncidentKey 是事故聚合键的六个维度加配置变更段。
//
// ChannelID 用 *int64：ChannelScope 非 assigned 时为 nil，不写 0
// （宪法：缺失绝不显示为零）。
type IncidentKey struct {
	PrimaryFaultClass string

	ChannelScope IncidentChannelScope
	ChannelID    *int64

	Model string

	// UserGroup 是 logs.group，即 NewAPI 网站分组（website_group）。
	// **不是** CustomerGroup.ID（Monitor 本地组织维度）。两者严格分离，不得混用。
	UserGroup string

	// IngressProtocol 是派生的入口协议（八个枚举之一，含三个异常值）。
	// 异常值也是合法键值——照常建事故，不丢数据。
	IngressProtocol IngressProtocol

	// Route 是归一化后的路由。/pg/ 与 /v1/ 的 chat 路径在协议上合并，
	// 但这里保留归一化值用于聚合；原值另存 ingress_route_raw。
	Route string

	// ConfigChangeRef 为空串表示无变更段。它一变即换键，
	// 从而实现配置变更前后自动分段保留（01.3 §8）。
	ConfigChangeRef string
}

// 字段分隔符。用 \x1f（unit separator）而不是常见的 | 或 :，
// 因为模型名、分组名里可能出现后者——曾经发生过「分组名含空格被截断」一类问题。
const incidentKeySep = "\x1f"

// channelKeyPart 把渠道三态编成键片段。
//
// assigned 带上具体 ID；另两种只带 scope 且 ID 位留空。
// 因此「未分配渠道」与「渠道未知」天然产生不同的键，结构上不可能合并。
func (k IncidentKey) channelKeyPart() string {
	switch k.ChannelScope {
	case ChannelScopeAssigned:
		if k.ChannelID == nil {
			// scope 说有渠道但 ID 缺失：这是上游构造错误，
			// 降级为 unknown 而不是假装成渠道 0。
			return string(ChannelScopeUnknown) + ":"
		}
		return string(ChannelScopeAssigned) + ":" + strconv.FormatInt(*k.ChannelID, 10)
	case ChannelScopeNotAssigned:
		return string(ChannelScopeNotAssigned) + ":"
	default:
		return string(ChannelScopeUnknown) + ":"
	}
}

// Canonical 返回规范化键串。
//
// 注意这里刻意**不含**任何时间、窗口、扫描序号或规则版本：
//   - 时间窗口入键会导致窗口滚动即重复建单（MON-003 的核心问题）
//   - ingress_rule_version 入键会导致改一次派生规则就让全部在途事故重开
func (k IncidentKey) Canonical() string {
	parts := []string{
		"fc=" + k.PrimaryFaultClass,
		"ch=" + k.channelKeyPart(),
		"model=" + k.Model,
		"grp=" + k.UserGroup,
		"iproto=" + string(k.IngressProtocol),
		"route=" + k.Route,
		"cfg=" + k.ConfigChangeRef,
	}
	return strings.Join(parts, incidentKeySep)
}

// DedupKey 返回规范键的 SHA-256 十六进制串，作为 Incident 表的唯一索引值。
//
// 用哈希而不是直接存规范串：规范串含模型名与分组名，长度不定且可能很长，
// 做 UNIQUE 索引在 SQLite 上有长度与排序开销。哈希定长 64 字符。
// 规范串本身也一并落库（dedup_key_canonical），保证可追溯、可人工核对。
func (k IncidentKey) DedupKey() string {
	sum := sha256.Sum256([]byte(k.Canonical()))
	return hex.EncodeToString(sum[:])
}

// NormalizeIncidentRoute 把路由归一化用于聚合。
//
// /pg/chat/completions 与 /v1/chat/completions 协议形状相同，聚合时合并到
// /v1/chat/completions；原值由调用方存入 ingress_route_raw，追溯时可分。
// 其余路径原样保留；空值归一为 unknown 标记而不是空串，避免与「字段存在但为空」混淆。
func NormalizeIncidentRoute(raw string) string {
	switch raw {
	case ingressRouteChatPG:
		return ingressRouteChatV1
	case "", "null":
		return "route_unknown"
	}
	return raw
}
