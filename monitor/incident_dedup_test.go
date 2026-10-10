package monitor

import "testing"

// int64Ptr 复用 usage_facts_jobs.go:1853 的既有定义，不重复声明。

func baseKey() IncidentKey {
	return IncidentKey{
		PrimaryFaultClass: "upstream_5xx",
		ChannelScope:      ChannelScopeAssigned,
		ChannelID:         int64Ptr(115),
		Model:             "gpt-4o",
		UserGroup:         "default",
		IngressProtocol:   IngressChatSSE,
		Route:             "/v1/chat/completions",
	}
}

// 六个维度任一变化都必须产生不同的键。
func TestIncidentDedupKeySixDimensions(t *testing.T) {
	base := baseKey().DedupKey()

	variants := map[string]IncidentKey{}

	k1 := baseKey()
	k1.PrimaryFaultClass = "transport_timeout"
	variants["fault_class"] = k1

	k2 := baseKey()
	k2.ChannelID = int64Ptr(116)
	variants["channel"] = k2

	k3 := baseKey()
	k3.Model = "claude-sonnet-4"
	variants["model"] = k3

	k4 := baseKey()
	k4.UserGroup = "vip"
	variants["user_group"] = k4

	k5 := baseKey()
	k5.IngressProtocol = IngressChatNonStream
	variants["ingress_protocol"] = k5

	k6 := baseKey()
	k6.Route = "/v1/messages"
	variants["route"] = k6

	seen := map[string]string{base: "base"}
	for name, k := range variants {
		got := k.DedupKey()
		if got == base {
			t.Errorf("%s 变化后键未变", name)
		}
		if prev, dup := seen[got]; dup {
			t.Errorf("%s 与 %s 键冲突", name, prev)
		}
		seen[got] = name
	}
}

// 滚动时间窗口绝不进键——这是 MON-003 的核心要求。
func TestIncidentDedupKeyExcludesRollingWindow(t *testing.T) {
	k := baseKey()
	canon := k.Canonical()
	// 键里不得出现任何时间戳量级的数字串或窗口字样。
	for _, forbidden := range []string{"window", "from_ts", "to_ts", "1760", "last_seen"} {
		if indexOf(canon, forbidden) >= 0 {
			t.Errorf("规范键含时间/窗口信息 %q: %s", forbidden, canon)
		}
	}
	// 同一键在不同时刻必须稳定。
	if k.DedupKey() != baseKey().DedupKey() {
		t.Fatal("同一键两次计算结果不同")
	}
}

// config_change_ref 变化即分段，前后事故分开保留。
func TestIncidentConfigChangeRefSegments(t *testing.T) {
	before := baseKey()
	before.ConfigChangeRef = ""
	after := baseKey()
	after.ConfigChangeRef = "cfg-20261009-1430"

	if before.DedupKey() == after.DedupKey() {
		t.Fatal("配置变更前后键相同，未实现分段")
	}

	third := baseKey()
	third.ConfigChangeRef = "cfg-20261009-1600"
	if third.DedupKey() == after.DedupKey() {
		t.Fatal("不同配置变更段键相同")
	}
}

// 不同渠道绝不合并。
func TestIncidentDifferentChannelNeverMerges(t *testing.T) {
	a := baseKey()
	a.ChannelID = int64Ptr(115)
	b := baseKey()
	b.ChannelID = int64Ptr(116)
	if a.DedupKey() == b.DedupKey() {
		t.Fatal("不同渠道被合并")
	}
}

// 不同入口协议绝不合并。
func TestIncidentDifferentProtocolNeverMerges(t *testing.T) {
	protos := []IngressProtocol{
		IngressChatNonStream, IngressChatSSE, IngressResponses,
		IngressClaudeNonStream, IngressClaudeSSE,
		IngressUnknown, IngressUnrecognized, IngressConflict,
	}
	seen := map[string]IngressProtocol{}
	for _, p := range protos {
		k := baseKey()
		k.IngressProtocol = p
		key := k.DedupKey()
		if prev, dup := seen[key]; dup {
			t.Fatalf("协议 %q 与 %q 键冲突", p, prev)
		}
		seen[key] = p
	}
}

// 「未分配渠道」绝不关联到任何真实渠道。
func TestIncidentNotAssignedChannelNeverLinksRealChannel(t *testing.T) {
	na := baseKey()
	na.PrimaryFaultClass = "route_no_channel"
	na.ChannelScope = ChannelScopeNotAssigned
	na.ChannelID = nil

	// 与任何真实渠道 ID 的键都不同，包括渠道 0。
	for _, id := range []int64{0, 1, 115, 999} {
		real := baseKey()
		real.PrimaryFaultClass = "route_no_channel"
		real.ChannelScope = ChannelScopeAssigned
		real.ChannelID = int64Ptr(id)
		if na.DedupKey() == real.DedupKey() {
			t.Fatalf("未分配渠道与真实渠道 %d 键相同", id)
		}
	}
}

// 「未分配渠道」与「渠道信息缺失」是两种不同事实，不得混为一谈。
func TestIncidentNotAssignedDistinctFromUnknownChannel(t *testing.T) {
	na := baseKey()
	na.ChannelScope = ChannelScopeNotAssigned
	na.ChannelID = nil

	unk := baseKey()
	unk.ChannelScope = ChannelScopeUnknown
	unk.ChannelID = nil

	if na.DedupKey() == unk.DedupKey() {
		t.Fatal("「未分配渠道」与「渠道信息缺失」被合并")
	}
	if na.ChannelScope.DisplayName() == unk.ChannelScope.DisplayName() {
		t.Fatal("两者页面文案相同，用户无法区分")
	}
}

// 渠道 ID 为空时不写 0，页面不显示「渠道 0」。
func TestIncidentChannelIDNullNotZero(t *testing.T) {
	for _, scope := range []IncidentChannelScope{ChannelScopeNotAssigned, ChannelScopeUnknown} {
		k := baseKey()
		k.ChannelScope = scope
		k.ChannelID = nil
		if k.ChannelID != nil {
			t.Fatalf("%s 的 ChannelID 应为 nil", scope)
		}
		// 键片段里不得出现 ":0"。
		canon := k.Canonical()
		if indexOf(canon, "ch="+string(scope)+":0") >= 0 {
			t.Fatalf("%s 的键里出现了渠道 0: %s", scope, canon)
		}
	}
	// 文案不得出现「渠道 0」。
	for _, s := range []IncidentChannelScope{ChannelScopeAssigned, ChannelScopeNotAssigned, ChannelScopeUnknown} {
		if indexOf(s.DisplayName(), "0") >= 0 {
			t.Fatalf("%s 文案含 0: %s", s, s.DisplayName())
		}
	}
}

// scope 声明 assigned 但 ID 缺失时降级为 unknown，不假装成渠道 0。
func TestIncidentAssignedWithoutIDDegradesToUnknown(t *testing.T) {
	broken := baseKey()
	broken.ChannelScope = ChannelScopeAssigned
	broken.ChannelID = nil

	unk := baseKey()
	unk.ChannelScope = ChannelScopeUnknown
	unk.ChannelID = nil

	if broken.DedupKey() != unk.DedupKey() {
		t.Fatal("assigned 但 ID 缺失时应降级为 unknown")
	}
	zero := baseKey()
	zero.ChannelScope = ChannelScopeAssigned
	zero.ChannelID = int64Ptr(0)
	if broken.DedupKey() == zero.DedupKey() {
		t.Fatal("ID 缺失被当成了渠道 0")
	}
}

// 含分隔符或空格的模型名、分组名不得串键。
func TestIncidentDedupKeyHandlesSeparatorsInNames(t *testing.T) {
	a := baseKey()
	a.Model = "gpt-4o"
	a.UserGroup = "team default"

	b := baseKey()
	b.Model = "gpt-4o\x1fteam"
	b.UserGroup = "default"

	if a.DedupKey() == b.DedupKey() {
		t.Fatal("字段值含分隔符导致键串台")
	}

	// 带空格的分组名必须完整保留（既有教训：分组名含空格曾被截断）。
	spaced := baseKey()
	spaced.UserGroup = "my group"
	if indexOf(spaced.Canonical(), "my group") < 0 {
		t.Fatalf("带空格分组名未完整保留: %s", spaced.Canonical())
	}
}

// 规范键可人工核对，哈希定长。
func TestIncidentDedupKeyShape(t *testing.T) {
	k := baseKey()
	if len(k.DedupKey()) != 64 {
		t.Fatalf("哈希长度 =%d want 64", len(k.DedupKey()))
	}
	canon := k.Canonical()
	for _, want := range []string{"fc=", "ch=", "model=", "grp=", "iproto=", "route=", "cfg="} {
		if indexOf(canon, want) < 0 {
			t.Errorf("规范键缺少字段前缀 %q: %s", want, canon)
		}
	}
}

// 路由归一化：空值归为显式标记而不是空串。
func TestNormalizeIncidentRouteEmptyIsExplicit(t *testing.T) {
	for _, raw := range []string{"", "null"} {
		if got := NormalizeIncidentRoute(raw); got != "route_unknown" {
			t.Errorf("route=%q 归一化为 %q want route_unknown", raw, got)
		}
	}
	if got := NormalizeIncidentRoute("/v1/messages"); got != "/v1/messages" {
		t.Errorf("正常路由被改写: %q", got)
	}
}

// 渠道 scope 枚举校验。
func TestIncidentChannelScopeValid(t *testing.T) {
	for _, s := range []IncidentChannelScope{ChannelScopeAssigned, ChannelScopeNotAssigned, ChannelScopeUnknown} {
		if !s.Valid() {
			t.Errorf("%q 应合法", s)
		}
	}
	if IncidentChannelScope("none").Valid() {
		t.Fatal("未定义取值不应合法")
	}
}
