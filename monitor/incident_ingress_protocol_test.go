package monitor

import "testing"

// boolPtr 复用 logchain_fault.go:152 的既有定义，不重复声明。

// 四条路径 × is_stream 的八组组合逐一对映射表。
func TestIngressProtocolMapsFourPathsToFiveClasses(t *testing.T) {
	cases := []struct {
		route  string
		stream bool
		want   IngressProtocol
	}{
		{"/v1/chat/completions", false, IngressChatNonStream},
		{"/v1/chat/completions", true, IngressChatSSE},
		{"/pg/chat/completions", false, IngressChatNonStream},
		{"/pg/chat/completions", true, IngressChatSSE},
		{"/v1/responses", false, IngressResponses},
		{"/v1/responses", true, IngressResponses},
		{"/v1/messages", false, IngressClaudeNonStream},
		{"/v1/messages", true, IngressClaudeSSE},
	}
	for _, c := range cases {
		got := DeriveIngressProtocol(IngressProtocolInput{
			RouteRaw: c.route, OtherUsable: true, Stream: boolPtr(c.stream),
		})
		if got != c.want {
			t.Errorf("route=%s stream=%v: got %q want %q", c.route, c.stream, got, c.want)
		}
		if !got.IsDerived() {
			t.Errorf("route=%s stream=%v: 正常映射结果应为已派生", c.route, c.stream)
		}
	}
}

// 路径缺失必须是 unknown，绝不能兜底成最常见的 chat_nonstream。
func TestIngressProtocolMissingPathIsUnknownNotChat(t *testing.T) {
	for _, raw := range []string{"", "null"} {
		got := DeriveIngressProtocol(IngressProtocolInput{
			RouteRaw: raw, OtherUsable: true, Stream: boolPtr(false),
		})
		if got != IngressUnknown {
			t.Fatalf("route=%q: got %q want %q", raw, got, IngressUnknown)
		}
		if got == IngressChatNonStream {
			t.Fatalf("route=%q 被兜底成了 Chat 非流式", raw)
		}
		if got.IsDerived() {
			t.Fatalf("route=%q: unknown 不应算作已派生", raw)
		}
	}
}

// other 列不可解析（NULL 或非法 JSON）时为 unknown。
func TestIngressProtocolInvalidJSONIsUnknown(t *testing.T) {
	got := DeriveIngressProtocol(IngressProtocolInput{
		RouteRaw: "/v1/chat/completions", OtherUsable: false, Stream: boolPtr(true),
	})
	if got != IngressUnknown {
		t.Fatalf("got %q want %q", got, IngressUnknown)
	}
}

// 白名单外的新端点是 unrecognized，与 unknown 区分开——
// 「出现了新端点」和「没采到」是两种不同的事实。
func TestIngressProtocolUnlistedPathIsUnrecognized(t *testing.T) {
	for _, raw := range []string{"/v1/embeddings", "/v1/audio/speech", "/v2/chat/completions"} {
		got := DeriveIngressProtocol(IngressProtocolInput{
			RouteRaw: raw, OtherUsable: true, Stream: boolPtr(false),
		})
		if got != IngressUnrecognized {
			t.Fatalf("route=%q: got %q want %q", raw, got, IngressUnrecognized)
		}
		if got == IngressUnknown {
			t.Fatalf("route=%q: 新端点被混成了 unknown", raw)
		}
	}
}

// Chat/Claude 路径遇 is_stream 为 NULL 时无法区分 SSE 与非流式 → conflict，不猜。
func TestIngressProtocolNullStreamIsConflictForChat(t *testing.T) {
	for _, raw := range []string{"/v1/chat/completions", "/pg/chat/completions", "/v1/messages"} {
		got := DeriveIngressProtocol(IngressProtocolInput{
			RouteRaw: raw, OtherUsable: true, Stream: nil,
		})
		if got != IngressConflict {
			t.Fatalf("route=%q stream=nil: got %q want %q", raw, got, IngressConflict)
		}
	}
}

// Responses 不依赖 is_stream（01.3 §7 五类里 Responses 未拆流式），
// 所以 is_stream 未知也能确定该类。
func TestIngressProtocolNullStreamStillResponses(t *testing.T) {
	got := DeriveIngressProtocol(IngressProtocolInput{
		RouteRaw: "/v1/responses", OtherUsable: true, Stream: nil,
	})
	if got != IngressResponses {
		t.Fatalf("got %q want %q", got, IngressResponses)
	}
}

// /pg/ 归入 chat 类用于聚合，但路由原值必须可追溯（归一化不丢原值）。
func TestIngressProtocolPgPrefixSharesClassKeepsRawRoute(t *testing.T) {
	pg := DeriveIngressProtocol(IngressProtocolInput{
		RouteRaw: "/pg/chat/completions", OtherUsable: true, Stream: boolPtr(true),
	})
	v1 := DeriveIngressProtocol(IngressProtocolInput{
		RouteRaw: "/v1/chat/completions", OtherUsable: true, Stream: boolPtr(true),
	})
	if pg != v1 {
		t.Fatalf("/pg/ 与 /v1/ 的 chat 应归同一协议类: %q vs %q", pg, v1)
	}
	// 归一化合并用于聚合，但原值由调用方存 ingress_route_raw。
	if NormalizeIncidentRoute("/pg/chat/completions") != "/v1/chat/completions" {
		t.Fatal("聚合归一化未把 /pg/ 合并到 /v1/")
	}
	if NormalizeIncidentRoute("/pg/chat/completions") == "/pg/chat/completions" {
		t.Fatal("归一化应当合并")
	}
}

// 八个枚举值全部合法，三个异常值不算已派生。
func TestIngressProtocolEnumAndDerivedFlag(t *testing.T) {
	all := []IngressProtocol{
		IngressChatNonStream, IngressChatSSE, IngressResponses,
		IngressClaudeNonStream, IngressClaudeSSE,
		IngressUnknown, IngressUnrecognized, IngressConflict,
	}
	for _, p := range all {
		if !p.Valid() {
			t.Errorf("%q 应为合法枚举", p)
		}
		if p.DisplayName() == "" {
			t.Errorf("%q 缺少页面文案", p)
		}
	}
	for _, p := range []IngressProtocol{IngressUnknown, IngressUnrecognized, IngressConflict} {
		if p.IsDerived() {
			t.Errorf("%q 是异常值，不应算作已派生", p)
		}
	}
	if IngressProtocol("chat").Valid() {
		t.Fatal("未定义取值不应合法")
	}
}

// 三个异常值的页面文案不得显示成某个具体协议。
func TestIngressProtocolAbnormalDisplayNotConcreteProtocol(t *testing.T) {
	concrete := map[string]bool{
		"Chat 非流式": true, "Chat SSE": true, "Responses": true,
		"Claude 非流式": true, "Claude SSE": true,
	}
	for _, p := range []IngressProtocol{IngressUnknown, IngressUnrecognized, IngressConflict} {
		if concrete[p.DisplayName()] {
			t.Fatalf("%q 的文案 %q 冒充了具体协议", p, p.DisplayName())
		}
	}
}

// 规则版本不进聚合键：改一次派生规则不应让在途事故全部重开。
func TestIngressProtocolRuleVersionNotInDedupKey(t *testing.T) {
	k := IncidentKey{
		PrimaryFaultClass: "upstream_5xx",
		ChannelScope:      ChannelScopeNotAssigned,
		Model:             "gpt-4o",
		UserGroup:         "default",
		IngressProtocol:   IngressChatSSE,
		Route:             "/v1/chat/completions",
	}
	canon := k.Canonical()
	if contains := len(canon) > 0 && (indexOf(canon, IngressProtocolRuleVersion) >= 0); contains {
		t.Fatalf("规范键里出现了规则版本: %q", canon)
	}
	if IngressProtocolRuleVersion == "" {
		t.Fatal("规则版本常量不应为空")
	}
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
