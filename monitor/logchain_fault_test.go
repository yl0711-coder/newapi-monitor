package monitor

// logchain_fault_test.go：归因规则的行为约束。
//
// 全部用例都取自 2026-08-21 / 08-24 两天的**生产真实原文**，不是编的。
// 注释里记了实测条数——那是判断某条规则可信度的唯一依据。
//
// 为什么这份测试比别的更要紧：归因判错的代价不是"少看到信息"，而是**找错人**。
// 判成"我方配置"会让人去改自己的渠道配置，而问题其实在上游。
// 我在评估阶段对同一个案子连错三次，全都是这个方向的错。

import (
	"strings"
	"testing"
)

// TestLogChainFaultMessageSourceBeatsStatusCode ★本文件最关键的一条★
//
// 上游会用 503/404 这种"看起来像上游故障"的状态码报告它自己的路由失败。
// 判别来源的唯一依据是 content 是否以 "status_code=" 开头：
// 有前缀 = 上游返回（new-api 原样抄录），无前缀 = 我方 new-api 自己生成。
//
// 2026-08-24 实测：这类消息 115 条，**无一例外都带 status_code 前缀**，全部来自上游。
// 其中 112 条集中在渠道 74/48，是那天错误率从 0.3% 飙到 4.97% 的原因。
func TestLogChainFaultMessageSourceBeatsStatusCode(t *testing.T) {
	for _, tc := range []struct {
		name      string
		content   string
		wantFault string
	}{
		{
			// 生产原文（渠道 74，08-24，实测 71 条同形）
			name:      "上游说它没有可用账号_带前缀_判上游",
			content:   `status_code=404, Model "gpt-5.4" is not supported by any configured account in this group`,
			wantFault: faultUpstream,
		},
		{
			// 生产原文（渠道 32/39，08-24，实测 2 条）。状态码是 503，
			// 若只看状态码会判上游——碰巧对，但理由错了；这里要的是按语义判对。
			name:      "上游无可用上游_带前缀_判上游",
			content:   "status_code=503, no available upstream in group (request id: 20260824000152110505362)",
			wantFault: faultUpstream,
		},
		{
			// 生产原文（渠道 79，08-21，实测 1 条）
			name:      "上游无可用渠道_带前缀_判上游",
			content:   "status_code=503, No available channel for model claude-fable-5 under group Claude max (distributor)",
			wantFault: faultUpstream,
		},
		{
			// **同样措辞但无 status_code 前缀** → 是我方 new-api 自己的路由失败。
			// 实测数据里目前 0 条，保留规则以防上游版本变化或我方行为改变。
			name:      "同样措辞_无前缀_判我方",
			content:   "No available channel for model gpt-4o under group default",
			wantFault: faultOurs,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := logChainAttributeFault(LogChainRow{Type: 5, Content: tc.content}, nil)
			if got.Fault != tc.wantFault {
				t.Errorf("归因错误\n原文: %s\ngot=%s want=%s\n依据: %s",
					tc.content, got.Fault, tc.wantFault, got.Why)
			}
			if got.Why == "" {
				t.Error("必须给出依据——没有依据的结论无法复核")
			}
		})
	}
}

// TestLogChainFaultNeverGuessesOnAmbiguous 模糊的必须判「待判」，绝不猜。
//
// 没有这一档，规则会被迫对每条给答案，而模糊的那些会得到看似确定实则瞎猜的结论。
func TestLogChainFaultNeverGuessesOnAmbiguous(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		reason  string
	}{
		{
			// 生产原文（08-21 实测 12 条 / 08-24 实测 3 条）。措辞指向上游，
			// 但无法排除我方限流阈值或客户突发——要真正区分需要上游侧配额数据（未采集）。
			name:    "429限流",
			content: "status_code=429, Upstream rate limit exceeded, please retry later",
			reason:  "无法区分上游配额、我方限流与客户突发",
		},
		{
			// 生产原文（08-21 实测 3 条）。可能上游真的全挂，
			// 也可能我方熔断/健康检查把它们全禁用了。
			name:    "全部上游不可用",
			content: "status_code=502, bad response status code 502, message: All upstreams are temporarily unavailable",
			reason:  "可能上游全挂，也可能我方熔断全禁用",
		},
		{
			name:    "通用500",
			content: "status_code=500, Upstream request failed",
			reason:  "需读原文",
		},
		{
			name:    "没见过的状态码",
			content: "status_code=418, I am a teapot",
			reason:  "无规则的状态码不得瞎判",
		},
		{
			name:    "原文里没有状态码",
			content: "something went wrong without any status code",
			reason:  "无状态码无法按规则归因",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := logChainAttributeFault(LogChainRow{Type: 5, Content: tc.content}, nil)
			if got.Fault != faultUnknown {
				t.Errorf("应判待判（%s），got=%s\n原文: %s\n依据: %s",
					tc.reason, got.Fault, tc.content, got.Why)
			}
			if got.Why == "" {
				t.Error("待判也必须说明为什么判不了，否则人不知道缺什么证据")
			}
		})
	}
}

// TestLogChainFaultStatusCodeMapping 状态码映射，并检查可信度标注。
func TestLogChainFaultStatusCodeMapping(t *testing.T) {
	for _, tc := range []struct {
		code      int
		content   string
		wantFault string
		wantConf  string
		note      string
	}{
		// 两天实测共 121 条，判据明确。
		{502, "status_code=502, bad response status code 502", faultUpstream, faultConfHigh, "实测 58 条"},
		{503, "status_code=503, Service temporarily unavailable", faultUpstream, faultConfHigh, "实测 49 条"},
		{524, "status_code=524, openai_error", faultUpstream, faultConfHigh, "实测 20 条"},
		// 403/401 **不给结论**。原因不是"样本少"，而是原文本身不含判别信息：
		// 鉴权失败可能是我方密钥失效（换密钥）也可能是上游主动封禁（联系上游），
		// 而原文只有 "bad response status code 403"，**再多样本也不会变得可区分**。
		//
		// 曾把它们映射成"我方/低可信度"，实测证明那是错的方向——见下面那个测试：
		// 401 的真实原文说的是上游数据库出错，责任方在上游。
		{403, "status_code=403, bad response status code 403", faultUnknown, faultConfNone, "403原文无判别信息_不给结论"},
		{401, "status_code=401, bad response status code 401", faultUnknown, faultConfNone, "401原文无判别信息_不给结论"},
		// 样本不足，必须标 low——不能与 502/503 那些同等呈现。
		{413, "status_code=413, bad response status code 413", faultDownstream, faultConfLow, "实测仅 1 条"},
		// 生产原文：Invalid 'input[60].id' —— 明确的客户参数错误。
		{400, "status_code=400, Invalid 'input[60].id': expected an ID", faultDownstream, faultConfMid, "客户参数错"},
	} {
		t.Run(tc.note, func(t *testing.T) {
			got := logChainAttributeFault(LogChainRow{Type: 5, Content: tc.content}, nil)
			if got.Fault != tc.wantFault {
				t.Errorf("HTTP %d 应判 %s，got=%s（依据: %s）", tc.code, tc.wantFault, got.Fault, got.Why)
			}
			if got.Confidence != tc.wantConf {
				t.Errorf("HTTP %d 可信度应为 %s，got=%s —— 样本不足的映射必须标低，"+
					"否则会与 502/503 那种高置信的混在一起显示", tc.code, tc.wantConf, got.Confidence)
			}
		})
	}
}

// TestLogChainFaultUpstreamInternalErrorBeatsAuthCode 上游明说是它自己的故障时，
// 必须判上游——即使状态码看起来像我方凭据问题。
//
// 生产原文（2026-08-22 实测 2 条）：
//
//	status_code=401, bad response status code 401,
//	message: 无效的令牌，数据库查询出错，请联系管理员
//
// 状态码 401 通常指向凭据无效（我方），但原文说的是**上游的数据库查询出错**
// 并让我们联系管理员。这是"状态码不足以定责"的第二个实例
// （第一个是 404/503 报告上游无可用账号），也是把 401 默认映射
// 从"我方"改成"待判"的直接原因。
func TestLogChainFaultUpstreamInternalErrorBeatsAuthCode(t *testing.T) {
	got := logChainAttributeFault(LogChainRow{
		Type:    5,
		Content: "status_code=401, bad response status code 401, message: 无效的令牌，数据库查询出错，请联系管理员 (request id: 2026082201)",
	}, nil)
	if got.Fault != faultUpstream {
		t.Errorf("上游明示自身数据库故障应判上游，got=%s（依据: %s）", got.Fault, got.Why)
	}
	if !strings.Contains(got.Why, "上游") {
		t.Errorf("依据须说明责任方在上游: %s", got.Why)
	}
}

// TestLogChainFaultInternalErrorRequiresUpstreamOrigin 内部故障措辞必须带来源门（P2-03）。
//
// "internal server error" / "database error" 我方 new-api 也会产出。
// 少了 status_code= 前缀这道来源门，我方自身故障会被判成上游，
// 运营据此去投诉上游而放过自己的问题。
func TestLogChainFaultInternalErrorRequiresUpstreamOrigin(t *testing.T) {
	// 带前缀 = 上游返回 → 判上游。
	up := LogChainRow{Type: 5, Content: "status_code=500, internal server error"}
	if got := logChainAttributeFault(up, nil); got.Fault != faultUpstream {
		t.Errorf("上游返回的内部错误应判上游: got=%s", got.Fault)
	}

	// 无前缀 = 我方 new-api 自己产出 → 绝不能判上游。
	ours := LogChainRow{Type: 5, Content: "internal server error while building fact snapshot"}
	if got := logChainAttributeFault(ours, nil); got.Fault == faultUpstream {
		t.Errorf("我方自身内部错误不得判上游: got=%s（依据: %s）", got.Fault, got.Why)
	}
}

// TestLogChainFaultOurTimeoutGate 我方超时闸门主动中断 → 我方。
// 生产原文（08-24 实测 5 条）："status_code=408, 响应时间 125.03s 超过阈值 120.00s"。
// 这个状态码是 408，但语义是我方阈值生效，不是上游超时。
func TestLogChainFaultOurTimeoutGate(t *testing.T) {
	got := logChainAttributeFault(LogChainRow{
		Type:    5,
		Content: "status_code=408, 响应时间 125.03s 超过阈值 120.00s",
	}, nil)
	if got.Fault != faultOurs {
		t.Errorf("我方超时闸门应判我方，got=%s（依据: %s）", got.Fault, got.Why)
	}
	// 依据里要提到阈值，人才知道该调阈值还是换上游。
	if !strings.Contains(got.Why, "阈值") {
		t.Errorf("依据应说明是我方阈值触发: %s", got.Why)
	}
}

// TestLogChainFaultClientGoneUsesRealDistribution 客户断连按实测分布归因。
//
// 2026-08-24 实测 708 条 client_gone 的耗时分布：
//
//	<5s    342 条(48%) 平均输出 2 tok    → 客户主动取消
//	5-30s  291 条(41%) 平均输出 35 tok   → 判不了
//	>=120s   7 条(1%)  平均输出 295 tok  → 疑似上游拖慢
//
// 关键：client_gone 平均耗时 13 秒，**并不比正常请求 15 秒长**——
// 所以"上游拖慢把客户等跑"不是主因，近半数是客户几秒内主动取消。
func TestLogChainFaultClientGoneUsesRealDistribution(t *testing.T) {
	tags := []string{logChainClientGoneEndReason}
	for _, tc := range []struct {
		name      string
		useTime   int64
		frtMs     int64
		outTokens int64
		wantFault string
	}{
		// —— 上游一字未回（无首字延迟），实测 66 条 ——
		// 实测 49/66 在 5 秒内：客户在上游来得及响应前就取消了。
		// 这一条是我最初判反的地方：无首字 + 耗时短指向**下游**，不是上游。
		{"无首字_2秒断开_客户抢先取消", 2, 0, 0, faultDownstream},
		{"无首字_4秒断开_仍算抢先取消", 4, 0, 0, faultDownstream},
		// 实测 17/66：等了 5 秒以上上游仍一字未回。
		{"无首字_15秒仍无响应_判上游", 15, 0, 0, faultUpstream},
		{"无首字_60秒仍无响应_判上游", 60, 0, 0, faultUpstream},

		// —— 上游已交付内容，实测 62 条 ——
		// 有产出就是"上游确实响应过"的最强证据，必须先于首字延迟判定，
		// 否则会出现"有 300 个 token 却说上游尚未开始返回内容"的自相矛盾依据。
		{"已交付300token_客户中途走_判下游", 3, 800, 300, faultDownstream},
		{"已交付少量token_客户中途走_判下游", 20, 1200, 35, faultDownstream},

		// —— 上游开口但零产出 ——
		// 实测 9 条：首字就要等 5 秒以上，上游慢是明确事实。
		{"首字延迟8秒且零产出_判上游", 20, 8000, 0, faultUpstream},
		// 实测 20 条：及时开口后 10 秒以上毫无产出，卡在上游。
		{"及时开口后停滞15秒_判上游", 17, 2000, 0, faultUpstream},
		// 实测 12 条：上游刚开口客户就走，来不及归咎上游。
		{"开口后1秒即走_判下游", 3, 2000, 0, faultDownstream},
		// 实测 19 条：开口后 2~10 秒断开且零产出，两种解释都通，不猜。
		{"开口后5秒断开零产出_判不了", 7, 2000, 0, faultUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := logChainAttributeFault(LogChainRow{
				Type: 2, EndReason: logChainClientGoneEndReason,
				UseTime: tc.useTime, FirstByteMs: tc.frtMs, CompletionTokens: tc.outTokens,
			}, tags)
			if got.Fault != tc.wantFault {
				t.Errorf("耗时 %ds 首字 %dms 产出 %d tok 应判 %s，got=%s（依据: %s）",
					tc.useTime, tc.frtMs, tc.outTokens, tc.wantFault, got.Fault, got.Why)
			}
			// 依据里必须带可复核的数字（耗时或 token 数），否则人无法验证这条推断。
			if !strings.Contains(got.Why, "秒") && !strings.Contains(got.Why, "token") {
				t.Errorf("客户断连的依据必须给出可复核的数字: %s", got.Why)
			}
		})
	}
}

// TestLogChainFaultSkipsNormalRows 正常请求不归因。
// 正常请求没有"谁的问题"，硬给一个责任方会让页面充满噪音。
func TestLogChainFaultSkipsNormalRows(t *testing.T) {
	got := logChainAttributeFault(LogChainRow{
		Type: 2, EndReason: "eof", CompletionTokens: 400,
	}, nil)
	if got.Fault != "" {
		t.Errorf("正常请求不该归因，got=%s", got.Fault)
	}
}

// TestLogChainFaultIsPureFunction 归因必须是纯函数：同一行两次调用结果必须相同，
// 且不依赖同页其它行。
//
// 为什么这条重要：如果归因依赖"同渠道涉及几个客户"这类跨行统计，
// 同一条请求在第 1 页与第 3 页会得到不同结论——那种不稳定的判断无法复核。
// 影响面分析有价值，但要单独做，不能混进逐行归因里。
func TestLogChainFaultIsPureFunction(t *testing.T) {
	row := LogChainRow{Type: 5, Content: "status_code=503, Service temporarily unavailable"}
	a := logChainAttributeFault(row, nil)
	b := logChainAttributeFault(row, nil)
	if a != b {
		t.Errorf("同一行两次归因结果不同: %+v vs %+v", a, b)
	}
}

// TestLogChainFaultStreamAnomalyGoesUpstream 流传输故障归上游，但只给中可信度。
// end_reason 是 new-api 写下的事实，指向传输层；但也可能是网络中途出问题，
// 所以不给高可信度，并把原值摆进依据里让人复核。
func TestLogChainFaultStreamAnomalyGoesUpstream(t *testing.T) {
	got := logChainAttributeFault(LogChainRow{
		Type: 2, EndReason: "scanner_error", CompletionTokens: 456,
	}, []string{"stream"})
	if got.Fault != faultUpstream {
		t.Errorf("流故障应判上游，got=%s", got.Fault)
	}
	if got.Confidence != faultConfMid {
		t.Errorf("流故障可信度应为中（也可能是网络问题），got=%s", got.Confidence)
	}
	if !strings.Contains(got.Why, "scanner_error") {
		t.Errorf("依据应含 end_reason 原值供复核: %s", got.Why)
	}
}

// TestLogChainFaultBillingOnlyIsUnknown 只有消费异常、没有流问题 → 待判。
// 扣费与交付不一致是事实，但责任方无从判断，规则不该猜。
func TestLogChainFaultBillingOnlyIsUnknown(t *testing.T) {
	got := logChainAttributeFault(LogChainRow{
		Type: 2, EndReason: "eof", CompletionTokens: 0,
	}, []string{"billing_unpaid"})
	if got.Fault != faultUnknown {
		t.Errorf("纯消费异常应判待判，got=%s（依据: %s）", got.Fault, got.Why)
	}
}
