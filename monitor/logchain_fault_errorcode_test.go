package monitor

// error_code 归因层的约束。
//
// 这一层的价值在于把原本"待判"的行判掉，而**判错的代价比不判更大**：
// 判成我方会让人去改自己的配置，而问题在上游。所以这里钉三件事：
//  1. 有判别力的 code 判对方向
//  2. 没判别力的 code 必须留在待判（绝不猜）
//  3. 来源门生效——不带 status_code= 前缀时不采信本层

import (
	"strings"
	"testing"
)

// errRow 造一条错误行。content 带 status_code= 前缀即视为上游返回。
func errRow(statusCode int, errorCode, content string) LogChainRow {
	return LogChainRow{
		Type:               5,
		Content:            content,
		UpstreamErrorCode:  errorCode,
		UpstreamStatusCode: statusCode,
	}
}

// TestFaultByErrorCodeJudgesPendingRows ★ 本组最要紧的一条 ★
//
// 这些 code 在状态码层全是"待判"，靠 error_code 才判得出方向。
// 每条的实测条数写在 logChainFaultByErrorCode 的注释里。
func TestFaultByErrorCodeJudgesPendingRows(t *testing.T) {
	cases := []struct {
		name       string
		row        LogChainRow
		wantFault  string
		wantInWhy  string
		wasPending bool // 该状态码在状态码层是否为待判
	}{
		{
			name:       "408 + 上游渠道超时 → 上游",
			row:        errRow(408, "channel:response_time_exceeded", "status_code=408, request timeout"),
			wantFault:  faultUpstream,
			wantInWhy:  "自身渠道响应超时",
			wasPending: true,
		},
		{
			name:       "500 + 上游未能发出请求 → 上游",
			row:        errRow(500, "do_request_failed", "status_code=500, do request failed"),
			wantFault:  faultUpstream,
			wantInWhy:  "未能向其渠道发出请求",
			wasPending: true,
		},
		{
			name: "403 + 我方账号额度不足 → 我方",
			// ★ 语义陷阱 ★ code 里的 user 指我方在上游的账号,不是我方客户。
			// 判成客户会让人去找客户,而实际要做的是去上游充值。
			row:        errRow(403, "insufficient_user_quota", "status_code=403, insufficient user quota"),
			wantFault:  faultOurs,
			wantInWhy:  "我方账号额度不足",
			wasPending: true,
		},
		{
			name:       "429 + 上游对我方限流 → 我方",
			row:        errRow(429, "user:concurrency_limited", "status_code=429, concurrency limited"),
			wantFault:  faultOurs,
			wantInWhy:  "并发限流",
			wasPending: true,
		},
		{
			name:       "400 + 客户数组超长 → 客户",
			row:        errRow(400, "array_above_max_length", "status_code=400, array above max length"),
			wantFault:  faultDownstream,
			wantInWhy:  "数组超过长度上限",
			wasPending: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := logChainAttributeFault(tc.row, nil)
			if got.Fault != tc.wantFault {
				t.Errorf("责任方: got=%s want=%s (why=%s)", got.Fault, tc.wantFault, got.Why)
			}
			if !strings.Contains(got.Why, tc.wantInWhy) {
				t.Errorf("依据未说明判据: got=%q 应含 %q", got.Why, tc.wantInWhy)
			}
			// 依据必须提到 error_code,否则人无法复核这条结论从哪来。
			if !strings.Contains(got.Why, "error_code=") {
				t.Errorf("依据应点出 error_code,便于复核: %q", got.Why)
			}
			if got.Confidence == faultConfNone {
				t.Error("判出了方向就不该是 none 可信度")
			}
			// 反向确认:该状态码在状态码层确实是待判——否则本用例证明不了"提升了准确率"。
			if tc.wasPending {
				if rule, ok := logChainFaultByStatus[tc.row.UpstreamStatusCode]; !ok || rule.fault != faultUnknown {
					t.Errorf("前提不成立: 状态码 %d 在状态码层不是待判,本层的价值无从体现",
						tc.row.UpstreamStatusCode)
				}
			}
		})
	}
}

// TestUpstreamThresholdMessageIsNotOurGate ★ 本文件最要紧的一条 ★
//
// 修的是一个既有误判：41 条被判成"我方超时闸门主动中断"，实际是上游的阈值。
//
// 原文 "status_code=408, 响应时间 125.03s 超过阈值 120.00s" 带 status_code 前缀，
// 说明这句话是上游说的。决定性证据：这些行我方 use_time **全为 0**——
// 若是我方闸门在 120s 掐断，use_time 应 ≈120；为 0 说明我方压根没观测到
// 125.03s 这个时长，观测到它的是上游。
//
// 判成我方会让人去调我方阈值，而该做的是换上游或找上游。方向完全相反。
func TestUpstreamThresholdMessageIsNotOurGate(t *testing.T) {
	// 带前缀（上游说的）→ 应判上游
	fromUpstream := LogChainRow{
		Type:               5,
		Content:            "status_code=408, 响应时间 125.03s 超过阈值 120.00s",
		UpstreamErrorCode:  "channel:response_time_exceeded",
		UpstreamStatusCode: 408,
		UseTime:            0, // 我方未观测到该时长，实测特征
	}
	got := logChainAttributeFault(fromUpstream, nil)
	if got.Fault != faultUpstream {
		t.Errorf("上游返回的超阈值消息应判上游: got=%s why=%s", got.Fault, got.Why)
	}
	if strings.Contains(got.Why, "我方超时闸门") {
		t.Errorf("依据不该说成我方闸门: %q", got.Why)
	}

	// 不带前缀（我方自己生成）→ 仍应判我方，原规则的用途保留
	fromOurs := LogChainRow{
		Type:    5,
		Content: "响应时间 125.03s 超过阈值 120.00s",
	}
	if got := logChainAttributeFault(fromOurs, nil); got.Fault != faultOurs {
		t.Errorf("我方自己的闸门消息仍应判我方: got=%s why=%s", got.Fault, got.Why)
	}
}

// TestErrorCodeLayerMayCorrectStatusVerdict 本层可以纠正状态码层的结论。
//
// 设计上不只是"填补待判"：error_code 是上游写下的原值，判别力高于我方
// 对状态码的解读。实测有 2 条 HTTP 400 原被按默认判成"客户请求参数有误"，
// 而 error_code=upstream_error 明示错误来自上游更上游——纠正是对的。
func TestErrorCodeLayerMayCorrectStatusVerdict(t *testing.T) {
	row := errRow(400, "upstream_error", "status_code=400, OpenAI error, 请重试")
	got := logChainAttributeFault(row, nil)
	if got.Fault != faultUpstream {
		t.Errorf("error_code 明示上游时应纠正状态码层的默认判断: got=%s why=%s", got.Fault, got.Why)
	}
	// 反向确认前提：400 在状态码层确实判客户，否则本用例证明不了"纠正"。
	if rule, ok := logChainFaultByStatus[400]; ok && rule.fault != faultDownstream {
		t.Errorf("前提不成立：400 在状态码层不是判客户（got=%s），本用例的意义需重新表述", rule.fault)
	}
}

// TestFaultByErrorCodeKeepsAmbiguousPending 没判别力的 code 必须留在待判。
//
// ★ 这条比上一条更重要 ★
// 把 unknown_error / bad_response_status_code / model_not_found 写进判据表
// 会让"待判"变成"看似确定实则瞎猜"。实测:
//
//	unknown_error            490 条,状态码 400~524 全谱
//	bad_response_status_code 466 条,判别力全在状态码本身
//	model_not_found          166 条,客户/我方配置/上游下架三者措辞相同
func TestFaultByErrorCodeKeepsAmbiguousPending(t *testing.T) {
	for _, code := range []string{"unknown_error", "bad_response_status_code", "model_not_found"} {
		t.Run(code, func(t *testing.T) {
			if _, exists := logChainFaultByErrorCode[code]; exists {
				t.Fatalf("%s 无判别力,不得写进判据表——那会把待判变成瞎猜", code)
			}
		})
	}
	// 走完整流程:403 + bad_response_status_code 应仍落待判(403 状态码层是待判),
	// 证明本层没有越权判掉它。
	got := logChainAttributeFault(
		errRow(403, "bad_response_status_code", "status_code=403, bad response status code 403"), nil)
	if got.Fault != faultUnknown {
		t.Errorf("无判别力的 code 不该被判出方向: got=%s why=%s", got.Fault, got.Why)
	}
}

// TestFaultByErrorCodeRequiresUpstreamOrigin 来源门必须生效。
//
// 实测那 1256 条全部带 status_code= 前缀,故 code 里的 user 指我方在上游的账号。
// 若将来出现我方自己分类的 error_code,user 的所指会翻转成我方客户——
// 那时采信本表会把"客户额度不足"判成"我方额度不足",方向正好相反。
// 这道门就是防止那种静默失准。
func TestFaultByErrorCodeRequiresUpstreamOrigin(t *testing.T) {
	// 同一个 code,但 content 不带 status_code= 前缀(即我方自己生成的消息)。
	row := errRow(0, "insufficient_user_quota", "insufficient user quota")
	got := logChainAttributeFault(row, nil)
	if got.Fault == faultOurs {
		t.Error("无 status_code= 前缀时不得采信 error_code:此时 user 的所指可能是我方客户")
	}
	if got.Fault != faultUnknown {
		t.Errorf("应退回待判: got=%s why=%s", got.Fault, got.Why)
	}
}

// TestFaultMessageRulesWinOverErrorCode 原文规则的优先级高于 error_code。
//
// 原文规则里有带来源门的精细判据(如"无可用渠道"要分我方/上游),
// 那些比 error_code 更具体,不能被本层抢走。
func TestFaultMessageRulesWinOverErrorCode(t *testing.T) {
	// 这条原文命中"no available channel"规则(判上游、高可信),
	// 同时带 do_request_failed(本层也判上游)。两者结论一致,但依据应来自原文规则。
	row := errRow(503, "do_request_failed",
		"status_code=503, No available channel for model gpt-5.4 under the current group")
	got := logChainAttributeFault(row, nil)
	if strings.Contains(got.Why, "error_code=do_request_failed") {
		t.Errorf("原文规则应优先于 error_code,依据不该来自本层: %q", got.Why)
	}
	if got.Fault != faultUpstream {
		t.Errorf("结论仍应是上游: got=%s", got.Fault)
	}
}

// TestUpstreamErrorFieldsOnlyOnErrorRows 消费行不得带上游失败分类。
//
// type=2 的 other 里没有这些键,读出来是空。限定 type=5 是为了让"空值"
// 只有一种含义:上游这次没给分类,而不是"这行本来就不该有"。
func TestUpstreamErrorFieldsOnlyOnErrorRows(t *testing.T) {
	src, err := readMonitorSource("logchain.go")
	if err != nil {
		t.Fatal(err)
	}
	// 接线处必须有 type=5 的守卫。
	idx := strings.Index(src, "r.UpstreamErrorType = o.ErrorType")
	if idx < 0 {
		t.Fatal("找不到上游失败分类的接线处")
	}
	// 往前找最近的守卫,应在 200 字符内出现 r.Type == 5。
	start := idx - 300
	if start < 0 {
		start = 0
	}
	if !strings.Contains(src[start:idx], "r.Type == 5") {
		t.Error("上游失败分类的接线缺 type=5 守卫:消费行会带上无意义的空值")
	}
}
