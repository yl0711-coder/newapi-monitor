package monitor

import (
	"strings"
	"testing"
)

func TestLogChainFaultEvidenceRefinesGenericError(t *testing.T) {
	row := LogChainRow{
		Type:               5,
		Content:            "status_code=500, bad response status code 500",
		UpstreamStatusCode: 500,
		UpstreamMatch: &LogChainUpstreamMatch{
			Confidence:         correlateExact,
			UpstreamStatusCode: 500,
			UpstreamContent:    "status_code=500, bad response status code 500",
		},
	}
	f := logChainAttributeFaultWithEvidence(row, nil)
	if f.Fault != faultUpstream || f.Confidence != faultConfHigh {
		t.Fatalf("精确上游证据应把通用 500 判为上游: %+v", f)
	}
	if f.Reason != "上游返回 500，模型供应商暂时无法处理请求" {
		t.Fatalf("应生成大白话原因: %q", f.Reason)
	}
}

func TestLogChainFaultEvidenceRefinesVerifiedClientDisconnect(t *testing.T) {
	row := LogChainRow{
		Type:                 2,
		EndReason:            logChainClientGoneEndReason,
		UseTime:              8,
		EdgeEvidenceVerified: true,
		EdgeEvidence: &LogChainEdgeEvidence{
			Status:     499,
			Completion: "client_closed",
		},
	}
	f := logChainAttributeFaultWithEvidence(row, []string{logChainClientGoneEndReason})
	if f.Fault != faultDownstream || f.Confidence != faultConfHigh {
		t.Fatalf("已验证 499 应判客户端断开: %+v", f)
	}
	if f.Reason != "入口日志确认客户先断开连接，未发现上游返回错误" {
		t.Fatalf("断连原因不够直白: %q", f.Reason)
	}
}

func TestLogChainFaultEvidenceUsesVerifiedUpstreamStatus(t *testing.T) {
	status := 502
	row := LogChainRow{
		Type:                 2,
		AnomalyTags:          []string{anomalyUndeliveredUnbilled},
		EdgeEvidenceVerified: true,
		EdgeEvidence:         &LogChainEdgeEvidence{Status: 502, UpstreamStatus: status},
	}
	f := logChainAttributeFaultWithEvidence(row, row.AnomalyTags)
	if f.Fault != faultUpstream || f.Confidence != faultConfHigh {
		t.Fatalf("已验证入口上游 502 应判上游: %+v", f)
	}
}

func TestLogChainFaultReasonKeepsUnknownEvidenceBoundary(t *testing.T) {
	f := logChainAttributeFaultWithEvidence(LogChainRow{
		Type:    5,
		Content: "status_code=401, bad response status code 401",
	}, nil)
	if f.Fault != faultUnknown {
		t.Fatalf("没有上游或入口证据时不能硬猜: %+v", f)
	}
	if f.Reason != "收到 HTTP 401，可能是我方密钥无效，也可能是上游封禁；当前日志无法区分，需核对上游后台" {
		t.Fatalf("401 原因应直接说明下一步: %q", f.Reason)
	}
}

func TestLogChainFaultEvidenceDoesNotGuessUpstreamAuthOwner(t *testing.T) {
	row := LogChainRow{
		Type: 5, Content: "status_code=403, forbidden",
		UpstreamMatch: &LogChainUpstreamMatch{
			Confidence:         correlateExact,
			UpstreamStatusCode: 403,
			UpstreamContent:    "status_code=403, forbidden",
		},
	}
	f := logChainAttributeFaultWithEvidence(row, nil)
	if f.Fault != faultUnknown {
		t.Fatalf("只有 generic 403 时不能把责任硬推给上游或我方: %+v", f)
	}
	if f.Reason == "" || !strings.Contains(f.Reason, "无法区分") {
		t.Fatalf("待判鉴权错误应说明缺什么证据: %+v", f)
	}
}

func TestLogChainFaultEvidenceExplicitInvalidKeyIsOurs(t *testing.T) {
	row := LogChainRow{
		Type: 5, Content: "status_code=401, invalid api key",
		UpstreamMatch: &LogChainUpstreamMatch{
			Confidence:         correlateExact,
			UpstreamStatusCode: 401,
			UpstreamContent:    "status_code=401, invalid api key",
		},
	}
	f := logChainAttributeFaultWithEvidence(row, nil)
	if f.Fault != faultOurs {
		t.Fatalf("上游明确说 invalid api key 时应指向我方凭证: %+v", f)
	}
}

func TestLogChainFaultReasonMatchesUpstreamClientGoneAttribution(t *testing.T) {
	row := LogChainRow{Type: 2, EndReason: logChainClientGoneEndReason, UseTime: 8}
	f := logChainAttributeFaultWithEvidence(row, []string{logChainClientGoneEndReason})
	if f.Fault != faultUpstream {
		t.Fatalf("超过 3 秒无输出应判上游侧疑似慢响应: %+v", f)
	}
	if strings.Contains(f.Reason, "客户等待超过 3 秒仍没有收到内容，连接随后断开") {
		t.Fatalf("原因不能把上游归因写成无责任方的旧文案: %q", f.Reason)
	}
	if !strings.Contains(f.Reason, "上游") || !strings.Contains(f.Reason, "3 秒") {
		t.Fatalf("原因应明确说明上游慢和 3 秒边界: %q", f.Reason)
	}
}

func TestLogChainFaultEvidenceDoesNotOverclaimNginx502(t *testing.T) {
	row := LogChainRow{
		Type:                 2,
		AnomalyTags:          []string{anomalyUndeliveredUnbilled},
		EdgeEvidenceVerified: true,
		EdgeEvidence:         &LogChainEdgeEvidence{Status: 502},
	}
	f := logChainAttributeFaultWithEvidence(row, row.AnomalyTags)
	if f.Fault != faultUnknown {
		t.Fatalf("没有 upstream_status 的入口 502 不能硬判我方或上游: %+v", f)
	}
	if !strings.Contains(f.Reason, "HTTP 502") || strings.Contains(f.Reason, "响应链路明显偏慢") {
		t.Fatalf("入口 502 的更强证据不应被高首字启发式覆盖: %q", f.Reason)
	}
}

func TestLogChainVerifiedClientDisconnectDoesNotTreatServerCancelAsClient(t *testing.T) {
	row := LogChainRow{Type: 2, EndReason: logChainClientGoneEndReason, EdgeEvidenceVerified: true,
		EdgeEvidence: &LogChainEdgeEvidence{Status: 200, Completion: "server_cancel"}}
	if logChainVerifiedClientDisconnect(row) {
		t.Fatal("server_cancel 不能被宽泛的 cancel 子串误判为客户主动断连")
	}
}

func TestLogChainHumanFaultReasonUsesSpecificUpstreamErrorCode(t *testing.T) {
	for _, tc := range []struct {
		code string
		want string
	}{
		{"channel:response_time_exceeded", "上游渠道响应太慢"},
		{"do_request_failed", "没有把请求成功发到模型渠道"},
		{"bad_response_body", "返回的数据格式不正确"},
	} {
		f := logChainAttributeFaultWithEvidence(LogChainRow{
			Type: 5, Content: "status_code=500, upstream error", UpstreamErrorCode: tc.code,
		}, nil)
		if f.Fault != faultUpstream || !strings.Contains(f.Reason, tc.want) {
			t.Fatalf("error_code=%s 的大白话不够具体: %+v", tc.code, f)
		}
	}
}

func TestLogChainAttributionSummaryReportsCoveredSubset(t *testing.T) {
	rows := []LogChainRow{
		{Type: 5, Fault: faultUpstream, UpstreamMatch: &LogChainUpstreamMatch{Confidence: correlateExact}},
		{Type: 5, Fault: faultUnknown, UpstreamMatch: &LogChainUpstreamMatch{Confidence: correlateExact}},
		{Type: 2, AnomalyTags: []string{anomalyUndeliveredUnbilled}, Fault: faultUnknown},
	}
	s := logChainAttributionSummaryForRows(rows)
	if s.Total != 3 || s.Attributed != 1 || s.Unknown != 2 || s.EvidenceCovered != 2 || s.EvidenceCoveredUnknown != 1 {
		t.Fatalf("归因覆盖统计错误: %+v", s)
	}
	if s.UnknownRatePct != 2.0/3.0*100 || s.CoveredUnknownRatePct != 50 || s.EvidenceMissing != 1 || s.EvidenceMissingRatePct != 100.0/3.0 {
		t.Fatalf("归因比例错误: %+v", s)
	}
}
