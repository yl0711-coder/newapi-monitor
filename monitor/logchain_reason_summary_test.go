package monitor

import (
	"strings"
	"testing"
)

func TestLogChainReasonSummaryIsMutuallyExclusiveAndComplete(t *testing.T) {
	rows := []LogChainRow{
		{Type: 5, Member: "客户A", ChannelID: 1, ChannelName: "c1", Content: "status_code=503 upstream down", UpstreamErrorCode: "bad_response_body", Fault: faultUpstream},
		{Type: 5, Member: "客户B", ChannelID: 1, ChannelName: "c1", Content: "status_code=503 upstream down", UpstreamErrorCode: "bad_response_body", Fault: faultUpstream},
		{Type: 2, Member: "客户A", ChannelID: 2, ChannelName: "c2", EndReason: "timeout", AnomalyTags: []string{"stream", anomalyUndeliveredUnbilled}, Fault: faultUnknown},
		{Type: 2, Member: "客户C", ChannelID: 3, ChannelName: "c3", AnomalyTags: []string{anomalyUndeliveredUnbilled}, Fault: faultUnknown},
		{Type: 2, Member: "正常客户", ChannelID: 9, ChannelName: "normal"},
	}
	br := computeLogChainBlastRadius(rows)
	if br.Rows != 4 {
		t.Fatalf("只应统计 4 条问题，got=%d", br.Rows)
	}
	sum := br.Reasons.OtherCount
	for _, item := range br.Reasons.Items {
		sum += item.Count
	}
	if sum != br.Rows {
		t.Fatalf("互斥原因合计=%d，应等于问题数=%d", sum, br.Rows)
	}
	faultSum := 0
	for _, item := range br.Faults {
		faultSum += item.Count
	}
	if faultSum != br.Rows {
		t.Fatalf("责任方合计=%d，应等于问题数=%d", faultSum, br.Rows)
	}
	var combined bool
	for _, item := range br.Reasons.Items {
		if strings.Contains(item.Reason, "流故障(timeout) + 未交付·未扣费") {
			combined = item.Count == 1 && item.Customers == 1 && item.Channels == 1
		}
	}
	if !combined {
		t.Error("一行命中多标签时应作为一个组合主原因，不能重复计数")
	}
}

func TestLogChainReasonSummaryTruncationIsStableAndExplicit(t *testing.T) {
	var rows []LogChainRow
	for i := 0; i < logChainRadiusMaxItems+3; i++ {
		rows = append(rows, LogChainRow{
			Type: 5, Member: "客户" + lcNum(i), ChannelID: int64(i + 1),
			Content: "status_code=5" + lcNum(i) + " error-" + lcNum(i), Fault: faultUnknown,
		})
	}
	first := computeLogChainBlastRadius(rows)
	if len(first.Reasons.Items) != logChainRadiusMaxItems || first.Reasons.OtherItems != 3 || first.Reasons.OtherCount != 3 {
		t.Fatalf("原因截断不完整: %+v", first.Reasons)
	}
	for i := 0; i < 5; i++ {
		again := computeLogChainBlastRadius(rows)
		if again.Reasons.Items[0].Reason != first.Reasons.Items[0].Reason {
			t.Fatalf("同数量原因排序不稳定: first=%q again=%q", first.Reasons.Items[0].Reason, again.Reasons.Items[0].Reason)
		}
	}
}

func TestLogChainPrimaryReasonScrubsSensitiveText(t *testing.T) {
	reason := logChainPrimaryReason(LogChainRow{
		Type: 5, Content: "status_code=500 api_key=secret-value user@example.com 123e4567-e89b-12d3-a456-426614174000",
	})
	for _, forbidden := range []string{"secret-value", "user@example.com", "123e4567-e89b-12d3-a456-426614174000"} {
		if strings.Contains(reason, forbidden) {
			t.Errorf("原因摘要泄露敏感原文 %q: %s", forbidden, reason)
		}
	}
	for _, want := range []string{"HTTP 500", "<redacted>", "<email>", "<uuid>"} {
		if !strings.Contains(reason, want) {
			t.Errorf("原因摘要缺少规范化结果 %q: %s", want, reason)
		}
	}
}

func focusedReasonRows(counts ...int) []LogChainRow {
	var rows []LogChainRow
	for i, count := range counts {
		for n := 0; n < count; n++ {
			rows = append(rows, LogChainRow{
				Type: 5, Member: "客户" + lcNum(n%3), ChannelID: int64(i + 1),
				UpstreamErrorCode: "reason_" + lcNum(i), Content: "reason text " + lcNum(i),
			})
		}
	}
	return rows
}

func TestLogChainFocusedReasonShapes(t *testing.T) {
	cases := []struct {
		name     string
		counts   []int
		want     string
		wantText []string
	}{
		{"少量记录仍描述表现", []int{3}, reasonShapeSmall, []string{"3 条", "不外推长期主因"}},
		{"恰好一半为单一主因", []int{5, 3, 2}, reasonShapeDominant, []string{"5 条", "占 50%"}},
		{"前两项恰好七成为双主因", []int{4, 3, 3}, reasonShapeDual, []string{"两类", "合计占 70%"}},
		{"未过阈值则原因分散", []int{4, 3, 2, 2}, reasonShapeDistributed, []string{"原因较分散", "未形成明确主因"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			br := computeLogChainBlastRadiusMode(focusedReasonRows(tc.counts...), logChainRadiusModeFocused, false)
			if br.Mode != logChainRadiusModeFocused || br.ReasonShape != tc.want {
				t.Fatalf("mode=%q shape=%q, want focused/%q; why=%s", br.Mode, br.ReasonShape, tc.want, br.ReasonWhy)
			}
			if br.Shape != "" {
				t.Fatalf("focused 不得再给通用集中度 shape，got=%q", br.Shape)
			}
			for _, want := range tc.wantText {
				if !strings.Contains(br.ReasonWhy, want) {
					t.Errorf("原因结论缺少 %q: %s", want, br.ReasonWhy)
				}
			}
			if !strings.Contains(br.ReasonWhy, "已全部返回") {
				t.Errorf("无更多记录时须说明覆盖完整: %s", br.ReasonWhy)
			}
		})
	}
}

func TestLogChainFocusedReasonStatesCurrentPageWhenMoreExists(t *testing.T) {
	br := computeLogChainBlastRadiusMode(focusedReasonRows(6, 2), logChainRadiusModeFocused, true)
	if !br.PageHasMore {
		t.Fatal("has_more 必须进入分析元数据")
	}
	for _, want := range []string{"仅分析当前页 8 条", "仍有更多记录"} {
		if !strings.Contains(br.ReasonWhy, want) {
			t.Errorf("分页覆盖说明缺少 %q: %s", want, br.ReasonWhy)
		}
	}
}
