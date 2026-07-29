package monitor

import "testing"

// 错误详情必须逐字保留中转站 logs.content；按关键字归类会丢失 status、代理超时等排障证据。
func TestBuildLogDetailErrorTypePreservesRawContent(t *testing.T) {
	raw := "status_code=524, The origin web server did not return a complete response within the 120-second Proxy Read Timeout window."
	if got := buildLogDetail(5, parseLogOther(`{"frt":-1000}`), raw); got != raw {
		t.Fatalf("type=5 错误详情被改写: got=%q want=%q", got, raw)
	}
}

// 退款原因同样保留中转站 other.reason 原文，不能被客户端侧二次解释。
func TestRefundReasonPreservesRawContent(t *testing.T) {
	var r LogRow
	r.Type = 6
	raw := "provider poloapi returned 500 at api.poloapi.com"
	o := parseLogOther(`{"task_id":"task-9f2","reason":"provider poloapi returned 500 at api.poloapi.com"}`)
	populateExpandFields(&r, o, "")
	if r.RefundReason != raw {
		t.Errorf("退款原因被改写: got=%q want=%q", r.RefundReason, raw)
	}
	if r.TaskID != "task-9f2" {
		t.Errorf("任务ID 应保留(new-api 官方客户端也展示),实际 %q", r.TaskID)
	}
}

// TestTaskLogNoPerTokenPrice 任务日志(model_price=-1,按任务/时长计费)不能展示"每 1M tokens 多少钱"。
// 这类日志的 model_ratio 是占位值,照公式渲染会给客户看到"输出价格 $0 / 1M tokens"这种错价。
func TestTaskLogNoPerTokenPrice(t *testing.T) {
	o := parseLogOther(`{"is_task":true,"task_id":"task-9f2","model_price":-1,"model_ratio":1,"group_ratio":1}`)
	if got := buildLogContent(2, o); got != "" {
		t.Errorf("任务日志不应有单价行,得到 %q", got)
	}
	// 非任务日志照常出单价
	o2 := parseLogOther(`{"model_ratio":1.25,"group_ratio":1,"completion_ratio":8}`)
	if got := buildLogContent(2, o2); got == "" {
		t.Error("普通消费日志应有单价行")
	}
}

// TestTaskLogNoDuplicateContent 已结算任务日志的"计费过程"就是 content 原文,
// 若"其他详情"也照原样展示,展开区会把同一句话连着显示两遍。
func TestTaskLogNoDuplicateContent(t *testing.T) {
	content := "任务完成,按实际时长 6s 计费 $0.12"
	var r LogRow
	r.Type = 2
	populateExpandFields(&r, parseLogOther(`{"is_task":true,"task_id":"task-9f2","model_price":-1,"model_ratio":1,"group_ratio":1}`), content)
	if r.BillingProcess != content {
		t.Fatalf("计费过程应是 content 原文,得到 %q", r.BillingProcess)
	}
	if r.OtherContent != "" {
		t.Errorf("与计费过程重复的其他详情应去掉,得到 %q", r.OtherContent)
	}
	// 两者不同时,其他详情要保留
	var r2 LogRow
	r2.Type = 2
	populateExpandFields(&r2, parseLogOther(`{"model_ratio":1.25,"group_ratio":1,"completion_ratio":8}`), "分组倍率 1.0x")
	if r2.OtherContent != "分组倍率 1.0x" {
		t.Errorf("内容不同时其他详情应保留,得到 %q", r2.OtherContent)
	}
}
