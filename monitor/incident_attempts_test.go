package monitor

import "testing"

// 按渠道 + 秒级时间去重，不按日志条数（复用 logchain.js:887 口径）。
func TestIncidentAttemptsDedupesByChannelAndSecond(t *testing.T) {
	rows := []IncidentAttemptRow{
		// 同一次尝试的两个侧面（错误日志 + 消费日志），应算一次。
		{ChannelID: 115, CreatedAt: 1760000000, RequestID: "r1"},
		{ChannelID: 115, CreatedAt: 1760000000, RequestID: "r1"},
		// 同渠道不同秒：另一次尝试。
		{ChannelID: 115, CreatedAt: 1760000003, RequestID: "r1"},
		// 不同渠道：另一次尝试。
		{ChannelID: 116, CreatedAt: 1760000003, RequestID: "r1"},
	}
	got := ComputeIncidentAttempts(rows)
	if got.VisibleAttempts != 3 {
		t.Fatalf("可见尝试数 =%d want 3", got.VisibleAttempts)
	}
	if got.LogRows != 4 {
		t.Fatalf("日志条数 =%d want 4", got.LogRows)
	}
	// 三个计数不得混用。
	if got.VisibleAttempts == got.LogRows {
		t.Fatal("尝试次数与日志条数相同，口径被混用")
	}
	if got.DistinctRequests != 1 {
		t.Fatalf("独立请求数 =%d want 1", got.DistinctRequests)
	}
}

// 不同请求在同一秒命中同一渠道：是两次尝试，不得合并。
// 这是本轮 review 指出的缺陷——原实现只用「渠道+秒」会算成一次。
func TestIncidentAttemptsDifferentRequestsSameChannelSameSecond(t *testing.T) {
	rows := []IncidentAttemptRow{
		{ChannelID: 115, CreatedAt: 1760000000, RequestID: "r1"},
		{ChannelID: 115, CreatedAt: 1760000000, RequestID: "r2"},
	}
	got := ComputeIncidentAttempts(rows)
	if got.VisibleAttempts != 2 {
		t.Fatalf("可见尝试数 =%d want 2（两个不同请求被跨请求合并）", got.VisibleAttempts)
	}
	if got.DistinctRequests != 2 {
		t.Fatalf("独立请求数 =%d want 2", got.DistinctRequests)
	}
	// 组内去重仍要生效：同一请求的重复行合并。
	rows = append(rows, IncidentAttemptRow{ChannelID: 115, CreatedAt: 1760000000, RequestID: "r1"})
	got = ComputeIncidentAttempts(rows)
	if got.VisibleAttempts != 2 {
		t.Fatalf("加入同请求重复行后 =%d want 2（组内去重失效）", got.VisibleAttempts)
	}
	if got.LogRows != 3 {
		t.Fatalf("日志条数 =%d want 3", got.LogRows)
	}
}

// 无 Request ID 的记录不得擅自合并，即使同渠道同秒。
func TestIncidentAttemptsNoIDSameChannelSameSecondNotMerged(t *testing.T) {
	rows := []IncidentAttemptRow{
		{ChannelID: 115, CreatedAt: 1760000000, RequestID: ""},
		{ChannelID: 115, CreatedAt: 1760000000, RequestID: ""},
	}
	got := ComputeIncidentAttempts(rows)
	if got.VisibleAttempts != 2 {
		t.Fatalf("可见尝试数 =%d want 2（无 ID 记录被擅自合并）", got.VisibleAttempts)
	}
	if got.UnlinkableRows != 2 {
		t.Fatalf("不可关联行数 =%d want 2", got.UnlinkableRows)
	}
	if got.DistinctRequests != 0 {
		t.Fatalf("独立请求数 =%d want 0", got.DistinctRequests)
	}
	if got.Exact {
		t.Fatal("存在不可关联行时 Exact 不应为真")
	}
}

// 无 ID 的行不得与有 ID 的行合并。
func TestIncidentAttemptsNoIDNotMergedWithLinkedRow(t *testing.T) {
	rows := []IncidentAttemptRow{
		{ChannelID: 115, CreatedAt: 1760000000, RequestID: "r1"},
		{ChannelID: 115, CreatedAt: 1760000000, RequestID: ""},
	}
	got := ComputeIncidentAttempts(rows)
	if got.VisibleAttempts != 2 {
		t.Fatalf("可见尝试数 =%d want 2", got.VisibleAttempts)
	}
	if got.UnlinkableRows != 1 {
		t.Fatalf("不可关联行数 =%d want 1", got.UnlinkableRows)
	}
}

// 取不到渠道时退化为按记录计数，且必须显式标记——不能隐藏。
func TestIncidentAttemptsDegradationIsVisible(t *testing.T) {
	rows := []IncidentAttemptRow{
		{ChannelID: 115, CreatedAt: 1760000000, RequestID: "r1"},
		{ChannelID: 0, CreatedAt: 1760000001, RequestID: "r1"},
		{ChannelID: 0, CreatedAt: 1760000002, RequestID: "r1"},
	}
	got := ComputeIncidentAttempts(rows)
	if got.RowsWithoutChannel != 2 {
		t.Fatalf("无渠道行数 =%d want 2", got.RowsWithoutChannel)
	}
	if got.Exact {
		t.Fatal("存在退化时 Exact 不应为真")
	}
	// 退化情况必须出现在说明里，不能静默。
	note := got.CompletenessNote()
	if indexOf(note, "退化为按记录计数") < 0 {
		t.Fatalf("说明未提及退化: %s", note)
	}
	if indexOf(note, "2") < 0 {
		t.Fatalf("说明未给出退化行数: %s", note)
	}
}

// 无 Request ID 的行不可关联，单独计数并说明。
func TestIncidentAttemptsUnlinkableRowsReported(t *testing.T) {
	rows := []IncidentAttemptRow{
		{ChannelID: 115, CreatedAt: 1760000000, RequestID: "r1"},
		{ChannelID: 115, CreatedAt: 1760000001, RequestID: ""},
	}
	got := ComputeIncidentAttempts(rows)
	if got.UnlinkableRows != 1 {
		t.Fatalf("不可关联行数 =%d want 1", got.UnlinkableRows)
	}
	// 无 ID 的行不计入独立请求数。
	if got.DistinctRequests != 1 {
		t.Fatalf("独立请求数 =%d want 1", got.DistinctRequests)
	}
	if got.Exact {
		t.Fatal("存在不可关联行时 Exact 不应为真")
	}
	if indexOf(got.CompletenessNote(), "无法按请求归并") < 0 {
		t.Fatalf("说明未提及不可关联: %s", got.CompletenessNote())
	}
}

// 即使没有任何退化，也必须说明这只是「当前可见」尝试数。
func TestIncidentAttemptsAlwaysNotesVisibilityLimit(t *testing.T) {
	rows := []IncidentAttemptRow{
		{ChannelID: 115, CreatedAt: 1760000000, RequestID: "r1"},
	}
	got := ComputeIncidentAttempts(rows)
	if !got.Exact {
		t.Fatal("无退化时 Exact 应为真")
	}
	note := got.CompletenessNote()
	if indexOf(note, "当前可见") < 0 {
		t.Fatalf("说明未标注「当前可见」: %s", note)
	}
	// 不得冒充为实际网络尝试数。
	if indexOf(note, "不等于实际网络尝试数") < 0 {
		t.Fatalf("说明未声明与实际尝试数的差距: %s", note)
	}
}

// 尝试次数不能证明客户端重试循环。
func TestIncidentAttemptsNeverProveClientRetryLoop(t *testing.T) {
	// 构造一个尝试次数很高的场景。
	rows := make([]IncidentAttemptRow, 0, 20)
	for i := int64(0); i < 20; i++ {
		rows = append(rows, IncidentAttemptRow{
			ChannelID: 115, CreatedAt: 1760000000 + i, RequestID: "r1",
		})
	}
	got := ComputeIncidentAttempts(rows)
	if got.VisibleAttempts != 20 {
		t.Fatalf("前置条件失败: 可见尝试数 =%d", got.VisibleAttempts)
	}
	// 20 次渠道尝试仍不能证明客户端重试循环——
	// 那可能全是 NewAPI 内部重试。
	if got.ProvesClientRetryLoop() {
		t.Fatal("渠道尝试次数被当成了客户端重试循环的证据")
	}
}

// 空输入不产生负数或恐慌。
func TestIncidentAttemptsEmptyInput(t *testing.T) {
	got := ComputeIncidentAttempts(nil)
	if got.VisibleAttempts != 0 || got.LogRows != 0 || got.DistinctRequests != 0 {
		t.Fatalf("空输入计数非零: %+v", got)
	}
	if !got.Exact {
		t.Fatal("空输入应为 Exact")
	}
}

// 负数渠道 ID 按取不到渠道处理。
func TestIncidentAttemptsNegativeChannelTreatedAsMissing(t *testing.T) {
	rows := []IncidentAttemptRow{
		{ChannelID: -1, CreatedAt: 1760000000, RequestID: "r1"},
	}
	got := ComputeIncidentAttempts(rows)
	if got.RowsWithoutChannel != 1 {
		t.Fatalf("负数渠道未按缺失处理: %+v", got)
	}
}

// itoa64 基本正确性（自实现，需要验证边界）。
func TestItoa64(t *testing.T) {
	cases := map[int64]string{
		0: "0", 1: "1", 9: "9", 10: "10", 115: "115",
		1760000000: "1760000000", -1: "-1", -115: "-115",
	}
	for in, want := range cases {
		if got := itoa64(in); got != want {
			t.Errorf("itoa64(%d) =%q want %q", in, got, want)
		}
	}
}
