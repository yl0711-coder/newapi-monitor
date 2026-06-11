package monitor

import "testing"

// disabled_channel_test.go:验证"禁用渠道不计入稳定性、重启用从启用时刻起算、按渠道仍全显"。

func TestNextEnabledSince(t *testing.T) {
	const now = int64(1000)
	cases := []struct {
		name               string
		status, prevStatus int
		prevSince, want    int64
	}{
		{"手动禁用→0", 2, 1, 500, 0},
		{"自动熔断→0", 3, 1, 500, 0},
		{"一直启用→保持原值", 1, 1, 500, 500},
		{"重启用(上轮禁用)→now", 1, 2, 0, now},
		{"新建即启用→now", 1, 0, 0, now},
		{"上轮启用且 since=0→保持0(自始启用,升级不重置)", 1, 1, 0, 0},
	}
	for _, c := range cases {
		if got := nextEnabledSince(c.status, c.prevStatus, c.prevSince, now); got != c.want {
			t.Errorf("%s: got %d want %d", c.name, got, c.want)
		}
	}
}

func TestStabilityExcludesDisabledChannels(t *testing.T) {
	m := newTestMonitor(t)
	const now = int64(1_900_000_000)
	for _, cs := range []ChannelSnap{
		{ID: 1, Status: 1, Groups: "g1", Models: "m1", EnabledSince: now - 3600}, // 久前启用
		{ID: 2, Status: 2, Groups: "g1", Models: "m1", EnabledSince: 0},          // 手动禁用
		{ID: 3, Status: 1, Groups: "g1", Models: "m1", EnabledSince: now - 120},  // 最近重启用
	} {
		if err := m.storeDB.Create(&cs).Error; err != nil {
			t.Fatal(err)
		}
	}
	b := func(off int64) int64 { return (now - off) / 60 * 60 }
	rows := []MetricSample{
		{BucketTs: b(1800), ChannelID: 1, ModelName: "m1", Grp: "g1", Success: 10}, // 启用渠道 → 计
		{BucketTs: b(1800), ChannelID: 2, ModelName: "m1", Grp: "g1", Failed: 50},  // 禁用渠道 → 不计
		{BucketTs: b(600), ChannelID: 3, ModelName: "m1", Grp: "g1", Failed: 30},   // 重启用之前 → 不计
		{BucketTs: b(60), ChannelID: 3, ModelName: "m1", Grp: "g1", Success: 5},    // 重启用之后 → 计
	}
	if err := m.upsertSamples(rows); err != nil {
		t.Fatal(err)
	}
	since := now - 7200

	// 总览:只剩 成功15 / 失败0(禁用的 50 + 重启用前的 30 都排除)
	sum, err := m.storeSummary(since, 7200)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Success != 15 || sum.Failed != 0 {
		t.Fatalf("storeSummary 应 成功15/失败0,得 成功%d/失败%d", sum.Success, sum.Failed)
	}
	// 按分组、按模型:同样排除
	grp, _ := m.storeDim("grp", since, 7200)
	if len(grp) != 1 || grp[0].Success != 15 || grp[0].Failed != 0 {
		t.Fatalf("按分组应 成功15/失败0,得 %+v", grp)
	}
	md, _ := m.storeDim("model_name", since, 7200)
	if len(md) != 1 || md[0].Failed != 0 {
		t.Fatalf("按模型失败应 0,得 %+v", md)
	}
	// 按渠道:不过滤,3 个渠道都保留,禁用渠道的失败仍可见(排障用),合计 50+30=80
	ch, _ := m.storeDim("channel_id", since, 7200)
	var chFailed int64
	for _, r := range ch {
		chFailed += r.Failed
	}
	if len(ch) != 3 || chFailed != 80 {
		t.Fatalf("按渠道应保留全部3渠道、失败合计80,得 %d渠道/失败%d", len(ch), chFailed)
	}
}
