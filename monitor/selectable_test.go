package monitor

import "testing"

// selectable_test.go:验证监控只统计"可选 (分组,模型)"——不可选(误路由 / 不可选分组)不计入聚合与报警。

func TestSelectableFilterExcludesUnselectable(t *testing.T) {
	m := newTestMonitor(t)
	const now = int64(1_900_000_000)
	for _, cs := range []ChannelSnap{
		{ID: 1, Status: 1, Groups: "codex", Models: "gpt-5.2", EnabledSince: now - 3600},
		{ID: 2, Status: 1, Groups: "internal_test", Models: "gpt-image-1", EnabledSince: now - 3600},
	} {
		if err := m.storeDB.Create(&cs).Error; err != nil {
			t.Fatal(err)
		}
	}
	// 可选对:只有 codex 是可见分组(internal_test 不在 /api/pricing 里)
	if err := m.storeDB.Create(&SelectablePair{Grp: "codex", Model: "gpt-5.2"}).Error; err != nil {
		t.Fatal(err)
	}
	b := func(off int64) int64 { return (now - off) / 60 * 60 }
	rows := []MetricSample{
		{BucketTs: b(600), ChannelID: 1, ModelName: "gpt-5.2", Grp: "codex", Success: 20},           // 可选 → 计
		{BucketTs: b(600), ChannelID: 1, ModelName: "gpt-image-1", Grp: "codex", Failed: 5},         // 误路由(codex 未配该模型)→ 不计
		{BucketTs: b(600), ChannelID: 2, ModelName: "gpt-image-1", Grp: "internal_test", Failed: 9}, // 不可选分组 → 不计
	}
	if err := m.upsertSamples(rows); err != nil {
		t.Fatal(err)
	}
	since := now - 7200

	sum, err := m.storeSummary(since, 7200)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Success != 20 || sum.Failed != 0 {
		t.Fatalf("总览应只算可选(成功20/失败0),得 成功%d/失败%d", sum.Success, sum.Failed)
	}
	md, _ := m.storeDim("model_name", since, 7200)
	if len(md) != 1 || md[0].Key != "gpt-5.2" {
		t.Fatalf("按模型应只剩 gpt-5.2(gpt-image-1 不可选,隐藏),得 %+v", md)
	}
	grp, _ := m.storeDim("grp", since, 7200)
	if len(grp) != 1 || grp[0].Key != "codex" {
		t.Fatalf("按分组应只剩 codex(internal_test 不可选,隐藏),得 %+v", grp)
	}
	// 按渠道不过滤:排障仍能看到误路由,失败合计 5+9=14
	ch, _ := m.storeDim("channel_id", since, 7200)
	var chFailed int64
	for _, r := range ch {
		chFailed += r.Failed
	}
	if chFailed != 14 {
		t.Fatalf("按渠道不过滤,失败合计应 14(误路由仍可见),得 %d", chFailed)
	}
}

// 空 selectable_pairs(未拉到 / 新部署首刷前)→ fail-open 不过滤,避免监控空窗。
func TestSelectableFilterFailOpenWhenEmpty(t *testing.T) {
	m := newTestMonitor(t)
	const now = int64(1_900_000_000)
	if err := m.storeDB.Create(&ChannelSnap{ID: 1, Status: 1, Groups: "codex", Models: "gpt-5.2", EnabledSince: now - 3600}).Error; err != nil {
		t.Fatal(err)
	}
	b := (now - 600) / 60 * 60
	if err := m.upsertSamples([]MetricSample{{BucketTs: b, ChannelID: 1, ModelName: "gpt-5.2", Grp: "codex", Success: 7}}); err != nil {
		t.Fatal(err)
	}
	sum, _ := m.storeSummary(now-7200, 7200)
	if sum.Success != 7 {
		t.Fatalf("selectable_pairs 为空应 fail-open 不过滤,成功应 7,得 %d", sum.Success)
	}
}
