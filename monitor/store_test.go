package monitor

import "testing"

// store_test.go:存储/聚合层集成测试。用临时 SQLite 塞入已知采样,验证从"采样行 → 看板数字"
// 这条聚合链路(三态、成功率、成本、错误分类、维度排序、趋势、UPSERT 幂等)算得对。

func newTestMonitor(t *testing.T) *Monitor {
	t.Helper()
	// 用与 LoadSettings 一致的 infra 默认阈值,使百分比判级逻辑在测试中按默认值生效。
	cfg := Settings{
		InfraMemAvailWarnPct:     25,
		InfraMemAvailBadPct:      15,
		InfraStorageAvailWarnPct: 25,
		InfraStorageAvailBadPct:  15,
		InfraCPUWarnPct:          70,
		InfraCPUBadPct:           85,
		InfraBurstWarnPct:        20,
		ProbeLatencyWarnMs:       500,
		ProbeLatencyBadMs:        1500,
		ProbeCertWarnDays:        30,
		ProbeCertBadDays:         7,
	}
	m := &Monitor{
		cfg:     cfg,
		chNames: map[string]string{},
	}
	if err := m.openStore(t.TempDir() + "/t.db"); err != nil {
		t.Fatalf("openStore: %v", err)
	}
	return m
}

func TestReplaceChannelSnapsKeepsDeletedLastSnapshot(t *testing.T) {
	m := newTestMonitor(t)
	if err := m.replaceChannelSnaps([]ChannelSnap{
		{ID: 1, Name: "old-name", Vendor: "OpenAI", BaseDomain: "a.example", Status: 1, UpdatedAt: 100},
		{ID: 2, Name: "deleted-later", Vendor: "Anthropic", BaseDomain: "b.example", Status: 1, UpdatedAt: 100},
	}, 100); err != nil {
		t.Fatal(err)
	}
	if err := m.replaceChannelSnaps([]ChannelSnap{
		{ID: 1, Name: "new-name", Vendor: "OpenAI", BaseDomain: "a.example", Status: 1, UpdatedAt: 200},
	}, 200); err != nil {
		t.Fatal(err)
	}
	var rows []ChannelSnap
	if err := m.storeDB.Order("id ASC").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Name != "new-name" || rows[0].DeletedAt != 0 {
		t.Fatalf("existing channel was not updated in place: %+v", rows)
	}
	if rows[1].Name != "deleted-later" || rows[1].BaseDomain != "b.example" || rows[1].DeletedAt != 200 {
		t.Fatalf("deleted channel lost its last snapshot: %+v", rows[1])
	}
	var current ChannelSnap
	// 权威整表结果即使在同一秒内刷新，也必须按 ID 准确判断删除。
	if err := m.replaceChannelSnapsAuthoritative([]ChannelSnap{
		{ID: 1, Name: "new-name", Vendor: "OpenAI", BaseDomain: "a.example", Status: 1, UpdatedAt: 200},
	}, 200); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.First(&current, "id = ?", 2).Error; err != nil || current.DeletedAt != 200 {
		t.Fatalf("same-second refresh missed deletion: %+v err=%v", current, err)
	}
	// 空结果代表读取异常，不能把仍存在的渠道误标删除。
	if err := m.replaceChannelSnaps(nil, 300); err != nil {
		t.Fatal(err)
	}
	current = ChannelSnap{}
	if err := m.storeDB.First(&current, "id = ?", 1).Error; err != nil || current.DeletedAt != 0 {
		t.Fatalf("empty refresh marked current channel deleted: %+v err=%v", current, err)
	}
	// 同 ID 重新出现时恢复为当前渠道，并同步最新名称。
	if err := m.replaceChannelSnaps([]ChannelSnap{{ID: 2, Name: "restored", Status: 1, UpdatedAt: 400}}, 400); err != nil {
		t.Fatal(err)
	}
	current = ChannelSnap{}
	if err := m.storeDB.First(&current, "id = ?", 2).Error; err != nil || current.DeletedAt != 0 || current.Name != "restored" {
		t.Fatalf("restored channel snapshot=%+v err=%v", current, err)
	}
	// 整表查询成功且返回空，表示所有渠道都已删除；快照仍保留。
	if err := m.replaceChannelSnapsAuthoritative(nil, 500); err != nil {
		t.Fatal(err)
	}
	var all []ChannelSnap
	if err := m.storeDB.Order("id ASC").Find(&all).Error; err != nil || len(all) != 2 || all[0].DeletedAt == 0 || all[1].DeletedAt != 500 {
		t.Fatalf("authoritative empty refresh did not retain tombstones: %+v err=%v", all, err)
	}
}

func TestStoreAggregation(t *testing.T) {
	m := newTestMonitor(t)
	const bucket = 1_700_000_000 / 60 * 60 // 任意分钟桶
	rows := []MetricSample{
		{BucketTs: bucket, ChannelID: 1, ModelName: "gpt", Grp: "g1",
			Success: 8, Anomaly: 1, Failed: 1, SumUseTime: 10, MaxUseTime: 5,
			Tokens: 100, Quota: 500000, Err5xx: 1, Lat1: 8, CompletionTokens: 80},
		{BucketTs: bucket, ChannelID: 2, ModelName: "gpt-4o", Grp: "g1",
			Success: 18, Anomaly: 0, Failed: 2, SumUseTime: 36, MaxUseTime: 4,
			Tokens: 200, Quota: 1000000, Err4xx: 2, Lat2: 18, CompletionTokens: 180},
	}
	if err := m.upsertSamples(rows); err != nil {
		t.Fatal(err)
	}

	since := int64(bucket - 60)
	const winSec = 900.0

	// —— 总览:三态 / 成本 / token / 错误分类 ——
	sum, err := m.storeSummary(since, winSec)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Total != 30 || sum.Success != 26 || sum.Anomaly != 1 || sum.Failed != 3 {
		t.Errorf("总览三态错: total=%d success=%d anomaly=%d failed=%d(应 30/26/1/3)",
			sum.Total, sum.Success, sum.Anomaly, sum.Failed)
	}
	if sum.Tokens != 300 {
		t.Errorf("tokens 应 300,实际 %d", sum.Tokens)
	}
	if !approx(sum.CostUSD, 3.0) { // quota 1.5M / 500000 = $3
		t.Errorf("cost 应 $3.0,实际 %v", sum.CostUSD)
	}
	if !approx(sum.ErrorRate, 10.0) { // 3/30
		t.Errorf("错误率应 10%%,实际 %v", sum.ErrorRate)
	}
	if sum.Err5xx != 1 || sum.Err4xx != 2 {
		t.Errorf("错误分类错: 5xx=%d 4xx=%d(应 1/2)", sum.Err5xx, sum.Err4xx)
	}

	// —— 按渠道:两行,按用户侧消费降序(渠道2 $2 在渠道1 $1 前)——
	ch, err := m.storeDim("channel_id", since, winSec)
	if err != nil {
		t.Fatal(err)
	}
	if len(ch) != 2 {
		t.Fatalf("应 2 个渠道,实际 %d", len(ch))
	}
	if ch[0].Key != "2" {
		t.Errorf("应按消费降序,渠道2($2)在前,实际首位 %q", ch[0].Key)
	}
	byCh := map[string]Row{ch[0].Key: ch[0], ch[1].Key: ch[1]}
	if r := byCh["1"]; r.Total != 10 || !approx(r.SuccessRate, 80) {
		t.Errorf("渠道1: total=%d successRate=%v(应 10 / 80%%)", r.Total, r.SuccessRate)
	}
	if r := byCh["2"]; r.Total != 20 || !approx(r.SuccessRate, 90) {
		t.Errorf("渠道2: total=%d successRate=%v(应 20 / 90%%)", r.Total, r.SuccessRate)
	}

	// —— 按分组:两条都在 g1,聚合成一行 ——
	grp, err := m.storeDim("grp", since, winSec)
	if err != nil {
		t.Fatal(err)
	}
	if len(grp) != 1 || grp[0].Key != "g1" || grp[0].Total != 30 {
		t.Errorf("分组聚合错: %+v", grp)
	}

	// —— 趋势:同一桶,成功 26 失败 3 ——
	trend, err := m.storeTrend(since, 15)
	if err != nil {
		t.Fatal(err)
	}
	if len(trend) != 1 || trend[0].Success != 26 || trend[0].Failed != 3 {
		t.Errorf("趋势错: %+v(应 1 桶 26/3)", trend)
	}
}

// TestStoreDimOrdersAllModelMonitorDimensionsByCost 验证 Top-N 排序口径不是旧的错误数。
// 高消费项即使没有错误，也必须排在低消费高错误项之前。
func TestStoreDimOrdersAllModelMonitorDimensionsByCost(t *testing.T) {
	m := newTestMonitor(t)
	const bucket = 1_700_000_000 / 60 * 60
	if err := m.upsertSamples([]MetricSample{
		{BucketTs: bucket, ChannelID: 11, ModelName: "high-cost-model", Grp: "high-cost-group", Success: 1, Quota: 2_000_000},
		{BucketTs: bucket, ChannelID: 22, ModelName: "high-error-model", Grp: "high-error-group", Failed: 20, Quota: 100_000},
	}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, dim, want string
	}{
		{name: "分组", dim: "grp", want: "high-cost-group"},
		{name: "渠道", dim: "channel_id", want: "11"},
		{name: "模型", dim: "model_name", want: "high-cost-model"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := m.storeDim(tc.dim, int64(bucket-60), 900)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 2 || rows[0].Key != tc.want {
				t.Fatalf("%s未按消费降序: %+v", tc.name, rows)
			}
		})
	}
}

// TestUpsertIdempotent:同一复合主键重复写入应【覆盖】而非累加(幂等),自愈采集重叠。
func TestUpsertIdempotent(t *testing.T) {
	m := newTestMonitor(t)
	const bucket = 1_700_000_000 / 60 * 60
	row := MetricSample{BucketTs: bucket, ChannelID: 1, ModelName: "gpt", Grp: "g1", Success: 5, Failed: 1}
	for i := 0; i < 3; i++ { // 写 3 次
		if err := m.upsertSamples([]MetricSample{row}); err != nil {
			t.Fatal(err)
		}
	}
	sum, err := m.storeSummary(int64(bucket-60), 900)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Success != 5 || sum.Failed != 1 {
		t.Errorf("UPSERT 应幂等(覆盖非累加),实际 success=%d failed=%d(应 5/1)", sum.Success, sum.Failed)
	}
}

// TestPruneOlderThan:留存清理只删早于 cutoff 的桶。
func TestPruneOlderThan(t *testing.T) {
	m := newTestMonitor(t)
	old := int64(1_700_000_000)
	recent := old + 86400
	if err := m.upsertSamples([]MetricSample{
		{BucketTs: old, ChannelID: 1, ModelName: "a", Grp: "g", Success: 1},
		{BucketTs: recent, ChannelID: 1, ModelName: "a", Grp: "g", Success: 1},
	}); err != nil {
		t.Fatal(err)
	}
	n, err := m.pruneOlderThan(old + 1) // 删早于 old+1 的(即 old 那条)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("应删 1 条,实际 %d", n)
	}
	if got := m.storeFreshness(); got != recent {
		t.Errorf("清理后最新桶应为 recent=%d,实际 %d", recent, got)
	}
}
