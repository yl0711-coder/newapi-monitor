package monitor

import "testing"

// infra_test.go:服务端健康监控的存储与快照/状态逻辑测试(不需真 AWS,构造采样验证聚合链路)。

func TestInfraStoreAndSnapshot(t *testing.T) {
	m := newTestMonitor(t)
	const bucket = 1_700_000_000 / 60 * 60
	rows := []InfraSample{
		// 数据库:总内存 1024MB,可用 60MB → 可用率 ~5.9%,低于 bad 阈值 15% 应判 bad
		{BucketTs: bucket, Resource: "DB-X", RType: "database", Metric: "free_mem_mb", Value: 60},
		{BucketTs: bucket, Resource: "DB-X", RType: "database", Metric: "mem_total_mb", Value: 1024},
		{BucketTs: bucket, Resource: "DB-X", RType: "database", Metric: "swap_mb", Value: 210},
		{BucketTs: bucket, Resource: "DB-X", RType: "database", Metric: "free_storage_gb", Value: 37},
		{BucketTs: bucket, Resource: "DB-X", RType: "database", Metric: "disk_total_gb", Value: 40},
		{BucketTs: bucket, Resource: "DB-X", RType: "database", Metric: "cpu", Value: 4},
		// 实例:AWS 侧 + host 侧合并(同名);总内存 2048MB,可用 1024MB → 已用 50%,ok
		{BucketTs: bucket, Resource: "Node-A", RType: "instance", Metric: "cpu", Value: 5},
		{BucketTs: bucket, Resource: "Node-A", RType: "instance", Metric: "status_failed", Value: 0},
		{BucketTs: bucket, Resource: "Node-A", RType: "instance", Metric: "mem_total_mb", Value: 2048},
		{BucketTs: bucket, Resource: "Node-A", RType: "host", Metric: "mem_avail_mb", Value: 1024},
		{BucketTs: bucket, Resource: "Node-A", RType: "host", Metric: "disk_used_pct", Value: 15},
		// LB:有不健康节点
		{BucketTs: bucket, Resource: "LB-X", RType: "lb", Metric: "healthy", Value: 1},
		{BucketTs: bucket, Resource: "LB-X", RType: "lb", Metric: "unhealthy", Value: 1},
	}
	if err := m.upsertInfra(rows); err != nil {
		t.Fatal(err)
	}
	// 幂等:重复写同键不报错、不重复
	if err := m.upsertInfra(rows); err != nil {
		t.Fatal(err)
	}
	if got := m.storeInfraLatest(); len(got) != len(rows) {
		t.Fatalf("storeInfraLatest 应 %d 行,得 %d", len(rows), len(got))
	}

	snap := m.computeInfraSnapshot(bucket + 30)
	if snap.Database == nil || snap.Database.Status != "bad" {
		t.Fatalf("DB 可用内存 ~5.9%% 应判 bad,得 %+v", snap.Database)
	}
	if snap.Database.Metrics["free_mem_mb"] != 60 {
		t.Fatalf("DB free_mem_mb 应为 60,得 %v", snap.Database.Metrics["free_mem_mb"])
	}
	// 派生百分比应已写入
	if p := snap.Database.Metrics["mem_avail_pct"]; p < 5.8 || p > 6.0 {
		t.Fatalf("DB mem_avail_pct 应约 5.86%%,得 %v", p)
	}
	if p := snap.Database.Metrics["storage_avail_pct"]; p < 92 || p > 93 {
		t.Fatalf("DB storage_avail_pct 应约 92.5%%,得 %v", p)
	}
	if len(snap.Instances) != 1 || snap.Instances[0].Name != "Node-A" {
		t.Fatalf("应有 1 个实例 Node-A,得 %+v", snap.Instances)
	}
	// host 指标应并入同名实例
	if snap.Instances[0].Metrics["mem_avail_mb"] != 1024 {
		t.Fatalf("实例应合并 host 的 mem_avail_mb=1024,得 %v", snap.Instances[0].Metrics["mem_avail_mb"])
	}
	// 已用内存% = (2048-1024)/2048 = 50%
	if p := snap.Instances[0].Metrics["mem_used_pct"]; p < 49 || p > 51 {
		t.Fatalf("实例 mem_used_pct 应约 50%%,得 %v", p)
	}
	if snap.Instances[0].Status != "ok" { // 已用 50% 内存、CPU 5% 不触发任何阈值
		t.Fatalf("实例状态应 ok,得 %s", snap.Instances[0].Status)
	}
	if snap.LB == nil || snap.LB.Status != "bad" {
		t.Fatalf("LB 有不健康节点应判 bad,得 %+v", snap.LB)
	}
	// 趋势改为前端按需经 storeInfraSeries 拉(GET /infra/series),验证该数据路径可取到 swap 序列
	if pts := m.storeInfraSeries("DB-X", "swap_mb", bucket-3600); len(pts) == 0 || pts[len(pts)-1].Value != 210 {
		t.Fatalf("storeInfraSeries 应取到 DB swap 序列(末值 210),得 %+v", pts)
	}
}

func TestInfraStatusThresholds(t *testing.T) {
	m := newTestMonitor(t)
	cases := []struct {
		name string
		r    InfraResource
		want string
	}{
		// DB 可用内存 10% < bad 15% → bad
		{"db-mem-bad", InfraResource{Type: "database", Metrics: map[string]float64{"mem_avail_pct": 10}}, "bad"},
		// DB 可用内存 20% 在 warn(25%)与 bad(15%)之间 → warn
		{"db-mem-warn", InfraResource{Type: "database", Metrics: map[string]float64{"mem_avail_pct": 20}}, "warn"},
		// DB 存储可用 10% < bad 15% → bad
		{"db-storage-bad", InfraResource{Type: "database", Metrics: map[string]float64{"storage_avail_pct": 10}}, "bad"},
		// DB CPU 90% > bad 85% → bad
		{"db-cpu-bad", InfraResource{Type: "database", Metrics: map[string]float64{"cpu": 90}}, "bad"},
		{"db-ok", InfraResource{Type: "database", Metrics: map[string]float64{"mem_avail_pct": 60, "cpu": 5}}, "ok"},
		{"inst-down", InfraResource{Type: "instance", Metrics: map[string]float64{"status_failed": 1}}, "bad"},
		// 实例已用内存 90% > 100-15=85% → bad
		{"inst-mem-bad", InfraResource{Type: "instance", Metrics: map[string]float64{"mem_used_pct": 90}}, "bad"},
		// 实例 CPU 75% 在 warn(70%)与 bad(85%)之间 → warn
		{"inst-cpu-warn", InfraResource{Type: "instance", Metrics: map[string]float64{"cpu": 75}}, "warn"},
		// 突发额度 10% < 20% → warn
		{"inst-burst-warn", InfraResource{Type: "instance", Metrics: map[string]float64{"burst": 10}}, "warn"},
		{"inst-ok", InfraResource{Type: "instance", Metrics: map[string]float64{"cpu": 10}}, "ok"},
		{"lb-bad", InfraResource{Type: "lb", Metrics: map[string]float64{"unhealthy": 2}}, "bad"},
		{"nosample", InfraResource{Type: "instance", Metrics: map[string]float64{}}, "nosample"},
	}
	for _, c := range cases {
		if got := m.infraStatus(c.r); got != c.want {
			t.Errorf("%s: 期望 %s, 得 %s", c.name, c.want, got)
		}
	}
}
