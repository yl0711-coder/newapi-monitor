package monitor

import "testing"

// infra_test.go:服务端健康监控的存储与快照/状态逻辑测试(不需真 AWS,构造采样验证聚合链路)。

func TestInfraStoreAndSnapshot(t *testing.T) {
	m := newTestMonitor(t)
	const bucket = 1_700_000_000 / 60 * 60
	rows := []InfraSample{
		// 数据库:内存告急(<80)
		{BucketTs: bucket, Resource: "DB-X", RType: "database", Metric: "free_mem_mb", Value: 60},
		{BucketTs: bucket, Resource: "DB-X", RType: "database", Metric: "swap_mb", Value: 210},
		{BucketTs: bucket, Resource: "DB-X", RType: "database", Metric: "free_storage_gb", Value: 37},
		{BucketTs: bucket, Resource: "DB-X", RType: "database", Metric: "cpu", Value: 4},
		// 实例:AWS 侧 + host 侧合并(同名)
		{BucketTs: bucket, Resource: "Node-A", RType: "instance", Metric: "cpu", Value: 5},
		{BucketTs: bucket, Resource: "Node-A", RType: "instance", Metric: "status_failed", Value: 0},
		{BucketTs: bucket, Resource: "Node-A", RType: "host", Metric: "mem_avail_mb", Value: 220},
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
	if got := m.storeInfraLatest(); len(got) != 10 {
		t.Fatalf("storeInfraLatest 应 10 行,得 %d", len(got))
	}

	snap := m.computeInfraSnapshot(bucket + 30)
	if snap.Database == nil || snap.Database.Status != "bad" {
		t.Fatalf("DB 内存 60MB 应判 bad,得 %+v", snap.Database)
	}
	if snap.Database.Metrics["free_mem_mb"] != 60 {
		t.Fatalf("DB free_mem_mb 应为 60,得 %v", snap.Database.Metrics["free_mem_mb"])
	}
	if len(snap.Instances) != 1 || snap.Instances[0].Name != "Node-A" {
		t.Fatalf("应有 1 个实例 Node-A,得 %+v", snap.Instances)
	}
	// host 指标应并入同名实例
	if snap.Instances[0].Metrics["mem_avail_mb"] != 220 {
		t.Fatalf("实例应合并 host 的 mem_avail_mb=220,得 %v", snap.Instances[0].Metrics["mem_avail_mb"])
	}
	if snap.Instances[0].Status != "ok" { // mem_avail 220 不触发 warn(阈值 200)
		t.Fatalf("实例状态应 ok,得 %s", snap.Instances[0].Status)
	}
	if snap.LB == nil || snap.LB.Status != "bad" {
		t.Fatalf("LB 有不健康节点应判 bad,得 %+v", snap.LB)
	}
	if len(snap.DBSwapTrend) == 0 {
		t.Fatalf("应有 DB swap 趋势点")
	}
}

func TestInfraStatusThresholds(t *testing.T) {
	cases := []struct {
		name string
		r    InfraResource
		want string
	}{
		{"db-mem-bad", InfraResource{Type: "database", Metrics: map[string]float64{"free_mem_mb": 70}}, "bad"},
		{"db-mem-warn", InfraResource{Type: "database", Metrics: map[string]float64{"free_mem_mb": 120}}, "warn"},
		{"db-ok", InfraResource{Type: "database", Metrics: map[string]float64{"free_mem_mb": 400, "cpu": 5}}, "ok"},
		{"inst-down", InfraResource{Type: "instance", Metrics: map[string]float64{"status_failed": 1}}, "bad"},
		{"inst-ok", InfraResource{Type: "instance", Metrics: map[string]float64{"cpu": 10}}, "ok"},
		{"lb-bad", InfraResource{Type: "lb", Metrics: map[string]float64{"unhealthy": 2}}, "bad"},
		{"nosample", InfraResource{Type: "instance", Metrics: map[string]float64{}}, "nosample"},
	}
	for _, c := range cases {
		if got := infraStatus(c.r); got != c.want {
			t.Errorf("%s: 期望 %s, 得 %s", c.name, c.want, got)
		}
	}
}
