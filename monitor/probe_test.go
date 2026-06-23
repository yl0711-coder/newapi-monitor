package monitor

import (
	"net/http"
	"testing"
)

// probe_test.go:端到端探活的定级、域名解析、以及探活行进快照(总览/Probes)的聚合测试。

func TestProbeStatus(t *testing.T) {
	m := newTestMonitor(t)
	cases := []struct {
		name string
		p    ProbeResource
		want string
	}{
		{"ok", ProbeResource{Reachable: true, HTTPCode: 200, LatencyMs: 120, HasCert: true, CertDays: 90}, "ok"},
		{"unreachable", ProbeResource{Reachable: false, HTTPCode: 0}, "bad"},
		{"non-200", ProbeResource{Reachable: true, HTTPCode: 502, LatencyMs: 50}, "bad"},
		{"latency-warn", ProbeResource{Reachable: true, HTTPCode: 200, LatencyMs: 800}, "warn"},
		{"latency-bad", ProbeResource{Reachable: true, HTTPCode: 200, LatencyMs: 2000}, "bad"},
		{"cert-warn", ProbeResource{Reachable: true, HTTPCode: 200, LatencyMs: 50, HasCert: true, CertDays: 20}, "warn"},
		{"cert-bad", ProbeResource{Reachable: true, HTTPCode: 200, LatencyMs: 50, HasCert: true, CertDays: 5}, "bad"},
		// 最差冒泡:延时只 warn 但证书已 bad → bad
		{"worst-bubbles", ProbeResource{Reachable: true, HTTPCode: 200, LatencyMs: 800, HasCert: true, CertDays: 3}, "bad"},
	}
	for _, c := range cases {
		if got := m.probeStatus(c.p); got != c.want {
			t.Errorf("%s: 期望 %s, 得 %s", c.name, c.want, got)
		}
	}
}

func TestProbeDomainList(t *testing.T) {
	got := probeDomainList(" nexusapi.link , https://routepath.link/api/status ,, nexusapi.link , http://pathgo.link ")
	want := []string{"nexusapi.link", "routepath.link", "pathgo.link"} // 去前缀/去路径/去空/去重
	if len(got) != len(want) {
		t.Fatalf("期望 %v, 得 %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("第 %d 项期望 %s, 得 %s", i, want[i], got[i])
		}
	}
}

func TestProbeSnapshot(t *testing.T) {
	m := newTestMonitor(t)
	const bucket = 1_700_000_000 / 60 * 60
	rows := []InfraSample{
		// 域名 A:可达 200,延时 120ms,证书 90 天 → ok
		{BucketTs: bucket, Resource: "a.example", RType: "probe", Metric: "reachable", Value: 1},
		{BucketTs: bucket, Resource: "a.example", RType: "probe", Metric: "status_code", Value: 200},
		{BucketTs: bucket, Resource: "a.example", RType: "probe", Metric: "latency_ms", Value: 120},
		{BucketTs: bucket, Resource: "a.example", RType: "probe", Metric: "cert_days", Value: 90},
		// 域名 B:不可达 → bad
		{BucketTs: bucket, Resource: "b.example", RType: "probe", Metric: "reachable", Value: 0},
		{BucketTs: bucket, Resource: "b.example", RType: "probe", Metric: "status_code", Value: 0},
		{BucketTs: bucket, Resource: "b.example", RType: "probe", Metric: "latency_ms", Value: 0},
	}
	if err := m.upsertInfra(rows); err != nil {
		t.Fatal(err)
	}
	snap := m.computeInfraSnapshot(bucket + 30)
	if len(snap.Probes) != 2 {
		t.Fatalf("应有 2 个探活域名,得 %d", len(snap.Probes))
	}
	// 排序后 a.example 在前
	if snap.Probes[0].Domain != "a.example" || snap.Probes[0].Status != "ok" {
		t.Fatalf("a.example 应 ok,得 %+v", snap.Probes[0])
	}
	if snap.Probes[1].Domain != "b.example" || snap.Probes[1].Status != "bad" || snap.Probes[1].Reachable {
		t.Fatalf("b.example 应 bad 且不可达,得 %+v", snap.Probes[1])
	}
	// 探活不应混入实例/DB/LB
	if len(snap.Instances) != 0 || snap.Database != nil || snap.LB != nil {
		t.Fatalf("探活不应混入实例/DB/LB,得 instances=%d db=%v lb=%v", len(snap.Instances), snap.Database, snap.LB)
	}
	// 总览:有一个探活异常 → 整体 bad;计数 1/2
	if snap.Overview.Status != "bad" {
		t.Fatalf("总览应 bad(有探活异常),得 %s", snap.Overview.Status)
	}
	if snap.Overview.ProbesTotal != 2 || snap.Overview.ProbesOK != 1 {
		t.Fatalf("总览探活计数应 1/2,得 %d/%d", snap.Overview.ProbesOK, snap.Overview.ProbesTotal)
	}
}

func TestIsCloudFront(t *testing.T) {
	cases := []struct {
		name string
		h    http.Header
		want bool
	}{
		{"via-cloudfront", http.Header{"Via": {"1.1 abc.cloudfront.net (CloudFront)"}}, true},
		{"x-amz-cf-id", http.Header{"X-Amz-Cf-Id": {"someid=="}}, true},
		{"none", http.Header{"Server": {"nginx"}}, false},
		{"other-via", http.Header{"Via": {"1.1 squid"}}, false},
	}
	for _, c := range cases {
		if got := isCloudFront(c.h); got != c.want {
			t.Errorf("%s: 期望 %v, 得 %v", c.name, c.want, got)
		}
	}
}

func TestBuildLock(t *testing.T) {
	// 403 → 锁生效 ok
	l := buildLock("172.26.0.20:80", map[string]float64{"locked": 1, "status_code": 403}, 10)
	if !l.Locked || l.Status != "ok" || l.HTTPCode != 403 {
		t.Fatalf("403 应锁生效 ok,得 %+v", l)
	}
	// 200 → 锁失效 bad
	b := buildLock("172.26.10.97:80", map[string]float64{"locked": 0, "status_code": 200}, 10)
	if b.Locked || b.Status != "bad" || b.HTTPCode != 200 {
		t.Fatalf("200 应锁失效 bad,得 %+v", b)
	}
	// 无样本 → nosample
	if n := buildLock("x", map[string]float64{}, -1); n.Status != "nosample" {
		t.Fatalf("无样本应 nosample,得 %+v", n)
	}
}

func TestProbeViaDrift(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.ProbeExpectCDN = true
	// 可达 200 但未经 CloudFront → warn(边缘漂移)
	if got := m.probeStatus(ProbeResource{Reachable: true, HTTPCode: 200, LatencyMs: 50, ViaCDN: false}); got != "warn" {
		t.Fatalf("脱离 CDN 应 warn,得 %s", got)
	}
	// 经 CloudFront → ok
	if got := m.probeStatus(ProbeResource{Reachable: true, HTTPCode: 200, LatencyMs: 50, ViaCDN: true}); got != "ok" {
		t.Fatalf("经 CDN 应 ok,得 %s", got)
	}
}

func TestLockSnapshot(t *testing.T) {
	m := newTestMonitor(t)
	const bucket = 1_700_000_000 / 60 * 60
	rows := []InfraSample{
		// 源站 A:403 锁生效
		{BucketTs: bucket, Resource: "172.26.0.20:80", RType: "lock", Metric: "locked", Value: 1},
		{BucketTs: bucket, Resource: "172.26.0.20:80", RType: "lock", Metric: "status_code", Value: 403},
		// 源站 B:200 锁失效
		{BucketTs: bucket, Resource: "172.26.10.97:80", RType: "lock", Metric: "locked", Value: 0},
		{BucketTs: bucket, Resource: "172.26.10.97:80", RType: "lock", Metric: "status_code", Value: 200},
	}
	if err := m.upsertInfra(rows); err != nil {
		t.Fatal(err)
	}
	snap := m.computeInfraSnapshot(bucket + 30)
	if len(snap.Locks) != 2 {
		t.Fatalf("应有 2 个源站锁,得 %d", len(snap.Locks))
	}
	// 排序后 172.26.0.20 在前(字符串序)
	if snap.Locks[0].Target != "172.26.0.20:80" || snap.Locks[0].Status != "ok" {
		t.Fatalf("A 应 ok,得 %+v", snap.Locks[0])
	}
	if snap.Locks[1].Status != "bad" {
		t.Fatalf("B 应 bad(锁失效),得 %+v", snap.Locks[1])
	}
	// 锁不应混入实例/DB/LB/探活
	if len(snap.Instances) != 0 || len(snap.Probes) != 0 {
		t.Fatalf("锁不应混入实例/探活,得 instances=%d probes=%d", len(snap.Instances), len(snap.Probes))
	}
	// 总览:一个锁失效 → 整体 bad;计数 1/2
	if snap.Overview.Status != "bad" {
		t.Fatalf("总览应 bad(有锁失效),得 %s", snap.Overview.Status)
	}
	if snap.Overview.LocksTotal != 2 || snap.Overview.LocksOK != 1 {
		t.Fatalf("总览锁计数应 1/2,得 %d/%d", snap.Overview.LocksOK, snap.Overview.LocksTotal)
	}
}

func TestSortInstancesByImportance(t *testing.T) {
	rs := []InfraResource{
		{Name: "Ubuntu-1"}, {Name: "Redis-NexusAPI-New"},
		{Name: "Ubuntu-NexusAPI-Slave-1"}, {Name: "Ubuntu-NexusAPI-Master"},
	}
	sortInstances(rs)
	want := []string{"Ubuntu-NexusAPI-Master", "Ubuntu-NexusAPI-Slave-1", "Redis-NexusAPI-New", "Ubuntu-1"}
	for i, w := range want {
		if rs[i].Name != w {
			t.Fatalf("第 %d 位应为 %s,得 %s(完整:%+v)", i, w, rs[i].Name, rs)
		}
	}
}

func TestInfraOverviewWorstBubbles(t *testing.T) {
	m := newTestMonitor(t)
	const bucket = 1_700_000_000 / 60 * 60
	// 实例 ok + DB ok,但有一个探活 warn(高延时) → 总览 warn
	rows := []InfraSample{
		{BucketTs: bucket, Resource: "Node-A", RType: "instance", Metric: "cpu", Value: 5},
		{BucketTs: bucket, Resource: "Node-A", RType: "instance", Metric: "status_failed", Value: 0},
		{BucketTs: bucket, Resource: "w.example", RType: "probe", Metric: "reachable", Value: 1},
		{BucketTs: bucket, Resource: "w.example", RType: "probe", Metric: "status_code", Value: 200},
		{BucketTs: bucket, Resource: "w.example", RType: "probe", Metric: "latency_ms", Value: 900},
	}
	if err := m.upsertInfra(rows); err != nil {
		t.Fatal(err)
	}
	snap := m.computeInfraSnapshot(bucket + 30)
	if snap.Overview.Status != "warn" {
		t.Fatalf("总览应 warn(探活延时偏高冒泡),得 %s", snap.Overview.Status)
	}
	if snap.Overview.InstancesTotal != 1 || snap.Overview.InstancesOK != 1 {
		t.Fatalf("实例计数应 1/1,得 %d/%d", snap.Overview.InstancesOK, snap.Overview.InstancesTotal)
	}
}
