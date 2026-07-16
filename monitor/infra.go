package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lightsail"
	lstypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
)

// infra.go:服务端健康监控(实例 / 数据库 / 负载均衡)。
// 数据来自 AWS Lightsail 指标接口(只读,对实例零影响——读 AWS 指标存储,不碰实例本身)。
// 主机 OS 内存/磁盘 AWS 取不到,由各节点 agent 推送(见 server.go 的 /internal/host)。
// 全部受 MONITOR_INFRA_ENABLED 开关控制,关闭时本文件逻辑完全不触发。
//
// 存储单位归一(写入 infra_samples.value):内存/swap = MB、存储 = GB、网络 = KB、
// 响应时间 = ms、CPU/突发额度 = %、连接/磁盘队列/状态/节点数 = 原值。

// infraTarget 是一个被监控资源:name=资源名,rtype=instance/database/lb。
// memTotalMB / diskTotalGB 来自资源发现时 AWS 一并返回的硬件规格(0=未知,LB 无此概念)。
type infraTarget struct {
	name        string
	rtype       string
	memTotalMB  float64 // 总内存(MB);0 表示未知
	diskTotalGB float64 // 系统盘总量(GB);0 表示未知
}

// metricSpec 描述一个要拉取的指标:AWS 指标名、统计量、单位、归一后存储的 key、归一除数。
type metricSpec struct {
	metric string
	stat   lstypes.MetricStatistic
	unit   lstypes.MetricUnit
	key    string
	scale  float64 // value/scale 后入库(如 bytes→MB 用 1048576;秒→毫秒用 0.001)
}

func specsFor(rtype string) []metricSpec {
	switch rtype {
	case "instance":
		return []metricSpec{
			{"CPUUtilization", lstypes.MetricStatisticAverage, lstypes.MetricUnitPercent, "cpu", 1},
			{"StatusCheckFailed", lstypes.MetricStatisticMaximum, lstypes.MetricUnitCount, "status_failed", 1},
			{"NetworkOut", lstypes.MetricStatisticAverage, lstypes.MetricUnitBytes, "net_out_kb", 1024},
			{"BurstCapacityPercentage", lstypes.MetricStatisticAverage, lstypes.MetricUnitPercent, "burst", 1},
		}
	case "database":
		return []metricSpec{
			{"CPUUtilization", lstypes.MetricStatisticAverage, lstypes.MetricUnitPercent, "cpu", 1},
			{"DatabaseConnections", lstypes.MetricStatisticMaximum, lstypes.MetricUnitCount, "connections", 1},
			{"FreeStorageSpace", lstypes.MetricStatisticAverage, lstypes.MetricUnitBytes, "free_storage_gb", 1073741824},
			{"DiskQueueDepth", lstypes.MetricStatisticAverage, lstypes.MetricUnitCount, "disk_queue", 1},
			{"FreeableMemory", lstypes.MetricStatisticAverage, lstypes.MetricUnitBytes, "free_mem_mb", 1048576},
			{"SwapUsage", lstypes.MetricStatisticAverage, lstypes.MetricUnitBytes, "swap_mb", 1048576},
		}
	case "lb":
		return []metricSpec{
			{"HealthyHostCount", lstypes.MetricStatisticMaximum, lstypes.MetricUnitCount, "healthy", 1},
			{"UnhealthyHostCount", lstypes.MetricStatisticMaximum, lstypes.MetricUnitCount, "unhealthy", 1},
			{"HTTPCode_LB_5XX_Count", lstypes.MetricStatisticSum, lstypes.MetricUnitCount, "err_5xx", 1},
			{"InstanceResponseTime", lstypes.MetricStatisticAverage, lstypes.MetricUnitSeconds, "resp_ms", 0.001},
		}
	}
	return nil
}

// lightsailClient 用 SDK 默认凭证链(AWS_ACCESS_KEY_ID/_SECRET 环境变量)创建客户端。
func (m *Monitor) lightsailClient(ctx context.Context) (*lightsail.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(m.cfg.AWSRegion))
	if err != nil {
		return nil, err
	}
	return lightsail.NewFromConfig(cfg), nil
}

// startInfra 启动服务端健康监控的独立采样循环(独立于日志采样,周期更长)。
func (m *Monitor) startInfra(ctx context.Context) {
	iv := time.Duration(m.cfg.InfraSampleSeconds) * time.Second
	if iv < 60*time.Second {
		iv = 60 * time.Second
	}
	m.sampleInfra(ctx) // 启动即拉一次
	m.startProbe(ctx)  // 端到端可用性探活(独立循环,默认 60s,对真实域名)
	go func() {
		t := time.NewTicker(iv)
		defer t.Stop()
		var ticks int
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.sampleInfra(ctx)
				m.evaluateInfraAlerts(time.Now().Unix())
				ticks++
				if ticks%10 == 0 { // 周期性清理过期 infra 采样
					if d := m.cfg.InfraRetentionDays; d > 0 {
						if n, err := m.pruneInfraOlderThan(time.Now().Unix() - int64(d)*86400); err == nil && n > 0 {
							slog.Info("清理过期 infra 采样", "rows", n)
						}
					}
				}
			}
		}
	}()
	slog.Info("服务端健康监控已启动", "interval", iv.String(), "region", m.cfg.AWSRegion)
}

// sampleInfra 拉一轮实例/DB/LB 的 AWS 指标写本地。fail-open:任一步失败仅记日志,不影响主监控。
func (m *Monitor) sampleInfra(ctx context.Context) {
	if !m.cfg.InfraEnabled {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	cl, err := m.lightsailClient(cctx)
	if err != nil {
		slog.Warn("infra: AWS 客户端初始化失败(忽略本轮)", "err", err)
		return
	}
	targets := m.infraTargets(cctx, cl)
	if len(targets) == 0 {
		slog.Warn("infra: 无监控目标(自动发现为空且未配 MONITOR_INFRA_RESOURCES)")
		return
	}
	bucket := time.Now().Unix() / 60 * 60
	var rows []InfraSample
	for _, t := range targets {
		for _, sp := range specsFor(t.rtype) {
			if v, ok := m.fetchMetric(cctx, cl, t, sp); ok {
				rows = append(rows, InfraSample{BucketTs: bucket, Resource: t.name, RType: t.rtype, Metric: sp.key, Value: v})
			}
		}
		// 合成「硬件总量」指标(来自资源发现时 AWS 已返回的规格,不额外调 API),
		// 写成普通 InfraSample,computeInfraSnapshot 与前端即可像普通指标一样读到。
		if t.memTotalMB > 0 {
			rows = append(rows, InfraSample{BucketTs: bucket, Resource: t.name, RType: t.rtype, Metric: "mem_total_mb", Value: t.memTotalMB})
		}
		if t.diskTotalGB > 0 {
			rows = append(rows, InfraSample{BucketTs: bucket, Resource: t.name, RType: t.rtype, Metric: "disk_total_gb", Value: t.diskTotalGB})
		}
	}
	if err := m.upsertInfra(rows); err != nil {
		slog.Warn("infra: 采样入库失败(忽略)", "err", err)
		return
	}
	slog.Info("infra 采样完成", "targets", len(targets), "rows", len(rows))
}

// infraTargets 决定监控哪些资源:显式配置(MONITOR_INFRA_RESOURCES)优先,否则自动发现。
func (m *Monitor) infraTargets(ctx context.Context, cl *lightsail.Client) []infraTarget {
	if s := strings.TrimSpace(m.cfg.InfraResources); s != "" {
		var out []infraTarget
		for _, part := range strings.Split(s, ",") {
			kv := strings.SplitN(strings.TrimSpace(part), ":", 2)
			if len(kv) == 2 && kv[0] != "" && kv[1] != "" {
				out = append(out, infraTarget{name: kv[1], rtype: kv[0]})
			}
		}
		return out
	}
	var out []infraTarget
	if r, err := cl.GetInstances(ctx, &lightsail.GetInstancesInput{}); err == nil {
		for _, in := range r.Instances {
			if in.Name == nil {
				continue
			}
			t := infraTarget{name: *in.Name, rtype: "instance"}
			if h := in.Hardware; h != nil {
				if h.RamSizeInGb != nil {
					t.memTotalMB = float64(*h.RamSizeInGb) * 1024
				}
				t.diskTotalGB = systemDiskGB(h.Disks)
			}
			out = append(out, t)
		}
	} else {
		slog.Warn("infra: 列实例失败", "err", err)
	}
	if r, err := cl.GetRelationalDatabases(ctx, &lightsail.GetRelationalDatabasesInput{}); err == nil {
		for _, d := range r.RelationalDatabases {
			if d.Name == nil {
				continue
			}
			t := infraTarget{name: *d.Name, rtype: "database"}
			if h := d.Hardware; h != nil {
				if h.RamSizeInGb != nil {
					t.memTotalMB = float64(*h.RamSizeInGb) * 1024
				}
				if h.DiskSizeInGb != nil {
					t.diskTotalGB = float64(*h.DiskSizeInGb)
				}
			}
			out = append(out, t)
		}
	} else {
		slog.Warn("infra: 列数据库失败", "err", err)
	}
	if r, err := cl.GetLoadBalancers(ctx, &lightsail.GetLoadBalancersInput{}); err == nil {
		for _, lb := range r.LoadBalancers {
			if lb.Name != nil {
				out = append(out, infraTarget{name: *lb.Name, rtype: "lb"}) // LB 无内存/磁盘概念,跳过总量
			}
		}
	} else {
		slog.Warn("infra: 列负载均衡失败", "err", err)
	}
	return out
}

// systemDiskGB 从实例硬件的磁盘列表取系统盘容量(GB);取不到系统盘则退而取首块盘,均无则 0。
func systemDiskGB(disks []lstypes.Disk) float64 {
	var first float64
	for i, d := range disks {
		if d.SizeInGb == nil {
			continue
		}
		sz := float64(*d.SizeInGb)
		if i == 0 || first == 0 {
			first = sz
		}
		if d.IsSystemDisk != nil && *d.IsSystemDisk {
			return sz
		}
	}
	return first
}

// fetchMetric 拉单个 (资源,指标) 的最近值。Lightsail 指标接口【一次只接受一个 statistic】,故逐个拉。
func (m *Monitor) fetchMetric(ctx context.Context, cl *lightsail.Client, t infraTarget, sp metricSpec) (float64, bool) {
	end := time.Now()
	start := end.Add(-2 * time.Hour)
	period := int32(300)
	stats := []lstypes.MetricStatistic{sp.stat}
	var dps []lstypes.MetricDatapoint
	var err error
	switch t.rtype {
	case "instance":
		var out *lightsail.GetInstanceMetricDataOutput
		out, err = cl.GetInstanceMetricData(ctx, &lightsail.GetInstanceMetricDataInput{
			InstanceName: aws.String(t.name), MetricName: lstypes.InstanceMetricName(sp.metric),
			Period: aws.Int32(period), StartTime: aws.Time(start), EndTime: aws.Time(end),
			Unit: sp.unit, Statistics: stats,
		})
		if out != nil {
			dps = out.MetricData
		}
	case "database":
		var out *lightsail.GetRelationalDatabaseMetricDataOutput
		out, err = cl.GetRelationalDatabaseMetricData(ctx, &lightsail.GetRelationalDatabaseMetricDataInput{
			RelationalDatabaseName: aws.String(t.name), MetricName: lstypes.RelationalDatabaseMetricName(sp.metric),
			Period: aws.Int32(period), StartTime: aws.Time(start), EndTime: aws.Time(end),
			Unit: sp.unit, Statistics: stats,
		})
		if out != nil {
			dps = out.MetricData
		}
	case "lb":
		var out *lightsail.GetLoadBalancerMetricDataOutput
		out, err = cl.GetLoadBalancerMetricData(ctx, &lightsail.GetLoadBalancerMetricDataInput{
			LoadBalancerName: aws.String(t.name), MetricName: lstypes.LoadBalancerMetricName(sp.metric),
			Period: aws.Int32(period), StartTime: aws.Time(start), EndTime: aws.Time(end),
			Unit: sp.unit, Statistics: stats,
		})
		if out != nil {
			dps = out.MetricData
		}
	default:
		return 0, false
	}
	if err != nil {
		slog.Warn("infra: 拉取指标失败", "resource", t.name, "metric", sp.metric, "err", err)
		return 0, false
	}
	dp, ok := latestDatapoint(dps, sp.stat)
	if !ok {
		return 0, false
	}
	scale := sp.scale
	if scale == 0 {
		scale = 1
	}
	return statValue(dp, sp.stat) / scale, true
}

// latestDatapoint 取含指定统计量的最新(时间最大)数据点。
func latestDatapoint(dps []lstypes.MetricDatapoint, stat lstypes.MetricStatistic) (lstypes.MetricDatapoint, bool) {
	var best lstypes.MetricDatapoint
	found := false
	for _, dp := range dps {
		if !hasStat(dp, stat) || dp.Timestamp == nil {
			continue
		}
		if !found || dp.Timestamp.After(*best.Timestamp) {
			best, found = dp, true
		}
	}
	return best, found
}

func hasStat(dp lstypes.MetricDatapoint, stat lstypes.MetricStatistic) bool {
	switch stat {
	case lstypes.MetricStatisticAverage:
		return dp.Average != nil
	case lstypes.MetricStatisticMaximum:
		return dp.Maximum != nil
	case lstypes.MetricStatisticMinimum:
		return dp.Minimum != nil
	case lstypes.MetricStatisticSum:
		return dp.Sum != nil
	}
	return false
}

func statValue(dp lstypes.MetricDatapoint, stat lstypes.MetricStatistic) float64 {
	switch stat {
	case lstypes.MetricStatisticAverage:
		if dp.Average != nil {
			return *dp.Average
		}
	case lstypes.MetricStatisticMaximum:
		if dp.Maximum != nil {
			return *dp.Maximum
		}
	case lstypes.MetricStatisticMinimum:
		if dp.Minimum != nil {
			return *dp.Minimum
		}
	case lstypes.MetricStatisticSum:
		if dp.Sum != nil {
			return *dp.Sum
		}
	}
	return 0
}

// ---- 快照(供 /infra 与 infra 告警) ----

// InfraResource 是一个资源在最近一次采样的健康视图。
type InfraResource struct {
	Name    string             `json:"name"`
	Type    string             `json:"type"`
	Status  string             `json:"status"` // ok / warn / bad / nosample
	AgeSec  int64              `json:"age_sec"`
	Metrics map[string]float64 `json:"metrics"`
}

// ProbeResource 是一个前端域名的端到端探活视图。
type ProbeResource struct {
	Domain    string  `json:"domain"`
	Status    string  `json:"status"`     // ok / warn / bad / nosample
	Reachable bool    `json:"reachable"`  // 最近一次是否可达
	HTTPCode  int     `json:"http_code"`  // HTTP 状态码(0=不可达/超时)
	LatencyMs float64 `json:"latency_ms"` // 响应延时(毫秒)
	CertDays  float64 `json:"cert_days"`  // TLS 证书剩余天数
	HasCert   bool    `json:"has_cert"`   // 是否取到证书天数
	ViaCDN    bool    `json:"via_cdn"`    // 响应是否经 CloudFront(脱离 CDN 即漂移)
	AgeSec    int64   `json:"age_sec"`    // 该探活数据新鲜度
}

// LockResource 是一个源站端点的锁完整性视图(直连不带头,期望 403)。
type LockResource struct {
	Target   string `json:"target"`    // 源站端点 host:port
	Status   string `json:"status"`    // ok(=403,锁生效) / bad(非403,锁失效) / nosample(连不上)
	Locked   bool   `json:"locked"`    // 是否 403
	HTTPCode int    `json:"http_code"` // 实际返回码(期望 403)
	AgeSec   int64  `json:"age_sec"`   // 数据新鲜度
}

// InfraOverview 是 NOC 顶部总览:整体状态 + 各类健康计数。
type InfraOverview struct {
	Status         string `json:"status"` // ok / warn / bad / nosample(最差状态冒泡)
	InstancesTotal int    `json:"instances_total"`
	InstancesOK    int    `json:"instances_ok"`
	DBStatus       string `json:"db_status"` // ok/warn/bad/nosample/absent
	LBStatus       string `json:"lb_status"`
	ProbesTotal    int    `json:"probes_total"`
	ProbesOK       int    `json:"probes_ok"` // 端到端全通计数(status==ok)
	LocksTotal     int    `json:"locks_total"`
	LocksOK        int    `json:"locks_ok"` // 源站锁生效计数(403)
}

// InfraAlert 是一条最近告警(供告警面板)。
type InfraAlert struct {
	Ts     int64  `json:"ts"`
	When   string `json:"when"`   // 已格式化时间
	Kind   string `json:"kind"`   // 告警类型(infra_db_mem 等)
	Target string `json:"target"` // 资源/域名
	Detail string `json:"detail"` // 标题/详情
	Status string `json:"status"` // bad(普通)/ warn(_FAILED 等)
}

// InfraSnapshot 是服务端监控一次快照:总览 + 端到端探活 + 实例 + 数据库 + 负载均衡 + 趋势 + 最近告警。
type InfraSnapshot struct {
	GeneratedAt string          `json:"generated_at"`
	DataAgeSec  int64           `json:"data_age_sec"`
	Overview    InfraOverview   `json:"overview"`
	Probes      []ProbeResource `json:"probes"`
	Locks       []LockResource  `json:"locks"`
	Instances   []InfraResource `json:"instances"`
	Database    *InfraResource  `json:"database"`
	LB          *InfraResource  `json:"lb"`
	Alerts      []InfraAlert    `json:"alerts"`
}

// computeInfraSnapshot 从本地 infra_samples 聚合最新视图(零 AWS 调用,纯读本地)。
func (m *Monitor) computeInfraSnapshot(nowUnix int64) InfraSnapshot {
	latest := m.storeInfraLatest()
	type acc struct {
		rtype   string
		metrics map[string]float64
		maxTs   int64
	}
	byRes := map[string]*acc{}
	for _, r := range latest {
		a := byRes[r.Resource]
		if a == nil {
			a = &acc{rtype: r.RType, metrics: map[string]float64{}}
			byRes[r.Resource] = a
		}
		// host 行并入同名实例(agent 的 node 名 = 实例名);若先有 host 后有 instance,保留 instance 类型。
		if r.RType != "host" {
			a.rtype = r.RType
		}
		a.metrics[r.Metric] = r.Value
		if r.BucketTs > a.maxTs {
			a.maxTs = r.BucketTs
		}
	}

	var snap InfraSnapshot
	snap.GeneratedAt = time.Unix(nowUnix, 0).Format("2006-01-02 15:04:05")
	var newest int64
	for name, a := range byRes {
		age := int64(-1)
		if a.maxTs > 0 {
			age = nowUnix - (a.maxTs + 60)
			if age < 0 {
				age = 0
			}
			if a.maxTs > newest {
				newest = a.maxTs
			}
		}
		// 端到端探活(rtype=probe)单独成 ProbeResource,不混入实例/DB/LB。
		if a.rtype == "probe" {
			snap.Probes = append(snap.Probes, m.buildProbe(name, a.metrics, age))
			continue
		}
		// 源站锁检查(rtype=lock)单独成 LockResource。
		if a.rtype == "lock" {
			snap.Locks = append(snap.Locks, buildLock(name, a.metrics, age))
			continue
		}
		res := InfraResource{Name: name, Type: rtypeOrInstance(a.rtype), AgeSec: age, Metrics: a.metrics}
		addDerivedPct(&res) // 派生百分比键(前端直接用),需在算 status 前完成
		res.Status = m.infraStatus(res)
		switch res.Type {
		case "database":
			d := res
			snap.Database = &d
		case "lb":
			l := res
			snap.LB = &l
		default:
			snap.Instances = append(snap.Instances, res)
		}
	}
	sortProbes(snap.Probes)
	sortLocks(snap.Locks)
	if newest > 0 {
		snap.DataAgeSec = nowUnix - (newest + 60)
		if snap.DataAgeSec < 0 {
			snap.DataAgeSec = 0
		}
	} else {
		snap.DataAgeSec = -1
	}
	// 实例按名稳定排序,避免每次刷新行序跳动。
	sortInstances(snap.Instances)
	// 各资源的指标趋势改为前端按需拉(GET /infra/series),不在快照里预算。
	snap.Overview = buildOverview(snap)
	snap.Alerts = m.recentInfraAlerts(nowUnix, 20)
	return snap
}

// buildProbe 把某域名的探活指标组装成 ProbeResource 并按阈值定级。
func (m *Monitor) buildProbe(domain string, mm map[string]float64, age int64) ProbeResource {
	p := ProbeResource{Domain: domain, AgeSec: age}
	if len(mm) == 0 {
		p.Status = "nosample"
		return p
	}
	p.Reachable = mm["reachable"] >= 1
	p.HTTPCode = int(mm["status_code"])
	p.LatencyMs = mm["latency_ms"]
	p.ViaCDN = mm["via_cdn"] >= 1
	if v, ok := mm["cert_days"]; ok {
		p.CertDays = v
		p.HasCert = true
	}
	p.Status = m.probeStatus(p)
	return p
}

// buildLock 把某源站端点的锁检查指标组装成 LockResource(403=ok,非403=bad)。
func buildLock(target string, mm map[string]float64, age int64) LockResource {
	l := LockResource{Target: target, AgeSec: age}
	if len(mm) == 0 {
		l.Status = "nosample"
		return l
	}
	l.HTTPCode = int(mm["status_code"])
	l.Locked = mm["locked"] >= 1
	if l.Locked {
		l.Status = "ok"
	} else {
		l.Status = "bad" // 拿到响应但不是 403 = 锁失效,最高优先
	}
	return l
}

// probeStatus 端到端探活定级:非 200/不可达=bad;延时超 bad 阈值=bad、超 warn=warn;
// 证书天数低于 bad=bad、低于 warn=warn(最差状态冒泡)。
func (m *Monitor) probeStatus(p ProbeResource) string {
	c := m.cfg
	if !p.Reachable || p.HTTPCode != 200 {
		return "bad"
	}
	st := "ok"
	worse := func(s string) {
		if rank(s) > rank(st) {
			st = s
		}
	}
	if p.LatencyMs >= c.ProbeLatencyBadMs {
		worse("bad")
	} else if p.LatencyMs >= c.ProbeLatencyWarnMs {
		worse("warn")
	}
	if p.HasCert {
		if p.CertDays <= c.ProbeCertBadDays {
			worse("bad")
		} else if p.CertDays <= c.ProbeCertWarnDays {
			worse("warn")
		}
	}
	// 期望经 CDN 却没走 CloudFront = 边缘漂移(DNS 指错/脱离 CDN),记黄。
	if c.ProbeExpectCDN && !p.ViaCDN {
		worse("warn")
	}
	return st
}

// rank 把状态映成可比较的严重度(越大越糟),供"最差状态冒泡"。
func rank(s string) int {
	switch s {
	case "bad":
		return 3
	case "warn":
		return 2
	case "ok":
		return 1
	default: // nosample / absent / ""
		return 0
	}
}

// worst 取多个状态里最差的(nosample 不视作"故障",仅在全部缺数据时冒泡)。
func worst(states ...string) string {
	best := ""
	for _, s := range states {
		if rank(s) > rank(best) {
			best = s
		}
	}
	if best == "" {
		return "nosample"
	}
	return best
}

// buildOverview 由实例/DB/LB/探活的状态聚合顶部总览(最差状态冒泡 + 健康计数)。
func buildOverview(snap InfraSnapshot) InfraOverview {
	o := InfraOverview{DBStatus: "absent", LBStatus: "absent"}
	var states []string
	for _, in := range snap.Instances {
		o.InstancesTotal++
		if in.Status == "ok" {
			o.InstancesOK++
		}
		states = append(states, in.Status)
	}
	if snap.Database != nil {
		o.DBStatus = snap.Database.Status
		states = append(states, snap.Database.Status)
	}
	if snap.LB != nil {
		o.LBStatus = snap.LB.Status
		states = append(states, snap.LB.Status)
	}
	for _, p := range snap.Probes {
		o.ProbesTotal++
		if p.Status == "ok" {
			o.ProbesOK++
		}
		states = append(states, p.Status)
	}
	for _, l := range snap.Locks {
		o.LocksTotal++
		if l.Status == "ok" {
			o.LocksOK++
		}
		states = append(states, l.Status)
	}
	o.Status = worst(states...)
	return o
}

func sortProbes(rs []ProbeResource) {
	for i := 1; i < len(rs); i++ {
		for j := i; j > 0 && rs[j-1].Domain > rs[j].Domain; j-- {
			rs[j-1], rs[j] = rs[j], rs[j-1]
		}
	}
}

func sortLocks(rs []LockResource) {
	for i := 1; i < len(rs); i++ {
		for j := i; j > 0 && rs[j-1].Target > rs[j].Target; j-- {
			rs[j-1], rs[j] = rs[j], rs[j-1]
		}
	}
}

func rtypeOrInstance(t string) string {
	if t == "" || t == "host" {
		return "instance"
	}
	return t
}

// instRank 按重要程度给实例排序权重(越小越靠前):Master > Slave > Redis > 其余。
func instRank(name string) int {
	n := strings.ToLower(name)
	switch {
	case strings.Contains(n, "master"):
		return 0
	case strings.Contains(n, "slave"):
		return 1
	case strings.Contains(n, "redis"):
		return 2
	default:
		return 3
	}
}

// sortInstances 按重要程度排;同档按名字稳定排序,避免每次刷新行序跳动。
func sortInstances(rs []InfraResource) {
	less := func(a, b InfraResource) bool {
		ra, rb := instRank(a.Name), instRank(b.Name)
		if ra != rb {
			return ra < rb
		}
		return a.Name < b.Name
	}
	for i := 1; i < len(rs); i++ {
		for j := i; j > 0 && less(rs[j], rs[j-1]); j-- {
			rs[j-1], rs[j] = rs[j], rs[j-1]
		}
	}
}

// addDerivedPct 往资源 Metrics 里写派生百分比键(供 status 与前端直接用)。
// 一律除零保护:total 缺失或为 0 时不产出该 pct 键。
func addDerivedPct(r *InfraResource) {
	m := r.Metrics
	if m == nil {
		return
	}
	memTotal, hasMemTotal := m["mem_total_mb"]
	switch r.Type {
	case "database":
		// 可用内存% = 可用内存 / 总内存
		if free, ok := m["free_mem_mb"]; ok && hasMemTotal && memTotal > 0 {
			m["mem_avail_pct"] = free / memTotal * 100
		}
		// 可用存储% = 可用存储 / 总磁盘
		if free, ok := m["free_storage_gb"]; ok {
			if total, ok2 := m["disk_total_gb"]; ok2 && total > 0 {
				m["storage_avail_pct"] = free / total * 100
			}
		}
	case "lb":
		// LB 无内存/磁盘总量,无派生
	default: // instance
		// 已用内存% = (总内存 - 可用内存) / 总内存;仅当 host-agent 的 mem_avail_mb 存在
		if avail, ok := m["mem_avail_mb"]; ok && hasMemTotal && memTotal > 0 {
			m["mem_used_pct"] = (memTotal - avail) / memTotal * 100
		}
		// disk_used_pct 直接来自 host-agent(已有),不在此派生
	}
}

// ---- 红线判定谓词:色标(infraStatus)与邮件告警(evaluateInfraAlerts)共用 ----
// 「页面变红」和「发邮件」必须永远同口径,所以"什么算命中红线"只在这里写一遍,两边都调用。
// 返回 (当前值, 是否命中);指标缺失视为未命中(监控自身取不到数不算故障,由 nosample 兜底)。

// dbMemBad 数据库可用内存低于红线。
func (m *Monitor) dbMemBad(mm map[string]float64) (float64, bool) {
	v, ok := mm["mem_avail_pct"]
	return v, ok && v < m.cfg.InfraMemAvailBadPct
}

// dbStorageBad 数据库可用存储低于红线。
func (m *Monitor) dbStorageBad(mm map[string]float64) (float64, bool) {
	v, ok := mm["storage_avail_pct"]
	return v, ok && v < m.cfg.InfraStorageAvailBadPct
}

// instanceDown 实例系统健康检查失败(StatusCheckFailed>=1)。
func instanceDown(mm map[string]float64) (float64, bool) {
	v, ok := mm["status_failed"]
	return v, ok && v >= 1
}

// lbUnhealthy 负载均衡存在不健康节点。
func lbUnhealthy(mm map[string]float64) (float64, bool) {
	v, ok := mm["unhealthy"]
	return v, ok && v >= 1
}

// infraStatus 由【百分比阈值】给资源一个红/黄/绿色标(无指标=nosample)。
// 阈值取自 Settings(可经环境变量配置);可用内存/存储「低于」即告急,CPU「高于」即告急,突发额度「低于」即黄。
func (m *Monitor) infraStatus(r InfraResource) string {
	mm := r.Metrics
	if len(mm) == 0 {
		return "nosample"
	}
	c := m.cfg
	has := func(k string) (float64, bool) { v, ok := mm[k]; return v, ok }
	switch r.Type {
	case "database":
		if _, hit := m.dbMemBad(mm); hit {
			return "bad"
		}
		if _, hit := m.dbStorageBad(mm); hit {
			return "bad"
		}
		if v, ok := has("cpu"); ok && v > c.InfraCPUBadPct {
			return "bad"
		}
		if v, ok := has("mem_avail_pct"); ok && v < c.InfraMemAvailWarnPct {
			return "warn"
		}
		if v, ok := has("storage_avail_pct"); ok && v < c.InfraStorageAvailWarnPct {
			return "warn"
		}
		if v, ok := has("cpu"); ok && v > c.InfraCPUWarnPct {
			return "warn"
		}
		if v, ok := has("connections"); ok && v > c.InfraDBConnWarn {
			return "warn"
		}
		if v, ok := has("disk_queue"); ok && v > c.InfraDBDiskQueueWarn {
			return "warn"
		}
		return "ok"
	case "lb":
		if _, hit := lbUnhealthy(mm); hit {
			return "bad"
		}
		if v, ok := has("healthy"); ok && v < 1 {
			return "bad"
		}
		if v, ok := has("resp_ms"); ok && v > c.InfraLBRespWarnMs {
			return "warn"
		}
		return "ok"
	default: // instance
		if _, hit := instanceDown(mm); hit {
			return "bad"
		}
		if v, ok := has("mem_used_pct"); ok && v > 100-c.InfraMemAvailBadPct {
			return "bad"
		}
		if v, ok := has("cpu"); ok && v > c.InfraCPUBadPct {
			return "bad"
		}
		if v, ok := has("cpu"); ok && v > c.InfraCPUWarnPct {
			return "warn"
		}
		if v, ok := has("mem_used_pct"); ok && v > 100-c.InfraMemAvailWarnPct {
			return "warn"
		}
		if v, ok := has("disk_used_pct"); ok && v > 100-c.InfraStorageAvailWarnPct {
			return "warn"
		}
		if v, ok := has("burst"); ok && v < c.InfraBurstWarnPct {
			return "warn"
		}
		return "ok"
	}
}

// evaluateInfraAlerts 评估基础设施告警(复用现有邮件 + 冷却);复用 alert_config 的开关/SMTP/收件人。
func (m *Monitor) evaluateInfraAlerts(now int64) {
	if !m.cfg.InfraEnabled {
		return
	}
	c := m.loadAlertConfig()
	if !c.Enabled || c.SMTPHost == "" || c.Recipients == "" {
		return
	}
	snap := m.computeInfraSnapshot(now)
	if d := snap.Database; d != nil {
		if v, hit := m.dbMemBad(d.Metrics); hit {
			free := d.Metrics["free_mem_mb"]
			m.fire(c, "infra_db_mem", d.Name, "数据库可用内存告急",
				fmt.Sprintf("数据库 %s 可用内存仅 %.1f%%(%.0f MB,阈值 %.0f%%),内存接近耗尽,有 OOM 重启风险。", d.Name, v, free, m.cfg.InfraMemAvailBadPct), now)
		}
		if v, hit := m.dbStorageBad(d.Metrics); hit {
			free := d.Metrics["free_storage_gb"]
			m.fire(c, "infra_db_storage", d.Name, "数据库存储不足",
				fmt.Sprintf("数据库 %s 可用存储仅 %.1f%%(%.1f GB,阈值 %.0f%%)。", d.Name, v, free, m.cfg.InfraStorageAvailBadPct), now)
		}
	}
	for _, in := range snap.Instances {
		if v, hit := instanceDown(in.Metrics); hit {
			m.fire(c, "infra_instance_down", in.Name, "实例健康检查失败",
				fmt.Sprintf("实例 %s StatusCheckFailed=%.0f,可能宕机或不可达。", in.Name, v), now)
		}
	}
	if lb := snap.LB; lb != nil {
		if v, hit := lbUnhealthy(lb.Metrics); hit {
			m.fire(c, "infra_lb_unhealthy", lb.Name, "负载均衡有不健康节点",
				fmt.Sprintf("负载均衡 %s 不健康节点数 %.0f。", lb.Name, v), now)
		}
	}
	for _, p := range snap.Probes {
		if !p.Reachable || p.HTTPCode != 200 {
			m.fire(c, "infra_probe_down", p.Domain, "站点端到端探活失败",
				fmt.Sprintf("域名 %s 探活异常:%s。可能站点不可达或返回非 200,客户可能受影响。", p.Domain, probeDownDetail(p)), now)
		}
		if p.HasCert && p.CertDays <= m.cfg.ProbeCertBadDays {
			m.fire(c, "infra_probe_cert", p.Domain, "TLS 证书即将过期",
				fmt.Sprintf("域名 %s 的 TLS 证书仅剩 %.1f 天(阈值 %.0f 天),过期将导致全站 HTTPS 失败。", p.Domain, p.CertDays, m.cfg.ProbeCertBadDays), now)
		}
	}
	// 源站锁失效:直连源站不带头本应 403,却返回非 403 = F-5 锁被回滚/失效,安全红线。
	for _, l := range snap.Locks {
		if l.Status == "bad" {
			m.fire(c, "infra_origin_lock", l.Target, "源站锁失效(可绕过 CDN 直连)",
				fmt.Sprintf("源站 %s 直连(不带 X-Origin-Verify)返回 HTTP %d(本应 403)。F-5 源站锁可能被回滚或失效,现在可绕过 CloudFront 直连后端,请立即排查 nginx 配置。", l.Target, l.HTTPCode), now)
		}
	}
}

// probeDownDetail 给探活失败一句人话描述。
func probeDownDetail(p ProbeResource) string {
	if !p.Reachable {
		return "不可达(超时或连接失败)"
	}
	return fmt.Sprintf("HTTP %d", p.HTTPCode)
}
