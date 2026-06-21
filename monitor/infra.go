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
type infraTarget struct {
	name  string
	rtype string
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
			if in.Name != nil {
				out = append(out, infraTarget{*in.Name, "instance"})
			}
		}
	} else {
		slog.Warn("infra: 列实例失败", "err", err)
	}
	if r, err := cl.GetRelationalDatabases(ctx, &lightsail.GetRelationalDatabasesInput{}); err == nil {
		for _, d := range r.RelationalDatabases {
			if d.Name != nil {
				out = append(out, infraTarget{*d.Name, "database"})
			}
		}
	} else {
		slog.Warn("infra: 列数据库失败", "err", err)
	}
	if r, err := cl.GetLoadBalancers(ctx, &lightsail.GetLoadBalancersInput{}); err == nil {
		for _, lb := range r.LoadBalancers {
			if lb.Name != nil {
				out = append(out, infraTarget{*lb.Name, "lb"})
			}
		}
	} else {
		slog.Warn("infra: 列负载均衡失败", "err", err)
	}
	return out
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

// InfraSnapshot 是服务端监控一次快照:实例数组 + 数据库 + 负载均衡 + DB 内存/swap 趋势。
type InfraSnapshot struct {
	GeneratedAt string          `json:"generated_at"`
	DataAgeSec  int64           `json:"data_age_sec"`
	Instances   []InfraResource `json:"instances"`
	Database    *InfraResource  `json:"database"`
	LB          *InfraResource  `json:"lb"`
	DBMemTrend  []InfraPoint    `json:"db_mem_trend"`
	DBSwapTrend []InfraPoint    `json:"db_swap_trend"`
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
		res := InfraResource{Name: name, Type: rtypeOrInstance(a.rtype), AgeSec: age, Metrics: a.metrics}
		res.Status = infraStatus(res)
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
	// DB 内存/swap 趋势(近 6 小时)。
	if snap.Database != nil {
		since := nowUnix - 6*3600
		snap.DBMemTrend = m.storeInfraSeries(snap.Database.Name, "free_mem_mb", since)
		snap.DBSwapTrend = m.storeInfraSeries(snap.Database.Name, "swap_mb", since)
	}
	return snap
}

func rtypeOrInstance(t string) string {
	if t == "" || t == "host" {
		return "instance"
	}
	return t
}

func sortInstances(rs []InfraResource) {
	for i := 1; i < len(rs); i++ {
		for j := i; j > 0 && rs[j-1].Name > rs[j].Name; j-- {
			rs[j-1], rs[j] = rs[j], rs[j-1]
		}
	}
}

// infraStatus 由阈值给资源一个红/黄/绿色标(无指标=nosample)。
func infraStatus(r InfraResource) string {
	m := r.Metrics
	if len(m) == 0 {
		return "nosample"
	}
	has := func(k string) (float64, bool) { v, ok := m[k]; return v, ok }
	switch r.Type {
	case "database":
		if v, ok := has("free_mem_mb"); ok && v < 80 {
			return "bad"
		}
		if v, ok := has("free_storage_gb"); ok && v < 8 {
			return "bad"
		}
		if v, ok := has("free_mem_mb"); ok && v < 150 {
			return "warn"
		}
		if v, ok := has("cpu"); ok && v > 80 {
			return "warn"
		}
		if v, ok := has("connections"); ok && v > 70 {
			return "warn"
		}
		if v, ok := has("disk_queue"); ok && v > 5 {
			return "warn"
		}
		return "ok"
	case "lb":
		if v, ok := has("unhealthy"); ok && v >= 1 {
			return "bad"
		}
		if v, ok := has("healthy"); ok && v < 1 {
			return "bad"
		}
		if v, ok := has("resp_ms"); ok && v > 2000 {
			return "warn"
		}
		return "ok"
	default: // instance
		if v, ok := has("status_failed"); ok && v >= 1 {
			return "bad"
		}
		if v, ok := has("mem_avail_mb"); ok && v < 100 {
			return "bad"
		}
		if v, ok := has("cpu"); ok && v > 80 {
			return "warn"
		}
		if v, ok := has("mem_avail_mb"); ok && v < 200 {
			return "warn"
		}
		if v, ok := has("disk_used_pct"); ok && v > 80 {
			return "warn"
		}
		if v, ok := has("burst"); ok && v < 20 {
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
		if v, ok := d.Metrics["free_mem_mb"]; ok && v < 80 {
			m.fire(c, "infra_db_mem", d.Name, "数据库可用内存告急",
				fmt.Sprintf("数据库 %s 可用内存仅 %.0f MB(阈值 80MB),内存接近耗尽,有 OOM 重启风险。", d.Name, v), now)
		}
		if v, ok := d.Metrics["free_storage_gb"]; ok && v < 8 {
			m.fire(c, "infra_db_storage", d.Name, "数据库存储不足",
				fmt.Sprintf("数据库 %s 可用存储仅 %.1f GB(阈值 8GB)。", d.Name, v), now)
		}
	}
	for _, in := range snap.Instances {
		if v, ok := in.Metrics["status_failed"]; ok && v >= 1 {
			m.fire(c, "infra_instance_down", in.Name, "实例健康检查失败",
				fmt.Sprintf("实例 %s StatusCheckFailed=%.0f,可能宕机或不可达。", in.Name, v), now)
		}
	}
	if lb := snap.LB; lb != nil {
		if v, ok := lb.Metrics["unhealthy"]; ok && v >= 1 {
			m.fire(c, "infra_lb_unhealthy", lb.Name, "负载均衡有不健康节点",
				fmt.Sprintf("负载均衡 %s 不健康节点数 %.0f。", lb.Name, v), now)
		}
	}
}
