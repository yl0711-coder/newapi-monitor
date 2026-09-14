package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lightsail"
	lstypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"gorm.io/gorm"
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
	// Lightsail 与托管 AWS 资源共享一轮只读采样预算。托管资源 API 即使
	// 暂未授权也 fail-open，不得阻断已有 Lightsail 数据。
	cctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	bucket := time.Now().Unix() / 60 * 60
	cl, err := m.lightsailClient(cctx)
	if err != nil {
		slog.Warn("infra: Lightsail 客户端初始化失败", "err", err)
		rows := append(append(managedDiscoveryRows(bucket, "Lightsail/实例", false, 0), managedDiscoveryRows(bucket, "Lightsail/数据库", false, 0)...), managedDiscoveryRows(bucket, "Lightsail/负载均衡", false, 0)...)
		if storeErr := m.upsertInfra(rows); storeErr != nil {
			slog.Warn("infra: Lightsail 发现失败状态入库失败", "err", storeErr)
		}
	} else {
		targets, discoveryRows := m.infraTargetsWithDiscovery(cctx, cl, bucket)
		if len(targets) == 0 {
			slog.Warn("infra: Lightsail 自动发现为空且未配 MONITOR_INFRA_RESOURCES")
		}
		rows := append([]InfraSample{}, discoveryRows...)
		for _, t := range targets {
			for _, sp := range specsFor(t.rtype) {
				if v, observedAt, ok := m.fetchMetric(cctx, cl, t, sp); ok {
					rows = append(rows, InfraSample{BucketTs: observedAt, Resource: t.name, RType: t.rtype, Metric: sp.key, Value: v})
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
			slog.Warn("infra: Lightsail 采样入库失败(忽略)", "err", err)
		} else if len(targets) > 0 {
			slog.Info("infra Lightsail 采样完成", "targets", len(targets), "rows", len(rows))
		}
	}
	// ECS/Fargate、RDS、ALB 可统一交给 CloudWatch。关闭托管资源采样时
	// 仍保留 Lightsail、域名探活与源站锁检查，不改变 ECS 日志链路。
	if !m.cfg.InfraManagedAWSDisabled {
		m.sampleManagedAWSInfra(cctx, time.Now().Unix()/60*60)
	}
}

func (m *Monitor) infraTargetsWithDiscovery(ctx context.Context, cl *lightsail.Client, bucket int64) ([]infraTarget, []InfraSample) {
	explicit := parseInfraTargets(m.cfg.InfraResources)
	var discovered []infraTarget
	var discoveryRows []InfraSample
	complete := map[string]bool{}
	assetInventories := map[string][]infraAssetObservation{}
	if instances, err := collectLightsailPages(ctx, func(token *string) ([]lstypes.Instance, *string, error) {
		r, err := cl.GetInstances(ctx, &lightsail.GetInstancesInput{PageToken: token})
		if err != nil {
			return nil, nil, err
		}
		return r.Instances, r.NextPageToken, nil
	}); err == nil {
		complete["instance"] = true
		for _, in := range instances {
			if in.Name == nil {
				continue
			}
			t := infraTarget{name: *in.Name, rtype: "instance"}
			if in.Arn != nil {
				state := "unknown"
				if in.State != nil {
					state = strings.ToLower(aws.ToString(in.State.Name))
				}
				assetInventories["instance"] = append(assetInventories["instance"], infraAssetObservation{Identity: aws.ToString(in.Arn), Resource: *in.Name, Kind: "instance", Platform: "Lightsail", CloudState: state})
			}
			if h := in.Hardware; h != nil {
				if h.RamSizeInGb != nil {
					t.memTotalMB = float64(*h.RamSizeInGb) * 1024
				}
				t.diskTotalGB = systemDiskGB(h.Disks)
			}
			discovered = append(discovered, t)
		}
		discoveryRows = append(discoveryRows, managedDiscoveryRows(bucket, "Lightsail/实例", true, len(instances))...)
	} else {
		slog.Warn("infra: 列实例失败", "err", err)
		discoveryRows = append(discoveryRows, managedDiscoveryRows(bucket, "Lightsail/实例", false, 0)...)
	}
	if databases, err := collectLightsailPages(ctx, func(token *string) ([]lstypes.RelationalDatabase, *string, error) {
		r, err := cl.GetRelationalDatabases(ctx, &lightsail.GetRelationalDatabasesInput{PageToken: token})
		if err != nil {
			return nil, nil, err
		}
		return r.RelationalDatabases, r.NextPageToken, nil
	}); err == nil {
		complete["database"] = true
		for _, d := range databases {
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
			discovered = append(discovered, t)
		}
		discoveryRows = append(discoveryRows, managedDiscoveryRows(bucket, "Lightsail/数据库", true, len(databases))...)
	} else {
		slog.Warn("infra: 列数据库失败", "err", err)
		discoveryRows = append(discoveryRows, managedDiscoveryRows(bucket, "Lightsail/数据库", false, 0)...)
	}
	if balancers, err := collectLightsailPages(ctx, func(token *string) ([]lstypes.LoadBalancer, *string, error) {
		r, err := cl.GetLoadBalancers(ctx, &lightsail.GetLoadBalancersInput{PageToken: token})
		if err != nil {
			return nil, nil, err
		}
		return r.LoadBalancers, r.NextPageToken, nil
	}); err == nil {
		complete["lb"] = true
		for _, lb := range balancers {
			if lb.Name != nil {
				discovered = append(discovered, infraTarget{name: *lb.Name, rtype: "lb"}) // LB 无内存/磁盘概念,跳过总量
			}
		}
		discoveryRows = append(discoveryRows, managedDiscoveryRows(bucket, "Lightsail/负载均衡", true, len(balancers))...)
	} else {
		slog.Warn("infra: 列负载均衡失败", "err", err)
		discoveryRows = append(discoveryRows, managedDiscoveryRows(bucket, "Lightsail/负载均衡", false, 0)...)
	}
	previous := m.knownLightsailResources(ctx)
	// The persistent registry retains missing hosts for manual archiving;
	// unlike inventory_present, these rows do not disappear on a later poll.
	if complete["instance"] {
		if err := m.observeInfraAssets(ctx, "lightsail:"+m.cfg.AWSRegion+":instance", assetInventories["instance"], time.Now().Unix()); err != nil {
			slog.Warn("infra: Lightsail 资源目录更新失败", "err", err)
		} else if err := m.retainMissingLightsailAssets(ctx, previous, discovered, time.Now().Unix()); err != nil {
			slog.Warn("infra: 历史下线实例保留失败", "err", err)
		}
	}
	lifecycleRows := lightsailPresenceRows(bucket, discovered, previous, complete)
	discoveryRows = append(discoveryRows, lifecycleRows...)
	retired := map[string]bool{}
	for _, row := range lifecycleRows {
		retired[row.Resource] = row.Value == 0
	}
	targets := m.filterInfraTargets(mergeInfraTargets(explicit, discovered))
	active := targets[:0]
	for _, target := range targets {
		if !retired[target.name] {
			active = append(active, target)
		}
	}
	return active, discoveryRows
}

func parseInfraTargets(value string) []infraTarget {
	var out []infraTarget
	for _, part := range strings.Split(value, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), ":", 2)
		if len(kv) != 2 {
			continue
		}
		rtype, name := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
		if name == "" || (rtype != "instance" && rtype != "database" && rtype != "lb") {
			continue
		}
		out = append(out, infraTarget{name: name, rtype: rtype})
	}
	return out
}

func mergeInfraTargets(explicit, discovered []infraTarget) []infraTarget {
	out := append([]infraTarget(nil), explicit...)
	positions := make(map[string]int, len(out))
	for i, target := range out {
		positions[target.rtype+":"+target.name] = i
	}
	for _, target := range discovered {
		key := target.rtype + ":" + target.name
		if pos, exists := positions[key]; exists {
			// 自动发现包含硬件规格，覆盖显式配置里的零值。
			out[pos] = target
			continue
		}
		positions[key] = len(out)
		out = append(out, target)
	}
	return out
}

func (m *Monitor) infraExcluded(name string) bool {
	for _, excluded := range m.cfg.InfraExcludeResources {
		if strings.TrimSpace(excluded) == name {
			return true
		}
	}
	return false
}

func (m *Monitor) filterInfraTargets(in []infraTarget) []infraTarget {
	out := make([]infraTarget, 0, len(in))
	for _, target := range in {
		if !m.infraExcluded(target.name) {
			out = append(out, target)
		}
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
func (m *Monitor) fetchMetric(ctx context.Context, cl *lightsail.Client, t infraTarget, sp metricSpec) (float64, int64, bool) {
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
		return 0, 0, false
	}
	if err != nil {
		slog.Warn("infra: 拉取指标失败", "resource", t.name, "metric", sp.metric, "err", err)
		return 0, 0, false
	}
	dp, ok := latestDatapoint(dps, sp.stat)
	if !ok {
		return 0, 0, false
	}
	scale := sp.scale
	if scale == 0 {
		scale = 1
	}
	return statValue(dp, sp.stat) / scale, dp.Timestamp.Unix() / 60 * 60, true
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
	Name             string             `json:"name"`
	DisplayName      string             `json:"display_name,omitempty"`
	Type             string             `json:"type"`
	Platform         string             `json:"platform,omitempty"`
	Group            string             `json:"group,omitempty"`
	Status           string             `json:"status"` // ok / warn / bad / nosample
	AgeSec           int64              `json:"age_sec"`
	Metrics          map[string]float64 `json:"metrics"`
	StaleMetrics     []string           `json:"stale_metrics,omitempty"`
	MissingMetrics   []string           `json:"missing_metrics,omitempty"`
	CoverageComplete bool               `json:"coverage_complete"`
	Containers       []InfraContainer   `json:"containers,omitempty"`
}

// InfraResourceGroup 是服务端页的业务边界。采集仍按 AWS 资源独立进行，
// 展示时再归并，避免 NexusAPI、Sub2API 与 Monitor/Eval 的告警和容量混在一起。
type InfraResourceGroup struct {
	Name          string          `json:"name"`
	Status        string          `json:"status"`
	Instances     []InfraResource `json:"instances"`
	Databases     []InfraResource `json:"databases"`
	LoadBalancers []InfraResource `json:"load_balancers"`
}

type InfraContainer struct {
	Name         string `json:"name"`
	State        string `json:"state"`
	Health       string `json:"health,omitempty"`
	RestartCount int    `json:"restart_count"`
	AgeSec       int64  `json:"age_sec"`
	Status       string `json:"status"` // ok / warn / bad
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
	Status   string `json:"status"`    // ok(403) / bad(非预期响应，需核验) / nosample(无有效响应)
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
	LocksOK        int    `json:"locks_ok"`      // 源站锁生效计数(403)
	LocksBad       int    `json:"locks_bad"`     // 已收到非预期 HTTP 响应，需核验
	LocksUnknown   int    `json:"locks_unknown"` // 无有效响应，不等于锁失效
	DiscoveryTotal int    `json:"discovery_total"`
	DiscoveryOK    int    `json:"discovery_ok"`
}

// InfraDiscovery makes AWS inventory permissions observable. Without these
// sentinels a discovery API failure could hide newly added resources while
// every already-known row remained green.
type InfraDiscovery struct {
	Name          string `json:"name"`
	Status        string `json:"status"`
	ResourceCount int    `json:"resource_count"`
	AgeSec        int64  `json:"age_sec"`
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
	RegistryError string               `json:"registry_error,omitempty"`
	GeneratedAt   string               `json:"generated_at"`
	DataAgeSec    int64                `json:"data_age_sec"`
	Overview      InfraOverview        `json:"overview"`
	Probes        []ProbeResource      `json:"probes"`
	Locks         []LockResource       `json:"locks"`
	Discoveries   []InfraDiscovery     `json:"discoveries,omitempty"`
	Instances     []InfraResource      `json:"instances"`
	Databases     []InfraResource      `json:"databases,omitempty"`
	LoadBalancers []InfraResource      `json:"load_balancers,omitempty"`
	Groups        []InfraResourceGroup `json:"groups,omitempty"`
	// Database/LB 保留给容量规划和旧前端；新代码应读取复数集合。
	Database   *InfraResource      `json:"database"`
	LB         *InfraResource      `json:"lb"`
	Alerts     []InfraAlert        `json:"alerts"`
	ManagedAWS infraManagedAWSView `json:"managed_aws"`
}

// computeInfraSnapshot 从本地 infra_samples 聚合最新视图(零 AWS 调用,纯读本地)。
func (m *Monitor) computeInfraSnapshot(nowUnix int64) InfraSnapshot {
	latest := m.storeInfraLatest()
	retired := retiredInfraResources(latest)
	assets, incarnations, registryErr := m.infraAssetProjection()
	type acc struct {
		rtype    string
		metrics  map[string]float64
		metricTs map[string]int64
		minTs    int64
	}
	byRes := map[string]*acc{}
	for _, r := range latest {
		if !m.monitorOwnsInfraResource(r.Resource, r.RType, "") {
			continue
		}
		asset, managed := assets[r.Resource]
		if m.infraExcluded(r.Resource) || !visibleInfraAsset(asset, managed) || retired[r.Resource] && !managed || r.Metric == infraPresenceMetric {
			continue
		}
		if incarnations[r.Resource] > 1 && r.BucketTs < infraGenerationStart(asset.FirstSeen) {
			continue
		}
		if r.RType == "lock" && !m.originLockConfigured(r.Resource) {
			// Retain historical samples; removing a configured target retires it
			// from the current dashboard and alert evaluation immediately.
			continue
		}
		a := byRes[r.Resource]
		if a == nil {
			a = &acc{rtype: r.RType, metrics: map[string]float64{}, metricTs: map[string]int64{}}
			byRes[r.Resource] = a
		}
		// host 行并入同名实例(agent 的 node 名 = 实例名);若先有 host 后有 instance,保留 instance 类型。
		if r.RType != "host" {
			a.rtype = r.RType
		}
		a.metrics[r.Metric] = r.Value
		a.metricTs[r.Metric] = r.BucketTs
		if a.minTs == 0 || r.BucketTs < a.minTs {
			a.minTs = r.BucketTs
		}
	}

	snap := InfraSnapshot{ManagedAWS: m.managedAWSInfraView()}
	if registryErr != nil {
		snap.RegistryError = "资源目录暂不可用，归档状态无法核验"
	}
	snap.GeneratedAt = time.Unix(nowUnix, 0).Format("2006-01-02 15:04:05")
	var oldest int64
	for name, a := range byRes {
		staleAfter := m.infraMetricFreshnessSec()
		staleMetrics := make([]string, 0)
		for metric, observedAt := range a.metricTs {
			metricAge := nowUnix - (observedAt + 60)
			if metricAge > staleAfter {
				delete(a.metrics, metric)
				staleMetrics = append(staleMetrics, metric)
			}
		}
		sort.Strings(staleMetrics)
		freshOldest := int64(0)
		for metric, observedAt := range a.metricTs {
			if _, retained := a.metrics[metric]; retained && (freshOldest == 0 || observedAt < freshOldest) {
				freshOldest = observedAt
			}
		}
		age := int64(-1)
		if freshOldest > 0 {
			age = max(int64(0), nowUnix-(freshOldest+60))
			if oldest == 0 || freshOldest < oldest {
				oldest = freshOldest
			}
		}
		if a.rtype == "inventory" {
			status := "nosample"
			if ok, exists := a.metrics["discovery_ok"]; exists {
				if ok >= 1 {
					status = "ok"
				} else {
					status = "bad"
				}
			}
			snap.Discoveries = append(snap.Discoveries, InfraDiscovery{Name: name, Status: status, ResourceCount: int(a.metrics["resource_count"]), AgeSec: age})
			continue
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
		missingMetrics := missingInfraMetrics(name, a.rtype, a.metrics)
		res := InfraResource{Name: name, DisplayName: infraDisplayName(name), Type: rtypeOrInstance(a.rtype),
			Platform: infraPlatform(name, a.rtype), Group: infraResourceGroup(name), AgeSec: age, Metrics: a.metrics, StaleMetrics: staleMetrics}
		res.MissingMetrics = missingMetrics
		res.CoverageComplete = len(missingMetrics) == 0
		if incarnations[name] > 1 {
			res.Containers = m.hostContainerSnapshot(name, nowUnix, assets[name].FirstSeen+1)
		} else {
			res.Containers = m.hostContainerSnapshot(name, nowUnix)
		}
		addDerivedPct(&res) // 派生百分比键(前端直接用),需在算 status 前完成
		res.Status = m.infraStatus(res)
		if (len(missingMetrics) > 0 || len(criticalStaleMetrics(staleMetrics)) > 0) && res.Status == "ok" {
			res.Status = "warn"
		}
		switch res.Type {
		case "database":
			snap.Databases = append(snap.Databases, res)
		case "lb":
			snap.LoadBalancers = append(snap.LoadBalancers, res)
		default:
			snap.Instances = append(snap.Instances, res)
		}
	}
	sortProbes(snap.Probes)
	sortLocks(snap.Locks)
	sort.Slice(snap.Discoveries, func(i, j int) bool { return snap.Discoveries[i].Name < snap.Discoveries[j].Name })
	if oldest > 0 {
		snap.DataAgeSec = nowUnix - (oldest + 60)
		if snap.DataAgeSec < 0 {
			snap.DataAgeSec = 0
		}
	} else {
		snap.DataAgeSec = -1
	}
	// Keep discovered/missing resources visible even before their first
	// sample or after time-series retention. Their manual membership persists.
	for name, a := range assets {
		if !m.monitorOwnsInfraResource(name, a.Kind, a.Platform) {
			continue
		}
		if a.State != "active" || m.infraExcluded(name) {
			continue
		}
		if _, exists := byRes[name]; exists {
			continue
		}
		if a.Kind != "instance" && a.Kind != "host" && a.Kind != "ecs_service" {
			continue
		}
		snap.Instances = append(snap.Instances, InfraResource{Name: name, DisplayName: infraDisplayName(name), Platform: a.Platform, Type: rtypeOrInstance(a.Kind), Group: infraResourceGroup(name), Status: "nosample", AgeSec: -1, Metrics: map[string]float64{}})
	}
	// 实例按名稳定排序,避免每次刷新行序跳动。
	sortInstances(snap.Instances)
	sortInfraResources(snap.Databases)
	sortInfraResources(snap.LoadBalancers)
	// 兼容仍只消费单个 DB/LB 的容量规划：优先选择 NexusAPI 资源，
	// 没有则稳定选择排序后的第一项。
	if len(snap.Databases) > 0 {
		d := preferredInfraResource(snap.Databases, "NexusAPI")
		snap.Database = &d
	}
	if len(snap.LoadBalancers) > 0 {
		l := preferredInfraResource(snap.LoadBalancers, "NexusAPI")
		snap.LB = &l
	}
	snap.Groups = buildInfraGroups(snap.Instances, snap.Databases, snap.LoadBalancers)
	// 各资源的指标趋势改为前端按需拉(GET /infra/series),不在快照里预算。
	snap.Overview = buildOverview(snap)
	snap.Alerts = m.recentInfraAlerts(nowUnix, 20)
	visibleAlerts := snap.Alerts[:0]
	for _, alert := range snap.Alerts {
		if !m.monitorOwnsInfraResource(alert.Target, "", "") {
			continue
		}
		hidden := false
		for resource, asset := range assets {
			if asset.State != "active" && (alert.Target == resource || strings.HasPrefix(alert.Target, resource+"/")) {
				hidden = true
				break
			}
		}
		if !hidden {
			visibleAlerts = append(visibleAlerts, alert)
		}
	}
	snap.Alerts = visibleAlerts
	return snap
}

// requiredInfraMetrics defines the minimum evidence needed before a resource
// can be presented as fully observed. Static capacity/control-plane rows prove
// discovery, but never prove that workload telemetry is healthy.
func requiredInfraMetrics(resource, rtype string) []string {
	switch rtype {
	case "ecs_service":
		return []string{"status_failed", "desired", "running", "cpu", "mem_used_pct"}
	case "database":
		if strings.HasPrefix(resource, "rds/") {
			return []string{"available", "cpu", "free_mem_mb", "free_storage_gb"}
		}
		return []string{"cpu", "free_mem_mb", "free_storage_gb"}
	case "lb":
		if strings.HasPrefix(resource, "alb/") {
			return []string{"status_failed", "healthy", "unhealthy"}
		}
		return []string{"healthy", "unhealthy"}
	case "host":
		return []string{"mem_avail_mb", "disk_used_pct"}
	case "instance", "":
		return []string{"status_failed", "cpu", "mem_avail_mb", "disk_used_pct"}
	default:
		return nil
	}
}

func missingInfraMetrics(resource, rtype string, metrics map[string]float64) []string {
	missing := make([]string, 0)
	for _, metric := range requiredInfraMetrics(resource, rtype) {
		if _, ok := metrics[metric]; !ok {
			missing = append(missing, metric)
		}
	}
	return missing
}

func (m *Monitor) infraMetricFreshnessSec() int64 {
	seconds := m.cfg.InfraSampleSeconds
	if seconds <= 0 {
		seconds = 300
	}
	limit := int64(seconds * 3)
	if limit < 15*60 {
		limit = 15 * 60
	}
	return limit
}

func infraDisplayName(name string) string {
	parts := strings.Split(name, "/")
	return parts[len(parts)-1]
}

func infraPlatform(name, rtype string) string {
	switch {
	case strings.HasPrefix(name, "ecs/") || rtype == "ecs_service":
		return "ECS/Fargate"
	case strings.HasPrefix(name, "rds/"):
		return "RDS"
	case strings.HasPrefix(name, "alb/"):
		return "ALB"
	case rtype == "host":
		return "Host agent"
	default:
		return "Lightsail"
	}
}

func infraResourceGroup(name string) string {
	n := strings.ToLower(name)
	switch {
	case strings.Contains(n, "sub2api"):
		return "Sub2API"
	case strings.Contains(n, "nexusapi"), strings.Contains(n, "video-demo"):
		return "NexusAPI"
	case strings.Contains(n, "monitor"), strings.Contains(n, "eval"), n == "ubuntu-1":
		return "Monitor / Eval"
	default:
		return "其他资源"
	}
}

func preferredInfraResource(resources []InfraResource, group string) InfraResource {
	for _, resource := range resources {
		if resource.Group == group {
			return resource
		}
	}
	return resources[0]
}

func sortInfraResources(resources []InfraResource) {
	for i := 1; i < len(resources); i++ {
		for j := i; j > 0; j-- {
			a, b := resources[j-1], resources[j]
			if a.Group < b.Group || (a.Group == b.Group && a.DisplayName <= b.DisplayName) {
				break
			}
			resources[j-1], resources[j] = resources[j], resources[j-1]
		}
	}
}

func buildInfraGroups(instances, databases, lbs []InfraResource) []InfraResourceGroup {
	byName := map[string]*InfraResourceGroup{}
	ensure := func(name string) *InfraResourceGroup {
		if byName[name] == nil {
			byName[name] = &InfraResourceGroup{Name: name}
		}
		return byName[name]
	}
	for _, resource := range instances {
		g := ensure(resource.Group)
		g.Instances = append(g.Instances, resource)
	}
	for _, resource := range databases {
		g := ensure(resource.Group)
		g.Databases = append(g.Databases, resource)
	}
	for _, resource := range lbs {
		g := ensure(resource.Group)
		g.LoadBalancers = append(g.LoadBalancers, resource)
	}
	order := []string{"NexusAPI", "Sub2API", "Monitor / Eval", "其他资源"}
	out := make([]InfraResourceGroup, 0, len(byName))
	for _, name := range order {
		g := byName[name]
		if g == nil {
			continue
		}
		states := make([]string, 0, len(g.Instances)+len(g.Databases)+len(g.LoadBalancers))
		for _, r := range g.Instances {
			states = append(states, r.Status)
		}
		for _, r := range g.Databases {
			states = append(states, r.Status)
		}
		for _, r := range g.LoadBalancers {
			states = append(states, r.Status)
		}
		g.Status = worst(states...)
		out = append(out, *g)
		delete(byName, name)
	}
	// 未知分组也稳定输出，便于未来显式分组扩展。
	for len(byName) > 0 {
		var smallest string
		for name := range byName {
			if smallest == "" || name < smallest {
				smallest = name
			}
		}
		g := byName[smallest]
		g.Status = "nosample"
		out = append(out, *g)
		delete(byName, smallest)
	}
	return out
}

func containerStatus(state, health string) string {
	switch state {
	case "missing", "dead", "exited", "removing":
		return "bad"
	case "paused", "restarting", "created":
		return "warn"
	case "running":
		if health == "unhealthy" {
			return "bad"
		}
		if health == "starting" || health == "unknown" {
			return "warn"
		}
		return "ok"
	default:
		return "warn"
	}
}

func safeContainerState(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "created", "running", "paused", "restarting", "removing", "exited", "dead", "missing":
		return value
	default:
		return "unknown"
	}
}

func safeContainerHealth(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "none", "starting", "healthy", "unhealthy":
		return value
	case "":
		return ""
	default:
		return "unknown"
	}
}

func (m *Monitor) replaceHostContainerSnapshots(node string, rows []HostContainerSnapshot) error {
	return m.storeDB.Transaction(func(tx *gorm.DB) error {
		return replaceHostContainerSnapshotsWithDB(tx, node, rows)
	})
}

func replaceHostContainerSnapshotsWithDB(tx *gorm.DB, node string, rows []HostContainerSnapshot) error {
	if err := tx.Where("node = ?", node).Delete(&HostContainerSnapshot{}).Error; err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	return tx.CreateInBatches(rows, 100).Error
}

func (m *Monitor) hostContainerSnapshot(node string, now int64, since ...int64) []InfraContainer {
	var rows []HostContainerSnapshot
	query := m.storeDB.Where("node = ?", node)
	if len(since) > 0 {
		query = query.Where("last_seen >= ?", since[0])
	}
	warnReadErr("host container snapshot", query.Order("name").Find(&rows))
	out := make([]InfraContainer, 0, len(rows))
	for _, row := range rows {
		age := now - row.LastSeen
		if age < 0 {
			age = 0
		}
		status := containerStatus(row.State, row.Health)
		// HostAgent 默认每分钟上报；超过 5 分钟未更新不能继续显示为绿色。
		if age > 300 && status == "ok" {
			status = "warn"
		}
		out = append(out, InfraContainer{Name: row.Name, State: row.State, Health: row.Health,
			RestartCount: row.RestartCount, AgeSec: age, Status: status})
	}
	return out
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
	code, hasCode := mm["status_code"]
	if !hasCode || code < 100 || code > 599 || math.IsNaN(code) || code != math.Trunc(code) {
		l.Status = "nosample"
		return l
	}
	l.HTTPCode = int(code)
	l.Locked = l.HTTPCode == http.StatusForbidden
	if l.Locked {
		l.Status = "ok"
	} else {
		l.Status = "bad" // 非预期响应仍告警，但不是已确认可绕过的证据。
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
		return 4
	case "warn":
		return 3
	case "nosample":
		return 2
	case "ok":
		return 1
	default: // absent / ""
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
	if len(snap.Databases) > 0 {
		dbStates := make([]string, 0, len(snap.Databases))
		for _, database := range snap.Databases {
			dbStates = append(dbStates, database.Status)
			states = append(states, database.Status)
		}
		o.DBStatus = worst(dbStates...)
	} else if snap.Database != nil {
		o.DBStatus = snap.Database.Status
		states = append(states, snap.Database.Status)
	}
	if len(snap.LoadBalancers) > 0 {
		lbStates := make([]string, 0, len(snap.LoadBalancers))
		for _, lb := range snap.LoadBalancers {
			lbStates = append(lbStates, lb.Status)
			states = append(states, lb.Status)
		}
		o.LBStatus = worst(lbStates...)
	} else if snap.LB != nil {
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
		switch l.Status {
		case "ok":
			o.LocksOK++
		case "bad":
			o.LocksBad++
		default:
			o.LocksUnknown++
		}
		states = append(states, l.Status)
	}
	for _, discovery := range snap.Discoveries {
		o.DiscoveryTotal++
		if discovery.Status == "ok" {
			o.DiscoveryOK++
		}
		states = append(states, discovery.Status)
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
		groupRank := func(group string) int {
			switch group {
			case "NexusAPI":
				return 0
			case "Sub2API":
				return 1
			case "Monitor / Eval":
				return 2
			default:
				return 3
			}
		}
		ga, gb := groupRank(a.Group), groupRank(b.Group)
		if ga != gb {
			return ga < gb
		}
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
	case "ecs_service":
		// Enhanced Container Insights gives absolute utilized/reserved task
		// memory and ephemeral storage. Standard AWS/ECS already provides
		// mem_used_pct; derive it only as a fallback when the standard point
		// is temporarily late.
		if used, ok := m["mem_used_mb"]; ok && hasMemTotal && memTotal > 0 {
			if _, exists := m["mem_used_pct"]; !exists {
				m["mem_used_pct"] = used / memTotal * 100
			}
		} else if usedPct, ok := m["mem_used_pct"]; ok && hasMemTotal && memTotal > 0 {
			m["mem_used_mb"] = memTotal * usedPct / 100
		}
		if used, ok := m["disk_used_gb"]; ok {
			if total, ok2 := m["disk_total_gb"]; ok2 && total > 0 {
				m["disk_used_pct"] = used / total * 100
			}
		}
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
	if v, ok := mm["mem_avail_pct"]; ok {
		return v, v < m.cfg.InfraMemAvailBadPct
	}
	v, ok := mm["free_mem_mb"]
	return v, ok && v < m.cfg.InfraDBFreeMemBadMB
}

// dbMemWarn 在数据库规格未知（RDS/CloudWatch 不提供总内存）时回退为绝对 MB 阈值。
// 有百分比时始终优先使用百分比，避免同一资源同时被两套口径判定。
func (m *Monitor) dbMemWarn(mm map[string]float64) (float64, bool) {
	if v, ok := mm["mem_avail_pct"]; ok {
		return v, v < m.cfg.InfraMemAvailWarnPct
	}
	v, ok := mm["free_mem_mb"]
	return v, ok && v < m.cfg.InfraDBFreeMemWarnMB
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
	containerState := ""
	for _, container := range r.Containers {
		containerState = worst(containerState, container.Status)
	}
	if len(mm) == 0 {
		if containerState != "" {
			return containerState
		}
		return "nosample"
	}
	c := m.cfg
	has := func(k string) (float64, bool) { v, ok := mm[k]; return v, ok }
	switch r.Type {
	case "database":
		if v, ok := has("available"); ok && v < 1 {
			return "bad"
		}
		if _, hit := m.dbMemBad(mm); hit {
			return "bad"
		}
		if _, hit := m.dbStorageBad(mm); hit {
			return "bad"
		}
		if v, ok := has("cpu"); ok && v > c.InfraCPUBadPct {
			return "bad"
		}
		if _, hit := m.dbMemWarn(mm); hit {
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
		if v, ok := has("status_failed"); ok && v >= 1 {
			return "bad"
		}
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
		if containerState == "bad" {
			return "bad"
		}
		if _, hit := instanceDown(mm); hit {
			return "bad"
		}
		if v, ok := has("unhealthy_containers"); ok && v >= 1 {
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
		if containerState == "warn" {
			return "warn"
		}
		if r.Type == "ecs_service" {
			if v, ok := has("registry_write_ok"); ok && v < 1 {
				return "warn"
			}
			if v, ok := has("task_discovery_ok"); ok && v < 1 {
				return "warn"
			}
		}
		return "ok"
	}
}

// evaluateInfraAlerts 评估基础设施告警(复用现有邮件 + 冷却);复用 alert_config 的开关/SMTP/收件人。
func (m *Monitor) evaluateInfraAlerts(now int64) {
	if !m.cfg.InfraEnabled || m.cfg.AlertsDisabled { // 见 Settings.AlertsDisabled 的断路器说明
		return
	}
	c := m.loadAlertConfig()
	if !c.Enabled || c.SMTPHost == "" || c.Recipients == "" {
		return
	}
	snap := m.computeInfraSnapshot(now)
	if snap.RegistryError != "" {
		m.fire(c, "infra_registry_failed", "resource-registry", "资源目录不可用", snap.RegistryError, now)
		// Only host/ECS membership depends on the registry. Do not silence
		// independent DB, LB, discovery, probe or certificate alarms merely
		// because archive state cannot be read. Never alarm archived hosts.
		snap.Instances = nil
	}
	databases := snap.Databases
	if len(databases) == 0 && snap.Database != nil {
		databases = []InfraResource{*snap.Database}
	}
	for _, d := range databases {
		if available, ok := d.Metrics["available"]; ok && available < 1 {
			m.fire(c, "infra_db_unavailable", d.Name, "数据库实例不可用",
				fmt.Sprintf("数据库 %s 的 AWS 状态不是 available，请立即检查 RDS 事件与连接。", d.Name), now)
		}
		if v, hit := m.dbMemBad(d.Metrics); hit {
			free := d.Metrics["free_mem_mb"]
			detail := fmt.Sprintf("数据库 %s 可用内存仅 %.0f MB(绝对红线 %.0f MB),内存接近耗尽,有 OOM 重启风险。", d.Name, v, m.cfg.InfraDBFreeMemBadMB)
			if _, hasPct := d.Metrics["mem_avail_pct"]; hasPct {
				detail = fmt.Sprintf("数据库 %s 可用内存仅 %.1f%%(%.0f MB,阈值 %.0f%%),内存接近耗尽,有 OOM 重启风险。", d.Name, v, free, m.cfg.InfraMemAvailBadPct)
			}
			m.fire(c, "infra_db_mem", d.Name, "数据库可用内存告急", detail, now)
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
		if v, ok := in.Metrics["unhealthy_containers"]; ok && v >= 1 {
			m.fire(c, "infra_ecs_unhealthy", in.Name, "ECS 服务存在不健康任务",
				fmt.Sprintf("ECS 服务 %s 当前有 %.0f 个不健康任务，请检查任务事件、健康检查和最近部署。", in.Name, v), now)
		}
		if v, ok := in.Metrics["cpu"]; ok && v > m.cfg.InfraCPUBadPct {
			m.fire(c, "infra_instance_cpu", in.Name, "实例 CPU 告急",
				fmt.Sprintf("实例 %s CPU %.1f%%，已超过红线 %.0f%%。", in.Name, v, m.cfg.InfraCPUBadPct), now)
		}
		if v, ok := in.Metrics["mem_used_pct"]; ok && v > 100-m.cfg.InfraMemAvailBadPct {
			m.fire(c, "infra_instance_mem", in.Name, "实例内存告急",
				fmt.Sprintf("实例 %s 内存使用率 %.1f%%，已超过红线 %.0f%%。", in.Name, v, 100-m.cfg.InfraMemAvailBadPct), now)
		}
		for _, container := range in.Containers {
			if container.Status != "bad" {
				continue
			}
			detail := fmt.Sprintf("节点 %s 的容器 %s 状态=%s", in.Name, container.Name, container.State)
			if container.Health != "" {
				detail += fmt.Sprintf(" health=%s", container.Health)
			}
			detail += fmt.Sprintf(" restart_count=%d。", container.RestartCount)
			m.fire(c, "infra_container", in.Name+"/"+container.Name, "关键容器异常", detail, now)
		}
	}
	lbs := snap.LoadBalancers
	if len(lbs) == 0 && snap.LB != nil {
		lbs = []InfraResource{*snap.LB}
	}
	for _, lb := range lbs {
		if failed, ok := lb.Metrics["status_failed"]; ok && failed >= 1 {
			m.fire(c, "infra_lb_down", lb.Name, "负载均衡状态异常",
				fmt.Sprintf("负载均衡 %s 的 AWS 状态不是 active。", lb.Name), now)
		}
		if v, hit := lbUnhealthy(lb.Metrics); hit {
			m.fire(c, "infra_lb_unhealthy", lb.Name, "负载均衡有不健康节点",
				fmt.Sprintf("负载均衡 %s 不健康节点数 %.0f。", lb.Name, v), now)
		}
	}
	for _, resource := range append(append(append([]InfraResource{}, snap.Instances...), databases...), lbs...) {
		if len(resource.MissingMetrics) > 0 {
			m.fire(c, "infra_metrics_missing", resource.Name, "服务端监控采集不完整",
				fmt.Sprintf("资源 %s 从未采到必需指标：%s。页面已标记为不可完整判定，请检查 AWS 权限、CloudWatch 或主机 agent。", resource.Name, strings.Join(resource.MissingMetrics, ", ")), now)
		}
		stale := criticalStaleMetrics(resource.StaleMetrics)
		if len(stale) == 0 {
			continue
		}
		m.fire(c, "infra_metrics_stale", resource.Name, "服务端监控指标断流",
			fmt.Sprintf("资源 %s 的关键指标超过 %d 分钟未更新：%s。页面已停止用这些旧值判定健康，请检查 AWS 权限、采集器和网络。", resource.Name, m.infraMetricFreshnessSec()/60, strings.Join(stale, ", ")), now)
	}
	for _, discovery := range snap.Discoveries {
		if discovery.Status != "ok" {
			m.fire(c, "infra_discovery_failed", discovery.Name, "AWS 资源发现失败",
				fmt.Sprintf("%s 发现接口未成功，新资源可能未出现在监控页；请检查 IAM 权限、AWS API 与网络。", discovery.Name), now)
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
	// 非预期响应必须告警并核验；无响应不算已确认失败，非 403 也不直接证明可绕过。
	for _, l := range snap.Locks {
		if l.Status == "bad" {
			m.fire(c, "infra_origin_lock", l.Target, "源站拦截响应异常（需核验）",
				fmt.Sprintf("源站 %s 直连(不带 X-Origin-Verify)返回 HTTP %d(预期 403)。请立即核验当前访问边界和拦截配置；仅凭非 403 响应不能确认业务接口可被绕过访问。", l.Target, l.HTTPCode), now)
		}
	}
}

func criticalStaleMetrics(metrics []string) []string {
	critical := map[string]bool{
		"available": true, "status_failed": true, "cpu": true,
		"free_mem_mb": true, "mem_avail_mb": true, "mem_used_pct": true,
		"desired": true, "running": true, "unhealthy_containers": true,
		"healthy": true, "unhealthy": true,
	}
	out := make([]string, 0, len(metrics))
	for _, metric := range metrics {
		if critical[metric] {
			out = append(out, metric)
		}
	}
	return out
}

// probeDownDetail 给探活失败一句人话描述。
func probeDownDetail(p ProbeResource) string {
	if !p.Reachable {
		return "不可达(超时或连接失败)"
	}
	return fmt.Sprintf("HTTP %d", p.HTTPCode)
}
