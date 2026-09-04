package monitor

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbtypes "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
)

// infra_aws_managed.go 采集 ECS/Fargate、RDS 与 ALB。它只读 AWS 控制面
// 和 CloudWatch，不进入业务容器，也不要求 Fargate 安装主机 agent。

type cloudWatchMetricAPI interface {
	GetMetricStatistics(context.Context, *cloudwatch.GetMetricStatisticsInput, ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricStatisticsOutput, error)
}

type managedMetricSpec struct {
	name  string
	key   string
	stat  cwtypes.Statistic
	scale float64
}

func (m *Monitor) sampleManagedAWSInfra(ctx context.Context, bucket int64) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(m.cfg.AWSRegion))
	if err != nil {
		slog.Warn("infra managed: AWS 客户端初始化失败(忽略本轮)", "err", err)
		return
	}
	cw := cloudwatch.NewFromConfig(cfg)
	rows := make([]InfraSample, 0, 64)
	resourceCount := 0

	if ecsRows, count, collectErr := collectECSInfra(ctx, ecs.NewFromConfig(cfg), cw, bucket); collectErr != nil {
		slog.Warn("infra managed: ECS/Fargate 自动发现失败(忽略本类)", "err", collectErr)
	} else {
		rows = append(rows, ecsRows...)
		resourceCount += count
	}
	if rdsRows, count, collectErr := collectRDSInfra(ctx, rds.NewFromConfig(cfg), cw, bucket); collectErr != nil {
		slog.Warn("infra managed: RDS 自动发现失败(忽略本类)", "err", collectErr)
	} else {
		rows = append(rows, rdsRows...)
		resourceCount += count
	}
	if albRows, count, collectErr := collectALBInfra(ctx, elasticloadbalancingv2.NewFromConfig(cfg), cw, bucket); collectErr != nil {
		slog.Warn("infra managed: ALB 自动发现失败(忽略本类)", "err", collectErr)
	} else {
		rows = append(rows, albRows...)
		resourceCount += count
	}

	rows = m.filterManagedInfraRows(rows)
	if err := m.upsertInfra(rows); err != nil {
		slog.Warn("infra managed: 采样入库失败(忽略本轮)", "err", err)
		return
	}
	if resourceCount > 0 {
		slog.Info("infra managed AWS 采样完成", "resources", resourceCount, "rows", len(rows))
	}
}

func (m *Monitor) filterManagedInfraRows(rows []InfraSample) []InfraSample {
	out := rows[:0]
	for _, row := range rows {
		if !m.infraExcluded(row.Resource) && !m.infraExcluded(infraDisplayName(row.Resource)) {
			out = append(out, row)
		}
	}
	return out
}

func collectECSInfra(ctx context.Context, client *ecs.Client, cw cloudWatchMetricAPI, bucket int64) ([]InfraSample, int, error) {
	var rows []InfraSample
	count := 0
	clusters := ecs.NewListClustersPaginator(client, &ecs.ListClustersInput{})
	for clusters.HasMorePages() {
		page, err := clusters.NextPage(ctx)
		if err != nil {
			return rows, count, err
		}
		for _, clusterARN := range page.ClusterArns {
			clusterName := arnTail(clusterARN)
			services := ecs.NewListServicesPaginator(client, &ecs.ListServicesInput{Cluster: aws.String(clusterARN)})
			for services.HasMorePages() {
				servicePage, err := services.NextPage(ctx)
				if err != nil {
					return rows, count, err
				}
				for start := 0; start < len(servicePage.ServiceArns); start += 10 {
					end := start + 10
					if end > len(servicePage.ServiceArns) {
						end = len(servicePage.ServiceArns)
					}
					described, err := client.DescribeServices(ctx, &ecs.DescribeServicesInput{
						Cluster: aws.String(clusterARN), Services: servicePage.ServiceArns[start:end],
					})
					if err != nil {
						return rows, count, err
					}
					for _, service := range described.Services {
						serviceName := aws.ToString(service.ServiceName)
						if serviceName == "" {
							continue
						}
						resource := "ecs/" + clusterName + "/" + serviceName
						failed := aws.ToString(service.Status) != "ACTIVE" || service.RunningCount < service.DesiredCount
						rows = append(rows,
							managedRow(bucket, resource, "ecs_service", "desired", float64(service.DesiredCount)),
							managedRow(bucket, resource, "ecs_service", "running", float64(service.RunningCount)),
							managedRow(bucket, resource, "ecs_service", "pending", float64(service.PendingCount)),
							managedRow(bucket, resource, "ecs_service", "containers_total", float64(service.DesiredCount)),
							managedRow(bucket, resource, "ecs_service", "containers_up", float64(service.RunningCount)),
							managedRow(bucket, resource, "ecs_service", "status_failed", boolFloat(failed)),
						)
						dims := []cwtypes.Dimension{{Name: aws.String("ClusterName"), Value: aws.String(clusterName)}, {Name: aws.String("ServiceName"), Value: aws.String(serviceName)}}
						rows = appendCloudWatchMetrics(ctx, rows, cw, bucket, resource, "ecs_service", "AWS/ECS", dims, []managedMetricSpec{
							{name: "CPUUtilization", key: "cpu", stat: cwtypes.StatisticAverage, scale: 1},
							{name: "MemoryUtilization", key: "mem_used_pct", stat: cwtypes.StatisticAverage, scale: 1},
						})
						count++
					}
				}
			}
		}
	}
	return rows, count, nil
}

func collectRDSInfra(ctx context.Context, client *rds.Client, cw cloudWatchMetricAPI, bucket int64) ([]InfraSample, int, error) {
	var rows []InfraSample
	count := 0
	paginator := rds.NewDescribeDBInstancesPaginator(client, &rds.DescribeDBInstancesInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return rows, count, err
		}
		for _, database := range page.DBInstances {
			identifier := aws.ToString(database.DBInstanceIdentifier)
			if identifier == "" {
				continue
			}
			resource := "rds/" + identifier
			rows = append(rows, managedRow(bucket, resource, "database", "available", boolFloat(aws.ToString(database.DBInstanceStatus) == "available")))
			if database.AllocatedStorage != nil {
				rows = append(rows, managedRow(bucket, resource, "database", "disk_total_gb", float64(*database.AllocatedStorage)))
			}
			dims := []cwtypes.Dimension{{Name: aws.String("DBInstanceIdentifier"), Value: aws.String(identifier)}}
			rows = appendCloudWatchMetrics(ctx, rows, cw, bucket, resource, "database", "AWS/RDS", dims, []managedMetricSpec{
				{name: "CPUUtilization", key: "cpu", stat: cwtypes.StatisticAverage, scale: 1},
				{name: "DatabaseConnections", key: "connections", stat: cwtypes.StatisticMaximum, scale: 1},
				{name: "FreeableMemory", key: "free_mem_mb", stat: cwtypes.StatisticAverage, scale: 1048576},
				{name: "FreeStorageSpace", key: "free_storage_gb", stat: cwtypes.StatisticAverage, scale: 1073741824},
				{name: "SwapUsage", key: "swap_mb", stat: cwtypes.StatisticAverage, scale: 1048576},
				{name: "DiskQueueDepth", key: "disk_queue", stat: cwtypes.StatisticAverage, scale: 1},
			})
			count++
		}
	}
	return rows, count, nil
}

func collectALBInfra(ctx context.Context, client *elasticloadbalancingv2.Client, cw cloudWatchMetricAPI, bucket int64) ([]InfraSample, int, error) {
	var rows []InfraSample
	count := 0
	paginator := elasticloadbalancingv2.NewDescribeLoadBalancersPaginator(client, &elasticloadbalancingv2.DescribeLoadBalancersInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return rows, count, err
		}
		for _, lb := range page.LoadBalancers {
			name, lbARN := aws.ToString(lb.LoadBalancerName), aws.ToString(lb.LoadBalancerArn)
			if name == "" || lbARN == "" {
				continue
			}
			resource := "alb/" + name
			active := lb.State != nil && string(lb.State.Code) == "active"
			rows = append(rows, managedRow(bucket, resource, "lb", "status_failed", boolFloat(!active)))
			healthy, unhealthy, healthErr := albTargetHealth(ctx, client, lbARN)
			if healthErr != nil {
				slog.Warn("infra managed: ALB 目标健康读取失败(保留其他指标)", "resource", name, "err", healthErr)
			} else {
				rows = append(rows,
					managedRow(bucket, resource, "lb", "healthy", float64(healthy)),
					managedRow(bucket, resource, "lb", "unhealthy", float64(unhealthy)),
				)
			}
			lbDimension := arnSuffix(lbARN, "loadbalancer/")
			if lbDimension != "" {
				dims := []cwtypes.Dimension{{Name: aws.String("LoadBalancer"), Value: aws.String(lbDimension)}}
				rows = appendCloudWatchMetrics(ctx, rows, cw, bucket, resource, "lb", "AWS/ApplicationELB", dims, []managedMetricSpec{
					{name: "HTTPCode_ELB_5XX_Count", key: "err_5xx", stat: cwtypes.StatisticSum, scale: 1},
					{name: "TargetResponseTime", key: "resp_ms", stat: cwtypes.StatisticAverage, scale: 0.001},
				})
			}
			count++
		}
	}
	return rows, count, nil
}

func albTargetHealth(ctx context.Context, client *elasticloadbalancingv2.Client, lbARN string) (int, int, error) {
	healthy, unhealthy := 0, 0
	paginator := elasticloadbalancingv2.NewDescribeTargetGroupsPaginator(client, &elasticloadbalancingv2.DescribeTargetGroupsInput{LoadBalancerArn: aws.String(lbARN)})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return healthy, unhealthy, err
		}
		for _, group := range page.TargetGroups {
			if group.TargetGroupArn == nil {
				continue
			}
			out, err := client.DescribeTargetHealth(ctx, &elasticloadbalancingv2.DescribeTargetHealthInput{TargetGroupArn: group.TargetGroupArn})
			if err != nil {
				return healthy, unhealthy, err
			}
			for _, target := range out.TargetHealthDescriptions {
				if target.TargetHealth != nil && target.TargetHealth.State == elbtypes.TargetHealthStateEnumHealthy {
					healthy++
				} else {
					unhealthy++
				}
			}
		}
	}
	return healthy, unhealthy, nil
}

func appendCloudWatchMetrics(ctx context.Context, rows []InfraSample, client cloudWatchMetricAPI, bucket int64, resource, rtype, namespace string, dimensions []cwtypes.Dimension, specs []managedMetricSpec) []InfraSample {
	for _, spec := range specs {
		value, ok, err := latestCloudWatchMetric(ctx, client, namespace, spec.name, dimensions, spec.stat, spec.scale)
		if err != nil {
			slog.Warn("infra managed: CloudWatch 指标读取失败(保留其他指标)", "resource", resource, "metric", spec.name, "err", err)
			continue
		}
		if ok {
			rows = append(rows, managedRow(bucket, resource, rtype, spec.key, value))
		}
	}
	return rows
}

func latestCloudWatchMetric(ctx context.Context, client cloudWatchMetricAPI, namespace, metric string, dimensions []cwtypes.Dimension, statistic cwtypes.Statistic, scale float64) (float64, bool, error) {
	end := time.Now()
	start := end.Add(-30 * time.Minute)
	out, err := client.GetMetricStatistics(ctx, &cloudwatch.GetMetricStatisticsInput{
		Namespace: aws.String(namespace), MetricName: aws.String(metric), Dimensions: dimensions,
		StartTime: aws.Time(start), EndTime: aws.Time(end), Period: aws.Int32(300), Statistics: []cwtypes.Statistic{statistic},
	})
	if err != nil {
		return 0, false, err
	}
	var value float64
	var latest time.Time
	found := false
	for _, point := range out.Datapoints {
		if point.Timestamp == nil || (found && !point.Timestamp.After(latest)) {
			continue
		}
		var raw *float64
		switch statistic {
		case cwtypes.StatisticMaximum:
			raw = point.Maximum
		case cwtypes.StatisticSum:
			raw = point.Sum
		default:
			raw = point.Average
		}
		if raw != nil {
			value, latest, found = *raw, *point.Timestamp, true
		}
	}
	if !found {
		return 0, false, nil
	}
	if scale <= 0 {
		scale = 1
	}
	return value / scale, true, nil
}

func managedRow(bucket int64, resource, rtype, metric string, value float64) InfraSample {
	return InfraSample{BucketTs: bucket, Resource: resource, RType: rtype, Metric: metric, Value: value}
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func arnTail(value string) string {
	parts := strings.Split(value, "/")
	return parts[len(parts)-1]
}

func arnSuffix(value, marker string) string {
	index := strings.Index(value, marker)
	if index < 0 {
		return ""
	}
	return value[index+len(marker):]
}
