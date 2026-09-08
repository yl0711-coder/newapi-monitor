# AWS 托管资源监控与 Fargate 采集入口

服务端监控在 `MONITOR_INFRA_ENABLED=true` 时自动发现同一区域内的 Lightsail、ECS/Fargate、RDS 和 ALB。Fargate 没有可登录的宿主机，因此不安装 Lightsail 主机 agent；Monitor 读取 ECS 服务期望/运行/等待任务数、任务当前健康状态、任务预留容量，以及 CloudWatch CPU、内存。启用 Container Insights 后，还会自动出现网络、临时磁盘已用量和重启次数。RDS 和 ALB 同样使用 AWS 只读控制面与 CloudWatch。

生产 Monitor IAM 身份除现有 Lightsail 权限外，需要以下只读权限：

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecs:ListClusters",
        "ecs:ListServices",
        "ecs:DescribeServices",
        "ecs:ListTasks",
        "ecs:DescribeTasks",
        "rds:DescribeDBInstances",
        "elasticloadbalancing:DescribeLoadBalancers",
        "elasticloadbalancing:DescribeTargetGroups",
        "elasticloadbalancing:DescribeTargetHealth",
        "cloudwatch:GetMetricStatistics"
      ],
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "aws:RequestedRegion": "us-west-2"
        }
      }
    }
  ]
}
```

任一资源类别暂未授权时，该类别本轮采集失败但已有 Lightsail、页面和其他业务任务继续运行；不会让 Monitor 启动失败。

## Fargate 与 Lightsail 指标口径

- 两者都有：CPU、内存使用率和服务/容器健康。Fargate 的任务预留内存会与标准内存百分比组合成已用/总量。
- Fargate 额外按 ECS 服务显示：期望任务、运行任务、等待任务、容器健康检查、重启次数。
- Fargate 不存在可归属给租户的宿主机 Load、Swap 和 Lightsail 突发额度，页面明确显示“不适用”，不能用虚构的 0 代替。
- 未启用 Container Insights 时，基础 CPU、内存已用/总量、任务预留临时磁盘总量和任务副本状态仍可用；网络、临时磁盘已用量与重启次数需要 Container Insights。可选指标无数据不会把服务误判为异常。

若要采集完整任务级指标，在 ECS 集群设置中启用 **Container Insights with enhanced observability**。这是 CloudWatch 的计费能力，启用前需要单独确认费用；Monitor 无需重启，指标产生后会在后续采样轮次自动显示。容器没有定义 `HEALTHCHECK` 时，ECS 会返回 `UNKNOWN`，Monitor 不会把它伪装成“已检查且健康”。

## Fargate 内部日志采集避免 404

`monitor.nexusapi.link` 的 `/internal/*` 先由 Caddy 做来源网络限制，再由 Monitor 校验 Bearer Token。Fargate 若把该域名解析到公网，会以任务临时公网 IP 到达 Caddy，因不在白名单而得到刻意返回的 404。任务公网 IP 会随滚动部署变化，禁止把它逐个加入白名单。

生产建议：

1. 在 ECS 所在 VPC 创建私有 DNS 记录，让 `monitor.nexusapi.link` 解析到 Monitor Lightsail 私网地址。
2. 确认 VPC 到 Lightsail 的 `172.26.0.0/16` 对等路由为 active。
3. Caddy `/internal/*` 来源允许列表增加 ECS VPC CIDR `172.31.0.0/16`；Monitor 的 Bearer Token 鉴权继续保留。
4. 从一条新 Fargate 任务验证 `/internal/nginx`、`/internal/nginx-errors`、`/internal/nginx-evidence/v1`、`/internal/rejections` 均返回 200，再滚动替换其余任务。
5. 验证 Caddy 日志里的 `remote_ip` 是 `172.31.x.x` 而非任务公网 IP，并确认错误批次水位连续推进。

这个方案不向公网开放内部接口，也不依赖易变的 Fargate 公网 IP。
