# AWS 托管资源监控与 Fargate 采集入口

服务端监控在 `MONITOR_INFRA_ENABLED=true` 时自动发现同一区域内的 Lightsail、ECS/Fargate、RDS 和 ALB。Fargate 不安装主机 agent；Monitor 读取 ECS 服务期望/运行/等待任务数和 CloudWatch CPU、内存。RDS 和 ALB 同样使用 AWS 只读控制面与 CloudWatch。

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

## Fargate 内部日志采集避免 404

`monitor.nexusapi.link` 的 `/internal/*` 先由 Caddy 做来源网络限制，再由 Monitor 校验 Bearer Token。Fargate 若把该域名解析到公网，会以任务临时公网 IP 到达 Caddy，因不在白名单而得到刻意返回的 404。任务公网 IP 会随滚动部署变化，禁止把它逐个加入白名单。

生产建议：

1. 在 ECS 所在 VPC 创建私有 DNS 记录，让 `monitor.nexusapi.link` 解析到 Monitor Lightsail 私网地址。
2. 确认 VPC 到 Lightsail 的 `172.26.0.0/16` 对等路由为 active。
3. Caddy `/internal/*` 来源允许列表增加 ECS VPC CIDR `172.31.0.0/16`；Monitor 的 Bearer Token 鉴权继续保留。
4. 从一条新 Fargate 任务验证 `/internal/nginx`、`/internal/nginx-errors`、`/internal/nginx-evidence/v1`、`/internal/rejections` 均返回 200，再滚动替换其余任务。
5. 验证 Caddy 日志里的 `remote_ip` 是 `172.31.x.x` 而非任务公网 IP，并确认错误批次水位连续推进。

这个方案不向公网开放内部接口，也不依赖易变的 Fargate 公网 IP。
