# 生产部署辅助文件

`monitor-ecs-task-role-cloudwatch-policy.example.json` 仅是 Monitor 生产 ECS
Task Role 的最小 CloudWatch Logs 只读参考模板。生产环境已经有运维维护的
`nexusapi-monitor-ro`，不需要使用该文件新建 IAM 策略或角色。

运维部署时需要：

1. 在 Monitor 的 ECS Task Definition 将 `TaskRoleArn` 设置为已有的
   `nexusapi-monitor-ro`，无需新建 IAM 资源。
2. 确认该角色已有 us-east-1、us-west-2 指定 CloudWatch Logs 的只读权限，并用
   IAM Policy Simulator 及窄时间窗口分别验证 `FilterLogEvents`、`StartQuery`、
   `GetQueryResults`、`StopQuery`。不要用参考模板另建策略覆盖现有权限。
3. 不要设置
   `AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY` 或 `AWS_SESSION_TOKEN`；程序会通过
   ECS Task Metadata 使用临时凭证。
4. `MONITOR_CLOUDWATCH_EVIDENCE_HMAC_KEY` 和
   `MONITOR_CLOUDWATCH_EVIDENCE_HMAC_KEY_ID` 不是 AWS 凭证，仍需由受控的
   Secrets Manager 注入；不要写进镜像、Compose 或仓库。

该参考文件只描述权限，不会调用 AWS，也不会自动修改现有 Role、策略或任务定义。

## 本地 8204 验收

本地 `docker-compose.local-production-readonly.yml` 仍从 gitignored `.env` 读取 AWS
临时凭证，仅用于 8204 调试。它不代表生产部署方式；生产 ECS 不注入任何
`AWS_ACCESS_KEY_ID` 等长期密钥，直接使用上面的 `nexusapi-monitor-ro` Task Role。
