# Monitor 2026-08-21 上线准备记录

## 结论

当前工作树可以进入“allowlist 提交 → release 分支 → CI 构建候选镜像”阶段，尚未部署。

- 变更范围只在 `newapi-monitor`；NewAPI rc4 与 `newapi-reject-collector` 工作树均为 clean，NexusAPI 没有 tracked 变更。
- 用户用量事实、Usage 用户用量与 Usage 日志是本次 P0 冻结链路；本轮收尾没有再改其架构。
- “用户侧最终感知”需求尚无不修改 NewAPI 且覆盖全部用户的可靠采集路径；本轮撤回未落地的受控客户端回传实现，页面不再展示不可用入口。
- “上游余额 + 上游消费 → 可用天数/充值预警”已记录为后续需求，本候选不冒充已实现。

## 本轮功能

1. 用户用量事实增加 5 分钟实时投影；只读已闭合本地事实，异常时 fail-closed，长期事实仍由小时发布、校验和自动修复保证完整一致。
2. AICodeWith 支持最多 64 把 Key 的不透明槽位管理、逐 Key 持久游标、分轮抓取和同一窗口原子发布；新增/删除 Key 验证失败时原配置、余额和已发布账单不变。
3. 网站倍率同步同时包含用户可选分组和“特殊可用分组”；不同网站、不同分组独立计算倍率差，倍率输入严格解析。
4. 稳定性报表继续提供历史日志推断与脱敏问题签名，并明确该口径不代表用户侧最终感知。
5. 稳定性问题文本在写入 SQLite 前脱敏。对 2026-08-20 线上只读备份的 5,632 条历史问题样本进行模式计数，未发现 Bearer、Authorization、access_token、api_key 或邮箱命中；不为此引入上线前全表重写。
6. 存储迁移计划已提升为 v14；即使本轮只从 AutoMigrate 集合撤回未上线的客户端证据模型，也会在升级前重新建立主库与事实库的成套恢复点。撤回不会删除既有 SQLite 表或数据。
7. Usage 使用日志首屏与翻页改为 `(created_at,type,id)` 复合游标；组织查询使用 `idx_created_at_type`，单成员查询使用 `idx_user_created_type`，分组查询使用已由管理员在生产创建并验证的 `idx_logs_user_group_created_type`，Request ID、Token 名和模型筛选交由优化器选择其选择性索引。页面查询增加 3 秒 MySQL 执行上限，导出/有界计数增加 8 秒上限，旧纯 ID 游标继续兼容。
8. Monitor 与 Usage 的聚合读取范围不再被新加入且无历史数据的成员整体截断；读取仍要求全部所选成员已发布，并按每个成员自己的事实起点过滤。缺失成员继续 fail-closed，新成员加入不会隐藏同组老成员的历史数据。

## Usage 日志只读执行计划

2026-08-22 通过回环 SSH 隧道和 `nexus_ro` 单连接执行只读程序级查询；未执行 `EXPLAIN ANALYZE`、压测或生产写入：

- 组织全部日志：旧计划使用 `PRIMARY` 倒序并估算产生约 421,072 行；新计划使用 `idx_created_at_type` 时间范围。
- 类型筛选：旧计划使用 `PRIMARY` 并估算产生约 42,107 行；新计划使用 `idx_created_at_type`。
- 模型筛选：保留优化器选择 `idx_logs_model_name` 等选择性索引，不以时间索引强制覆盖。
- 分组筛选：使用生产已验证的 `(user_id, group, created_at, type)` 复合索引 `idx_logs_user_group_created_type`。
- 单成员筛选：使用 `idx_user_created_type`。
- Request ID 精确查询：新旧均使用 `idx_logs_request_id`，未被时间索引提示覆盖。
- Token 名与详情包含搜索仍不能由普通 B-tree 消除扫描；服务端继续执行关键词长度、时间窗口、并发门和 3 秒 SQL 上限，失败关闭而不放大来源压力。

索引完成后的只读程序查询中，分组命中约 552ms、分组空结果约 392ms、模型命中约 565ms、Token 精确命中约 452ms、Request ID 空结果约 426ms；测试窗口 `Slow_queries` 增量为 0。详情包含搜索仍会扫描当日候选范围，实测命中约 1.306s，因此继续保留一天窗口、并发门和 3 秒上限。这些是单连接功能验证，不冒充并发压测结果。

## Allowlist

只允许提交以下路径；禁止使用 `git add .`：

```text
.env.example
docker-compose.example.yml
docs/delivery-evidence.md（删除）
docs/test-reports/2026-08-21-monitor-release-preparation.md
monitor/channel_management.go
monitor/channel_management.js
monitor/channel_management_test.go
monitor/channel_upstream.go
monitor/channel_upstream_test.go
monitor/channel_upstream_usage_test.go
monitor/delivery_evidence.go（删除）
monitor/delivery_evidence_test.go（删除）
monitor/monitor.go
monitor/page.html
monitor/portal.go
monitor/portal.html
monitor/portal_test.go
monitor/server.go
monitor/settings.go
monitor/stability.css
monitor/stability.js
monitor/stability_problem.go
monitor/stability_sampler.go
monitor/stability_store.go
monitor/stability_test.go
monitor/store.go
monitor/store_migration_backup.go
monitor/usage.go
monitor/usage_test.go
monitor/usage_facts.go
monitor/usage_facts_history_test.go
```

## 本机已通过门禁

- `go test ./... -count=1`
- `go test ./monitor -count=1`（本轮完整包约 28s）
- Usage 日志复合游标、旧游标兼容、导出快照与 Portal 游标定向测试通过
- 新成员不截断同组历史的 Monitor + Usage 端到端回归通过
- `go test -race ./monitor -count=1`（256.255s）
- AICodeWith 失败 Key 原子回滚定向测试 20 轮
- `go vet ./...`
- `node --check`：`channel_management.js`、`stability.js`、`range_picker.js`
- `gofmt -l monitor` 为空
- `git diff --check`

本机 Go 为 1.27，而仓库固定的 `golangci-lint v1.64.8` 无法解析该版本的
export data；本机 lint 结果无效，不能记作通过。提交后的 CI 固定 Go 1.26.6
并从同一工具链编译 lint，必须绿灯后才允许构建发布镜像。

## 本机候选镜像冒烟

- 标签：`newapi-monitor:release-audit-20260822`（仅本机 worktree 证据，不是发布 tag）
- 镜像 ID：`sha256:82fe418aa839958203eab4548ef1bce7f95afa71065f5814aa24f6ab8512c7c0`
- OCI revision：`worktree-release-audit-20260822`
- 默认 UID/GID：`1000/1000`
- 完全离线、`--network none`、只读根文件系统启动；`/live` 返回 `ok`，`/ready` 返回 `ready`。
- 空卷启动完成两库迁移，`/ready` 返回 `store.ok=true`、`facts_store.ok=true`，来源 worker 明确为 disabled。
- `docker stop -t 40` 后：`ExitCode=0`、`OOMKilled=false`、`RestartCount=0`。
- 冒烟容器与两只临时卷已删除；镜像保留供提交前本机对照。正式制品必须在提交后重建。

## 构建与运行前硬门禁

1. 按 allowlist 提交到 release 分支；提交后重新执行本页全部门禁。
2. CI 必须以该提交构建 Linux/amd64 镜像，OCI `org.opencontainers.image.revision` 必须等于冻结 commit；推送后只使用 registry RepoDigest，不使用可变 tag。
3. 生产机先预拉镜像，再执行实际 `.env.cluster` 与 Compose 预检；确认数据卷/备份卷、UID 1000 权限、空间、配额和卷外不可变备份。
4. 停旧实例并确认 ExitCode=0、OOMKilled=false、来源租约释放后，候选才允许启动；不得并行运行两个会访问来源的 Monitor。
5. 启动后必须验证 `/live`、`/ready` JSON、用户用量发布水位、Usage 页面与日志、SQLite `quick_check`、备份集和恢复演练。
6. 真实运行观察必须确认数据库查询节流、来源租约、无重启/OOM、用户用量与中转站账单差异在设计延迟内；任何硬阈值触发立即停止候选并按已验证恢复点回滚。

## 不属于本候选的承诺

- 不修改 NewAPI 源码。
- 不依赖正式 NewAPI Nginx 新字段。
- 未接入客户端 SDK 时，不提供“全站用户真实感知成功率”。
- 不自动调整渠道权重、自动补偿或改变计费。
- 不包含“余额还能使用多少天”和充值预警；该功能需以已完整同步的上游消费为基础另行开发。
