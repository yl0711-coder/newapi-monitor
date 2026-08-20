# Monitor 2026-08-21 上线准备记录

## 结论

当前工作树可以进入“allowlist 提交 → release 分支 → CI 构建候选镜像”阶段，尚未部署。

- 变更范围只在 `newapi-monitor`；NewAPI rc4 与 `newapi-reject-collector` 工作树均为 clean，NexusAPI 没有 tracked 变更。
- 用户用量事实、Usage 用户用量与 Usage 日志是本次 P0 冻结链路；本轮收尾没有再改其架构。
- 受控客户端证据是可选 Monitor 输入；未接入受控客户端时必须显示“不可判断”，不得用服务端或历史日志推断客户端成功。
- “上游余额 + 上游消费 → 可用天数/充值预警”已记录为后续需求，本候选不冒充已实现。

## 本轮功能

1. 用户用量事实增加 5 分钟实时投影；只读已闭合本地事实，异常时 fail-closed，长期事实仍由小时发布、校验和自动修复保证完整一致。
2. AICodeWith 支持最多 64 把 Key 的不透明槽位管理、逐 Key 持久游标、分轮抓取和同一窗口原子发布；新增/删除 Key 验证失败时原配置、余额和已发布账单不变。
3. 网站倍率同步同时包含用户可选分组和“特殊可用分组”；不同网站、不同分组独立计算倍率差，倍率输入严格解析。
4. 稳定性报表区分历史日志推断、服务端协议事实与受控客户端结果；客户端结果按请求开始时间归档，跨日迟到结果仍归原请求窗口，孤儿结果单列且不进入成功率。
5. 稳定性问题文本在写入 SQLite 前脱敏。对 2026-08-20 线上只读备份的 5,632 条历史问题样本进行模式计数，未发现 Bearer、Authorization、access_token、api_key 或邮箱命中；不为此引入上线前全表重写。
6. 存储迁移计划已提升为 v13，迁移前双库成套快照、运行期 backup-set、恢复激活与失败关闭逻辑保持启用。

## Allowlist

只允许提交以下路径；禁止使用 `git add .`：

```text
.env.example
docker-compose.example.yml
docs/delivery-evidence.md
docs/usage-live-projection.md
docs/test-reports/2026-08-21-monitor-release-preparation.md
monitor/channel_finance_test.go
monitor/channel_management.go
monitor/channel_management.js
monitor/channel_management_test.go
monitor/channel_upstream.go
monitor/channel_upstream_test.go
monitor/channel_upstream_usage.go
monitor/channel_upstream_usage_test.go
monitor/delivery_evidence.go
monitor/delivery_evidence_test.go
monitor/monitor.go
monitor/page.html
monitor/portal.go
monitor/portal.html
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
monitor/store_migration_backup_test.go
monitor/usage.go
monitor/usage_facts.go
monitor/usage_facts_live_projection.go
monitor/usage_facts_live_projection_test.go
monitor/website_groups.go
monitor/website_groups_test.go
```

## 已通过门禁

- `go test ./... -count=1`
- `go test ./monitor -count=1`（27.509s）
- `go test -race ./monitor -count=1`（277.070s）
- 客户端跨窗口/孤儿结果、AICodeWith 失败 Key 原子回滚定向测试 20 轮
- `go vet ./...`
- `golangci-lint run ./...`（0 告警）
- `node --check`：`channel_management.js`、`stability.js`、`range_picker.js`
- `gofmt -l monitor` 为空
- `git diff --check`

## 本机候选镜像冒烟

- 标签：`newapi-monitor:release-prep-20260821-7b4731f4`（仅本机 worktree 证据，不是发布 tag）
- 镜像 ID：`sha256:85516eaa56d335877575bc7b9bfa910ab9c8e895ec1259b2ba7cd27e0d2f90a0`
- OCI revision：`worktree-7b4731f4b644a2c9`
- 默认 UID/GID：`1000/1000`
- 完全离线、`--network none`、只读根文件系统启动；`/live` 返回 `ok`，`/ready` 返回 `ready`。
- `/data/nexus_monitor.db`、`/data/usage-facts.db` 与 `/backup` 均以默认身份通过读写检查。
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
