# Monitor 生产运维手册

本文只涉及 `newapi-monitor` 容器及其独立数据卷。本次发布不修改、不构建、不部署 NewAPI 代码或镜像；Monitor 只兼容现有 NewAPI 接口和只读数据。NewAPI `logs` 复合索引若需增加，属于独立 DBA 变更，必须另行审批，不在 Monitor 容器启动或发布中自动执行。

> 用户用量事实层在进入任何生产变更前，必须先使用隔离的本地 MySQL 合成/脱敏数据集完成容量、并发、一致性和故障注入验收。不得把 SSH 隧道、线上 DSN 或线上 NewAPI URL 用作“本机验收”数据源；当前阻断项见 [`test-reports/2026-08-14-usage-facts-local-director-acceptance.md`](test-reports/2026-08-14-usage-facts-local-director-acceptance.md)。

## 数据边界

- `NEWAPI_LOG_DSN` 必须使用只读账号。
- Monitor 只向自己的两份 SQLite 写入数据：`MONITOR_STORE_PATH` 保存配置/权限/稳定性等控制数据，`MONITOR_USAGE_FACTS_STORE_PATH` 独立保存高增长的用量事实和脱敏资料。未显式配置后者时，默认是主库同目录的 `usage-facts.db`。
- 两份 SQLite 都应视为业务数据，并放在同一个持久化卷中；运行期 facts 同步失败可以只停用事实写入，但**启动/迁移前**是双库共同闸门：任一现有库损坏、无法锁定或无法生成成套快照时，整个新进程在主库任何 `AutoMigrate` 前退出。已经打开事实读时仍保持 fail-closed，防止静默回扫生产 `logs`。
- Redis 只用于可选用量缓存；不可用时自动降级，不能作为恢复来源。

## 来源生命周期与健康端点

生产固定配置 `MONITOR_SOURCE_WORKER_ENABLED=true`、`MONITOR_SOURCE_LEASE_REQUIRED=true`和稳定的 `MONITOR_SOURCE_LEASE_NAME`。实际 MySQL advisory lock 名由该配置名加 `DBName` 的稳定摘要组成：同一数据库即使通过 host/IP 别名连接仍互斥，同一 MySQL 实例上的不同 schema 不会误互斥。lease 持有一条专用连接，已包含在 Monitor 原有的 4 连接上限内。

- 启动时会校验 DSN、`logs/channels/users/tokens/options` 只读权限和必需列。坏 DSN、1045/1049/1142/1146/1054 及 x509 证书错误直接拒绝启动，不伪装成网络抖动。
- `connection refused`、连接重置和超时等短暂故障不退出进程；服务继续读两份 SQLite，后台指数退避恢复。来源只有在 `ready`且持有 lease 的 epoch 中才能运行 sampler、facts、stability 和上游账户轮询。
- 断线或失租后先 cancel 当前 epoch，封闭新子任务并等待全部退场，再释放 lease。默认最长退场 20 秒；超时则进入 `critical_drain_timeout`、隔离 lease 并禁止新 epoch，不允许跨 epoch/跨实例重叠。Compose 必须保持 `stop_grace_period: 40s`，覆盖 HTTP 5 秒和来源退场 20 秒预算。

健康端点均不需要登录：

- `GET /live`：只证明 HTTP 进程存活，MySQL、SQLite、Redis、facts 或上游故障都不会使它失败。容器 healthcheck 只能使用此端点，避免来源短断触发重启风暴。`GET /health` 是向后兼容别名。
- `GET /ready`：请求路径只读后台原子状态，不查 MySQL，也不现场扫 SQLite。主 SQLite 或已启用的 facts SQLite 不可读时返回 503 `not_ready`；来源、采样、facts 水位、稳定性覆盖/问题采集、备份新鲜度或 Redis 异常返回 200 `degraded`并列出不含凭据/路径的 reason code。发布切流要同时检查 HTTP 码和 JSON `status`，不能只看 200。

来源故障时先查 `/ready` 的 `source.state/last_failure_at/next_retry_at/failure_streak`。`degraded_network` 下不要重启容器，修复网络后 supervisor 会自愈；`standby_lease` 表示另一实例是唯一 worker，当前实例只服务 SQLite；`blocked_config` 表示权限/表结构/证书等永久错误，自动重试已停止，需修复配置并重启。任何来源状态变化都不会删除、清空或降级已发布 facts。

## 上线前检查

1. 确认 `/data` 使用具名数据卷，而不是容器可写层。首发前在旧容器还运行时记录 `docker inspect nexusapi-monitor` 的 `/data` `Mount.Name`；真实生产 preflight 要求新配置精确复用该卷，或者指向已通过 backup-set 恢复并激活的新卷。不得照抄 example 卷名挂空卷。候选启动前必须按真实 Compose 命令停止旧 Monitor，再用 `docker inspect` 确认已退出。`BEGIN IMMEDIATE` 只保证复制期间两库不变化，不是跨进程租约。
2. 确认首次升级原样复用旧生产 `MONITOR_SESSION_SECRET`，且不进入仓库、镜像或发布输出；更换它会让既有登录会话失效。
3. 固定配置独立的 `MONITOR_UPSTREAM_CREDENTIAL_SECRET` 并纳入密钥备份。从“上游凭据回退使用 Session Secret”的旧版首次升级时，启动会在一个 SQLite 事务内将所有旧密文重封到新密钥；任一行无法用新或旧密钥解密时整体回滚并拒绝启动。完成换钥后再改动任一密钥都会让凭据不可解密。
4. 确认只读 DSN 仅有 `logs/channels/users/tokens/options` 的 `SELECT` 权限。先在 `information_schema.statistics` 检查 `logs` 的 `idx_user_created_type(user_id,created_at,type)`。若不存在，只能作为独立数据库变更：在维护窗口执行 `ALTER TABLE logs ADD INDEX idx_user_created_type (user_id, created_at, type), ALGORITHM=INPLACE, LOCK=NONE`，观察锁等待/复制延迟，再对 facts 单小时 SQL 执行 `EXPLAIN`。Monitor 上线程序本身不会创建该索引；未获得承担 DBA 职责的技术负责人批准时不得擅自执行，也不能与 Monitor 回填同时进行。
5. 当前版本不接入 Nginx 旁路采集时，确认 `MONITOR_NGINX_ENABLED=false`，且不启动 nginxcollector。
6. 确认 `GET /live` 返回 200；再检查 `GET /ready` 的 HTTP 码和 JSON `status/degraded_reasons`。已知历史补全可以是 200 `degraded`，但不能是本地库 503、`blocked_config` 或 `critical_drain_timeout`。
7. 管理员登录后检查 `GET /stability/health`：本地库可达、主采样有成功时间、问题采集无长期积压。
8. 候选 migration plan 首次启动后，在日志确认“迁移前双库快照已校验并原子发布”；同一 plan 重启则应显示“复用固定迁移前快照”。检查 `/backup/pre-migrate-*/manifest.json` 与 `READY` 属于同一批次；随后管理员登录检查 `GET /usage/facts-status`：`store.integrity_ok=true` 且 `facts_store.integrity_ok=true`。启用自动备份 5 分钟后，两者的 `last_backup_success_at` 和 `last_backup_bytes` 都应为非零。

## 明日受控首发口径

当时间不足 7 天时，可以把第 7 天改为上线后容量/趋势复核，但不能把尚未发生的观察写成已通过。明日客户切流只能按“受控首发”签字，并同时满足：

1. 最终代码树的 ordinary/race/vet/lint 全绿，镜像固定为 `repository@sha256:digest`，实际生产 Compose 渲染结果和 hash 已归档。
2. 技术负责人完成下方来源完整性声明；只读 preflight、索引探针和单小时 `EXPLAIN` 通过。
3. 最终 digest 生成成套 backup-set，复制到独立故障域，并在 `--network none` 下恢复到全新卷；两库 `quick_check`、ACL、published facts 实读通过。
4. 旧 Monitor 已停止且证明来源查询为零。候选先在 Caddy 后方隔离运行 2 小时，然后继续跑满连续 24 小时混合 pilot；未满 24 小时时只能发布候选/影子，不标记客户 GA。
5. Usage 全历史首版只在全体 tracked 成员签名完成、`facts.read_active=true` 后开入口；Stability 大迁移保持关闭，不与 Usage 冷回填同时运行。
6. 外部看板和当班人已覆盖 DB CPU/连接/复制延迟/DiskQueue、NewAPI p95/5xx、Monitor source starts/查询时长、WAL/卷/RSS/restart。当前没有独立“一键只停 cold”入口；触发软线时执行已演练的停候选命令并保留卷/游标。

实际生产真相源是 `NexusAPI/deploy/docker-compose.monitor.yml` 和 `.env.cluster`，不是本仓库的 example。维护窗前必须在部署主机执行：

```bash
NexusAPI/deploy/monitor-release-preflight.sh \
  --stage phase-a \
  --env-file NexusAPI/deploy/.env.cluster
```

这里展示的是首次 `phase-a`；后续 `pilot`、`cutover`、`rollback` 的配置组合、命令和
顺序以 `NexusAPI/deploy/RUNBOOK.md` 的阶段状态机为唯一真相源，不能省略 `--stage`。
它是发布门禁，不启动 Monitor 服务：拒绝可变镜像标签、占位 epoch/密钥/token、相同 data/backup 卷、空数据卷、错误的旧卷来源、uid 1000 或现有 SQLite 文件不可写、错误 Usage 阶段或不存在的镜像/卷；它会用短命、无网络容器执行身份与权限探针并立即删除探针文件。输出不含密钥的 image/volume/provenance/epoch/service-config/Compose/Caddyfile/preflight/rollback SHA-256 供归档。

来源签字不需要公司必须设置独立 DBA 岗位；技术负责人可承担该职责，但必须把责任边界写明。归档模板：

```text
负责人：<姓名>        签字时间：<CST ISO-8601>
SOURCE_MODE: complete
SOURCE_EPOCH: <本次固定值>
我确认：对当前所有 active 成员，hot logs 自 users.created_at 起至签字时可见历史完整，
无未接入的归档/冷库/删除缺口；只读账号与 Monitor 看到同一范围；
logs.idx_user_created_type(user_id,created_at,type) 已通过只读探针。
若归档、路由、权限视图或可见历史改变，我会在改变前切换 SOURCE_EPOCH 并触发全域重签。
```

## 用户用量事实层：fail-closed 发布

事实层只把按小时/日聚合后的用量和脱敏资料写入独立事实库，不保存日志原文、API Key 或请求内容。全历史按成员持久化 discovery/backfill/tail/verify 游标：完整自然日使用带服务端硬超时的维度 SQL 加独立 control SQL，当前日才使用小时 Tail；范围超限时逐级缩小到单成员/单日或小时修复。分钟采样、facts 和稳定性历史共用一个后台来源槽、至少 2 秒 query-start 间隔及低优先级 duty。任何时候不得同时启动 Usage 全历史迁移和 Stability 大迁移。

`SOURCE_MODE=complete` 不是代码能自行证明的事实。承担 DBA 职责的技术负责人必须以只读 SQL、归档策略和保留策略共同签收：hot `logs` 从每个 active 用户 `users.created_at` 起没有历史缺口；未来若迁冷，必须先上线 hot+cold adapter/manifest，再改变 `SOURCE_EPOCH` 并全域重签，不能先删除热库。启动 preflight 会验证硬依赖的 `idx_user_created_type`，但索引存在也不等于历史完整。

首次上线使用与最终生产相同的读取策略：

1. 预创建独立、带容量配额的数据卷和备份卷；固定 `FULL_HISTORY_ENABLED=true`、`SOURCE_MODE=complete`、技术负责人签收的 `SOURCE_EPOCH`、source duty 20%、history delay 30 秒。生产聚合始终保持本地 facts 意图；未发布或签名不一致时返回 503，绝不回扫生产 `logs`。
2. v5 分类维护采用独立阶段：先停旧容器并取得可恢复的成套卷外快照；候选以 `CLASSIFICATION_MIGRATION_ENABLED=true`、`READ_ENABLED=false` 启动，只撤销旧发布授权并创建持久重签任务。该组合在代码中仍是本地 fail-closed，不会回源聚合。迁移任务建立后，以同一镜像改为 `CLASSIFICATION_MIGRATION_ENABLED=false`、`READ_ENABLED=true` 运行 worker。
3. 使用管理员会话检查 `GET /usage/facts-status` 和 `GET /usage/facts-history`：来源 mode/epoch、逐成员 stage、backfill 与 verify 水位、paused/error、磁盘阻断和发布签名必须一致。首个 v5 baseline 只有全体 tracked 成员完成才可发布；以后新增成员 pending 只对管理员可见，不得把组织未完成成员当零。
4. `facts.read_active=true` 后才开放聚合入口；stats/matrix/token/followups/Portal 的来源 `logs` 查询计数必须为零。任何 epoch、分类或查询语义变化先 fail-closed，全域重签完成后自动恢复，不允许混合新旧口径。

ETA 不得沿用旧“每小时一个 SQL”的静态公式。生产先做 2 小时小样本和 24 小时混合负载 pilot，记录 signed member-days/s、SQLite bytes/row、WAL 峰值、来源 query/rows/duration，再据实推算。背景来源全局并发 1、start spacing 至少 2 秒、cold duty 不超过 20%。软线为 DB CPU≥60% 连续两个 5 分钟或较基线+10pp、连接≥70%、DiskQueue≥5、复制延迟≥2 秒持续三个周期、NewAPI p95 同时恶化≥10%且≥50ms、重复3024/锁等待；当前实现不能只暂停cold，触发后应停止候选并保留卷/游标，连续15分钟稳定后才可最小负载半开。硬线为 CPU≥75%、连接≥85%、p95+25%、5xx+0.5pp、任何数据/ACL/hash错误、磁盘≥85%或绝对余量破线、OOM/restart、来源并发>1或spacing<2秒；立即停止并回滚。

运行中：

- `completed_hours` 和 `verified_hours/status` 必须分开看；backfill 100% 但 verify 未完成不等于 ready。新增成员只创建该成员任务，不重扫或隐藏已签成员；remove/rejoin、来源回退和已证明 mismatch 会原子撤销受影响成员签名，其他公司继续服务。
- 高优先 Tail 每轮只读近期闭合小时；full-history job 优先消费同 epoch 已存在的小时 proof，缺失才发 low 查询。完整日只有 detail/control 一致且本地 hash/proof 完整时原子切换 daily 与 ServingGeneration。no-history 和注册日至首日志前缀必须有来源边界证明，并持续做本地反证。
- 本地 rolling audit、recent source audit 与 cold source audit 使用独立持久游标；发现已证明损坏先撤成员授权并建 exact repair，修好和复核后才重发。单个坏成员不能拖住同批健康成员；网络/lease/budget 未执行不消耗成员永久重试次数。
- 每条来源查询都有超时；同步循环 panic/意外退出会在 5 秒后重启。`loop_heartbeat_at` 长期不更新或 `last_fact_failure_at` 持续增加时应检查只读 DSN，不要切回页面直扫生产日志。
- `GET /usage/cache-stats` 的 `source_budget` 可观察 aggregate/detail/export 三条来源库泳道；`facts_read_budget` 显示本地事实冷查询闸门，容量固定为 2。`interactive_waiters`、本地 `waiters` 或 `max_hold_ms` 长期上升时，应先缩短范围并定位慢查询，不能增加无界并发。
- 单项缓存载荷上限为 4 MiB，本机 L1 总上限仍为 128 项/16 MiB；同键冷查询由 singleflight 合并。矩阵在 `成员数×自然日数>20,000` 时由服务端在查询和缓存填充前拒绝，只返回资料及明确提示。原始日志、CSV、当前用户资料和余额不缓存。
- 启动 readiness 只验证持久发布签名和有界尾小时，不重新全扫多年历史；运行期 rolling audit 负责持续发现 SQLite 逻辑损坏。`quick_check` 只证明文件结构，不证明业务内容正确。
- **不要把 `READ_ENABLED=false` 当普通回滚。** 只有显式 classification maintenance 组合会维持本地 fail-closed intent；其他配置变更前必须先验证不会回到来源聚合。最安全的异常策略是保持本地 facts intent、返回 503、修复/恢复 SQLite。

原始日志分页和 CSV 导出仍读取来源 `logs`，不属于 facts 聚合缓存。包含匹配无法使用普通 B-tree 前缀索引，服务端因此在发 SQL 前执行硬限制：日志页 token 模糊查询最多 31 天、content 详情最多 7 天；CSV 的 token 模糊查询最多 7 天、content 详情最多 1 天；token/content 关键词分别至少 2/3 个 Unicode 字符。超限请求直接返回 400，不占用来源查询槽。

## 渠道测试流量分类（兼容现有 NewAPI）

渠道管理的手动测试和定时测试会真实调用上游，但不是用户业务请求。Monitor 不要求 NewAPI 增加任何字段：成功测试只在 `type=2 AND COALESCE(token_name,'')='模型测试' AND COALESCE(content,'')='模型测试'` 三项同时命中时识别；NULL-safe 比较是 v5 的全域语义修复，避免 SQL 三值逻辑把合法消费漏掉。失败测试只按 NewAPI 现有 root 合成请求的完整稳定特征（`type=5`、`user_id=1`、无 token、无 request_id）识别。识别后的请求、Token 和 quota 写入独立 `channel_test_hour_samples`；用户 usage/facts、稳定性汇总、原始问题列表、日志明细和 CSV 均排除这些流量。

现有 NewAPI 日志没有手动/定时、单渠道/全渠道来源，Monitor 只能如实标记为 `legacy`，不根据时间或数量猜测。普通/固定价测试的 quota 按“分组前成本基数”计算；`billing_mode=tiered_expr` 的旧 quota 已含网站分组倍率，Monitor 会先除以网站倍率再乘上游倍率，避免重复计费。若 NewAPI 本身关闭消费或错误日志，未写入的测试没有任何可观测记录，Monitor 无法反向恢复其次数或成本；这是“NewAPI 不改”约束下的明确边界。

- **不要删除或重建 Monitor SQLite。** 启动时 `AutoMigrate` 只追加分类列和测试成本表；渠道倍率、上游账户、本地权限及历史配置继续保留。
- 分类规则升级时旧聚合会被 fail-closed，不会继续冒充用户流量。正常 60 秒采样和最近窗口滚动汇总照常运行；历史报表在补数期间显示已有完整小时并明确提示“小时数据待补”，不会显示假零。
- 分钟稳定性采样故意只处理用户流量，不写内部测试成本；`channel_test_hour_samples` 由完整小时自动修洞或人工补数生成。小时需先等待结束后 10 分钟定稿，自动修洞在启动 45 秒后首次运行、随后每 30 分钟最多修 1 小时，因此一条新测试成本正常约在请求后 10～100 分钟进入 Monitor 本地表。必须保持 `MONITOR_STABILITY_BACKFILL_ENABLED=true` 和 `MONITOR_STABILITY_AUTO_REPAIR=true`；关闭任一项时须安排人工补数，否则测试审计仍在 NewAPI 日志中，但 Monitor 的渠道燃烧会永久缺口。
- 在维护窗口以超级管理员调用 `POST /admin/stability/backfill?days=7`（按实际验收范围改为 30 或留存天数），再用 `GET /admin/stability/backfill` 检查 `job.status=complete`、`failed_hours=0` 和目标区间覆盖率。任务保持来源单并发，但会把连续缺口按 `2→4→6→12` 个完整小时合成一次来源聚合；每个小时仍在独立本地事务中原子替换并生成零流量 proof。整个 range 最多接收 20,000 个聚合维度，超时或超限会自动降为单小时；单个病态小时在受控重试后进入 `failed_hour_ts`，任务继续处理其他小时并以 `partial` 结束，不能把 partial 当完成签收。
- 所有后台来源查询默认至少间隔 2 秒启动；稳定性迁移默认将来源 SQL duty 限制为 20%（查询 1 秒后至少让路 4 秒，且不低于固定 2 秒）。range 明细查询后还会执行一条独立的来源控制总数 SQL，逐小时核对用户/内部测试的 requests、tokens、quota；不一致的 chunk 拒绝发布。两条 MySQL SELECT 都带 `MAX_EXECUTION_TIME(8000)` 服务端硬限制，客户端超时即使配置为 20 秒，也不允许单条数据库执行超过 8 秒。`GET /admin/stability/backfill` 暴露 `source_throttle`、完成/失败/已处理比例、当前 batch、来源查询次数和 ETA。ETA 是基于已观测查询耗时和当前 batch 的滚动估计，不是上线承诺。
- `partial` 任务不会伪装成完成：先看 `failed_hour_ts` 和对应来源慢查询/基数，再以超级管理员显式调用 `POST /admin/stability/backfill/retry?id=<job-id>`。重试会保留已经 complete 的小时，只重新扫描失败/缺失小时；不得删除台账后整段重跑。
- 分类规则升级造成数千小时缺口时，普通 `MONITOR_STABILITY_AUTO_REPAIR` 只负责最新小时修洞，不能承担历史迁移。必须先保持 `MONITOR_STABILITY_CLASSIFICATION_MIGRATION_ENABLED=false`，完成只读 `EXPLAIN` 和 2 小时/24 小时 pilot；确认来源 CPU、连接、慢查询和复制延迟在止损线内后，才在独立维护窗口显式改为 `true` 并重启 Monitor。该开关同时要求“小时聚合重签”和“原始错误分钟重签”两域完成；`GET /admin/stability/backfill` 只有在 `hourly_migration_status` 完成且 `problem_migration.status=complete` 时才返回 `migration_ready_to_disable=true`。原始错误实时 Tail 使用独立持久水位和高优先级 lane；冷迁移由独立低优先 worker 连续提交单个 12→6→3→1 分钟窗口，仍受全局 2 秒起步间隔、20% duty、8 秒 SQL 硬上限和高优先任务抢占，不再被主采样器“一分钟一窗”的节拍人为拖慢。`problem_migration` 暴露百分比、退避/暂停原因、已观测速率和 ETA；样本不足、停滞或暂停时不会伪造 ETA。两域完成前必须保持开关为 `true`；提前关闭会把持久 raw cursor 显示为 `paused_disabled` 并令 health 降级，而不是隐藏缺口。任务完成后再改回 `false`，且不要删除 SQLite 中的任务或小时台账。任何时候都不得同时启动 Usage 扩窗。
- 本次只发布 Monitor，NewAPI 保持现有代码、协议和镜像不变。`MONITOR_UPSTREAM_SYNC_ENABLED` 只控制上游余额轮询；新增的消费日志同步由 `MONITOR_UPSTREAM_USAGE_SYNC_ENABLED` 独立控制且默认关闭。首次发布必须保持关闭，先验证已有 Monitor/Usage 业务，再按账户灰度开启。
- usage facts 的 v5 分类变化可能影响任意用户，不能再用“是否存在 `user_id=1`”豁免，也不能在普通启动里全表 DELETE。旧派生数据存在时普通启动 fail-closed；只有显式 classification maintenance 才撤销发布/取消旧任务并按成员全历史重签，旧 facts 保留供恢复核对。

## 上游账户使用日志同步

- 余额快照与上游消费账单是两条独立同步链；页面刷新只读 Monitor SQLite，不访问上游。消费账单必须同时满足全局 `MONITOR_UPSTREAM_USAGE_SYNC_ENABLED=true` 和账户显式开启才会运行。首次上线保持全局闸门为 `false`；已有 Monitor/Usage 验收后，先只保留一个账户开关为开，再将全局闸门改为 `true` 重建 Monitor。按“一个账户 → 验证当天水位、账单差额、429/5xx 和实例负载 → 观察至少两个正常周期 → 再开下一个账户”的顺序灰度，禁止一次性全开。
- 当天追平和历史补全是两条独立健康状态和退避计数。当天和历史同时到期时一定先刷新当天；只有当天完整读取并原子发布成功，才会继续历史任务。当天健康但未到刷新时间时，到期的历史窗口可单独推进；如果当天遇到 401/429/5xx/网络错误，历史车道也至少暂停到当天重试时间，不得绕过退避重复打上游。历史窗口自身失败只让历史任务独立退避，不会拖慢当天追平。任一失败都保留上次完整本地汇总，不会把远端错误、超时或半页结果写成零消费。
- NewAPI 使用现有 OFFSET 日志接口：当天只重读最近 3 小时重叠窗口以吸收晚到日志，历史每轮至多补 1 个中国自然日。Monitor 校验每页 `total`、预期行数，并在多页扫描后重读首页指纹；并发变化会使整窗口失败重试。单窗超过 5,000 行时按时间二分，单秒仍超过 5,000 行则 fail-closed。单轮当天+历史共用 60 次请求硬预算。
- Sub2API 优先调用认证后的 `/api/v1/usage/dashboard/trend`，一次读取一个中国自然日的小时汇总；当天每轮重读当日并保留当前未闭合小时，历史每轮至多补 1 日。仅当该路由明确返回 404/405 时，才固定回退 `/api/v1/usage/stats` 并保存一个真实的单日桶；其他错误不会静默降级，也不会伪造小时分布。若 access token 过期，Monitor 使用 refresh token 刷新一次并原子保存轮换后的凭据。
- AICodeWith 按 Key 读取日账单。多把 Key 分批执行、按远端 `api_key_id` 去重，全部 Key 覆盖同一冻结窗口后才原子求和发布；中途失败不会发布部分 Key 的金额。单轮最多处理 4 把 Key，每把 Key 的接口请求遵守 10 次/分钟约束；历史接口单次最多补 31 个中国自然日。
- 默认当天间隔 30 分钟并带确定性单向抖动；所有账户共用全局串行锁和 host 访问保护，同一调度周期每分钟只选择 1 个到期账户。429 优先遵守 `Retry-After`，网络错误和可重试 5xx 会触发持久化退避/熔断。管理页“上游账户同步”按账户分开显示余额、账单适配器、粒度、当天水位、当天错误和历史游标，不得用“历史未完成”推断当天不新鲜。
- 本地只保存脱敏聚合，不保存上游原始日志、提示词、Key 或 IP。Sub2API 小时模式最多约 24 行/账户/日（单日兼容模式为 1 行），即每账户每年约 8,760 行；实际 SQLite 空间还包含索引、WAL 和备份，应通过 `/ready` 与备份状态按现有磁盘水位管理，不把理论行数当作磁盘承诺。

逐账户启用后的验收至少包括：余额同步仍正常、`usage_status=ok`、适配器符合预期、当天水位连续前进、相同中国自然日的上游金额与对方后台一致、历史游标只向前移动、失败后旧汇总仍可读且下次自动恢复。Sub2API 旧版出现“单日汇总（兼容模式）”是可用但粒度受限，不应显示成小时完整度。

## 受控历史补数与旧事实库升级

全历史模式下，超级管理员只允许对“一个已发布成员 × 一个已闭合 CST 自然日”建精确修复单：

```http
POST /usage/facts-history/repair
Content-Type: application/json

{
  "user_id": 123,
  "day": "2026-07-01",
  "reason": "账务核对发现该日金额不一致",
  "request_id": "ticket-20260701-123-v1",
  "confirm": "REPAIR_FULL_HISTORY_DAY"
}
```

- `request_id` 是稳定幂等键；同键同参数重复提交返回同一任务，同键改参数会拒绝。当前日、未发布成员、旧 revision/source epoch 或跨日范围一律拒绝。
- 修复与 rolling audit 共用一个 `fhr-*` 持久任务和低优先来源闸。真正已证明错数时，建单、撤销该成员发布签名和切换缓存世代在一个 SQLite 事务内完成；其他成员继续使用原签名。修好并独立对账后才重新发布该成员。
- 单日日聚合若触发 20,000 行/时限保护，会持久降为 24 个小时读取；日终仍需独立日控制数一致才原子替换。滚动来源审计只是“日查询装不下/控制竞态”时不会先撤客户，而是走不撤旧签名的逐小时核验；只有形成确定不一致时才替换或进入精确修复。
- 管理页“全历史同步进度”展示 `精确日修复`、`小时降档修复`、`逐小时来源核验` 的游标、失败和重试按钮。任务 `paused` 后先处理来源/容量原因，再按 job ID 重试；不要删除 job、proof 或直接改游标。
- 全历史启用时旧的 `POST /usage/facts-repair` 会明确拒绝，避免把只由 legacy worker 消费的游标写成永久卡死状态。

以下 `/usage/facts-repair` 仅用于仍处于有限窗口 legacy 模式的旧事实库：

8 天以外的晚到日志、事后账务修订，以及旧候选库缺少成员日语义 proof 时，不得通过关闭 facts 读取让页面回扫来源库。超级管理员在维护窗口调用：

```http
POST /usage/facts-repair
Content-Type: application/json

{"from":"2026-07-01","to":"2026-07-07","mode":"manual","confirm":"REPAIR_LOCAL_FACTS"}
```

- 日期是 CST、首尾均包含，只允许已完全闭合且位于当前已发布快照内的自然日；`manual` 单次最多 31 天。
- 接口只删除指定成员×小时的本地完成证明并回退本地成员游标；随后复用既有的串行回填，每 15 秒最多一小时、每批最多 200 人，不增加新的来源并发。整日 24 小时未全部重建前，旧的自洽日事实继续服务；页面不回源。
- `facts.repair_active/repair_*` 显示进度和失败时间。任务进行中、候选名单/窗口正在回填、范围越界或无完整发布快照时请求会被拒绝。
- 旧候选库若 `proof_migration_required=true`，先备份，再按状态返回的 `proof_migration_from/through` 使用 `mode=proof_migration`；该模式单次最多 366 天，并且仍从批准的本地来源重新计算，绝不能用现有日事实“自我证明”。完成后确认 `proof_migration_required=false` 和语义审计通过。

## 事实层纯本地验收

事实同步和来源 SQL 的验收必须叠加两份 Compose 文件，后者会把 `NEWAPI_LOG_DSN` 固定到隔离的 `mysql:8.4`，清空 NewAPI URL，并把 Monitor、Redis、MySQL 放入无外网出口的 internal 网络。覆盖会强制启用 v5 全历史协议（`SOURCE_MODE=complete`、固定本地 epoch、成员 source floor 与首次全员原子发布），不再用 90 天有限窗口代替本次上线语义：

```bash
docker volume create newapi-monitor-local-data
MONITOR_ACCEPTANCE_IMAGE=newapi-monitor:local-acceptance \
  docker compose \
    -f docker-compose.local-acceptance.yml \
    -f docker-compose.local-facts-acceptance.yml \
    up -d --force-recreate local-newapi-mysql redis monitor
```

表结构和只读账号由 [`../dev/local-facts-mysql-init.sql`](../dev/local-facts-mysql-init.sql) 初始化。只允许从宿主机 `127.0.0.1:13316` 通过 `local_loader` 导入合成或已批准的脱敏数据；不得传入线上 DSN、SSH 隧道或线上 API 地址。没有接近目标规模的数据集时，只能验读路径和代码正确性，不能判定来源库压力验收通过。

仓库内的 `dev/local-facts-loader` 会额外校验账号、库名、回环地址和显式确认串，只能重建 `newapi_local_acceptance`；`dev/local-facts-loadtest` 同样拒绝非回环 HTTP 地址，可执行矩阵边界、366 天查询、原始模糊查询、status 与在线备份的混合负载。`dev/local-facts-resource-sample.sh` 只接受 `nxmon-facts-*` 临时容器名和 `/private/tmp/newapi-monitor-facts-acceptance-*` 报告目录。目标规模合成集约 147 万 logs，MySQL tmpfs 本地实测需要至少 4 GiB。

注意：部分 Docker 实现会让仅连接 `internal: true` 网络的容器无法通过宿主机发布端口访问（当前 OrbStack 即如此）。这时不得为了跑压测而去掉 internal 隔离；应让验收客户端加入同一内部网络命名空间并请求其 `127.0.0.1:8090/8091`。在已加载合成数据、建立临时 Portal 账号且全员 `read_active=true` 后，可用以下 runner；它只接受合成 `@local.test` 账号、受限报告路径和本地测试 session secret：

```bash
mkdir -p /private/tmp/newapi-monitor-facts-acceptance-<run>
LOCAL_FACTS_PORTAL_EMAIL='synthetic-portal@local.test' \
LOCAL_FACTS_PORTAL_PASSWORD='synthetic-portal-pass' \
LOCAL_FACTS_LOADTEST_DURATION=10m \
LOCAL_FACTS_LOADTEST_REPORT=/private/tmp/newapi-monitor-facts-acceptance-<run>/load.json \
  dev/run-local-facts-loadtest.sh /private/tmp/newapi-monitor-local-synth-v5.env
```

runner 会把当前源码的 Linux/amd64 `local-facts-loadtest` 二进制临时复制到运行中的 `nxmon-facts-monitor-*` 容器 `/tmp`，只调用容器本身的回环 HTTP；结束时删除二进制，报告再复制回受限的 `/private/tmp` 路径。

本地验收至少覆盖：20,000/20,001 矩阵格边界且拒绝路径来源 SQL 为零；同键与不同键冷并发；256 MiB 内存限制下的 stats/matrix、后台同步、备份混合负载；90→366 天扩窗不重查已有 proof；整点滑窗不回退游标；事实行删除/篡改的语义审计与恢复；`facts.read_active=true` 后所有聚合页面来源 `logs` 查询计数为零。

## 本机连接生产只读源的真实效果验收

合成数据门禁通过后，如需在本机 `8100/8101` 查看真实业务分布，可叠加
[`../docker-compose.local-production-readonly.yml`](../docker-compose.local-production-readonly.yml)。这不是“纯本地验收”，也不是部署：候选代码、SQLite、备份和 Redis 仍全部在本机，仅通过回环 SSH 隧道读取生产 MySQL。

必须通过 [`../dev/run-local-production-readonly.sh`](../dev/run-local-production-readonly.sh) 操作。脚本会拒绝非 `nexus_ro`、非 `nexusapi`、非 `host.docker.internal:13316` 的 DSN；隧道只绑定 `127.0.0.1`，数据库探针只执行一条按不存在 ID 的 `SELECT`。线上密码仍只保存在受控 env-file，绝不能提交到仓库或打印到日志。

```bash
# 1. 建立隧道并验证 nexus_ro；不启动/重建容器。
dev/run-local-production-readonly.sh preflight

# 2. 预创建两个不同的外部卷（不要用 down -v）。
docker volume create newapi-monitor-local-data
docker volume create newapi-monitor-local-backup

# 3. 使用与生产候选完全相同的不可变 digest 重建本机 Monitor。
MONITOR_PROD_READONLY_IMAGE='ghcr.io/yl0711-coder/newapi-monitor@sha256:<candidate-digest>' \
  dev/run-local-production-readonly.sh up

# 4. 查看实际容器 image ID，并严格检查 /live 和 /ready；404 会直接失败。
dev/run-local-production-readonly.sh status
```

`build` 子命令只用于开发调试，其产出的可变本地 tag 不能执行 `up`。最终验收的 `up` 只接受 `repository@sha256:<64-hex>`，并在启动后比对容器实际 image ID。

该模式使用与线上相同的 `https://nexusapi.link` 做管理员登录身份校验和公开只读信息获取，但不持有 NewAPI 管理 Token；邮件、上游账户同步、基础设施探测和 Nginx 写入仍强制关闭，生产库只允许 `SELECT`。主采样保持 60 秒，来源 epoch 启动补缺按本地水位且最多 1 小时；facts 采用与生产一致的全历史 mode/epoch、30 秒 cold delay 和 20% duty。启动前必须提供同一份技术负责人签收的 `SOURCE_EPOCH`；不能再用 1→7→90→366 天有限窗口结果冒充全历史验收。

真实效果验收仍须遵守止损：先确认 `/usage/facts-status` 的完整性和 `read_active`，再打开聚合页面；记录来源 SQL、超时和扫描量。原始日志/CSV 仍直接读生产 `logs`，不得用高并发或长范围反复刷新。功能与数据路径可与拟上线版本一致，但本机 CPU、磁盘、网络和本地 Redis 不等同于生产资源，绝对耗时不能直接外推。该脚本直连 8100/8101，绕过生产 Caddy/TLS、可信代理、Secure Cookie、basic-auth、infra/告警和真实 upstream，因此只能签 facts/页面数据路径，不能签代理登录语义或生产外部告警；这些必须在最终环境另验。生产候选启动前还必须执行脚本 `stop`，确认本机容器、隧道和同名 MySQL advisory lease 均已释放。

## 一致性备份

### 迁移前强制成套快照

只要主库是文件 SQLite 且已有数据，每次启动在任何 GORM Migrator/`AutoMigrate` 前都会执行迁移闸门；它不受 `MONITOR_STORE_BACKUP_ENABLED` 影响：

1. 按 main、facts 固定顺序分别取得 `BEGIN IMMEDIATE`，两把锁都成功后才开始复制；活动写事务超过 5 秒会使启动失败。部署仍必须先停止旧容器，因为空闲进程不一定持锁。
2. 在锁内对源库执行 `quick_check`，并记录全部应用表的精确行数和 schema SHA-256；再通过只读 SQLite 连接执行 `VACUUM INTO`，因此已提交但尚未 checkpoint 的 WAL 数据也进入独立 `.db` 快照。
3. 对两份快照再次执行 `quick_check`、schema/逐表行数比对和文件 SHA-256。全部通过后才把 `manifest.json` 与其 SHA-256 `READY` 一起原子发布为 `/backup/pre-migrate-<UTC纳秒>/`。
4. 任一库、任一校验或 manifest 落盘失败都会删除未发布临时目录并拒绝启动；主库和 facts 库都不会进入迁移。首次安装时不存在旧文件，不会制造无意义的空快照；如果 facts 文件尚不存在，manifest 会明确记录 `present=false`。

manifest 还记录编译期 `migration_plan`。同一 plan 后续重启会完整复核并复用首次固定的旧 schema 快照，不会每次新增一套、更不会因保留策略把旧镜像所需的原始回滚点挤掉；只有代码显式升级 migration plan 后才固定新的成套快照。因此任何新增/调整 `AutoMigrate` 模型或迁移后转换时，发布负责人必须同步升级代码中的 plan ID，并把“旧 schema 快照仍被固定”纳入镜像验收。

成套迁移快照默认独立保留 3 份，由 `MONITOR_STORE_MIGRATION_BACKUP_RETENTION` 控制（最大 30）；它与在线日备份的 7 份保留互不混用。升级前必须在容器内执行（宿主的 `/data`、`/backup` 不是这两个卷）：`docker exec nexusapi-monitor sh -c 'du -sh /data/nexus_monitor.db* /data/usage-facts.db* /backup'` 和 `docker exec nexusapi-monitor df -h /data /backup`。备份卷可用空间至少应大于 `1.2 × (main.db + main WAL + facts.db + facts WAL)`，否则不要启动候选镜像，实际写满时闸门也会 fail-closed。

### 运行期在线备份

默认启用在线备份：启动 5 分钟后执行第一次，之后每 24 小时执行一个成套任务。任务在同一成员生命周期/facts 发布 barrier 下按 main→facts 执行 `VACUUM INTO`，两库都通过 `quick_check`、文件 SHA-256、active member revision 与 published signature 交叉核对后，最后才原子发布 `backup-set-<UTC纳秒>.json`。恢复时只允许选择通过 manifest 验证的成套文件，孤立 `.db` 不是可签收恢复点。默认保留 7 个备份集。任一备份、交叉核对或 manifest 失败都删除本次未发布文件，不会覆盖运行库或上一份有效备份。

生产 Compose 把该目录挂到独立 external backup volume，但“external named volume”不自动等于卷外灾备。发布门禁仍要求将已验证备份集加密复制到另一故障域/对象存储，使用受管密钥和不可变保留，并定期恢复到全新卷。未有 external backup age 与 restore-drill age 外部监控时，Monitor 的 `backup_set_verified=true` 只代表本地成套备份成功，不得宣称 DR 达标。

容量不能写死为旧的 8 GiB。上线前用 2 小时小样本和连续 24 小时混合 pilot 先校正 `rows/day`、SQLite `bytes/row`、WAL 峰值与 `VACUUM INTO` 时长；上线后连续 7 天每日重算容量和趋势，它是运行观察期，不得在第 1 天冒充“7 天已验收”。data 卷按 `(live main + facts + WAL峰值 + CSV/temp峰值) / 0.6` 起配；backup 卷单独按最多 7 组 runtime 双库、3 组 migration 双库、1 组临时双库和余量计算（若降低保留数必须显式签字）。32 GiB 只能作为首次 pilot 候选，不是容量签收。数据卷必须是有独立配额的 LV/块盘；普通 Docker named volume 若看到宿主整盘，百分比水位没有隔离意义。

超级管理员可调用 `POST /admin/store/backup` 异步触发同一套一致性备份。成功接收返回 202；已有任务运行返回 409；接口不会等待大事实库备份完成。必须继续观察两份 store 的 `backup_running`、`backup_set_verified`、`backup_set_success_at/failure_at`、单库成功/失败时间和字节数，不能只以 202 作为成功依据。开始前会按 `main + main WAL + facts + facts WAL + max(20%, 2 GiB)` 检查备份目标可用空间；不足则在 `VACUUM INTO` 前拒绝。磁盘满时临时文件会被清理，运行库和上一份有效备份应保持可读。

不要在 Monitor 正常写入时直接复制 `monitor.db` 或 `usage-facts.db`，否则可能遗漏 WAL 中的数据。

最稳妥的维护窗口方案是只短暂停止 Monitor 容器，然后备份整个数据卷：

```bash
docker compose --env-file .env.cluster -f docker-compose.monitor.yml stop monitor
DATA_VOLUME="${MONITOR_DATA_VOLUME:?set the inspected production data volume}"
ARCHIVE_DIR="${ARCHIVE_DIR:?set an off-volume archive directory}"
docker run --rm \
  -v "$DATA_VOLUME:/source:ro" \
  -v "$ARCHIVE_DIR:/archive" \
  alpine:3.23 \
  tar -C /source -czf "/archive/monitor-$(date +%Y%m%d-%H%M%S).tar.gz" .
docker compose --env-file .env.cluster -f docker-compose.monitor.yml start monitor
```

具名卷实际名称可能带 Compose 项目前缀，执行前用以下命令只读确认：

```bash
docker volume ls
docker inspect nexusapi-monitor --format '{{range .Mounts}}{{println .Name .Destination}}{{end}}'
```

运行期 main/facts 备份已在同一应用 barrier 中执行，只有两库都通过 hash/`quick_check`、成员 revision 和 published signature 交叉校验后才发布共同 `backup-set` manifest。这仍只是“本地成套恢复点”；生产 DR 另外要求把已验证备份集复制到实例外加密不可变存储，并定期恢复到全新卷。卷外备份年龄和最近恢复演练时间必须进入外部监控；未完成前不能把 `backup_set_verified=true` 写成“DR 已绿”。

## 恢复演练

候选 `/app/monitor` 二进制包含两个纯本地恢复子命令。它们都不加载应用配置、不读取 `NEWAPI_LOG_DSN`、不启动 HTTP/后台任务，并且只能写全新空卷。恢复先验 manifest/hash/`quick_check`与 main/facts 交叉签名，复制到隐藏暂存名后再验一次，最后才写 READY。目标非空、目标为符号链接、文件被篡改或任何校验失败都会拒绝，绝不覆盖。

### 运行期 backup-set 新卷恢复

日常灾备使用 `restore-backup-set`。来源必须是独立备份卷或已下载的卷外不可变备份，不是运行中的 `/data`：

```bash
set -euo pipefail
# 这是并行的零网络恢复演练，不切换生产卷，因此不要停止正在服务的Monitor。
BACKUP_VOLUME="${MONITOR_BACKUP_VOLUME:?set the inspected backup volume}"
restore_nonce="$(date -u +%Y%m%d%H%M%S)-$$"
RESTORE_DATA_VOLUME="monitor-runtime-restore-$restore_nonce"
CANDIDATE_IMAGE='ghcr.io/yl0711-coder/newapi-monitor@sha256:<candidate-digest>'
RUNTIME_MANIFEST='backup-set-<UTC-nanoseconds>.json'

case "$CANDIDATE_IMAGE" in *@sha256:????????????????????????????????????????????????????????????????) ;; *) exit 1 ;; esac
docker image inspect "$CANDIDATE_IMAGE" >/dev/null
docker volume inspect "$BACKUP_VOLUME" >/dev/null
if docker volume inspect "$RESTORE_DATA_VOLUME" >/dev/null 2>&1; then
  printf 'restore target already exists: %s\n' "$RESTORE_DATA_VOLUME" >&2
  exit 1
fi
docker volume create "$RESTORE_DATA_VOLUME" >/dev/null
docker run --rm --network none --entrypoint /app/monitor \
  -v "$BACKUP_VOLUME:/snapshot:ro" \
  -v "$RESTORE_DATA_VOLUME:/data" \
  "$CANDIDATE_IMAGE" \
  restore-backup-set \
  --manifest "/snapshot/$RUNTIME_MANIFEST" \
  --target-dir /data \
  --main-name nexus_monitor.db \
  --facts-name usage-facts.db \
  --confirm RESTORE_RUNTIME_BACKUP_SET
```

上述流程只在独立新卷和 `network none` 审计容器内运行，生产 Monitor 应继续服务。
只有真实接管/切换恢复卷时，才先把公网 Caddy 切到选择性维护配置，再优雅停止生产
Monitor、确认 source lease 释放，并把已验收的新卷交给目标镜像；不得把“恢复演练”
写成日常停机任务。

命令成功时 `/data/STORE_BACKUP_RESTORE_READY` 最后出现。用同一候选 digest 直接启动零网络审计容器；它会在任何迁移前复核 READY 并原子激活为 `STORE_BACKUP_RESTORE_ACTIVATED`：
审计必须从受控文件注入与备份来源相同的**现用 Session Secret**和固定的
**dedicated upstream credential secret**；不得使用假密钥。恢复库中若有旧上游
密文，启动会在恢复副本上完成事务换钥；任一密文无法验证都必须拒绝激活。

```bash
set -euo pipefail
: "${restore_nonce:?run this in the same controlled shell as the restore step}"
: "${RESTORE_DATA_VOLUME:?retain the freshly restored volume name}"
: "${CANDIDATE_IMAGE:?retain the verified candidate digest}"
RESTORE_SECRET_ENV="${RESTORE_SECRET_ENV:?set a root-readable 0600 env file outside the repository}"
RESTORE_SOURCE_EPOCH="${RESTORE_SOURCE_EPOCH:?copy the signed source epoch from the backup release record}"
RESTORE_AUDIT_BACKUP_VOLUME="monitor-restore-audit-backup-$restore_nonce"
audit_container="monitor-restore-audit-$restore_nonce"
test "$(stat -c %a "$RESTORE_SECRET_ENV")" = 600
if docker volume inspect "$RESTORE_AUDIT_BACKUP_VOLUME" >/dev/null 2>&1; then
  printf 'audit backup target already exists: %s\n' "$RESTORE_AUDIT_BACKUP_VOLUME" >&2
  exit 1
fi
if docker inspect "$audit_container" >/dev/null 2>&1; then
  printf 'audit container already exists: %s\n' "$audit_container" >&2
  exit 1
fi
test "$RESTORE_DATA_VOLUME" != "$RESTORE_AUDIT_BACKUP_VOLUME"
docker volume create "$RESTORE_AUDIT_BACKUP_VOLUME" >/dev/null
candidate_image_id=$(docker image inspect -f '{{.Id}}' "$CANDIDATE_IMAGE")
audit_container_created=false
cleanup_restore_audit() {
  status=$?
  if [ "$audit_container_created" = true ]; then
    docker stop -t 40 "$audit_container" >/dev/null 2>&1 || true
    docker rm "$audit_container" >/dev/null 2>&1 || true
  fi
  return "$status"
}
trap 'cleanup_restore_audit' EXIT
trap 'exit 130' HUP INT TERM
docker run -d --name "$audit_container" --network none \
  -v "$RESTORE_DATA_VOLUME:/data" \
  -v "$RESTORE_AUDIT_BACKUP_VOLUME:/backup" \
  --env-file "$RESTORE_SECRET_ENV" \
  -e 'NEWAPI_LOG_DSN=nexus_ro:disabled@tcp(127.0.0.1:1)/nexusapi' \
  -e MONITOR_NEWAPI_BASE_URL=https://invalid.local \
  -e MONITOR_ADDR=:8090 \
  -e MONITOR_PORTAL_ADDR=:8091 \
  -e MONITOR_STORE_PATH=/data/nexus_monitor.db \
  -e MONITOR_USAGE_FACTS_STORE_PATH=/data/usage-facts.db \
  -e MONITOR_STORE_BACKUP_DIR=/backup \
  -e MONITOR_SOURCE_WORKER_ENABLED=false \
  -e MONITOR_SOURCE_LEASE_REQUIRED=false \
  -e MONITOR_LOCAL_SNAPSHOT_ONLY=true \
  -e MONITOR_USAGE_FACTS_ENABLED=true \
  -e MONITOR_USAGE_FACTS_READ_ENABLED=true \
  -e MONITOR_USAGE_FACTS_FULL_HISTORY_ENABLED=true \
  -e MONITOR_USAGE_FACTS_LOCAL_READ_ONLY=true \
  -e MONITOR_USAGE_FACTS_HISTORY_SOURCE_MODE=complete \
  -e MONITOR_USAGE_FACTS_HISTORY_SOURCE_EPOCH="$RESTORE_SOURCE_EPOCH" \
  -e MONITOR_UPSTREAM_SYNC_ENABLED=false \
  -e MONITOR_UPSTREAM_USAGE_SYNC_ENABLED=false \
  -e MONITOR_STABILITY_ENABLED=false \
  -e MONITOR_INFRA_ENABLED=false \
  -e MONITOR_STORE_BACKUP_ENABLED=false \
  "$CANDIDATE_IMAGE"
audit_container_created=true
audit_json=$(docker inspect "$audit_container")
test "$(printf '%s' "$audit_json" | jq -er '.[0].Image')" = "$candidate_image_id"
test "$(printf '%s' "$audit_json" | jq -er '.[0].HostConfig.NetworkMode')" = none
test "$(printf '%s' "$audit_json" | jq -er '.[0].Mounts[] | select(.Destination == "/data") | .Name')" = "$RESTORE_DATA_VOLUME"
test "$(printf '%s' "$audit_json" | jq -er '.[0].Mounts[] | select(.Destination == "/backup") | .Name')" = "$RESTORE_AUDIT_BACKUP_VOLUME"
live_verified=false
for attempt in $(seq 1 120); do
  if docker exec "$audit_container" wget -qO- http://127.0.0.1:8090/live >/dev/null 2>&1; then
    live_verified=true
    break
  fi
  audit_json=$(docker inspect "$audit_container")
  printf '%s' "$audit_json" | jq -e \
    '.[0].State.Running == true and .[0].State.OOMKilled == false and .[0].RestartCount == 0' >/dev/null
  sleep 5
done
test "$live_verified" = true
ready_verified=false
for attempt in $(seq 1 120); do
  audit_ready=$(docker exec "$audit_container" wget -qO- http://127.0.0.1:8090/ready 2>/dev/null || true)
  if printf '%s' "$audit_ready" | jq -e '
    .status != "not_ready" and .store.ok == true and .facts_store.ok == true and
    .source.state == "disabled" and .source.worker_enabled == false and
    .source.lease_required == false and .source.lease_held == false and
    ([.degraded_reasons[]? | select(. == "facts_not_published" or
      . == "local_store_unavailable" or . == "facts_store_unavailable")] | length) == 0
  ' >/dev/null 2>&1; then
    ready_verified=true
    break
  fi
  test "$(docker inspect -f '{{.State.Running}}' "$audit_container")" = true
  sleep 5
done
test "$ready_verified" = true
printf '%s\n' "$audit_ready" | jq '{status,store,facts_store,source,degraded_reasons}'
docker logs "$audit_container"
docker stop -t 40 "$audit_container"
audit_json=$(docker inspect "$audit_container")
printf '%s' "$audit_json" | jq -e \
  '.[0].State.Running == false and .[0].State.ExitCode == 0 and .[0].State.OOMKilled == false and .[0].RestartCount == 0'
docker rm "$audit_container"
audit_container_created=false
trap - EXIT HUP INT TERM
unset audit_json audit_ready candidate_image_id live_verified ready_verified
```

`RESTORE_SECRET_ENV` 只允许包含受控的 `MONITOR_SESSION_SECRET` 和
`MONITOR_UPSTREAM_CREDENTIAL_SECRET`，用完按密钥管理策略销毁；发布日志不记录文件内容。
`RESTORE_AUDIT_BACKUP_VOLUME` 是本次审计的独立可写卷：即使关闭运行期备份，
启动迁移闸仍会生成紧邻激活前的双库快照。开始前按两库+WAL+20%余量验容；
不得把只读恢复来源卷或 `/data` 同时当写入目标。审计证据归档前保留该卷。

审计容器名和两个目标卷每次都必须全新且唯一；`docker volume create` 对旧卷会静默
复用，因此命令先用 `inspect` 明确拒绝残留对象。失败 trap 只停止/删除本次创建的
审计容器，绝不删除证据卷；修复后必须另建新卷重演，不能复用失败卷。

容器必须先经 40 秒优雅停止且正常退出，不得对已验收的 SQLite/WAL
直接 `rm -f`强杀。停止后再以只读工具核对两库 `quick_check`、成员 ACL、
published revision/fingerprint/generation 和 Portal 聚合；日志必须证明
没有来源连接。上述任一步失败都丢弃恢复目标卷，不能在原卷上手修后
冒充演练成功；只有失败且明确丢弃整卷时才允许强制删容器。

### 迁移前快照回滚

schema 回滚使用 `restore-pre-migration`。`CANDIDATE_IMAGE` 只负责校验/恢复，最终启动必须是与该快照配对的 `OLD_IMAGE`：

```bash
docker compose --env-file .env.cluster -f docker-compose.monitor.yml stop monitor
test "$(docker inspect -f '{{.State.Running}}' nexusapi-monitor)" = false

OLD_BACKUP_VOLUME="${MONITOR_BACKUP_VOLUME:?set the inspected backup volume}"
RESTORE_DATA_VOLUME="monitor-rollback-$(date +%Y%m%d%H%M%S)"
CANDIDATE_IMAGE='ghcr.io/yl0711-coder/newapi-monitor@sha256:<candidate-digest>'
OLD_IMAGE='ghcr.io/yl0711-coder/newapi-monitor@sha256:<old-digest>'
SNAPSHOT='pre-migrate-<UTC-nanoseconds>'

docker volume create "$RESTORE_DATA_VOLUME"
docker run --rm --network none --entrypoint /app/monitor \
  -v "$OLD_BACKUP_VOLUME:/snapshot:ro" \
  -v "$RESTORE_DATA_VOLUME:/data" \
  "$CANDIDATE_IMAGE" \
  restore-pre-migration \
  --snapshot "/snapshot/$SNAPSHOT" \
  --target-dir /data \
  --confirm RESTORE_PRE_MIGRATION_SNAPSHOT
```

本地目录演练可执行 `dev/restore-pre-migration.sh SNAPSHOT_DIR EMPTY_TARGET_DIR`；目标目录可以不存在，但若已存在必须完全为空。

恢复命令必须完整成功并由候选镜像再次核对双库/manifest/READY 后，才可在受控 Compose override 中同时把 `image` 固定为 `$OLD_IMAGE`、把 `/data` 卷改为 `$RESTORE_DATA_VOLUME`。旧制品可能没有 `/live`、`/ready`，此时只能用其既有 `/health` 加登录 ACL、倍率版本、历史趋势和 facts 功能探针签收，不能把404写成健康失败。迁移前恢复的半完成标记不被旧镜像识别，因此任何恢复命令非零、READY缺失或双库复核未完成时，严禁启动旧镜像。验收完成前保留原迁移后数据卷；**不得把旧镜像直接指向已经执行过新 schema 迁移的原卷。**

## 功能降级与回滚

若稳定性新功能异常，但原模型监控、用户用量和服务端监控仍需继续使用：

```dotenv
MONITOR_STABILITY_ENABLED=false
```

修改后只重建 `monitor` 容器。该开关停止稳定性长期汇总和原始错误采集，不删除已有表和历史数据。

镜像回滚必须是“旧镜像 + 对应迁移前成套快照”：停止并确认候选容器退出，按上一节把 snapshot 恢复到新卷，再让不可变旧镜像只挂载该新卷。禁止仅修改镜像标签后复用已迁移数据卷；原卷不删除，留作问题分析和前滚恢复。最后验证用户用量、模型监控、服务端监控和登录。

## 日常观测

- 主采样超过 3 个采样周期没有成功：按降级处理并检查只读数据库连通性。
- 原始问题采集出现积压：页面问题排行只包含已完整分钟；采集器会按固定预算续跑，不应通过提高单次上限盲目加压生产库。
- 原始错误签名延迟约 10 分钟定稿，用于容纳 360 秒长请求和日志落库抖动；这不影响稳定性主报表和原模型监控的新鲜度。
- 监控数据卷持续增长：检查问题签名数量、保留天数和备份是否成功，不直接删除 SQLite 文件。
- 自动备份 5 分钟后仍无成功时间，或最后失败时间晚于最后成功时间：先检查独立备份卷可用空间和 `/backup` 权限；不要在运行库上执行修复命令。
- 90 天报表采用首屏轻量、分组详情按需加载；若接口变慢，先检查本地 SQLite 和容器内存，不应在页面请求中增加生产库查询。
