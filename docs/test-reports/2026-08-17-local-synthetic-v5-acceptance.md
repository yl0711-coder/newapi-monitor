# 2026-08-17 本机合成 v5 全历史 facts 验收

## 范围与隔离

- 候选镜像：`ghcr.io/yl0711-coder/newapi-monitor@sha256:759a8116bc3399315b9cdd6c9e261f358bea30220edb36e8368d869b913c7284`
- OCI revision：`d4f272c907782c147b42278deab425fb001883ea`
- 运行平台：`linux/amd64`；本机为 Apple Silicon，因此 Docker 使用受控模拟执行。
- 仅使用 Docker `internal` 网络、tmpfs MySQL、临时 Redis、独立的 data/backup Docker 卷；没有 SSH 隧道、线上 DSN、线上 NewAPI URL 或生产容器。
- 合成来源：2 名用户、144 条历史 type=2 日志，后续再注入 1 条尾小时日志；所有账号、模型、请求 ID 均为 `synthetic-*` / `local-*`。

## 本轮发现并修复的本地验收配置缺口

旧的合成 MySQL 初始化脚本无法通过当前候选的真实来源预检：缺 `options` 表，`users` 缺 `created_at`，`logs` 缺全历史边界查询强制使用的 `idx_user_created_type`。同时 facts 覆盖仍是旧的 90 天语义，未开启 full-history source mode/epoch。

本轮已修改本地验收资产：

- `dev/local-facts-mysql-init.sql`：补齐 `options`、用户注册边界和索引；
- `dev/local-facts-loader/main.go`：为合成用户写入与日志范围一致的注册时间；
- `docker-compose.local-facts-acceptance.yml`：强制 full-history complete source mode、固定本地 epoch、无墙钟节流的隔离测试配置、`linux/amd64`、失败不重启；
- `docs/monitor-operations.md`：说明该覆盖不是 90 天兼容验收。

## 结果

| 验收项 | 结果 |
| --- | --- |
| 候选启动与真实来源 schema/index preflight | 通过 |
| 来源租约 | `ready`、worker running、lease held |
| 首次发布 | 两名 tracked 成员均完成后才一次性变为 2/2 published |
| 全历史成员签名 | 两人各 92/92 小时 complete + verified；无失败/退避 |
| 读面 | `read_active=true`、`snapshot_usable=true`、`semantic_audit_ok=true` |
| 管理 matrix/stats | 2 名成员、3 天、6 个 facts cells；没有 source fallback |
| Followups 覆盖闸 | 仅 3/30 天时返回不可用说明，不产生“30 天无消费”误判 |
| 尾同步 | 注入 quota=4321 的新日志后，右水位推进 20:00→21:00，日汇总包含该值，未产生同步失败 |
| 重启 checkpoint | graceful stop（exit 0）后重启仍 `read_active=true`，发布成员和矩阵不丢失 |
| Portal | 组账号可读 overview、成员详情、日志；仅见本组合成成员 |
| runtime backup-set | 双 store `backup_set_verified=true`，产出 main/facts manifest |
| 新卷恢复 | `restore-backup-set` 在 `--network none` 新卷成功；READY→ACTIVATED 后 source disabled 仍可读同一 facts snapshot |

本轮候选启动瞬间记录过一次 `SQLITE_BUSY` 的近期 Tail 警告；随后 `last_fact_failure_at=0`、无连续告警、Tail 正常推进。它没有造成 read gate 打开、数据丢失或持久失败，但应继续在最终 2h/24h pilot 观察 SQLite WAL/锁等待。

## 工具限制（未作为产品通过项）

当前 OrbStack 对仅接入 `internal` 网络的容器不提供宿主回环端口转发。因此 `dev/local-facts-loadtest` 的宿主 `127.0.0.1` 模式不能直接用于这套隔离栈；本报告的 HTTP 验证从 Monitor 容器网络命名空间执行。需要新增专用 network-namespace loadtest runner 后，才能签 20,000 格和 256 MiB 的长时间合成负载报告。

## 未替代的上线门禁

本机合成验收只证明协议、恢复和读面在隔离数据上成立；不替代生产 complete-source 签字、真实 MySQL `EXPLAIN`、容量/卷外恢复、Caddy/TLS/XFF、真实 2h + 连续 24h pilot 和最终不可变镜像/配置签收。
