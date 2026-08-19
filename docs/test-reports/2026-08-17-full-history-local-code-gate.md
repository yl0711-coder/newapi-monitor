# 2026-08-17 全历史 Usage / Stability v5 本地代码门禁

## 结论

当前冻结 Go 工作树的本地代码门禁通过：Go 全仓 ordinary/race、
`go vet`、`golangci-lint`、gofmt 和 diff-check 全绿。独立 QA 从最终共享树重建
fresh 快照，与共享树全部 Go 文件 checksum 0 差异。门禁覆盖全历史持久
任务、发布签名/水位 fence、逐小时核验降档、成员级修复撤权、Stability
实时/冷迁移双水位、上游凭据事务换钥、v11 迁移前回滚点和 runtime
backup-set 新卷恢复激活。

这是**本地/假来源代码与容器验收**，不是生产上线签字。本次没有
访问真实 NewAPI 来源，没有对生产卷或容器执行写入。已构建本地候选
镜像并完成零网络空卷冒烟，但本地 tag/image ID 不是 registry 的不可变
RepoDigest，不能代替最终生产制品签收。

## 基线

- 日期：2026-08-17（Asia/Shanghai）。
- 分支：`main`；基础 commit `0079433b18c33527e9084aedc88aec2739c6b761`。
- 验收对象包含尚未提交的工作树变更；发布前必须提交、构建不可变
  RepoDigest，并比对下述 Go 内容签名。
- 迁移计划 ID：`main-facts-schema-20260817-v11`。
- Go 文件内容清单 SHA-256：
  `bfe8bf7fd24d18a80cc006c3f991858e507015f4ac32d804eead0ad31200eb23`。
- 独立完整快照内容清单 SHA-256：
  `a272d17204e1f974b8f80b792bec0dcabb51e694aa66877b37ade15cd8654246`。

## 执行结果

- `go test ./... -count=1`：通过（Monitor 19.729s）。
- `go test -race ./... -count=1`：通过（Monitor 140.489s）。
- 恢复/v11/换钥 5 项组合 ordinary 20 轮、race 10 轮：通过。
- `go vet ./...`、`golangci-lint run ./...`：通过，lint 0 告警。
- `gofmt -l`：空；`git diff --check`：通过。
- 实际生产 Compose 的 Phase A + maintenance 和 Phase B + live 均通过
  `monitor-release-preflight.sh --config-only`，两份 Caddyfile 均通过固定 Caddy
  digest 的 `caddy validate`。
- 本地镜像 `newapi-monitor:tomorrow-candidate-local-final`（本地 image ID
  `sha256:68615c95f61ecd6cdb09386e4f9249b534d8f6eab28b147a34adf6ffe3e3f6ca`）冒烟：
  默认 UID/GID 1000、只读根文件系统、独立 data/backup 卷、`network=none`、
  `/live=200`、空卷 `/ready=degraded/facts_not_published`、无 OOM/重启，SIGTERM
  优雅退出码 0。冒烟容器和临时卷已删除。

## 本轮重点锁定

- 来源日查询命中 20k 行上限、3024 或控制竞态时，持久降为逐小时低优先级核验；不把“未在 5 秒内完成”当成“已证明错数”，已签成员在核验结论出来前保持服务。
- 小时核验到日边界后使用独立 day control，在单个 SQLite 事务中重建 daily/proof、清理 staging 并返回滚动审计周期。
- prune 不删除活跃 `repair_hour` / `source_audit_hour` / Tail 的未签日 staging，避免跨 8 天或重启后无法恢复。
- readiness 重开前重读持久发布签名并比较 `ServingGeneration` 及左/右水位，过期 goroutine 不能把新世代覆盖回旧范围。
- Stability raw-problem 迁移即使关闭执行开关也保留持久 `paused_disabled` 缺口，`/health` 与 `/ready` 都保持 degraded，冷迁移不能再被实时 Tail 的成功水位掩盖。
- `/ready` 要求运行期 main/facts 成对备份 manifest 已校验；单库备份成功而成套快照失败、缺失、未校验或过期都会明确降级。
- facts 卷在启动本地探针即采样 60/70/80/85% 水位；80% 停 cold/verify/backup，85% 或无法读取挂载时同时阻断独立高优先 Tail 与资料快照写入，旧发布仍保持只读。
- 分类/查询语义重置会在同一事务取消成员的旧 repair/local/source-audit 租约；最后一名失效发布成员被撤销时，成员表、指纹、左右水位和发布时间一并清空并切换世代。
- 多年单用户单令牌查询已补 `(user_id, token_id, date_ts)` 索引，并用 SQLite `EXPLAIN QUERY PLAN` 锁定实际采用该索引。
- Portal 授权快照同时 fence 发布指纹、ServingGeneration 和发布范围；事实模式未 ready 时统一 fail-closed，不回源、不泄漏 partial/rejoin 成员。
- 生产 Compose 要求预创建的 external 数据/备份卷，且显式提供 full-history mode/epoch/duty 与分类迁移开关；中英文 README 已同步 fail-closed 及备份语义。

## 仍为生产 NO-GO 的外部门禁

1. **来源完整性**：`SOURCE_MODE=complete` 和 `SOURCE_EPOCH` 只是声明。
   本项目由技术负责人承担 DBA 职责，必须在目标环境用只读 SQL、归档/保留
   策略和索引证据签收 hot `logs` 自每个 active 用户注册日起无缺口。
2. **最终制品/配置**：当前仓库没有真实 `.env.cluster`，尚未得到提交后的
   registry RepoDigest、最终 Compose config hash、生产卷名和密钥延续证据。
3. **卷外灾备**：应用层 runtime backup-set 和恢复协议已在代码中闭环；
   仍需在真实大库上完成独立配额卷、卷外加密不可变副本、全新卷
   zero-source 容器恢复，并落实控制面 RPO ≤5 分钟的外部快照/增量方案。
4. **来源压力 pilot**：需对最终 digest/config 先跑 2 小时小样本，再连续
   24 小时混合负载，记录 RSS/OOM/restart/WAL/卷水位、来源 CPU/连接/
   复制延迟/NewAPI p95 和 signed member-days/s。
5. **切流正确性**：必须等全体 tracked 成员 published、`facts.read_active=true`、
   发布左右水位与独立对账通过，并在真实 Caddy/TLS 下验证 Portal/ACL/
   XFF 限流。全历史实际 ETA 超过 24 小时时，仍只能继续影子。

以上五项没有完成前，客户切流结论必须保持 **NO-GO**；可以继续发布
隔离候选/影子并收集 pilot 证据。上线后 7 天是强制观察期，不是明日
首发的前置等待项；但连续 24 小时证据不能伪造或压缩。

后续逐项执行见 [`2026-08-17-production-validation-checklist.md`](2026-08-17-production-validation-checklist.md)。
