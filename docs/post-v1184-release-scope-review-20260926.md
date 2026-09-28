# v1.18.4 后本地优化：发布范围收敛

## 2026-09-27 提交范围确认

用户已确认将下述 A、B、C 三组已验收修改一起提交并打新 tag 触发 CI；本次计划版本为 `v1.18.5`，基于 `19fde5c`，不合入 main、不部署。为保持与整体验收产物一致，本次以一个明确范围的修复提交交付，不再临时拆分共享入口。

文档白名单为本轮六份 `post-v1184-*20260926.md`，另加 `post-v1184-full-preflight-20260927.md`，共七份。完整竞态分片、安全扫描、实际镜像以及基线报表对照结果以 2026-09-27 完整验收记录为准；下文是各阶段的历史记录。

## 当前结论与操作边界

基线为 `19fde5c`（v1.18.4），开发分支为 `feature/finance-report-performance`。
本轮完成本地复核与发布清单，不提交、不打 tag、不合 main、不访问或调整生产。
未发现需要新增产品修复的阻断项；这不等于无 bug，也不等于最终候选镜像已通过发布门禁。

此前四轮的全量测试、金额对照及限资源 Docker 并发验收通过。当前可以准备显式范围的候选提交；必须对实际提交产物跑 CI，不能用含未跟踪源码的工作树测试替代提交验收。

## 已完成的范围

| 逻辑组 | 用户可见结果 | 不包含的能力 |
| --- | --- | --- |
| A：经营核算恢复、正确性及读取优化 | 有条件展示已校验升级前快照；失败状态保留至成功；历史小时修订使缓存失效；赠送证据分批读取与有界缓存；成本关联查询加速 | 不补造金额，不完成所有历史回填，不改采集频率、不开发四维报表 |
| B：上游余额口径 | 自有 modelapi.link 不计入余额汇总，明细仍保留 | 不改成本、充值、利润，不排除停止使用供应商的实际余额 |
| C：模型统计覆盖标识 | 分清正常实时尾段、历史缺口及两条来源的覆盖证明 | 不修复分钟事实积压，不新增历史拒绝日志覆盖台账 |

A 中共享的成本查询同时被渠道管理使用，必须一起回归，不能只验经营核算。B、C 是此前已做的明确修复，不是本轮新增需求；建议按逻辑分提交，最终 tag 包含哪些提交仍需发布时确认。若只发布 A，应在独立候选中拆分共享前端资源版本及测试，再复验，不能直接丢弃开发分支里的 B、C。

## 候选文件白名单

### A：产品与回归

```text
monitor/channel_economics_current_query.go
monitor/channel_economics_current_query_test.go
monitor/channel_economics_report.go
monitor/finance.js
monitor/finance_facts_reader_test.go
monitor/finance_fast_snapshot.go
monitor/finance_gift_allocation.go
monitor/finance_gift_boundary_read.go
monitor/finance_gift_cache_guard.go
monitor/finance_gift_evidence_cache.go
monitor/finance_gift_evidence_cache_test.go
monitor/finance_report.go
monitor/finance_report_cache.go
monitor/finance_report_coverage_revision_test.go
monitor/finance_report_hour_revision.go
monitor/finance_report_queue.go
monitor/finance_report_queue_test.go
monitor/finance_report_snapshot.go
monitor/finance_report_source_version.go
monitor/finance_report_test.go
monitor/finance_report_upgrade_snapshot.go
monitor/finance_report_upgrade_snapshot_test.go
monitor/monitor.go
monitor/sync_status_ui_test.go
dev/tests/finance-refresh.test.mjs
```

### B、C：产品与回归

```text
monitor/channel_management.js
monitor/channel_management_test.go
monitor/channel_upstream.go
monitor/channel_balance_policy_test.go
monitor/model_statistics.go
monitor/model_statistics.js
monitor/model_statistics_coverage.go
monitor/model_statistics_coverage_test.go
dev/tests/coverage-balance.test.mjs
```

### 共享入口及本轮记录

`monitor/page.html` 同时包含三组资源版本及同步状态 UI 变更，分批发布时必须按差异拆分。
文档仅包含本次 `docs/post-v1184-*20260926.md` 六份记录（包括本文件）。

明确排除：

- 已存在的 `dev/ecs_acceptance_run.py` 改动，保留、不回滚。
- 其他日期的未跟踪文档及临时工具，不使用 `git add .`。
- `local-data/`、SQLite、备份、真实报表 JSON、凭据及私有验收目录。
- NewAPI、AWS/IAM/ECS、部署配置、采集器、依赖升级和数据库结构变更。

本轮 `go.mod`、`go.sum`、Dockerfile、CI、deploy、cmd、internal 没有差异。已有 `.gitignore` 覆盖整个 `local-data/`，已验证普通文本与数据库路径均被忽略；Docker 构建上下文默认拒绝，仅放行生产源码和页面，不放行 dev、文档或数据库。

## 2026-09-26 补充门禁

- `git diff --check`：通过。
- 前端 Node 测试：110 通过、1 个既有产物条件跳过、0 失败。
- Linux 依赖策略：662 个生产/测试依赖包通过，不引用已停止维护的 OpenPGP 包。首次沙箱内 go list 失败不计作通过，取得本机缓存读取权限后重跑通过。
- CI 分片覆盖：1,918 项，五片分别 431 / 397 / 417 / 402 / 271；这是分片完整性检查，不代表本次重跑全量 race。
- Go 漏洞扫描：本机和 Linux/amd64（CGO=0）目标均未发现可达或已导入包漏洞；模块级 GO-2026-5932 指向未引用的 `golang.org/x/crypto/openpgp`，未豁免或屏蔽告警。Linux 检查使用本机编译的扫描器分析目标源码；首次直接跨平台 go run 因执行格式不兼容失败，不计作扫描通过，改用正确方式重跑通过。
- 最近全量 Go 测试：第四轮 257.591 秒通过；关键 race、vet、金额逐项对照及 Docker 验收见四轮记录。

正式提交后的 CI 必须继续通过全量 race 分片、密钥扫描、构建上下文探针及实际 Linux 镜像漏洞扫描。不能把当前 Go 源码扫描等同于 Alpine 运行镜像无漏洞。

## 保留风险与已知限制

1. 赠送证据缓存额外保留一个本地 SQLite 只读连接用于 `data_version`，不连接生产业务数据库、不持有长期事务。事实库任意提交都会使旧缓存失效，持续回填时收益会降低；读取或版本校验失败时不复用证据，不允许因此接受错误金额。
2. 升级前快照只作明确标记的旧结果展示，受版本、配置、时间及哈希限制；它不代表当前完整金额，也不保证任意老版本均可回退展示。
3. 成本查询固定使用既有 `idx_channel_economics_hour_publications_hour_ts`。正常建库已有该索引且回归验证通过；缺失或更名会使查询报错。未来结构调整必须同步维护查询及计划回归，不应吞错回退到未经验证的金额。
4. 队列失败摘要为进程内有界状态，重启不保证保留；本轮不是持久审计系统。
5. 本地 65 秒负载包含缓存请求、冷重算及模拟短写事务，不能替代真实上游同步 SQL 的整日压力测试。观察到主库累计连接等待，不能宣传 SQLite 压力已彻底消除。
6. 历史账单缺失、充值折扣证明不足、分钟采集落后及跨日拒绝覆盖仍需独立跟进。本轮没有访问线上，不提供新的完成百分比或预计回填完成时间。

## 发布与回滚要求（尚未执行）

1. 按确定范围形成候选提交，核对所有新增源码已跟踪、排除项未暂存；对提交本身复验。
2. 用户确认后才打新 tag、等待 CI；不移动已有 tag，不先合 main。
3. 上线前核对实际生产镜像及配置并做可恢复备份；只更新 Monitor，不开新生产开关、不改回填频率。
4. 验证缓存展示与异步更新、赠送验证无超时回归、渠道成本查询、余额口径及模型覆盖 UI（以所选发布范围为准）。历史不完整状态不能当发布故障自动抹去。
5. 本轮无新 schema；需要回滚时恢复上一个明确版本镜像与原配置，通常不需还原数据库。上线后的正常采集数据不应因回滚镜像而被覆盖；若出现实际数据损坏需另行评估，不能盲目恢复旧库。
