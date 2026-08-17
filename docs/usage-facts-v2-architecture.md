# Monitor / Usage 全历史事实控制层与派生数据恢复架构

- 文档状态：**Draft v2，已按预付费业务和当前分组语义修正；P0-R 已实现并通过真实只读数据闭环，P0-GA 未开发完成；未授权据此上线**
- 设计日期：2026-08-16（Asia/Shanghai）
- 适用范围：Monitor 管理端用户用量、Usage Portal、公司分组、事实同步、完整性审计、修复、备份恢复；
  并冻结渠道管理/稳定性派生数据的存储与可重建边界
- 不在范围：修改 NewAPI 代码或业务写入、替代 NewAPI 账务系统、无界原始日志查询
- 前置约束：NewAPI 只提供只读 `logs/users/tokens`；Monitor 不写来源库

## 1. 背景与问题

现有候选实现把事实同步定义成 90/366 天滚动窗口。`TrackedUser.GroupID`
是 Monitor 当前的组织与 Portal 访问范围，不是历史财务归属；用户是在 NewAPI
预先充值后消费，Monitor/Usage 不决定应收、扣款或结算。该候选实现可以降低
短期聚合查询压力，但不能满足“用户全部历史用量、成员变更可追溯、
展示数据可发现并可修复”的正式要求：

1. `usageFactBackfillDays()` 将历史硬限制为最多 366 天，事实清理最多保留 732 天；
2. 成员删除是物理删除当前名单，没有可续用的停用水位和操作审计；
3. 重新加入只会重扫固定窗口，无法精确补齐停用期间；
4. 纠正所属公司仅覆盖当前 `GroupID`，缺少轻量审计和缓存/权限一致性保证；
5. 本地 proof/hash 能发现部分逻辑损坏，但不能证明同步结果与来源一致；
6. 当前受控 repair 仅允许已发布窗口，无法修复候选快照缺口；
7. 旧候选快照缺少 2026-08-01 的 36 条成员日 proof；P0-R 已将该缺口转为
   持久精确修复任务，通过中途重启续跑、全范围复审和 90 天原子发布验收。

本设计不重写已经通过本地验收的 facts/query/cache 引擎，而是在现有小时事实、日事实、
proof、候选/服务版和缓存上增加全历史控制层：每用户真实来源边界、持久化 job/repair、
独立来源对账和统一来源调度。

## 2. 设计目标与非目标

### 2.1 目标

1. 对当前被管理用户，覆盖来源 `logs` 中仍可取得的全部用户消费/退款日志；
2. Monitor 与 Usage 对同一事实、同一时间、同一权限范围返回一致数字；
3. 新增、删除、重新加入均不丢数据、不重计；纠正所属公司不改写事实，只改变当前公司汇总和查看范围；
4. 页面聚合只读本地事实库，任何事实异常都不得回退成来源 `logs` 宽扫；
5. 能区分真实零值、尚未采集、部分覆盖、来源不可用、本地损坏和修复中；
6. 能发现 SQLite 结构损坏、本地业务行缺失/篡改、同步语义错误和来源晚到/修订；
7. 修复任务精确到用户与自然日/小时，可续跑、可审计、可限速、可人工批准；
8. 首次全历史导入、稳态 Tail、来源复核和人工修复共享来源预算，不形成负载突刺；
9. 新控制层与现有 serving 快照兼容，切换和回滚不需要重建或修改 NewAPI。

### 2.2 非目标

1. 控制层不保存请求内容、完整 Token、密钥或原始日志；
2. 控制层不允许网页执行无界原始日志、模糊搜索或 CSV 导出；
3. 控制层不参与充值、扣额或决定客户可用额度；NewAPI 的预付费账户状态仍是唯一权威；
4. 控制层不承诺恢复来源已经永久清理且没有任何备份/档案的记录；
5. 控制层不通过提高后台并发来缩短首次导入时间；
6. 用户用量恢复不夹带重写稳定性、渠道余额或 Nginx 采集模块；跨域只统一数据等级、归档和恢复协议。

## 3. 必须冻结的数据口径

### 3.1 来源事实

用户用量来源集合定义为：

```text
logs.user_id = 目标用户
AND logs.type IN (2, 6)
AND traffic_class = user
AND created_at ∈ [from, to)
```

- `type=2`：用户消费记录；
- `type=6`：退款记录；
- NewAPI 内部渠道测试流量由 Monitor 当前分类规则排除，并记录
  `classification_version`；
- 时间区间全部使用 CST（UTC+8）自然日，左闭右开；
- quota、Token、请求数全部使用整数，禁止以浮点数保存或累计；
- `logs.group` 是 NewAPI 网站分组维度，命名为 `website_group`；
- `CustomerGroup.ID` 是 Monitor 本地当前客户组织与 Portal 访问维度，命名为 `customer_group_id`；
- 两种“分组”不得复用字段、表或查询条件。

事实字段至少守恒：

```text
消费请求数 = COUNT(type=2)
退款记录数 = COUNT(type=6)
输入 Tokens = SUM(type=2.prompt_tokens)
输出 Tokens = SUM(type=2.completion_tokens)
消费 quota = SUM(type=2.quota)
退款 quota = SUM(type=6.quota)
净消费 quota = 消费 quota - 退款 quota
```

如果现有 UI 对退款符号另有展示约定，只能在展示层转换，事实层仍分别保存消费和退款。

### 3.2 “全部历史”的定义

“全部历史”定义为：该用户在逻辑用量来源中符合上述分类规则的全部记录。
当前逻辑来源是 NewAPI MySQL `logs`；未来旧日志迁往档案存储后，逻辑来源由
“热 MySQL + 冷档案”组成，业务定义仍是全历史，不得把“已移出热库”当成“无数据”。
每个用户单独保存：

- `source_floor_hour`：首次边界探测时最早来源日志所属小时；
- `source_ceiling_hour`：探测时最后来源日志后的第一个小时；
- `coverage_through`：从 `source_floor_hour` 开始已连续验证完成的水位；
- `source_floor_checked_at`：最近一次确认来源左边界的时间；
- `source_segments`：逻辑时间段对应的权威存储层、manifest/checksum 和覆盖水位；
- `source_history_status`：`complete/archive_unavailable/boundary_unknown`，不得以零值代替不可用。

无日志用户记录“来源探测成功且无日志”，不得从 Unix epoch 创建空小时。
来源后续恢复出更早日志时，新的边界探测可向左扩展，但绝不能悄悄缩短已声明的覆盖范围。
热/冷迁移必须先对重叠区间进行总数与 hash 对账，再发布不重叠的权威路由；
路由发布失败时继续使用旧来源，绝不同时累加热库与档案的重复日志。

NewAPI `users` 表提供 `created_at BIGINT`。2026-08-16 对生产库的最小只读统计确认：
当前 143 个用户该字段均为有效正数，最早注册时间为 2026-05-11。程序不要求运营选择起点，
程序按以下顺序自动确定：

1. 读取 `users.created_at`；
2. 读取该用户符合事实口径的 `MIN(logs.created_at)`；
3. 两者都有效时取更早值所属的 CST 自然日起点，避免迁移数据出现日志早于注册时间而漏数；
4. 只有注册时间时，从注册日建立 no-history coverage；
5. 只有日志时间时，从最早日志日开始；
6. 两者都无效时标记 `source_boundary_unknown` 并告警，不能伪装成已完成。

注册日至最早日志日之间可由成功的来源 MIN 探测证明为 `known_empty` 区间，无需逐日查询空数据。

### 3.3 用户、名单与所属公司

三者是不同概念：

1. 用户事实：按 `user_id` 永久保存，与当前是否在名单、当前属于哪个客户无关；
2. 当前名单：决定 Monitor 当前是否展示和继续同步该用户；
3. 所属公司：`CustomerGroup` 是公司，`TrackedUser.GroupID` 是该 NewAPI 账号唯一正确的公司。
   账号由公司创建并归公司所有，员工离职/换人不影响 user_id 或历史用量；
   Portal 是平台方分配的只读查看范围，客户不能管理公司或成员。

推荐正式口径：

| 报表 | 口径 |
|---|---|
| Monitor 单用户 | 当前 active 用户在来源可得范围内的全部历史 |
| Monitor 当前名单 | 仅当前 active 用户；移除立即隐藏 |
| 公司汇总 | 该公司下所有 active + facts-ready 用户 × 请求时间范围 |
| Usage Portal | 平台分配给该公司的只读账号，查看该公司 ready 用户的已验证历史 |
| 累计总消耗 | 仅在该用户全历史覆盖完整时显示正式值；部分覆盖只显示“已覆盖小计” |

运营不选择历史起点；用户第一次加入 Monitor 时，程序自动探测并同步个人全历史。
控制层迁移只原样保留 `CustomerGroup` 与 `TrackedUser.GroupID` 的当前映射，不推断历史分组，
不迁移、重算或改写 facts。切换前后必须比对分组数、每组用户 ID 集合 hash、
未分组集合和 Portal 账号映射；不一致则拒绝切换。

页面和 API 不再使用容易误解为业务转移的“改组”，统一命名为“纠正所属公司”。
它只用于用户初始被平台运营分错公司后，从 A 纠正到 B 或待分配，
不是 NewAPI 日志里的 `website_group`。纠正后：用户事实、个人累计和 Monitor
全体 active 用户总计均不变；旧组立即失去该用户的全部访问权，新组立即按当前成员
语义看到该用户全部已验证历史，因此 A/B 组汇总会变化。这是当前视图的成员范围变化，
不是历史消费被搬迁。纠正页面明确提示：“该用户全部历史用量将纳入目标公司，
原公司立即不再可见”，仅平台超级管理员可以确认。不支持账号转让或按日期切割公司历史。

### 3.4 预付费账户与展示边界

NewAPI 是先充值、后消费的预付费系统。充值、实时扣额、可用额度和是否能继续请求
由 NewAPI 决定；Monitor/Usage 只做消费观测、运营分析和客户自查，不生成应收、
不扣款、不改额度。

时间序列和维度明细以逻辑日志来源事实为准。`users.quota/used_quota`、余额和 Token
累计值是 NewAPI 账户状态快照，可用作独立差异告警，但不得由 Monitor 回写，
也不得悄悄覆盖日志维度事实。两者不一致时应标记“来源账户与日志明细待核对”，
由管理员排查，绝不自动改账。

## 4. 核心不变量

任何实现和测试都必须证明以下不变量：

1. **唯一事实**：相同 `user_id + date/hour + 维度 + classification_version` 最多一条；
2. **幂等**：相同来源区间重复导入，最终事实和金额完全一致，不发生累计叠加；
3. **完整或明确不完整**：没有 coverage 证明的日期不能被解释成零消费；
4. **事实与当前组织分离**：纠正所属公司不得写 facts；查询时才以当前成员集合组合用户事实；
5. **删除不毁数据**：普通移除不删除事实、proof、coverage 和操作审计；
6. **权限立即收紧**：当前名单/Portal 权限与服务快照取交集，删除后旧快照不能继续授权；
7. **发布原子性**：候选未通过完整性和来源对账前，不进入正式累计值；
8. **修复不造证明**：proof 缺失或不一致必须重读来源，不能仅根据本地事实补写 proof；
9. **失败不宽扫**：Redis、SQLite、同步器或审计失败时，聚合页面不回退来源 `logs`；
10. **版本明确**：事实包含查询语义、流量分类、schema 和来源规则版本；
11. **缓存一致**：事实变更切换 facts generation；名单、公司或权限变更后重新计算
    当前成员集合 fingerprint，并与 Portal `AuthVer` 一起进入 cache key 和响应版本；
12. **来源只读**：同步、对账、修复均不写 NewAPI。

## 5. V2 数据模型

V2 是现有 `/data/usage-facts.db` 上的控制层版本，不复制第二套 facts 查询引擎。
已有小时/日 facts、成员小时/日 proof、候选/服务 generation、已发布成员、
查询、缓存和矩阵护栏全部复用。schema 只增表/列/索引，不删旧列、不重命名大表；
任何 AutoMigrate 前使用现有成套迁移快照。只有整库损坏/重建演练才创建
`usage-facts-next.db`，完整验证后原子切读，不在损坏库上边删边补。

### 5.1 主库：当前投影与轻量操作审计

`TrackedUser` 继续是当前 active 名单和所属公司 `GroupID` 的唯一业务投影。
只增加很小的 `usage_member_audits`，用于 API 幂等、追责与恢复人工配置；
它不是事件溯源系统，不参与 facts 查询：

```text
id, request_id UNIQUE
action                 -- add/remove/rejoin/correct_company/group_rename/group_delete
user_id NULL
before_group_id, after_group_id
before_active, after_active
actor, reason, created_at
```

审计只允许 INSERT。API 在同一本地事务中修改当前投影并写审计，提交后唤醒 worker；
worker 启动和周期调度仍以 `TrackedUser` 全量对账为权威，因此进程在 API 提交后崩溃也能恢复。
删除/重加递增 facts 成员状态中的 `tracked_revision`，防止陈旧 job 改变 ready/serving；
纠正公司不修改 revision、不创建 facts job，只使 A/B 公司当前成员 fingerprint 变化。

### 5.2 事实库：在现有状态/proof 上增补字段

`UsageFactMemberState` 保留旧列兼容回滚，新增/正式使用：

```text
user_id, active, tracked_revision
source_floor_hour NULL
source_ceiling_hour NULL
coverage_through_hour NULL
tail_through_hour NULL
source_floor_checked_at
source_history_status
coverage_status       -- discovering/backfilling/verifying/ready/repairing/failed/inactive
last_success_at, last_failure_at, last_error
classification_version, query_semantics_version
```

`UsageFactMemberDayState` 直接扩展为日 coverage/proof：

```text
user_id, date_ts PRIMARY KEY
status                -- pending/staged/complete/repairing/failed
source_rows
requests, refund_records
prompt_tokens, completion_tokens
consume_quota, refund_quota
source_result_hash
fact_content_hash
classification_version, query_semantics_version
source_checked_at, completed_at
job_id, attempts, last_error
```

此表既证明有流量日，也证明成功查询后的零流量日。全历史不保存所有空小时 proof。

### 5.3 事实表继续使用现有结构

`UsageDailyFact`：

```text
date_ts, user_id, channel_id, website_group,
model_name, token_id PRIMARY KEY
token_name
requests, refund_records
prompt_tokens, completion_tokens
consume_quota, refund_quota
classification_version, query_semantics_version
```

建议索引：

```text
(user_id, date_ts)
(date_ts, user_id)
(user_id, token_id, date_ts)
```

`UsageHourFact` 仅保存最近 8～14 天与正在修复/未闭合的小时，字段沿用当前小时事实；
完成日原子构建日事实后，超过近期校正范围的小时明细可清理。

### 5.4 作业与暂存

`UsageFactJob`（唯一新的核心控制表）：

```text
id, idempotency_key UNIQUE
job_type              -- candidate_gap/discover/backfill_chunk/reconcile_day/repair_day/source_audit
priority
user_id NULL
from_ts, through_ts
reason
status                -- queued/leased/staged/verifying/complete/failed/paused/cancelled
attempts, next_retry_at
lease_owner, lease_until
created_at, started_at, completed_at
last_error
requested_by, approved_by
```

Worker 在来源连接之外构建内存结果；进程在提交前崩溃只会使租约过期重试。
校验独立控制总数和 hash 后，在一个 SQLite 事务中：

1. 删除目标 `user_id + date_ts` 的旧日事实；
2. 插入 staging 日事实；
3. 写入 `UsageFactMemberDayState=complete`；
4. 写 job 审计并递增 candidate generation；
5. 完成 job 并释放租约。

因此页面只能看到旧的完整日或新的完整日，不会看到删除与插入之间的空洞。

### 5.5 服务快照继续复用现有发布模型

`UsageFactSyncState.Generation/ServingGeneration/Published*` 和 `UsageFactPublishedMember`
继续分离候选与服务版。只在状态中增加全历史覆盖 manifest/hash 和口径版本，
不建第二套 snapshot/member 表。

新成员只有全历史覆盖至当前 Tail、安全审计和来源对账均通过后，才进入正式 serving snapshot。
候选期间可在管理员详情中显示“已覆盖小计”，但不进入老板总计和客户正式累计。

历史 repair 成功后以“日分区原子替换 + serving generation 切换”发布；不复制整个全历史库。

## 6. 成员生命周期状态机

### 6.1 首次新增

```text
API 写当前名单投影 + 轻量审计
  → worker 周期对账并立即唤醒
  → discover 来源 MIN/MAX(created_at)
  → 创建从 source_floor 到当前闭合日的历史 chunk jobs
  → Tail 始终优先维护近期数据
  → 全历史 coverage + 本地语义审计 + 来源对账通过
  → 原子纳入 serving snapshot
```

已有成员不回退游标、不重扫历史。来源无日志时写入明确的 no-history coverage 并完成发布。
同一 active 用户重复添加是幂等资料刷新，不更新原 `AddedAt`、不触发全历史补数；
若携带不同公司，必须走显式“纠正所属公司”接口和风险确认，不得由通用 `Save()` 静默修改。

### 6.2 删除

```text
API 写轻量审计 + 递增 tracked revision + 移出当前名单
  → 当前页面和 Portal 权限立即取交集隐藏
  → facts 状态改 inactive，停止 Tail
  → 事实、coverage、操作审计、最后水位全部保留
  → 下一 serving snapshot 移除该成员
```

普通删除不是 GDPR/合规擦除。若未来需要不可逆擦除，必须设计独立的双人批准流程、
来源边界和备份处理，不复用普通“移出名单”。
已在执行的旧 job 可以幂等落事实，但因其 `tracked_revision` 过期，不得把已删用户重新纳入 serving。

### 6.3 删除后重新加入

```text
API 写 rejoin 审计 + 新的 tracked revision + 当前公司
  → 恢复旧 facts 与 coverage
  → 优先补 [旧 coverage_through, 当前闭合小时) 的停用缺口
  → 对停用前历史执行分片 source hash 复核
  → 缺口和复核均通过后重新进入 serving snapshot
```

只重放目标用户，写入采用 replace 幂等语义，不重复累计。若来源左边界比旧记录更早，
再创建向左扩展任务。

### 6.4 纠正所属公司与删除公司

“纠正所属公司”事务只允许平台超级管理员执行。它校验用户 active、目标公司存在，
然后更新当前 `GroupID` 并追加轻量操作审计。该操作：

- 不修改、搬迁、删除或重算任何 facts；
- 不创建来源同步 job；
- 旧组 Portal 立即失去该用户全部访问权，新组按当前成员语义看到全部已验证历史；
- 用户个人值和 Monitor 全体 active 总计不变，A/B 组汇总按成员集合变化；
- 公司页面文案使用“当前成员范围用量”，不暗示历史财务归属；
- 不递增 `tracked_revision`，因为事实同步任务与用户本身均未变化。

公司不是可随意删除的展示标签。正式规则是：

1. 仍有成员时拒绝删除，管理员必须先逐个纠正成员归属；禁止批量变成“未分组”；
2. Portal 已启用时拒绝删除，必须先停用 Portal、递增 `AuthVer` 并确认旧 Session 失效；
3. 只有无成员且 Portal 已停用的误建公司才允许删除；常规场景优先软归档，重命名不影响 facts；
4. 删除或归档只写本地控制数据与审计，不读取来源 `logs`，不改变任何用户事实。

Portal 每次请求必须同时满足当前 Portal 公司、当前 active 名单与已发布成员。
缓存键复用现有 `portalMemberFingerprint(sorted user_ids)`，并包含 facts generation 与 `AuthVer`。
查询开始与返回前各读取一次当前 fingerprint；若变化则丢弃旧结果并按新权限重试，
避免纠正归属或移除成员期间旧公司收到在途结果。客户 Portal 始终只读，不能添加、删除、
纠正成员或管理公司。

## 7. 同步调度与来源保护

### 7.1 统一、持久化的来源调度器

仅有“后台查询不并发”不够：必须同时约束查询启动间隔、来源连接占用时间、
任务优先级和重启后的退避。控制层建立一个统一 orchestrator，但按物理来源使用独立预算：

- NewAPI MySQL：用量 Tail/资料、repair、历史补全、稳定性历史任务与受限日志/导出共享 DB gate；
- 第三方渠道 API：按 provider/account 独立限速、分页和 429 熔断，不占 NewAPI DB gate；
- Nginx/拒绝/主机采集：按 node/batch 限制本地 IO，不占 DB gate；
- 三类来源共享本地 SQLite 写入/备份错峰和容器 CPU/内存总预算。

固定原则：

1. 每个物理来源只允许一个 `source-enabled` 实例持有租约，蓝绿容器不得同时采集同一来源；
2. NewAPI 重型聚合 SQL 并发固定为 1，即使连接池上限为 4，后台聚合也只占 1 条；
3. job、租约、下次运行时间、退避和熔断都持久化到 SQLite，容器重启不得清零；
4. 暂停期不积累 token，恢复后从正常节奏继续，不突发补跑；
5. 完整查询优先级为：受限交互查询 → 近期 Tail → 已发布数据 repair
   → 资料/余额快照 → 新增/重加历史 → 近期复核 → 全历史滚动复核；
6. 每一类错误有独立熔断状态，轻量 profile 查询成功不能清除 facts 聚合的失败计数。

### 7.2 历史导入粒度

全历史不按小时扫描：

1. 先按用户发现最早/最晚日志；
2. 首次灰度为最多 50 人 × 1 天；连续 10 次满足耗时和结果门槛后，
   chunk 才按 `1→2→4→7` 天逐级放大，200 人 × 7 天只是验证后上限；
3. 结果行数、扫描量或耗时超过门槛时，二分成更小用户/时间区间，最小为一小时；
4. 查询成功后同时为 chunk 内零流量日写 coverage；
5. 单来源槽，不因积压增加并发；积压只延长 ETA；
6. 历史/repair 查询在首次灰度以 30～36 秒启动间隔运行，稳定后才降为 15～18 秒；
7. 每 5 分钟最多 20 条重型来源 SQL（上限 0.067 QPS），累计来源连接占用时间不超过 15 秒；
8. 每个 chunk 的维度聚合 SQL 与独立控制总数 SQL 必须都通过，请求数、消费/退款 quota、
   输入/输出 Token 完全一致后才能原子发布；
9. SQL statement 硬超时为 5 秒；20 秒只是 worker 清理上限，不是数据库执行上限；
10. 目标 P50≤500ms、P95≤750ms、P99≤2s；单次 >2s 缩小 chunk，最小 chunk 仍 >5s
   则标记 blocked 并告警，不跳过或伪造 coverage；
11. SQL `LIMIT` 只限制结果行，不能阻止 MySQL 先扫描/分组/使用临时表；
    必须使用 chunk 拆分、statement timeout 和真实执行计划保护。

来源 SQL 必须保持：

```text
user_id IN (...)
AND created_at >= ? AND created_at < ?
AND type IN (2, 6)
AND NOT internal_channel_test
```

生产已存在 `(user_id, created_at, type)`，上线前仍需在批准的只读短查询中确认真实计划；
不得让 Monitor 启动时自动创建或修改来源索引。

### 7.3 实时与晚到数据

本批继续复用现有“闭合小时 + 10 分钟安全延迟 + 每 5 分钟调度”的 Tail，
不在恢复用户用量时再引入一套 5 分钟切片事实。普通日志可见延迟为：

```text
等待当前小时闭合 0～60 分钟 + 安全延迟 10 分钟 + 调度等待 0～5.5 分钟
```

因此应对外诚实标注约 10～75.5 分钟，平均约 42.75 分钟；账户余额/资料快照约 0～5.5 分钟。
实时性保护规则：

- Tail 最高优先级，历史补全和全历史复核必须让路；
- Tail 成功写入新小时后同时推进相同成员游标，避免下一轮重复查询同一来源小时；
- 最近 14 天晚到/修订由低频小时/日复核发现，hash 变化进入精确 repair；
- 全历史来源复核默认 30 天轮转，预算不足时延长并告警，不抢 Tail；
- 来源断开时页面保留最后签收水位并显示延迟，不把未同步区间解释为零；
- 只有在当前版本稳定上线、来源负载有余量且产品明确需要更低延迟后，才单独评审 5 分钟切片事实；
  该优化必须独立压测和灰度，不能夹带进本次恢复。

### 7.4 持久化熔断与止损

程序能够基于自身来源查询指标自动执行：

- 单次 >2s：缩小 chunk；
- 超时、>5s 或连接耗尽类错误：暂停 bulk 15 分钟；
- 连续 3 次 >2s，或 1 小时内重复硬失败：暂停 bulk 60 分钟；
- 半开只放行一个最小 chunk，连续 3 次成功才关闭熔断；
- 401/403、schema 不兼容或权限错误：锁定等待人工处理；429 严格遵守 `Retry-After`。

基础设施止损线使用过去 7 天同时段基线：DB CPU 连续两个 5 分钟窗口 ≥60%
或增加 ≥10 个百分点、连接使用率 ≥70%、DiskQueueDepth ≥5，或 NewAPI P95
同时增加 ≥10% 且 ≥50ms，则暂停历史/repair/全历史审计。DB CPU ≥75%、连接 ≥85%、
NewAPI P95 增加 ≥25% 或 5xx 明显增长，则停止全部来源同步，页面继续服务最后已签收本地快照。

这些基础设施阈值只有在部署系统向调度器提供 `max_connections`、DB CPU/磁盘队列和
APM/LB P95 指标后才能自动执行。在此之前它们是运维停机线，不得在文档中冒充程序已实现的能力。
出现任何锁等待/死锁、来源与 facts 数值不一致、权限异常时冻结 serving 发布。
回滚只切回最后已验证本地快照，绝不关闭 facts 读取后让页面回扫 `logs`。

### 7.5 热 MySQL 与未来档案存储

实现上现在就定义统一 `UsageSource`接口，本批只实现 MySQL adapter；
待档案介质确定后再增加 adapter，不因此提前修改 NewAPI：

```text
DiscoverBoundary(user_id)
Aggregate(user_ids, [from,to), classification_version)
IndependentControls(user_ids, [from,to), classification_version)
PartitionManifest([from,to))
```

每个返回值都携带实际覆盖范围、`source_system_id`、分区/generation、规则版本和控制 hash。
每个时间分区只有一个 primary 权威来源，secondary 只用于双读校验，不累加。

热库迁档时使用无损切换协议：

1. 以闭合时间 `T` 固定迁移分区，保留稳定日志身份（优先 `source_system_id + logs.id`）；
2. 档案 manifest 记录行数、最小/最大 ID 和时间、消费/退款数、Token、quota 与规范内容 SHA-256；
3. 热库与档案逐分区零差异后原子切换 primary，重叠观察期继续双读但只计 primary；
4. 切换或校验失败则回到旧路由；档案晚到/修订生成新 generation 并精确创建 repair；
5. 档案不可用时，已签收本地事实继续展示，新成员历史补全/旧历史复核标记
   `source_tier_unavailable`，不得记为零或已完成。

档案必须保留内部测试分类、Token/模型/渠道维度和重新核算所需字段。
如果只保留总金额，就无法在分类规则升级后重新验证，不能称为完整永久来源。

## 8. 完整性、来源对账与异常处理

### 8.1 三层校验

| 层 | 机制 | 发现的问题 |
|---|---|---|
| 文件层 | `PRAGMA quick_check`、schema/version、文件哈希 | SQLite 页、索引、schema 损坏 |
| 业务层 | day coverage、事实控制总数、内容 hash、snapshot manifest | 误删、漏写、改值、proof 缺失、迁移残缺 |
| 来源层 | 对同一用户/日期重新执行独立控制查询并比较 | 同步 SQL、分类、时区或本地自证错误 |

来源控制查询必须与正式维度查询解耦，至少独立核对：消费/退款记录数、输入/输出 Tokens、
消费/退款 quota；不能只复用同一份内存聚合结果当作“来源复核”。

### 8.2 稳态审计 SLO（待压测签字）

- 当前日与最近完整日：每轮本地审计；
- 全部本地日事实/proof：使用持久化分区审计游标轮转，目标 24 小时内覆盖一轮；
- 最近 14 天来源复核：目标 24 小时内覆盖；
- 高金额、刚修复、分类规则变化日期：优先复核；
- 全历史来源复核：低优先级轮转，默认目标 30 天，来源预算不足时延长并告警；
- 来源不可用不会把“未复核”写成“已通过”。

审计和 status/progress 都不得每分钟对每个用户 `COUNT` 整个全历史窗口。
覆盖数、缺口数与分区状态在落盘事务中增量维护，完整扫描只由低频分片审计进行，
并在页面 facts 查询繁忙时让路。

### 8.3 Repair Job

发现以下任一情况都创建幂等 repair job：

- 缺 day coverage/proof；
- fact hash 与 proof 不一致；
- 来源控制数与事实不一致；
- 晚到日志导致来源 hash 变化；
- 分类/查询语义版本变化影响该用户日期；
- 恢复备份后 snapshot manifest 与当前数据不一致。

修复流程：

```text
detect
  → enqueue(user/date, reason)
  → source read under global gate
  → stage
  → independent controls + hash verify
  → atomic day replace
  → local semantic audit
  → source control recheck
  → serving generation publish
```

来源失败：保留 job、指数退避、记录错误、告警；不删除旧事实。
旧事实已确认损坏时：受影响汇总返回 `available=false` 或明确 partial，不能返回 0；
未受影响日期可继续展示，并携带 verified coverage。

2026-08-01 缺失已作为首个正式 repair 验收：重新读取该日来源、重建事实和 proof，
中途重启后续跑，再通过全候选语义复审发布；未手工插入 proof。经过独立 SQL 的
来源控制对账仍是生产签字前独立门槛，不能被本地 hash 自洽替代。

## 9. 读取、缓存与页面契约

所有聚合 API 返回统一元数据：

```json
{
  "available": true,
  "partial": false,
  "verified_from": 0,
  "verified_through": 0,
  "source_floor": 0,
  "serving_generation": 0,
  "last_source_sync_at": 0,
  "last_verified_at": 0,
  "message": ""
}
```

规则：

1. 正式累计总消耗只有 `partial=false` 才展示；
2. `partial=true` 时只能标注“已覆盖小计”，不能与正式总数使用相同标题；
3. 请求范围与 verified coverage 相交时，可展示交集日期和补全提示；
4. 真实零值必须有 coverage 证明；没有证明显示“未完成/不可用”；
5. cache key 包含 facts serving generation、当前成员集合 fingerprint、Portal `AuthVer`、
   权限主体、用户、Token、时间与口径版本；
6. 事实发布或当前名单/分组/权限变更后分别换 generation，旧缓存自然失效；
7. Redis 故障由有界 L1 承担，不回源 `logs`；
8. 保留 20,000 矩阵单元、冷 facts 并发 2、4 MiB 单项、16 MiB L1、singleflight 护栏；
9. 全历史图表按年/月降采样或分页，禁止“全成员 × 全历史 × 全维度”一次返回。

Monitor 和 Usage 必须调用相同 domain service/query builder，不得各自复制一套 SQL 口径。

### 9.1 新成员/重加成员的 partial 可见性

“仅管理员能看 partial”是本批的默认安全决策：它牺牲新成员首次出现在客户 Portal
的速度，换取客户不会看到一个“看似累计、实际缺历史”的数字。

规则：

1. 新成员先进入 candidate，Tail 与近期日优先，管理员可以立即看到真实已覆盖数据；
2. 未完成全历史、Tail、本地审计和独立来源对账前，该成员不进入客户 Portal、
   老板“完整成员累计”、排行、跟进结论或自动告警；
3. 管理端主页明确显示“完整成员 N / 当前成员 M，还有 K 人补全中，
   完整合计暂不包含这些成员”；partial 区域只能标题为“已覆盖小计”；
4. Portal 不显示 partial 成员的身份或小计；若当前组存在 pending 成员，可显示不泄露成员信息的
   “管理员正在更新成员数据”提示，已发布成员仍按旧 serving snapshot 展示；
5. NewAPI 当前余额/used quota 快照可在管理员 partial 详情中单独展示，
   但不得与“已覆盖历史事实”混成同一口径。

管理员进度至少包含：当前阶段、逻辑来源最早时间、已验证范围、完成分区/总分区、
百分比、缺口数、来源层、Tail lag、最近成功/错误、重试与退避、滚动吞吐和 ETA 区间。
边界发现前分母未知，显示“正在确认最早历史”，不伪造 0%；来源退避或吞吐不稳时暂停 ETA。
进度接口只读持久化计数，复杂度为 O(成员/任务数)，不扫全 facts、不读来源库。

原子纳入 serving 的门槛：逻辑来源边界已确定、floor 到当前水位无缺口、
本地业务审计通过、来源对账零差异、无影响该用户的 repair、当前名单/权限仍有效且 Tail lag 达标。

## 10. 备份、损坏与灾难恢复

### 10.1 备份

- 迁移前：Monitor 主库 + 现有 facts 库使用同一 `backup_epoch` 生成成套一致快照和 manifest；
- 备份分层：小时级 24～48 份使用可验证的增量/卷快照（或 SQLite online backup），
  不对全历史大库每小时完整 `VACUUM INTO`；日级紧凑全量保留 14 份、周备 8 份、月备 12 份，对象存储生命周期转冷；
- 备份必须加密复制到独立持久存储，不能只与 live DB 同处 `/data` 卷；
- manifest 包含 schema、行数、关键控制总数、coverage、serving generation、来源目录 generation、
  控制审计位置、文件 SHA-256、配置指纹和不可变镜像 digest；密钥只保存引用 ID；
- 成员删除、纠正所属公司、Portal 凭证变更等控制审计必须以 `request_id` 幂等复制到卷外备份；
  如果尚未建成这个能力，任何灾难恢复后 Portal 都必须 fail-closed，直到当前 ACL 复核完成；
- 每份上传后校验对象大小和 SHA-256；每日自动恢复到临时空卷做 quick_check/语义冒烟；
  每次 schema 发布前做完整恢复，每月做生产规模计时恢复，每季度演练整卷丢失/事实损坏/主库损坏/档案不可用/密钥恢复；
- 备份失败必须监控告警，没有可验证恢复的备份等于没有备份。

### 10.2 恢复

只恢复到新空卷：

1. 停止候选写入；
2. 选择 manifest 完整且文件哈希通过的成套备份；
3. 恢复到新卷，运行 quick_check、schema、inventory、snapshot manifest；
4. 启动相同不可变镜像；
5. 执行全业务语义审计和来源抽样；
6. 使用测试端口验证 Monitor/Usage；
7. 人工批准后切换；旧卷保留至观察期结束。

不得让旧镜像直接打开已被新 schema 迁移的原卷。

建议恢复目标（SLO，不是尚未实测的承诺）：

| 对象 | RPO | RTO |
|---|---:|---:|
| Portal 撤权、成员删除、当前分组 | 确认变更近乎 0；否则恢复后 Portal 关闭 | 权限位置一致后 ≤30 分钟 |
| 其他 Monitor 控制数据/成员审计 | ≤5 分钟 | ≤30 分钟 |
| 已签收 facts 本地快照 | 本地最多重放 1 小时；逻辑事实 RPO=0 | 管理员最后一致页面 ≤30 分钟 |
| 近期 Tail 新鲜度 | 可从逻辑日志来源重放 | 恢复后 ≤2 小时追平 |

只有使用生产同级容量、同一镜像 digest、同类磁盘和真实备份下载链路连续 3 次达标，
才能签收上表 RTO。全历史低优先级复核可在页面恢复后继续，不属于 30 分钟 RTO。

### 10.3 容量

固定 8 GiB 不是全历史保证。容量取决于用户数、历史年限、模型/渠道/Token
维度基数、渠道/稳定性派生数据、业务增长、WAL 和同时存在的回滚/重建文件。
上线前必须生成完整影子库并实测：

```text
Scontrol = Monitor 不可重建控制数据压测峰值
Sfacts   = 用量、渠道、稳定性全部派生事实 VACUUM 后实测大小
Slegacy  = 回滚观察期旧结构/旧文件
Srebuild = schema 升级或损坏重建时并存的新派生库，默认 1×Sfacts
Swal     = 补全 + Tail + 页面 + 备份混合负载的 WAL/temp 峰值
Stemp    = 本地备份暂存；备份卷独立时可为 0
G90      = P95 每日增长 × 90 天 × 用户增长系数

/data >= (Scontrol + Sfacts + Slegacy + Srebuild + Swal + Stemp + G90) / 0.70
```

现有“200 人 × 366 天约 395 MiB”是当前结构合成样本，只能作为早期参考，不能外推全历史。
生产 `/data` 以 32 GiB 作为合理初始值，但只有全历史影子库和混合负载后使用率 <60%
才签收；否则直接改为 64 GiB，不以“现在文件很小”为理由冒险。
卷使用率 60% 预警、70% 必须扩容、80% 自动暂停历史补全/全历史审计、
85% 停止所有来源写入并保持最后已签收快照只读。
live SQLite 必须使用本地持久 SSD 类文件系统，不得直接放在对象存储、NFS 或 SMB。

## 11. 迁移与发布策略

本次不新建第二套 facts 引擎，也不等待“全部永久能力”完成后才恢复当前页面。
交付拆成两个可独立签收的轨道。

### 轨道 A：先恢复当前用户用量

1. 冻结本文数据口径，先对 Monitor 主库和现有 facts 库生成迁移前成套快照；
2. 在现有 repair 控制上增加持久化 `candidate_gap` job，精确修复
   2026-08-01 的 36 个 member-day；禁止重跑完整 90 天，禁止手工补 proof；
3. 重读该日来源、原子替换事实/proof、做独立控制对账和全候选语义审计；
4. 审计通过后发布现有候选，恢复 7/30/90 天正式页面；聚合 API 继续只读现有 SQLite；
5. 使用每用户注册时间和来源 `MIN(created_at)` 探测真正左边界，只追加当前候选左侧缺口；
6. 已知生产最早注册日在 2026-05-11，现有 90 天候选约从 2026-05-18 开始；
   若来源没有更早迁移日志，当前“全部历史”只需再补约 7 天，但必须以实际边界探测为准。

这条轨道不改现有 query/cache/page 主体，不迁移第二份事实库，也不修改 NewAPI。
一个不超过 200 人的缺失自然日只需 24 个既有小时来源查询；按当前 15～18 秒节奏，
来源执行窗口约 6～7.2 分钟，连同审计、测试和发布按几十分钟级安排，而不是重跑数十小时。
左侧约 7 天按旧小时同步理论约 42～50 分钟，加入独立对账和退避后按 1～2 小时窗口安排；
若边界探测发现更早日志，则 ETA 按实际分区数动态更新，不能预先承诺固定完成时间。

### 轨道 B：补齐永久控制层

1. 在现有 facts 表上只增 `source_floor/coverage/tracked_revision` 和持久 job/repair；
2. 冷历史使用日 chunk，近期继续复用现有小时 Tail、日折叠、proof 和 serving generation；
3. 在现有来源 gate 上增加启动间隔、持久退避/熔断和重启续跑，不重写查询引擎；
4. 增加独立来源控制对账、滚动审计、成员新增/删除/重加/纠正公司的状态测试；
5. 管理员进度先上线，Portal 只消费已签收 serving snapshot；
6. 使用隔离的本地 MySQL 脱敏克隆或等规模合成库完成容量、并发和故障注入；
7. 构建不可变 Monitor 镜像，以最终 digest 重跑备份恢复、Caddy、真实登录态和权限黑盒测试；
8. 生产 `/data`、独立备份目标和回滚卷准备就绪后才进入小范围灰度。

轨道 B 是若干小的可回滚工作包，不允许重新创建 `usagev2` 全套并复制已有能力。
每完成一个包都必须保留轨道 A 已恢复的 serving 快照；未完成的历史范围只显示管理员进度，
绝不能让客户正式页面回扫来源或显示伪完整值。

### 灰度与正式切换

- 先选择平台内部管理员和少量公司读取已签收 serving snapshot；
- 比较 SQLite、来源独立控制数和账户状态，观察来源查询、内存、WAL、错误和页面延迟；
- 无差异后逐公司放开；客户仍只读，不获得任何成员管理能力；
- 观察期内保留旧镜像、旧卷和最后一致快照，不清理已验证事实；
- 只有来源查询量、关键数值差异、备份和 repair 均满足门槛后才关闭回滚窗口。

### 回滚

回滚只切换 Monitor 不可变镜像/数据卷或恢复最后已验证 serving snapshot，不修改 NewAPI。
出现以下任一情况立即停止灰度：

- 来源数据库慢查询、连接、CPU 或锁等待越过止损线；
- SQLite facts/来源控制数/账户状态出现无法解释的关键差异；
- repair 不可续跑或出现重复累计；
- SQLite quick_check/manifest 失败；
- Portal 越权、跨客户数据泄露或正式累计显示 partial；
- Monitor OOM、持续 5xx 或备份失败。

## 12. 可观测性

至少暴露以下指标，且不得包含账号密码、Token 或用户隐私原文：

- facts source 查询：按任务类型的 count、duration、rows、timeout、error；
- 来源 gate：等待时间、占用者、拒绝数、退避状态；
- jobs：queued/leased/failed、最老任务年龄、重试次数；
- coverage：active 用户 ready/partial/failed、最早来源、连续水位、Tail lag；
- integrity：quick_check、local hash、source audit 成功/失败和检测延迟；
- repair：原因、范围、进度、最后错误、完成时间；
- serving snapshot：generation、成员数、through、manifest、发布时间；
- SQLite：文件/WAL/空闲、query duration、busy、checkpoint；
- cache：L1/Redis hit、fill、payload reject、singleflight；
- 页面：Monitor/Usage 按端点 P50/P95/P99、5xx、partial/unavailable；
- 备份：最近成功、耗时、大小、恢复演练时间。

状态页面不能用一个绿色“healthy”掩盖事实不完整。服务存活、来源可用、事实完整、来源已复核、
备份可恢复必须分别展示。

## 13. 安全与权限

1. 数据库只读账号仅拥有 `SELECT`；
2. repair API 仅超级管理员，单次范围有限，必须显式确认并留下审批记录；
3. 成员操作审计的 actor/request_id 可追踪，重复请求幂等；
4. Portal 每次查询都基于当前 Portal 公司、active 名单、已发布成员、成员 fingerprint 和 `AuthVer`，不能只信缓存；
5. SQLite 不保存 Token 明文、请求内容和完整日志；
6. 日志/metrics 不输出 DSN、密码、Cookie、Session、完整邮箱或 Token；
7. 在两个已开通 Portal 的公司间纠正归属属于高风险授权变更，必须明示新/旧公司的全历史访问后果并二次确认；
8. 备份加密、访问控制和保留删除策略必须纳入部署配置。

## 14. 开发拆分与依赖顺序

每一阶段必须先测试再进入下一阶段，禁止同时大范围改写：

1. `recovery patch`：候选缺口持久 repair、来源重读、原子发布和当前页面恢复；
2. `domain contract`：金额、时间、分类、个人/公司口径测试；
3. 当前名单/公司投影、轻量审计、tracked revision 与旧 API 兼容；
4. 现有 schema 的 additive migration、迁移前快照和回滚测试；
5. 来源边界发现、冷历史日 chunk 和现有近期小时 Tail 协同；
6. persistent job、来源控制对账、滚动审计和精确 repair；
7. Monitor 管理员进度、Portal 已签收数据与成员 fingerprint 权限隔离；
8. 统一来源节奏、持久熔断、状态、告警和运维 CLI/文档；
9. 故障注入、生产同级性能/容量、不可变制品、恢复演练和灰度。

代码只按职责从现有大文件中小步抽取，不创建第二套 `usagev2/query/cache/snapshot`。
建议新增文件仅限控制层，例如 `usage_facts_jobs.go`、`usage_facts_boundary.go`、
`usage_facts_source_control.go`；已有 `usage_facts.go`、hour sync、semantic、repair、reliability
继续作为唯一实现。抽取必须先有行为锁定测试，禁止在恢复当前页面前做纯重构。

## 15. 测试与验收矩阵

### 15.1 正确性

- 0/1/多条消费、退款、负值/NULL/极大整数、特殊模型/Token/website group；
- CST 00:00、月底、年底、闰日；
- 日 chunk 二分后与单小时事实、来源精确一致；
- 重复执行、并发租约、进程在 source/stage/commit 各阶段被杀死；
- Monitor 与 Usage 同用户同范围同口径数值一致；
- 资料快照 `used_quota` 与 logs 不一致时明确告警，不偷偷覆盖。

### 15.2 成员生命周期

- 首次新增有多年日志用户：覆盖最早来源到当前；
- 首次新增无日志用户：正确完成 no-history；
- 删除立即隐藏且 facts 不删；
- 删除期间产生消费、再加入：只补缺口、不重复；
- 重加前历史被来源修订：source audit 检出并 repair；
- 纠正公司 A→B：facts、个人累计和 Monitor 全体总计不变，A 立即失去全部访问权，B 看到全部已验证历史；
- 补全中纠正公司不重启 facts job，完成后只按最新当前公司展示；
- 有成员公司拒绝删除；Portal 启用公司拒绝删除；空公司归档/删除不改 facts；
- 客户 Portal 不能新增、删除、重加或纠正成员；
- 删除后的陈旧 job 不得凭旧 tracked revision 把成员重新发布；
- 控制层迁移前后公司数、每公司用户 ID 集合 hash、未分组集合和 Portal 映射完全一致；
- Portal 旧 Session、缓存和旧 snapshot 均不能越权读取已移除成员。

### 15.3 完整性与修复

- 删除 2026-08-01 proof；
- 删除一条日事实、修改 quota、交换维度、损坏 coverage；
- 同时删除事实与本地 proof，验证来源审计仍能发现；
- 来源晚到消费/退款和历史修订；
- repair 来源超时、失败、重启续跑、重复批准；
- staging 成功但提交前崩溃、提交成功但响应丢失；
- 修复前后来源控制数、事实 hash、snapshot manifest 一致。

### 15.4 故障与恢复

- 来源断开、慢查询、连接池满、账号权限错误；
- 两个 source-enabled 实例争抢租约，仅一个能查来源；退避期重启不清零、恢复不突发；
- 热库/档案重叠分区分别注入一条遗漏、一条重复和一条晚到，验证检出、不重计和精确 repair；
- 来源路由切换中断、档案不可用，已签收页面继续且 pending 范围不伪装成零；
- Redis 不可用、损坏值、恢复；
- SQLite busy、WAL 膨胀、磁盘 70%/85%/写满；
- quick_check 失败、facts 文件截断、恢复旧备份；
- 容器 SIGTERM/SIGKILL、OOM、宿主机重启；
- 备份失败、独立存储不可达、恢复到新卷；
- 旧快照继续服务或受影响范围明确 unavailable，页面绝不回源宽扫。

### 15.5 性能

- 接近真实历史年限、用户数和维度基数；
- 历史 backfill + Tail + repair + 页面冷/热并发混合；
- 256 MiB/最终生产内存限制下 RSS、GC、SQLite、WAL、备份峰值；
- 来源 SQL `EXPLAIN`、扫描行、P50/P95/P99/max、timeout；
- 中转站 NewAPI 请求延迟、数据库 CPU/连接/锁等待/慢查询对照；
- 矩阵 20,000/20,001、不同 key 冷并发、缓存 payload 边界；
- 全历史月/年降采样与分页。
- progress/status 在最大成员/任务量下不扫全 facts，不与页面查询争抢 SQLite CPU/IO。

### 15.6 安全

- 管理员、不同公司 Portal、移除成员、纠正公司前后 ID 越权与在途响应；
- SQL 注入、XSS、CSV 公式、超大日期/请求体、repair 重放；
- 缓存 key 权限隔离；
- 日志、状态、备份 manifest 无敏感信息。

## 16. P0 上线门槛

### 16.1 当前页面恢复门槛（P0-R）

以下条件通过后，可以恢复本机/预发布的当前 facts serving，不必等待永久控制层全部开发完成：

1. 迁移前成套快照已生成并完成新空卷恢复冒烟；
2. 2026-08-01 缺口通过持久 `candidate_gap` repair 重读来源并修复，不存在人工造 proof；
3. 当前候选全范围本地语义审计、独立来源控制抽样和关键金额对账差异为零；
4. 7/30/90 天及 followups 在完整/partial/不可用状态均符合页面契约，聚合 API 来源 `logs` 查询计数为零；
5. repair 中断续跑、重复执行、来源超时和发布前崩溃不会双记或发布不完整候选；
6. 最终候选镜像、本机 Caddy、Monitor/Usage 真实登录态和权限黑盒验收通过。

P0-R 只恢复已验证的当前范围，不等同于允许正式上线“全历史”承诺。

2026-08-16 实施状态：第 1、2、3、5 项已通过真实数据验收；修复日的独立 24 小时来源控制对账中，
请求、退款记录、输入/输出 Tokens、消费/退款 quota 六组整数与 SQLite 全部一致。第 4 项的后端契约和回归已通过，
真实登录黑盒仍待补；第 6 项 Caddy/浏览器视觉验收仍未签收。
运行证据见 `docs/test-reports/2026-08-16-usage-facts-candidate-gap-repair-local-acceptance.md`。

### 16.2 生产全历史门槛（P0-GA）

以下全部满足才允许进入生产灰度：

1. 本文业务口径、程序自动判断规则、数据字典和查询公式已签字冻结；
2. 全历史覆盖到每个 active 用户的逻辑来源起点，no-history 用户有明确证明；
3. 全历史来源控制对账和现有 facts 重叠范围对账差异为零；
4. 新增、删除、重加、纠正所属公司、公司归档/删除的数值与权限测试全部通过；
5. 用户用量的永久逻辑来源可重放；渠道上游、Nginx/拒绝/主机等非 NewAPI 来源已有卷外归档或明确降级边界；
6. 文件、业务、来源三层完整性和所有故障注入均能发现并恢复；
7. 聚合 API 在 facts 异常时不执行来源 `logs`；
8. 实际规模性能、容量、来源负载、容器资源通过；单实例来源租约、持久化调度/熔断、5 秒 SQL 上限和基础设施止损通道可执行；
9. 独立存储备份与新卷恢复演练通过；
10. 最终不可变 Monitor 镜像 digest 完成全套回归；
11. Caddy/TLS、真实登录态 Monitor 与 Usage 页面完成视觉和权限验收；
12. 技术、运维、测试三角色均签字“通过”，不存在未解决 P0/P1 功能或数据风险；
13. 用户明确授权后才 commit、push、灰度或生产切换。

## 17. 开发前已冻结决策

1. **历史起点由程序判断**：使用 `users.created_at` 与最早有效用量日志的较早值；
   无日志用户写明 no-history coverage，运营不选择日期。
2. **旧分组迁移不动数据**：原样保留当前 `CustomerGroup` 和 `TrackedUser.GroupID`；
   不推断历史分组、不建立时间区间、不迁移或重算 facts。切换前后成员集合/Portal 映射 hash 必须一致。
3. **纠正所属公司是当前范围变更**：A→B 时 facts、个人累计和 Monitor 全体 active 总计不变；
   A 立即失去该用户全部访问权，B 看到该用户全部已验证历史，所以 A/B 分组汇总按当前成员变化。
   账号仍归公司所有，不存在员工带走历史或按任职日期切割。跨已开通 Portal 的公司纠正要明示后果并二次确认；
   有成员或启用 Portal 的公司禁止直接删除，客户 Portal 无成员管理权限。
4. **预付费边界**：NewAPI 先充值后消费，并独立决定扣额和可用额度。
   Monitor/Usage 只做观测与客户自查，不建应收、账单冻结、扣款或冲正模型；数据差异只告警，不自动改账。
5. **全历史是逻辑多层来源**：日志不永久丢弃，但可以从热 MySQL 迁到档案存储。
   控制层现在定义 `UsageSource` 和分区目录，本批只实现 MySQL adapter；档案确定后增加 adapter。
   迁档必须 manifest/hash 零差异、原子切换单一 primary，不重不漏；档案不可用不得伪装成零。
6. **实时性和中转站保护共同决定**：使用单 source-enabled 实例、重型并发 1、持久化调度/熔断、
   现有闭合小时 Tail（诚实延迟 10～75.5 分钟）和自适应日 chunk。历史灰度先用 50 人 × 1 天、30～36 秒间隔，
   验证后才逐级扩到 200 人 × 7 天、15～18 秒间隔；SQL 硬超时 5 秒，P95 目标 ≤750ms。
   程序自动依查询耗时/错误拆分、退避、熔断；DB CPU/连接/APM 止损只在接入线上指标后才是自动能力，
   接入前是必须执行的运维停机线。任何回滚都不允许页面回扫 `logs`。
7. **容量、RPO/RTO 以实测签收**：`/data` 按 `Scontrol+Sfacts+Slegacy+Srebuild+Swal+Stemp+G90`
   再除以 0.70 计算，32 GiB 只是初始建议；全历史影子+混合负载超过 60% 就使用 64 GiB。
   备份放独立加密对象存储，不以固定 100 GiB 冒充长期规划。目标为权限撤销 RPO 近 0（否则 Portal 恢复后关闭）、
   其他控制数据 RPO≤5 分钟、派生事实逻辑 RPO=0（本地快照最多重放 1 小时）、
   最后签收页面 RTO≤30 分钟、Tail 追平≤2 小时；
   必须连续 3 次生产同级恢复演练达标才能宣称实现。
8. **管理员 partial、客户只看已签收数据**：该设计合理并保留。管理员看阶段、来源起点、
   已覆盖范围、完成比例、缺口、Tail lag、最近错误/退避、吞吐与 ETA 区间；
   边界未知时不显示假百分比，不稳定时不承诺 ETA。partial 不进入 Portal、完整累计、排行与告警；
   管理端必须同时标明“完整 N/当前 M，合计排除 K 人”，避免静默少算。

以上已经把需要的业务决策冻结；不再需要老板或运营选择历史日期、历史分组或结算规则。
开发后能否进入灰度，只取决于第 16 节的实测证据，不因文档完成而自动视为已验收。

## 18. Monitor 各域实际存储与可恢复边界

### 18.1 四级数据分类

| 等级 | 定义 | 处理原则 |
|---|---|---|
| L0 永久逻辑来源 | 能重新生成业务事实的原始或无损规范化记录 | RPO=0；热库迁冷档不能改变逻辑覆盖；必须有 manifest/hash 和可读性巡检 |
| L1 Monitor 独有控制数据 | 公司/成员/Portal、备注/跟进、费率版本、上游账号配置、审计 | 不能从 NewAPI 重建；卷外加密备份，权限变更近实时复制，恢复后先复核 ACL |
| L2 可重建派生事实 | 用量、渠道、稳定性聚合、proof、coverage、watermark、serving snapshot、repair job | SQLite 可损坏或丢卷，但必须能从 L0 重建；本地快照用于缩短 RTO，不是永久来源 |
| L3 可丢缓存 | L1/Redis cache、singleflight、临时 staging、过期 lease | 不备份；丢失后有界重算，绝不因此回扫无界来源 |

“不接受永久丢失”并不等于每一条聚合 SQLite 写入都做跨机同步复制；正确含义是：
L0 永久保留且可重放，L1 有独立备份，L2 任意本地故障后都能从最近签收快照加 L0 重建到一致状态。
因此 L2 的**逻辑 RPO=0**，而“本地快照最多 1 小时”只表示恢复时最多重放 1 小时，不是丢失 1 小时业务事实。

### 18.2 当前代码真实存储形态

| 功能域 | Monitor 当前保存内容 | 是否原始 | 权威重建来源 | 当前缺口/决策 |
|---|---|---:|---|---|
| 用户用量 | 小时/日 × user/channel/website_group/model/token 的请求、Token、消费/退款 quota；成员小时/日 proof、coverage、发布快照；用户/Token 当前资料快照 | 否，聚合事实 | NewAPI `logs/users/tokens`，未来为热 MySQL + 冷档案 | 事实可重建；`TrackedUser`、`CustomerGroup`、Portal、备注/跟进属于 L1，必须单独备份 |
| 渠道管理 | 当前/最近渠道配置快照、用户流量小时聚合、内部测试小时聚合、上游账号小时消费和当前余额 | 否，快照/聚合 | NewAPI 渠道/日志；第三方上游 API | 第三方接口可能只保留有限天数或账号被删除；必须永久保存每次拉取的规范化分页归档及控制 hash，否则历史上游消费不能承诺可重建 |
| 稳定性 | 分钟/小时 × channel/model/website_group 指标；Token 指标；问题签名计数和一条截断代表消息；Nginx/拒绝/主机指标聚合 | 否；代表消息也不是完整原始日志 | NewAPI 日志、Nginx/拒绝采集源、主机采样源 | NewAPI 部分可由日志档案重建；Nginx/拒绝/主机若无卷外批次归档，SQLite 目前可能是唯一副本，必须增加 `source+node+batch_id+payload_hash` 归档 |
| 缓存 | JSON 响应、singleflight 状态、临时 lease | 否 | L2 facts | 可直接丢弃；受 20k cells、4 MiB 单项、16 MiB L1 和冷查询并发 2 保护 |

渠道与稳定性不需要把所有 HTTP body、密钥或完整请求内容永久复制进 Monitor SQLite。
应在独立对象存储保存“足以重放当前聚合口径的规范化来源分区”：时间、稳定身份、所需维度、
控制总数、来源版本和内容 hash；敏感字段删除或加密，凭证只存密钥引用。SQLite 只保存服务查询所需聚合。

### 18.3 统一恢复协议，分域证明

三个域共用同一控制流程，但不能用一套 hash 假装证明所有数据：

```text
detect → enqueue(domain/source/partition/reason/version)
  → 全局来源节奏与熔断
  → 读取 L0 → 生成内存/staging 结果
  → 独立控制数/hash
  → 原子替换目标分区
  → 本地语义审计
  → 发布新 serving generation
```

- 用户用量按 `user/day` 核对消费/退款记录数、Token、消费/退款 quota、来源分区和分类版本；
- 渠道上游按 `account/domain/hour-or-day` 核对页数、记录数、金额和响应 fingerprint；
- 稳定性按 `source/node/hour` 核对输入批次 hash、请求/成功/失败/延迟控制数；
- proof 必须来自重新读取权威来源，不能根据剩余 SQLite 行“自证正确”；
- 来源暂时不可用时继续展示最后签收快照和明确水位，受影响的新范围显示 pending/unavailable，绝不显示 0；
- SQLite 整库损坏时恢复到新空卷，先恢复 L1 和最后签收 L2，再从 L0 补 checkpoint 之后的增量并全审计；
- 若第三方旧历史此前从未归档且接口已不可查询，只能把该区间标记 `source_unavailable`，不能伪称已可恢复。

### 18.4 管理员统一进度

Monitor 管理员应有一张跨域同步状态页；客户 Portal 不展示未签收成员或内部错误。每个域至少显示：

- `source`、阶段、当前 serving generation、最后签收时间；
- 已验证起止、当前 watermarks、完成分区/总分区、百分比、缺口和 repair 数；
- Tail lag、过去 15 分钟/1 小时吞吐、来源查询 P95/P99；
- 最近成功、最近错误、连续失败、下次重试、退避/熔断原因；
- 来源层（热库/冷档/第三方归档）、最近备份、最近恢复演练；
- 可解释 ETA 区间；边界或分母未知时显示“正在确认历史边界”，不显示虚假百分比。

状态接口只读增量维护的计数和 job 表，不允许为刷新进度扫描全历史 facts 或查询来源。
管理员可查看 partial 的“已覆盖小计”，但总览必须同时显示“完整 N/当前 M，正式合计排除 K 人”；
客户 Portal 始终只展示已签收成员和最后一致 serving snapshot。该取舍最适合当前业务：
管理员能回答客户进度，客户不会把部分历史误认为正式累计，已完成成员也不会被一个新成员长期阻塞。

## 19. 最短交付方案与变更边界

为避免再次一周大改，开发必须遵守以下边界：

1. **先修当前缺口（已完成代码与真实只读数据闭环）**：只实现 `candidate_gap` 持久 repair 和必要测试，修复 2026-08-01 并发布 90 天快照；
2. **再扩真实左边界**：按用户自动探测 `users.created_at + MIN(logs.created_at)`，只补现有候选左侧真实缺口；
3. **再补成员状态**：新增/删除/重加使用 `tracked_revision`；纠正所属公司只更新当前投影、审计和成员 fingerprint；
4. **再加永久作业控制**：复用现有 source gate、小时 Tail、日事实、proof、candidate/serving、query/cache；只增加持久 job、节奏/熔断和独立来源控制；
5. **最后补跨域归档**：先保证 L1 卷外备份；随后为第三方上游和 Nginx/拒绝/主机批次建立 L0 归档。该工作不阻塞已经可重建的用户用量本机恢复，但未完成前不能宣称相应域可从任意整卷损坏中零丢失恢复；
6. 每个工作包都必须可单独回滚、单独验收，不修改 NewAPI，不复制已有 facts 引擎，不在恢复页面前进行目录级纯重构。

时间只能作为排期窗口，不能替代验收证据。按现有生产规模和已知边界，当前缺日是分钟级来源工作量，
候选左侧若确为约 7 天是小时级工作量；若实际来源更早或出现慢查询，程序应自动延长 ETA，
而不是提高并发或牺牲中转站稳定性。正式完成的定义始终是：数值对账、故障恢复、来源负载、
权限和真实页面共同通过，而不是“任务跑到了 100%”。
