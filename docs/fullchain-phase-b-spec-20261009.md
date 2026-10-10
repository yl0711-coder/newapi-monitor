# 全链路第二批 · B 期（Incident 核心）实施规格与对照清单

日期：2026-10-09。状态：**规格待确认，未动代码。**
范围：MON-003 / 004 / 005 / 006。
权威顺序：**01.3 > 02.1 / 02.2 > 第二批分期计划**。本文不新造任何字段、枚举或指标语义。

基线：分支 `release/v1.18.1`，HEAD `ab93262`，工作区 25 个已验收 M 文件为起点。
8204 运行 `newapi-monitor:v119`，只读 SSH 隧道已通。

> 本文是**实施前规格**。第 9 节「待确认项」未获答复前不进入编码；
> 第 8 节「跨期依赖」中的项目一律不在 B 期宣称完成。

---

## 1. 复用核查结论（先查谁已有，再决定建什么）

已核查 `internal/observability/`、`monitor/alert*.go`、`monitor/store.go`、
`monitor/logchain_radius.go`、`monitor/logchain_fault.go`、`monitor/stability*.go`、
`monitor/sampler.go`、`monitor/group_governance.go`、`monitor/server.go`、CI 分片门禁。

### 1.1 直接复用，不重新实现

| 能力 | 位置 | B 期用法 |
|---|---|---|
| 24 项 `fault_class` | `internal/observability/contract.go:21-58` | `primary_fault_class` / `companion_fault_classes` 取值域 |
| 4 级 `evidence_level` | 同上 `:10-19` | 证据等级，**不得加第五级** |
| 5 个治理状态 | 同上 `:60-70` | 前三层状态取值域 |
| 4 级 SEV | 同上 `:72-81` | 事故严重度 |
| RFC3339 UTC 校验 | `identifiers.go:26-34` | 所有机器时间 |
| HMAC 引用（带 key_id） | `identifiers.go:41-81` | 证据引用，`DisallowUnknownFields` 已防裸 ID 泄漏 |
| 7 项指标 + 未知值纪律 | `metrics.go:11-99` | 影响面指标；`nil` 必须配 `unavailable` |
| `requireRole` / `roleAdmin=10` / `roleRoot=100` | `monitor/auth.go:34-35, 270-295` | 读走 view 组，审批走 root 组 |
| `noStoreSensitive` | `monitor/server.go:144-148` | 挂在路由组而非 handler 内 |
| 操作人身份 `c.GetString("uname")` | `monitor/auth.go:292` | 审批轨迹的 actor，**明文**（审批人要可读） |
| 只增审计台账形状 | `monitor/usage_member_lifecycle.go:69-94` | 照搬行形状，不照搬 HMAC 化 |
| 乐观并发 + 幂等审批 | `monitor/channel_cost_decision.go:250-430` | MON-006 审批与冲突仲裁的**现成范式** |
| revision 守卫 + 同事务写审计 | `monitor/infra_assets.go:186-226` | 状态跃迁原子性 |
| plan ID 门禁 | `monitor/store_migration_backup.go:39-40` | 见 §4 |
| 渠道快照 `ChannelSnap` + 软删保留 | `monitor/store.go:292-311, 1395-1418` | `applied_newapi_state` 的既有数据源 |
| 渠道同步 worker（LOW 泳道） | `monitor/sampler.go:1355-1379` | 1~5 分钟漂移检测挂这里 |
| 窗口级覆盖证明 | `monitor/stability_backfill.go:235-300` | 影响面覆盖缺口标注 |
| 分桶缺口状态机 | `monitor/stability.go:609-662` | MON-004 缺口判定 |
| 截断显式标记 | `monitor/logchain_radius.go:47-65` | `OtherItems/OtherCount` 防「分页冒充全量」 |
| 归因判据 | `monitor/logchain_fault.go` | **唯一事实源，只调用不复制** |

### 1.2 确认不存在，B 期从零建

`Incident` 实体、去重键、冷却期、四层状态存储、状态跃迁表、冲突仲裁、
限时人工覆盖、漂移事件、通用审计表。
（全仓 grep `Incident` 仅命中两处注释，均写明「本文件不是 Incident」。）

### 1.3 不复用及原因

- `AlertLog` + `inCooldown`（`alert.go:123-130, 184-191`）：按**告警类别**冷却，
  不是六维事故键，且无事故实体。B 期另建，**不动既有告警行为**。
- `logChainInvestigationTask`（`logchain_investigation.go:138-155`）：内存态短期排障任务，
  进程重启即失。**不能当事故记录**（这正是 MON-003 要解决的问题）。
- `CloudWatchInvestigationAudit` 的 HMAC 化身份：审批轨迹需人可读审批人，只借行形状。
- 问题预警页（`alerts_page.go`）：前置拒绝统计，与事故聚合不同口径，不合并。

---

## 2. 数据模型

新增 **6 张表**，全部落**主库**（`monitor.db`，不是 usage-facts 库）。
时间列沿用既有 Unix 秒 `int64` 存储 + 对外 RFC3339 UTC 投影（兼容既有约定）。

### 2.1 `Incident` — 事故主体

```
id                      string  PK   incident_id，Monitor 生成，全局唯一
schema_version          string       固定 observability.v1
-- 六维稳定键（滚动窗口绝不入键）--
primary_fault_class     string       observability.FaultClass
channel_id              int64        0 = 未命中渠道（见 §9 待确认 Q3）
model                   string
user_group              string       = logs.group（website_group），非 CustomerGroup
protocol                string       见 §9 待确认 Q1
route                   string       = other.request_path
config_change_ref       string       空串 = 无变更段
dedup_key               string  UNIQUE  六维 + config_change_ref 的规范化哈希
-- 观测属性（非键）--
severity                string       SEV0..3
observed_state          string       治理状态
first_seen_ts           int64
last_seen_ts            int64
cooldown_until_ts       int64
status                  string       open / cooling / closed
companion_fault_classes string       JSON 数组
evidence_level          string       事故级最高证据等级
revision                int64        乐观并发
created_at/updated_at   int64
```

唯一索引：`uniqueIndex` on `dedup_key`。
辅助索引：`(last_seen_ts)`、`(channel_id, last_seen_ts)`、`(severity, status)`。

**去重语义**：`dedup_key` 由六维 + `config_change_ref` 规范化拼接后哈希。
同键在 `cooldown_until_ts` 内 → `UPDATE` 原行（刷新 `last_seen_ts`、必要时升级 severity）；
超期 → 新建行。`config_change_ref` 变化即换键，自然实现配置变更前后分段。
**不同 channel_id / protocol 产生不同 key，结构上不可能误合并。**

### 2.2 `IncidentObservation` — 事故证据观测

```
id                int64  PK auto
incident_id       string  idx
source            string       newapi / nginx / reject / host / cloudwatch
source_event_id   string       来源内唯一，重放去重
observed_ts       int64
fault_class       string
evidence_level    string
window_from_ts    int64
window_to_ts      int64
sample_count      int64
coverage_complete bool         false = 本次观测有缺口
metrics_json      string       observability.Metrics（未知即 unavailable）
detail_json       string       已脱敏摘要，不含正文/密钥
```
唯一索引：`(source, source_event_id)` —— 重放幂等。

### 2.3 `IncidentImpact` — MON-005 影响面

```
id                 int64 PK
incident_id        string idx
dimension          string   channel / model / protocol / group / node / user
item               string   维度取值（user 维度存 HMAC 引用，不存原值）
distinct_requests  int64    独立请求数
attempt_count      int64    当前可见尝试数（按渠道+秒级时间去重，复用 logchain.js:887 口径）
rows_without_channel int64  取不到渠道、退化为按记录计数的行数（logchain.js:889）
unlinkable_rows    int64    无 Request ID、无法按请求归并的行数（logchain.js:877）
attempts_exact     bool     无退化且无不可关联行时为真；即便为真也只是「当前可见」
log_rows           int64    日志条数
-- 三者分列存储，不合并、不互相代替 --
window_from_ts     int64
window_to_ts       int64
coverage_complete  bool
coverage_gap_note  string   缺口说明，覆盖不全时必填
truncated          bool     维度取值被截断
other_count        int64    截断掉的条目数
```
唯一索引：`(incident_id, dimension, item, window_from_ts)`。

### 2.4 `IncidentState` — MON-006 四层状态

```
incident_id             string PK（一事故一行）
observed_state          string   Monitor 算出
recommended_state       string   建议
recommended_by          string   monitor / eval
approved_state          string   人工批准，空 = 未批准
approved_by             string   明文
approved_reason         string
approved_at_ts          int64
override_expires_ts     int64    限时人工覆盖到期，0 = 无覆盖
override_recovery_cond  string
-- 实际配置层：保留真实值，不转译为治理状态 --
applied_enabled         *bool
applied_group           string
applied_priority        *int64
applied_weight          *int64
applied_synced_at_ts    int64
applied_snapshot_stale  bool
drift_detected          bool
drift_detail            string
revision                int64
```
前三层取值域 = 5 个治理状态。
**`applied_*` 存 NewAPI 真实启停/分组/优先级/权重，为空用 `nil` 不用零值**
（按宪法：缺失绝不显示为零）。

### 2.5 `IncidentStateTransition` — 审批轨迹（只增）

```
id           int64 PK
incident_id  string idx
layer        string   observed / recommended / approved / applied_newapi
from_state   string
to_state     string
actor        string   明文；系统写入为 "system"
reason       string
evidence_ref string
occurred_ts  int64
```
只增不改不删（照 `UsageMemberAudit` 形状）。

### 2.6 `IncidentContinuity` — MON-004 连续窗口判定状态

```
dedup_key             string PK
consecutive_windows   int64
last_window_to_ts     int64
baseline_value        *float64   nil = 基线不可用
baseline_sample       int64
last_window_sample    int64
low_sample            bool
gap_windows           int64      缺采集窗口数
hard_signal           bool
multi_source_confirmed bool
updated_at            int64
```

**缺口纪律**：窗口无数据或采集缺失时 → `gap_windows++`，
`consecutive_windows` **既不递增也不清零**（冻结）。
即缺口既不算连续异常，也不算恢复。这条由专门测试钉住。

---

## 3. 接口

全部挂既有路由组，不新建引擎。读走 `view`（roleAdmin），写走 roleRoot 组。
敏感响应挂在**路由组**上的 `noStoreSensitive`。

| 方法 | 路径 | 权限 | 说明 |
|---|---|---|---|
| GET | `/incidents` | roleAdmin | 列表；时间窗/渠道/模型/协议/分组/fault_class/严重度/状态检索 |
| GET | `/incidents/:id` | roleAdmin | 详情：六维键、四层状态、影响面、观测时间线、去重说明 |
| GET | `/incidents/:id/impact` | roleAdmin | 影响面（带覆盖证明与缺口标注） |
| GET | `/incidents/:id/transitions` | roleAdmin | 审批/跃迁轨迹 |
| POST | `/incident/approve` | roleRoot | 写 `approved_state`；幂等键 + revision 乐观并发 |
| POST | `/incident/override` | roleRoot | 限时人工覆盖（必填理由/有效期/恢复条件） |
| POST | `/incident/search` | roleAdmin | 精确 user/request 检索走 **POST body**，不入 URL |

- 响应 `schema_version` 必填；未识别主版本**拒绝并告警**，不静默降为 `unknown`。
- 对外时间 RFC3339 UTC；页面显示北京时间并保留时区。
- 前端：`page.html` 新增 `incidents` 页签 —— **侧栏与小屏 nav 两处都要改**，
  新增 `incidents.js` 并在 `page.html` 挂 `?v=1`（改文件必同步 bump）。

---

## 4. 迁移方案

1. 6 张新表加入主库 AutoMigrate（`monitor/store.go:1006-1027`）。
2. **必须 bump plan ID 两份**（`store_migration_backup.go:39-40`）：
   v58 → **v59**，标记 `incident-core-v1`，`preMigrationCombinedPlanID` 同步。
   已有测试会断言新表进入 AutoMigrate 后 planID 必须出现对应标记。
3. 启动时自动在 AutoMigrate 前锁库 + `VACUUM INTO` 快照（既有机制，不改）。
4. 回滚三件套：**旧镜像 + v58 迁移前快照 + 新卷**。禁止只换 tag 复用已迁移卷。
5. 纯新增表，不改既有列、不删列、不改既有语义 → 既有页面数据口径零影响。
6. feature flag：`MONITOR_INCIDENT_CORE_ENABLED`（默认 **false**），
   按 02.1 §10「可回滚」要求。关闭时不建表写入、不起 worker、页签不显示。

---

## 5. 必要依赖

| 依赖 | 现状 | B 期处理 |
|---|---|---|
| `fault_class` 等共享枚举 | A 期已有 | 直接用 |
| 归因判据 | `logchain_fault.go` 已有 | 调用，不复制；缺契约映射器需新建（见 §9 Q2） |
| 渠道配置快照 | `ChannelSnap` 已有，sampler 60s | 复用；漂移检测挂 LOW 泳道 |
| 生产库访问闸门 | `source_lifecycle.go` | 新查询**必须过闸门**，不另开连接 |
| 共享泳道 | `usageDetailGate` 容量 1 | **事故查询不得占用**（见 §9 Q4） |
| JSON Schema 漂移守卫 | `schema.go:20-76` 反射重建 | 加 Incident `$defs` 必须同步改生成函数，否则测试失败 |
| CI 竞态分片 | 6 个分片按**测试函数名首字母** | 新测试名必须落且仅落一个分片；`TestIncident*` → `g-n` 分片 |

---

## 6. 需求条目 → 实现位置 → 测试用例 对照清单

> 「验收结果」列实施后才填，现在一律空白。不得预先标成已完成。

### MON-003 事故持久化与去重

| 需求条目（来源） | 实现位置 | 测试用例 | 验收结果 |
|---|---|---|---|
| 可持久保存 Incident，不用短期排障任务代替（02.1 MON-003 / 计划 §3） | `incident.go` `Incident` 表 | `TestIncidentPersistsAcrossRestart` | |
| 六维聚合键（01.3 §8） | `incident_dedup.go` `buildDedupKey` | `TestIncidentDedupKeySixDimensions` | |
| 滚动窗口不入键（01.3 §8 / 02.2 §7） | 同上，窗口仅存观测列 | `TestIncidentDedupKeyExcludesRollingWindow` | |
| 同键冷却期内更新原事故（01.3 §8） | `incident_engine.go` upsert | `TestIncidentSameKeyWithinCooldownUpdatesOriginal` | |
| 冷却期外新建 | 同上 | `TestIncidentSameKeyAfterCooldownCreatesNew` | |
| 不同渠道/协议不误合并（01.3 §8） | `dedup_key` 结构保证 | `TestIncidentDifferentChannelNeverMerges`、`TestIncidentDifferentProtocolNeverMerges` | |
| `config_change_ref` 前后分段（01.3 §8） | 键含该字段 | `TestIncidentConfigChangeRefSegments` | |
| 重启后事故与去重关系不丢 | SQLite 持久化 | `TestIncidentDedupSurvivesRestart` | |
| 并发更新安全（任务要求） | `revision` 乐观并发 | `TestIncidentConcurrentUpsertNoDuplicate`（`-race`） | |
| 严重度升级立即通知（01.3 §8） | 预留 hook；**通知闭环属 C 期** | `TestIncidentSeverityUpgradeMarksImmediate` | |
| 迁移兼容（任务要求） | plan ID v59 | `TestIncidentPlanIDBumped` | |

### MON-004 连续异常判定

| 需求条目 | 实现位置 | 测试用例 | 验收结果 |
|---|---|---|---|
| 连续窗口状态（02.1 MON-004） | `IncidentContinuity` | `TestIncidentContinuityCountsConsecutiveWindows` | |
| 最小样本门（01.3 / 既有 minSample=20） | `incident_continuity.go` | `TestIncidentLowSampleDoesNotTriggerIncident` | |
| 基线（30 天，02.1 §3.1） | 复用 `stability` 基线 | `TestIncidentBaselineUnavailableBlocksSlowFault` | |
| 硬信号可立即触发（02.1 §8） | `hard_signal` 分支 | `TestIncidentHardSignalTriggersWithoutConsecutive` | |
| 多源确认（02.1 §8 SEV1） | `multi_source_confirmed` | `TestIncidentSev1RequiresTwoWindowsOrTwoSources` | |
| **缺采集间隔不算连续异常** | `gap_windows` 冻结逻辑 | `TestIncidentGapWindowNotCountedAsAnomaly` | |
| **缺采集间隔不算恢复** | 同上 | `TestIncidentGapWindowNotCountedAsRecovery` | |
| 低样本/盲区分流（02.1 MON-004） | SEV3 + 数据盲区标记 | `TestIncidentBlindSpotNotReportedHealthy` | |
| `client_gone`/单次 429/5xx/语义质量不单独产生隔离建议（02.1 §8） | 建议生成守卫 | `TestIncidentSingleSignalNeverSuggestsQuarantine` | |

### MON-005 事故影响范围

| 需求条目 | 实现位置 | 测试用例 | 验收结果 |
|---|---|---|---|
| 渠道/模型/协议/分组/节点/用户六维（02.1 MON-005） | `IncidentImpact` | `TestIncidentImpactAllDimensions` | |
| 明确统计窗口 + 覆盖证明（02.1 §6.1 / 任务要求） | `window_*` + `coverage_complete` | `TestIncidentImpactCarriesWindowAndCoverage` | |
| **分页部分日志不冒充全量**（计划 §3 MON-005） | 复用 `PageHasMore`/截断标记 | `TestIncidentImpactPagedSampleNotFullScope` | |
| **日志条数/尝试次数/独立请求数不混用** | 三列分存 + 退化说明列 | `TestIncidentAttemptsDedupesByChannelAndSecond`、`TestIncidentAttemptsDegradationIsVisible`、`TestIncidentAttemptsUnlinkableRowsReported` | 已实现并通过 |
| 尝试次数不冒充实际网络尝试数 | `CompletenessNote()` 恒声明可见性局限 | `TestIncidentAttemptsAlwaysNotesVisibilityLimit` | 已实现并通过 |
| 尝试次数不得证明客户端重试循环 | `ProvesClientRetryLoop()` 恒 false | `TestIncidentAttemptsNeverProveClientRetryLoop` | 已实现并通过 |
| 分类、证据等级、责任分别判断 | `DecideResponsibility()` 三输入独立 | `TestResponsibilityCeilingByEvidenceLevel` | 已实现并通过 |
| 置信度与证据等级不互换 | `EvidenceLevelFor()` 只收关联方式 | `TestEvidenceLevelFromCorrelationNotConfidence` | 已实现并通过 |
| 429 默认不归责 | 需关联 + 指向上游成因双条件 | `TestRateLimitDefaultsToInsufficientEvidence` 等 3 项 | 已实现并通过 |
| 空输出不归上游、不支持 DEGRADED | 无条件 `insufficient_evidence` | `TestEmptyOutputNeverBlamesUpstream`、`TestEmptyOutputSupportsNoDisposition` | 已实现并通过 |
| 计费异常不凭日志来源归平台 | 无条件 `insufficient_evidence` | `TestBillingAnomalyNotBlamedOnPlatformByLogSource` | 已实现并通过 |
| 无观察点分类非永久禁止 | `CanProduceFaultClass` 双向 | `TestFaultClassNeedingExternalEvidenceNotProducedWithoutIt` | 已实现并通过 |
| Eval 不能代替客户端遥测 | 两类需 `hasClientTelemetry` | `TestClientFaultsRequireClientTelemetryNotEval` | 已实现并通过 |
| 覆盖不全保留已知事实并标注缺口 | `coverage_gap_note` 必填 | `TestIncidentImpactIncompleteCoverageKeepsFactsAndFlagsGap` | |
| 不显示为 0（宪法 / 02.1 §6.1） | 未知用 nil | `TestIncidentImpactUnknownNotZero` | |
| 用户维度不落原始标识（02.1 §9） | HMAC 引用 | `TestIncidentImpactUserDimensionHashed` | |

### MON-006 四层状态与审计

| 需求条目 | 实现位置 | 测试用例 | 验收结果 |
|---|---|---|---|
| 四层字段严格分离（01.3 §6 / 计划 §2） | `IncidentState` | `TestIncidentFourLayerStatesSeparate` | |
| 前三层只用 5 个治理状态 | 枚举校验 | `TestIncidentGovernanceStateEnumOnly` | |
| **实际配置保留真实启停/分组/优先级/权重，不当治理状态** | `applied_*` 原值列 | `TestIncidentAppliedStateKeepsRawNewAPIValues` | |
| 1~5 分钟只读同步实际配置（01.3 §6） | LOW 泳道 worker | `TestIncidentAppliedStateSyncInterval` | |
| 漂移生成 `configuration_drift` 数据质量事件，不自动覆盖（01.3 §6） | `drift_detected` | `TestIncidentDriftFlaggedNeverAutoApplied` | |
| `correlated` 最多 DEGRADED（01.3 §5） | 建议守卫 | `TestIncidentCorrelatedCapsAtDegraded` | |
| `inferred` 只给排查方向 | 同上 | `TestIncidentInferredGivesDirectionOnly` | |
| `unavailable` 不支持健康或处置结论 | 同上 | `TestIncidentUnavailableNoHealthConclusion` | |
| **即使 exact 也不自动隔离** | 同上 | `TestIncidentExactStillRequiresApproval` | |
| 冲突仲裁五行表（01.3 §12） | `incident_arbitration.go` | `TestIncidentArbitrationMatrix`（5 行逐行） | |
| 任一关键源不可用不判 HEALTHY（01.3 §12） | 仲裁守卫 | `TestIncidentKeySourceUnavailableNeverHealthy` | |
| 限时人工覆盖（理由/批准人/有效期/恢复条件） | `override_*` | `TestIncidentOverrideRequiresAllFields` | |
| 覆盖到期重新计算，不永久压制（01.3 §12） | 到期检查 | `TestIncidentOverrideExpiryRecomputes` | |
| 审批轨迹完整只增 | `IncidentStateTransition` | `TestIncidentTransitionLedgerAppendOnly` | |
| 审批幂等 + 乐观并发 | 复用 `channel_cost_decision` 范式 | `TestIncidentApproveIdempotent`、`TestIncidentApproveRevisionConflict` | |
| 自动写 NewAPI 为 0（02.1 §11.7） | 无写路径 | `TestIncidentNeverWritesNewAPI` | |

### 回归（不得影响既有功能）

| 对象 | 测试 |
|---|---|
| 客户维护 | 既有 `customer_health_test.go` 全量 + `dev/tests/customer-health-*.test.mjs` |
| 客户排障 | 既有 `logchain_*_test.go`、`logchain_ui_test.go` |
| 问题预警 | 既有 `alerts*` 测试 + `dev/tests/alerts-watermark.test.mjs` |
| 模型统计 | `model_statistics_test.go` + `dev/tests/model-statistics-window.test.mjs` |
| 归因judge一致性 | 既有 `TestStabilityFaultMatchesLogchain` |
| 共享泳道不被挤占 | 新增 `TestIncidentQueryDoesNotOccupyUsageDetailGate` |
| CI 分片 | `node dev/check-race-shards.mjs` |

---

## 7. B 期明确不做（不得宣称完成）

- **MON-007** 完整 SEV 通知闭环、outbox、ACK、超时升级、静默、双通道、dead-man → **C 期**
- **MON-008** Monitor–Eval 联调、Eval Run 创建、结果消费、DLQ、撤回 → **D 期**
- **MON-009** 人工处置包完整形态 → C 期
- **MON-010** 24h/7d/30d 统一口径与 30 天基线 → E 期
- **两段式恢复门禁**（01.3 §13）：B 期只建状态字段与跃迁记录，
  **不实现门禁判定**，不能用 FRT 代替 TTFT 通过恢复 → D 期
- MON-001 持久统一 Source Event 层、MON-002 全节点 Owner/凭证/时钟 → 仍是缺口
- A 期只读投影不因 B 期而升级为「完整统一事件系统」

---

## 8. 跨期依赖

| 依赖项 | 被谁阻塞 | 影响 B 期什么 |
|---|---|---|
| 完整 Source Event envelope 未冻结（MON-001） | 双方接口评审 | `IncidentObservation` 先只接**本地已有事实**，外部事件接入留 D 期 |
| Eval 接口路径/鉴权/事件顺序未冻结（MON-008） | Monitor–Eval 双方 | `IncidentObservation.source` 预留 `eval` 取值但 B 期不写入 |
| 全节点 source/node Owner、凭证到期、时钟偏差未接通（MON-002） | 采集侧 | 多源确认只能用已接通的源；未接通源计 `telemetry_gap` |
| `protocol` 维度生产无对应字段 | 见 §9 Q1 | **阻塞去重键定稿** |
| 协议级观察点缺失 | 外部需加事件 | 协议类 fault_class 无法产出，不是「无故障」 |
| 通知通道与接收人未确认 | 运维 | 严重度升级只落标记，不发通知 |

---

## 9. 待确认项（文档未明确，不自行猜测）

按阻塞程度排序。**Q1 不答复无法定稿去重键。**

**Q1（阻塞）`protocol` 维度取什么值？**
01.3 §8 要求六维含 `protocol`，但生产 `logs` 表**没有 protocol 字段**。
现有可得的只有：`other.request_path`（填充率 100%，取值
`/v1/chat/completions`、`/v1/responses`、`/v1/messages`、`/pg/chat/completions`）
和 `is_stream` 布尔。
01.3 §7 的协议分类是 `Chat 非流式 / Chat SSE / Responses / Claude 非流式 / Claude SSE`。
候选：(a) 由 `request_path + is_stream` 派生出这 5 类；
(b) `protocol` 与 `route` 都取 `request_path`（则两维冗余）；
(c) 暂记 `unknown` 等 MON-001 补采。
**我倾向 (a)**，它与 01.3 §7 的五类协议成功条件一一对应，但这是派生而非源数据，需您确认是否接受。

**Q2 `logchain_fault.go` 的本地归因类别 → 24 项 `fault_class` 的映射由谁定稿？**
目前只有 `observability_investigation.go:61-125` 一个局部映射器。
本地类别与契约枚举非一一对应，且部分契约值 Monitor 当前**永远无法产出**
（协议级、thinking、工具、客户端三类无观察点）。
我可以产出映射表并把无观察点的值明确标为不可产出 —— 这算 B 期范围，还是需要单独评审？

**Q3 `channel_id` 在「路由前拒绝/无可用渠道」时取什么？**
`route_no_channel` 事故本就没有渠道。既有纪律是 `channel_id=0` 不归责渠道。
键里用 `0` 可行，但会把所有无渠道事故按其余五维聚合 —— 确认可接受？

**Q4 事故查询走哪条泳道？**
`usageDetailGate` 容量 1，与客户 Portal 日志查询同泳道。
事故页查询若共用，会挤占客户查询（这是既有教训）。
候选：(a) 新建独立 gate；(b) 走 `acquireBackgroundSourceLow`。
**我倾向 (a) 独立 gate + 独立超时**，避免任何挤占。

**Q5 冷却期默认多长？**
01.3 只说「冷却期内更新原事故」，未给数值。
既有告警按类别 `categoryCooldownMin` 配置。
我建议按 SEV 分级可配（SEV0/1 短、SEV2/3 长），默认值请您定，或我先给 shadow 默认值
（按 02.1 §8「初始阈值仅用于 shadow，至少 7 天后校准」）。

**Q6 连续窗口的窗口大小与连续次数阈值？**
02.1 §8 只给定性（SEV1 两个连续窗口、SEV2 至少三个窗口），未给窗口时长。
既有 `anomalyBurst` 用 spark 桶。需确认窗口粒度（1 分钟/5 分钟/小时）。

**Q7 B 期是否需要前端页签？**
任务说「功能能正常使用、展示数据有依据」，02.1 §6.2 要求 Incident 工作台。
但完整工作台含 Eval Run、处置包（C/D 期内容）。
候选：(a) B 期出只读列表+详情页签，C/D 期增补；(b) B 期只做后端 + API，页面留 C 期。
**我倾向 (a)**，否则无法验「功能能正常使用」。

**Q8 feature flag 默认值？**
我按 02.1 §10 设 `MONITOR_INCIDENT_CORE_ENABLED` 默认 false。
8204 验收时需显式开启。确认接受？

---

## 10. 守住的边界（实施时逐条自查）

- 只读旁路：只写 Monitor 本地 SQLite，**不改 NewAPI 代码/生产库/渠道配置**
- 自动写 NewAPI 数量 = 0，由 `TestIncidentNeverWritesNewAPI` 钉住
- 证据不足不下确定结论：`correlated` ≤ DEGRADED，`inferred` 仅方向，
  `unavailable` 不支持健康或处置结论，`exact` 仍需人工审批
- 缺失绝不显示为零；未知用 `nil` + `unavailable`
- 不采集正文/密钥/Authorization/thinking/工具参数；`raw_error` 只存脱敏摘要
- 敏感标识走 POST body，不入 URL、浏览器历史、接入日志
- 纯新增，不改既有列与口径；既有五个页面行为零变化
- 不新建 IAM 资源，复用 `nexusapi-monitor-ro`
- 改 js/css 同步 bump `page.html` 的 `?v=N`；侧栏与小屏 nav 两处都改
- 不自动提交；动手前先 `.local-test-kit/snap.sh` 做仓库外还原点
