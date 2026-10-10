# B 期待确认项定稿方案（Q1～Q8 回复）

日期：2026-10-09。状态：**方案待确认，未动实现代码。**
配套：`docs/fullchain-phase-b-spec-20261009.md`（数据模型/接口/迁移/对照清单）。
依据顺序：**01.3 > 02.1 / 02.2 > 第二批分期计划**。

> 本文回复您 2026-10-09 给出的八条口径。Q1/Q2/Q3/Q4/Q7/Q8 已按您的口径定稿；
> **Q5 冷却期、Q6 窗口粒度给出具体建议值与依据，等您确认后才写入默认值**。

---

## Q1 入口协议派生（已按口径定稿）

### 1.1 命名：只叫「入口协议」，不叫「协议」

机器字段名 **`ingress_protocol`**，不叫 `protocol`。
理由：您明确要求「不能据此认定实际上游协议、协议调用成功」。
字段名自带边界，比靠注释约束可靠。01.3 §8 聚合键中 `protocol` 维度
由 `ingress_protocol` 承担，并在接口与页面标注「入口协议（派生）」。

### 1.2 四路径 × is_stream → 五类协议映射

既有生产实测依据（已固化在 `monitor/logchain.go:280-293`）：
2026-08-25 近 5 天 197371 行 type=2，`other.request_path` 填充率 **100%**，
取值只有 `/v1/responses`、`/v1/chat/completions`、`/v1/messages`、`/pg/chat/completions`。

> ⚠️ 该实测是 2026-08-25 的 type=2 口径。本期要覆盖 type=5（错误日志），
> 错误行上 `request_path` 的填充率**尚未实测**（见 §1.6 未验证项）。

| `request_path` | `is_stream` | `ingress_protocol` | 01.3 §7 对应协议 |
|---|---|---|---|
| `/v1/chat/completions` | 0 | `chat_nonstream` | Chat 非流式 |
| `/v1/chat/completions` | 1 | `chat_sse` | Chat SSE |
| `/pg/chat/completions` | 0 | `chat_nonstream` | Chat 非流式 |
| `/pg/chat/completions` | 1 | `chat_sse` | Chat SSE |
| `/v1/responses` | 0 | `responses` | Responses |
| `/v1/responses` | 1 | `responses` | Responses |
| `/v1/messages` | 0 | `claude_nonstream` | Claude 非流式 |
| `/v1/messages` | 1 | `claude_sse` | Claude SSE |

两点说明：

- `/pg/chat/completions` 与 `/v1/chat/completions` 归同一类：
  `/pg/` 是 playground 前缀，协议形状相同。**路径原值另存**（见 §1.4），
  聚合时合并、追溯时可分。
- `/v1/responses` 的流式与非流式**都归 `responses`**：
  01.3 §7 的五类里 Responses 只有一类，没有拆 SSE。
  若您要求拆成 `responses_nonstream` / `responses_sse`（变六类），我按您说的改 —— 这是
  我对 01.3 §7 的读法，不是实测结论。

### 1.3 显式处理缺失、无法识别、矛盾（不默认归 Chat 非流式）

机器枚举共 **8 个值**，五类正常 + 三类异常，异常值不可省：

```go
type IngressProtocol string

const (
    IngressChatNonStream   IngressProtocol = "chat_nonstream"
    IngressChatSSE         IngressProtocol = "chat_sse"
    IngressResponses       IngressProtocol = "responses"
    IngressClaudeNonStream IngressProtocol = "claude_nonstream"
    IngressClaudeSSE       IngressProtocol = "claude_sse"

    // 以下三者绝不可被折叠成上面任何一类
    IngressUnknown      IngressProtocol = "ingress_unknown"      // 路径缺失/空/other 非法 JSON
    IngressUnrecognized IngressProtocol = "ingress_unrecognized" // 路径有值但不在白名单
    IngressConflict     IngressProtocol = "ingress_conflict"     // 数据自相矛盾
)
```

判定顺序（**先异常后正常**，防止缺失被静默兜底）：

1. `other IS NULL` 或 `NOT JSON_VALID(other)` 或 `request_path` 缺失/空/`"null"`
   → `ingress_unknown`，且 `evidence_level = unavailable`
2. `is_stream IS NULL`（流式状态未知，无法区分 SSE 与非流式）
   → 路径在 Chat/Claude 白名单时为 `ingress_conflict`；
   路径为 `/v1/responses` 时仍可定 `responses`（该类不依赖 is_stream）
3. `request_path` 有值但不在四项白名单
   → `ingress_unrecognized`（**不是** unknown —— 这是「出现了新端点」的信号，
   需要人去看，不能和「没采到」混在一起）
4. 命中白名单 → 按 §1.2 表派生

`ingress_unknown` / `ingress_unrecognized` / `ingress_conflict` 三者：

- 是**合法的聚合键取值**，会正常建事故（不丢数据）
- 但页面与接口显示为「入口协议未知／未识别／数据矛盾」，不显示成某个具体协议
- `ingress_unknown` 不参与「协议健康」判断（无观察点 ≠ 正常）

### 1.4 来源可追溯 + 派生规则版本

`Incident` 与 `IncidentObservation` 各增三列：

```
ingress_protocol         string   派生结果（上面 8 个枚举之一）
ingress_route_raw        string   request_path 原值，未归并、未改写
ingress_stream_raw       *bool    is_stream 原值；NULL 保持 nil 不写 false
ingress_rule_version     string   派生规则版本，当前 "ingress-proto-v1"
```

- `ingress_route_raw` 保留 `/pg/` 与 `/v1/` 的区别，派生合并不丢原值。
- `ingress_stream_raw` 用 `*bool`：**NULL 不写成 false**（宪法：缺失不显示为零）。
- `ingress_rule_version` 随映射规则变更递增（`v1` → `v2`），
  历史事故保留当时版本，不被追溯改写。规则版本变更**不进聚合键**
  （否则改一次规则全部事故重开），但在详情页显式展示。

### 1.5 测试用例

| 用例 | 断言 |
|---|---|
| `TestIngressProtocolMapsFourPathsToFiveClasses` | 8 组（路径×流式）组合逐一对映射表 |
| `TestIngressProtocolMissingPathIsUnknownNotChat` | 路径缺失 → `ingress_unknown`，**不是** `chat_nonstream` |
| `TestIngressProtocolInvalidJSONIsUnknown` | `other` 非法 JSON → `ingress_unknown` + `unavailable` |
| `TestIngressProtocolUnlistedPathIsUnrecognized` | 新端点 → `ingress_unrecognized`，与 unknown 区分 |
| `TestIngressProtocolNullStreamIsConflictForChat` | Chat 路径 + `is_stream` NULL → `ingress_conflict` |
| `TestIngressProtocolNullStreamStillResponses` | `/v1/responses` + NULL → `responses`（不依赖 is_stream） |
| `TestIngressProtocolPgPrefixSharesClassKeepsRawRoute` | `/pg/` 归 `chat_*` 但 `ingress_route_raw` 保原值 |
| `TestIngressProtocolStreamRawNullNotFalse` | `ingress_stream_raw` 为 nil 不落 false |
| `TestIngressProtocolRuleVersionRecorded` | 写入 `ingress-proto-v1` |
| `TestIngressProtocolRuleVersionNotInDedupKey` | 规则版本变更不改 `dedup_key` |
| `TestIngressProtocolNeverRaisesEvidenceLevel` | 派生成功**不**提升 evidence_level（见 §1.6） |
| `TestIngressProtocolNotUpstreamProtocol` | 派生值不写入任何上游协议字段 |

### 1.6 边界（写进代码注释，不只写文档）

- 入口协议**只说明客户端打进来的端点形状**，不证明上游用了同一协议。
- 派生成功**不提高 evidence_level**：路径字段齐全只说明入口可识别，
  与上游证据强度无关。`TestIngressProtocolNeverRaisesEvidenceLevel` 钉死。
- 不据此认定「协议调用成功」—— 成功判定属 01.3 §7 协议终态，需 Eval（D 期）。

**未验证项**：type=5 错误行的 `request_path` 填充率。
既有 100% 实测只覆盖 type=2。我设计上已让缺失走 `ingress_unknown` 而非兜底，
所以填充率低也不会产生错误归类，但**错误事故里 unknown 占比可能偏高**。
这需要一次生产只读抽样才能给出数字 —— 见文末「需要您授权的一件事」。

---

## Q2 证据条件 → 故障分类 → 责任归属（已按口径定稿）

### 2.1 三者严格分离

按您的要求，建立三级而非两级映射。**故障分类 ≠ 责任归属**：

```
证据条件  →  fault_class（01.3 的 24 值，描述"发生了什么"）
          →  responsibility（描述"该找谁"，独立字段）
```

责任归属枚举（沿用 `logchain_fault.go` 已验证的归因结果，不新造）：

```
responsibility: upstream / platform / client / user_config / unknown / insufficient_evidence
```

**`insufficient_evidence` 与 `unknown` 是两个值，不可互换**：
- `unknown` = 观察齐全但判不出是谁（01.3 `FaultUnknown` 对应）
- `insufficient_evidence` = 证据不足，**无权归责任何一方**

### 2.2 映射表

`logchain_fault.go` 是唯一事实源，B 期只做「本地归因 → 契约枚举」的映射器，
不复制判据。映射表（节选主干，完整表随实现产出并由测试逐行钉住）：

| 证据条件（logchain_fault 已验证判据） | `fault_class` | `responsibility` | 最高支持的建议 |
|---|---|---|---|
| 上游返回 5xx，`actual_channel` 明确 | `upstream_5xx` | `upstream` | exact→仍需审批 |
| 连接建立失败 | `transport_connect` | `upstream` | DEGRADED |
| 上游首字节超时 | `transport_timeout` | `upstream` | DEGRADED |
| 首字节慢（有 TTFB 实测） | `upstream_first_byte_slow` | `upstream` | DEGRADED |
| 流式中断（已过首事件） | `stream_interrupted_midstream` | `upstream` | DEGRADED |
| 客户端三秒内断连（既有判据） | `client_gone` | `client` | 不支持隔离建议 |
| 429 / 容量拒绝 | `rate_limit_capacity` | `upstream` 或 `user_config` | 单次不支持隔离 |
| 路由前无可用渠道 | `route_no_channel` | `platform` | 见 Q3 |
| 模型不存在 | `model_not_found` | `user_config` | 仅提示 |
| 鉴权/额度/账户 | `auth_quota_account` | `user_config` | 仅提示 |
| 采集缺口、窗口无数据 | `telemetry_gap` | `insufficient_evidence` | **不支持任何健康或处置结论** |
| 观察齐全但判不出 | `unknown` | `unknown` | 仅排查方向 |

硬规则（测试钉死）：

- **`telemetry_gap` 的 responsibility 恒为 `insufficient_evidence`**，
  不得为 `upstream`。采集坏了不是上游的错。
- **`unknown` 与 `telemetry_gap` 不可互相替代**：
  `TestFaultUnknownNeverCollapsesIntoTelemetryGap` 双向断言。
- `evidence_level ∈ {inferred, unavailable}` 时 `responsibility`
  **强制降为 `insufficient_evidence`**，不论 fault_class 指向谁。
  即「证据不足不能直接归责上游」由类型系统层面保证，不靠调用方自律。

### 2.3 待单独确认的歧义项（不自行决定）

| 歧义 | 情况 | 我的读法 | 需要您定 |
|---|---|---|---|
| A | 429 既可能是上游限流，也可能是客户令牌超额 | 两者判据不同（上游 429 vs 本地额度拒绝），应分 `upstream` / `user_config` | 确认可分，或要求统一记 `unknown` |
| B | `empty_or_truncated_output` 无正文采集，只能看 token 数 | 归 `upstream` 但 evidence 最高 `correlated` | 确认 |
| C | `billing_anomaly` 是 Monitor 自己算出来的派生结论 | 归 `platform`（我方核算问题）而非 `upstream` | 确认 |
| D | 协议级 / thinking / 工具类 4 个 fault_class | Monitor **无观察点，永远产不出**，应显式标为「不可产出」而非「无故障」 | 确认这 4 项列为 D 期依赖 |
| E | `client_retry_loop` / `client_compaction` | 需跨请求关联，当前排障不并归重试链（既有已知限制） | 确认 B 期不产出这两类 |

映射表定稿本身我按 B 期范围做；**上面 5 项歧义需您逐条确认后才写入代码**。

---

## Q3 `route_no_channel` 的渠道表示（已按口径定稿）

### 3.1 三态分离，不用 0 混淆

您指出不能与「渠道信息缺失」混为一谈 —— 所以**不用 `channel_id = 0`**
（既有代码里 0 同时承担「未命中」和「未知」两种含义，正是要避免的歧义）。

改为**显式两列**：

```
channel_scope  string   "assigned" | "not_assigned" | "unknown"
channel_id     *int64   仅 channel_scope="assigned" 时非 nil；其余为 NULL
```

| 场景 | `channel_scope` | `channel_id` | 含义 |
|---|---|---|---|
| 正常命中渠道 | `assigned` | 实际渠道 ID | 可归责该渠道 |
| 路由前被拒，确实没分配渠道 | `not_assigned` | `NULL` | **确知没有渠道**，这是事实不是缺失 |
| 采集缺口，渠道信息没采到 | `unknown` | `NULL` | **不知道有没有渠道** |

### 3.2 三处统一说明

- **存储**：`channel_id` 用 `*int64`，NULL 不写 0。
- **聚合键**：`dedup_key` 用 `channel_scope` + `channel_id` 的规范化拼接 ——
  `assigned:115` / `not_assigned:` / `unknown:`。
  因此「未分配渠道」事故与「渠道未知」事故**天然不同键，不会合并**。
- **接口**：JSON 输出 `{"channel_scope":"not_assigned","channel_id":null}`，
  页面显示「未分配渠道」/「渠道信息缺失」两种不同文案，不显示「渠道 0」。
- **渠道指标**：只有 `assigned` 进渠道可靠率分母（沿用既有纪律）。

测试：`TestIncidentNotAssignedChannelNeverLinksRealChannel`、
`TestIncidentNotAssignedDistinctFromUnknownChannel`、
`TestIncidentChannelIDNullNotZero`。

---

## Q4 独立并发门控（已按口径定稿）

### 4.1 门控设计

```
incidentQueryGate      容量 4（独立 chan struct{}，不碰 usageDetailGate）
incidentGateTimeout    5s     取不到槽位即返回「系统繁忙」，不排队堆积
incidentQueryTimeoutMS 3000   SQLite 查询上限
```

容量 4 的依据：事故页查的是**本地 SQLite 持久化结果**（非生产库），
SQLite WAL 支持多读并发；取 4 既能让多人同时看，又给总负载留上限。

### 4.2 页面刷新不扫生产库

- 列表 / 详情 / 影响面 / 轨迹 **全部只读本地表**，零生产库查询。
- 生产库只在**后台事故引擎**按窗口推进时访问，且必须走既有
  `source_lifecycle` 闸门 + `acquireBackgroundSourceLow`，不新开连接。
- 页面上**不提供**「立即重算影响面」之类能触发生产扫描的按钮
  （这正是「刷新触发大范围扫描」的来源）。若后续需要，走异步任务而非同步刷新。

### 4.3 超时与取消

- 每个 handler `context.WithTimeout`，取消后 SQLite 查询随 ctx 终止。
- 导出类操作强制分页上限，不整表读内存。

测试：`TestIncidentQueryDoesNotOccupyUsageDetailGate`（断言 `usageDetailGate`
在事故查询期间仍可被占用）、`TestIncidentGateRejectsWhenSaturated`、
`TestIncidentQueryCancelStopsSQL`、`TestIncidentPageReadsLocalOnlyNoProductionSQL`。

---

## Q5 冷却期建议值（**需您确认后才写入默认值**）

### 5.1 建议值

按 SEV 分级，依据既有告警冷却量级（`monitor/alert.go:106,109,93`：
错误类 30 分钟、观察类 60 分钟、余额类 720 分钟）：

| SEV | 建议冷却 | 依据 |
|---|---|---|
| SEV0 | **15 分钟** | 最严重，需快速反映新进展；短于既有错误类 30 分钟 |
| SEV1 | **30 分钟** | 对齐既有错误类告警冷却 |
| SEV2 | **60 分钟** | 对齐既有观察类冷却 |
| SEV3 | **180 分钟** | 低优先级，避免长尾噪声刷事故 |

### 5.2 配置方式

沿用既有 `AlertConfig` 的形状：落 `IncidentConfig`（JSON 存 `settings` 表），
环境变量可覆盖，并**带上下界夹取**（照 `alert.go:146-147` 的写法）：

```
MONITOR_INCIDENT_COOLDOWN_SEV0_MIN   默认 15    夹取 [5, 1440]
MONITOR_INCIDENT_COOLDOWN_SEV1_MIN   默认 30    夹取 [5, 1440]
MONITOR_INCIDENT_COOLDOWN_SEV2_MIN   默认 60    夹取 [5, 1440]
MONITOR_INCIDENT_COOLDOWN_SEV3_MIN   默认 180   夹取 [5, 1440]
```

越界值回落默认而非报错（既有做法一致）。

### 5.3 必须保证的两条（与冷却值无关，恒成立）

- **同键冷却期内只更新同一个 Incident**：`dedup_key` UNIQUE +
  `INSERT ... ON CONFLICT(dedup_key) DO UPDATE`，由数据库唯一约束保证，
  不依赖应用层先查后写（并发下先查后写会产生重复行）。
- **重启后延续**：冷却状态存 `cooldown_until_ts` 列，不放内存。
- **扫描不新建事故**：引擎按「窗口推进水位」驱动，同一窗口重复扫描
  走同一 `dedup_key` 的 UPDATE 分支，`last_seen_ts` 取窗口上界而非扫描时刻。
  `TestIncidentRescanSameWindowDoesNotCreateNew` 钉住。
- **滚动窗口变化不新建**：窗口不在键里（§Q1/spec §2.1）。

**请确认 15/30/60/180 这组值，或给出您要的数值。** 确认前我不写默认值。

---

## Q6 窗口粒度建议值（**需您确认后才写入默认值**）

### 6.1 建议值：5 分钟

依据既有时间格栅（不另造）：
- 采样器默认 `MONITOR_SAMPLE_SECONDS=60`，最小 10 秒（`monitor/sampler.go:156-158`）
- 分钟桶 `bucket_ts`，聚合到 `hour_ts` 小时事实（`monitor/store.go:55,569`）

建议 **5 分钟** = 5 个分钟桶，理由：

| 粒度 | 问题 |
|---|---|
| 1 分钟 | 样本太少，大量窗口触发最小样本门，连续判定几乎不成立 |
| **5 分钟** | 既能在 SEV1「连续两窗口」下 10 分钟内发现，又有足够样本 |
| 1 小时 | SEV2「三个窗口」要 3 小时才确认，太慢 |

配置：`MONITOR_INCIDENT_WINDOW_SECONDS` 默认 **300**，夹取 `[60, 3600]`，
且**必须是 60 的整数倍**（否则与分钟桶错位，窗口边界会切开桶）。

### 6.2 最小样本门

建议 **20**（`MONITOR_INCIDENT_MIN_SAMPLE`，夹取 `[5, 1000]`）。
低于该值 → `low_sample = true`，**不产生 SEV2 事故**，按 02.1 MON-004
走 SEV3 + 数据盲区标记。

### 6.3 连续窗口判定（按文档，不降门槛）

严格按 02.1 §8：

- **SEV1**：连续 **2** 个窗口，**或** 单一来源 + Eval／其他来源佐证
  （B 期无 Eval，故只能走「连续 2 窗口」或「两个已接通来源」）
- **SEV2**：满足最小样本 **且** 持续 **≥3** 个窗口

### 6.4 防「同窗口重复扫描算三次」

这是必须用结构防住的，不靠调用约定：

`IncidentContinuity` 增列 `last_counted_window_to_ts`。
递增 `consecutive_windows` 的**唯一条件**：

```
本窗口 window_to_ts > last_counted_window_to_ts
且 window_from_ts == last_counted_window_to_ts   （严格相邻，无跳窗）
```

- 同窗口重复扫描：`window_to_ts` 相等 → 不满足 `>`，**不递增**（幂等）
- 跳窗（中间有缺口）：`from != last_to` → 不递增，走 §6.5 缺口分支

测试：`TestIncidentSameWindowRescannedThreeTimesCountsOnce`、
`TestIncidentConsecutiveRequiresAdjacentWindows`。

### 6.5 缺口纪律（既不算异常也不算恢复）

窗口无数据或采集缺失 → `gap_windows++`，
`consecutive_windows` **冻结**（不递增、不清零），
`last_counted_window_to_ts` **不前移**。

缺口结束后的下一个窗口因 `from != last_to` 不满足相邻，
**连续计数从 1 重新起算**（不接续缺口前的计数，也不清零历史事故）。

测试：`TestIncidentGapWindowNotCountedAsAnomaly`、
`TestIncidentGapWindowNotCountedAsRecovery`、
`TestIncidentGapBreaksAdjacencyRestartsCount`。

**请确认 5 分钟窗口 / 最小样本 20 这两个值。**

---

## Q7 最小展示入口（已按口径定稿）

按您「优先复用现有页面结构，不强制新增独立导航 Tab」：

**不新增顶级 Tab**，挂在既有「稳定性」页签下作为子视图
（`monitor/stability.js` 已有子视图切换结构，复用它）。
因此 `page.html` 的侧栏与小屏 nav **都不动** —— 这同时消掉了「新增页签要改两处 nav」的风险。

展示内容（只读，仅五项）：

1. 事故列表：六维键、SEV、状态、首见/末见时间、去重次数
2. 事故详情：完整聚合键（含 `ingress_protocol` 派生来源与规则版本）
3. 影响范围：各维度 + 独立请求数/尝试次数/日志条数三列分列 + 覆盖缺口标注
4. 判定依据：连续窗口数、样本量、基线、缺口窗口数、硬信号、多源确认、证据等级
5. 四层状态 + 变更历史（只读展示；审批操作入口属最小可用范围，走 roleRoot）

**明确不做**：通知、ACK、升级、Eval 联动、处置包生成、自动隔离按钮。

资产：新增 `monitor/incidents.js`，在 `page.html` 挂 `?v=1`（改文件同步 bump）。

---

## Q8 开关（已按口径定稿）

```
MONITOR_INCIDENT_CORE_ENABLED   默认 false
```

| 状态 | 行为 |
|---|---|
| 关闭（默认） | 不起事故引擎 worker、不做漂移检测、不写新表、稳定性页不显示事故子视图 |
| 关闭 | **不删除、不清空任何历史事故数据**（只停新增处理，表与数据保留） |
| 开启 | 引擎按窗口推进；8204 验收时显式开启 |

**关闭时既有功能零影响**：采集、客户维护、客户排障、问题预警、模型统计
的代码路径不经过任何事故逻辑分支（纯新增，不改既有调用链）。

**但表结构仍会建**（AutoMigrate 不受 flag 控制），所以 plan ID 必须 bump
到 v59 —— 开关不能替代迁移门禁。

生产启用另行确认，B 期不碰生产部署。

测试：`TestIncidentFlagOffDoesNotStartWorker`、
`TestIncidentFlagOffPreservesHistory`、
`TestIncidentFlagOffExistingPagesUnchanged`。

---

## 需要您授权的一件事

我想对生产库做**一次极小窗口只读抽样**，只为两个数字：

1. type=5（错误日志）行上 `other.request_path` 的填充率
   —— 决定错误事故里 `ingress_unknown` 的占比量级
2. `request_path` 是否出现过四项白名单之外的新端点
   —— 决定 `ingress_unrecognized` 是否已经在真实数据里存在

查询形态：`SELECT request_path, is_stream, type, COUNT(*) ... GROUP BY`，
近 7 天、只读、`nexus_ro` 账号（服务端仅 SELECT 权限）。

我已经写好探针 `.local-test-kit/protocol-probe/main.go`，但执行时被
权限策略拦下（判定为「直连生产库查询」）。**这是正确的拦截**，我没有绕。

这两个数字不阻塞编码 —— 我的设计已让缺失走 `ingress_unknown` 而非兜底，
填充率低也不会错误归类。但**在拿到数字之前，我不会宣称派生覆盖率**。

请您选一种：
- (a) 授权我执行该探针（您可先看 `main.go` 的 SQL）
- (b) 您自己跑一次把结果给我
- (c) 先不查，我在实现里把填充率标为「未实测」，等 shadow 期用真实数据回填
