# Q2 责任映射：已确认口径（2026-10-09 修订版）

状态：**已获用户确认并据此实现。** 本文已按用户 2026-10-09 的修订口径重写，
此前版本中 A/B/C/E 四项的建议结论均**已作废**，不得再用于指导实现。

实现位置：`monitor/incident_responsibility.go`、`monitor/incident_attempts.go`。
测试：`monitor/incident_responsibility_test.go`（17 项）、
`monitor/incident_attempts_test.go`（8 项）。

---

## 总体原则（用户确认）

**故障分类、证据等级、责任归属必须分别判断**，不能只按 fault_class 固定映射责任方。

因此实现里**没有** `fault_class → responsibility` 静态表。
`DecideResponsibility()` 接收三样独立输入（分类、证据等级、关联事实），分别判断。

**归因置信度 ≠ 证据等级**，不得互相换算：

| 概念 | 回答什么 | 取值 | 来源 |
|---|---|---|---|
| 归因置信度 | 这条判据本身多可靠 | `faultConfHigh/Mid/Low/None` | 实测样本量 |
| 证据等级 | 这个结论怎么关联出来的 | `exact/correlated/inferred/unavailable` | 实际关联方式 |

`EvidenceLevelFor()` 刻意**只接受关联方式**，不接受置信度参数，
从签名上杜绝「把 high/mid/low 换成 exact/correlated/inferred」。

证据取得方式 → 证据等级，严格对应 01.3 §87-90 四档：

| 取得方式 | 证据等级 | 依据 |
|---|---|---|
| `direct_fact` | `exact` | §87「同一 ID 或 Attempt 的直接事实」 |
| `deterministic_reproduction` | `exact` | §87「确定性定向复现」 |
| `multi_source` | `correlated` | §88「多源在相同维度和窗口高度相关」 |
| `local_only` | `inferred` | §89 启发式推断 |
| `time_window_only` | `inferred` | §89 弱相关推断 |
| `observation_missing` | `unavailable` | §90 没有必要观察点 |
| 未知取值 | `unavailable` | 不放行成任何可用等级 |

**既不自动升级，也不一律降级**：多源关联不升 `exact`（关联 ≠ 直接事实），
但直接事实与确定性复现**必须**能取得 `exact`——
「不能仅凭关联就升为 exact」不等于「禁止直接事实取得 exact」。

**证据等级天花板**：`inferred` 与 `unavailable` 一律返回 `insufficient_evidence`，
不论 fault_class 指向谁。由结构保证，不靠各分支自觉。

**隔离建议的证据门槛**（§87-88）：采用**白名单**——只有 `exact` 放行。
`correlated` 最多支持 DEGRADED，空值与非法枚举一并拒绝。
放行只表示「证据等级这一关过了」，**不代表可跳过**其他治理条件与人工审批
（§87 原文亦为「仍须结合影响与审批」）。

---

## A｜429 限流

**已确认**：默认不归责。

| 条件 | fault_class | responsibility |
|---|---|---|
| 仅我方 429 日志 | `rate_limit_capacity` | `insufficient_evidence` |
| 可靠关联到对应请求 **且** 证据确实指向模型上游限流 | `rate_limit_capacity` | `upstream` |

两个条件**都**要满足（`UpstreamEvidenceLinked && UpstreamEvidenceIndicatesUpstreamCause`）。
只关联到「上游这一跳出现过」不够，必须那份证据指向上游限流成因。

**不得仅凭错误文本包含 "upstream" 归责。** 既有判据
（`logchain_fault.go:251-254`）实测 15 条原文均为 `Upstream rate limit exceeded`，
措辞指向上游，仍拒绝归责。文本措辞不是证据。

测试：`TestRateLimitDefaultsToInsufficientEvidence`、
`TestRateLimitLinkedButNotUpstreamCauseStaysInsufficient`、
`TestRateLimitUpstreamOnlyWithLinkedUpstreamCause`。

---

## B｜`empty_or_truncated_output`

**已确认**：**不归 upstream**（此前「归 upstream + 证据上限 correlated」已作废）。

`completion_tokens=0` 即使同时存在扣费，也不能单独证明实际响应内容为空，
更不能证明一定是上游责任。既有判据 `logchain_fault.go:859-862` 同样保持待判：

```go
// 只有其它消费异常、没有流问题：扣费与交付不一致，但责任方无从判断。
return logChainFault{Fault: faultUnknown, Confidence: faultConfNone,
    Why: "计费与交付不一致，但无流中断等旁证，责任方需人工核对"}
```

**口径**：**仅有**零 token、扣费记录时不足以定责，
保留异常事实与疑似排查方向，`responsibility = insufficient_evidence`。
仅凭 token 字段的启发式判断不能自动取得 `correlated`，更不能据此建议 DEGRADED。

**但这是「证据不足时不定责」，不是「永远不定责」**（2026-10-09 review 修正）：
可靠关联到对应请求、且证据确实指向上游成因时，归 `upstream`。
证据补齐后不禁止定责。

| 输入 | responsibility |
|---|---|
| 仅 `completion_tokens=0` + 扣费 | `insufficient_evidence` |
| 关联到上游这一跳，但证据未指向上游成因 | `insufficient_evidence` |
| 可靠关联 **且** 证据指向上游成因 | `upstream` |

测试：`TestEmptyOutputWithOnlyTokenEvidenceStaysInsufficient`、
`TestEmptyOutputLinkedWithoutCauseStaysInsufficient`、
`TestEmptyOutputBlamesUpstreamOnceCauseConfirmed`、
`TestEmptyOutputTokenOnlySupportsNoQuarantine`。

---

## C｜`billing_anomaly`

**已确认**：保留分类，但**不凭日志来源归 platform**（此前结论已作废）。

`total tokens is 0`（`cloudwatch_logs_parse_newapi.go:81`）能证明 NewAPI
记录了零用量异常，但不能单独证明根因在平台——也可能涉及上游 usage 缺失、
解析或其他链路问题。

**口径**：
- 保留 `billing_anomaly` 分类
- 责任未定位前 `responsibility = insufficient_evidence`
- **原始事件可核验 ≠ 责任结论为 exact**
- 不凭这条日志单独产生渠道隔离建议
- **但不写成「所有计费异常都与渠道无关」**

同样是「证据不足时不定责」而非「永远不定责」（2026-10-09 review 修正）：

| 输入 | responsibility |
|---|---|
| 仅 `total tokens is 0` 日志 | `insufficient_evidence` |
| 证据指向上游（如上游 usage 缺失）| `upstream` |
| 证据证实我方记账/解析链路缺陷 | `platform` |

「不产生隔离建议 ≠ 与渠道无关」在实现里的体现：
`TestBillingAnomalyNoQuarantineYetChannelStillRelevant` 断言两面——
同等责任与 `exact` 证据下该分类被拒，但 `upstream_5xx` 放行，
说明拒绝是针对分类本身，不是「计费异常与渠道无关」这种一般结论。

---

## D｜四个无观察点的分类

**已确认**：当前不推断，**但不是永久禁止接收**（此前「永不输出」的测试设计已修正）。

保留四个枚举：`protocol_invalid`、`responses_incomplete`、
`thinking_signature_invalid`、`tool_call_invalid`。

**口径**：
- B 期缺少必要证据时显示「无观察点／不可判定」，
  **不显示「没有此类故障」**
- 不采集正文、thinking signature 和工具参数，
  **不等于不能接收 Eval 的脱敏校验结果、故障事件和证据引用**
- 测试保证「缺少必要证据时不擅自输出」，
  **而不是永久禁止这些分类**——否则阻碍后续联调

实现：`CanProduceFaultClass(fc, hasExternalEvidence, hasClientTelemetry)`。
对这四类，`hasExternalEvidence=false` 时不输出，`=true` 时**允许**输出。

测试 `TestFaultClassNeedingExternalEvidenceNotProducedWithoutIt` 双向断言：
无证据不输出 **且** 有证据时不被禁止。

---

## E｜`client_retry_loop` / `client_compaction`

**已确认**：复用归并能力，但 B 期**不据此产出** `client_retry_loop`。
（此前「B 期可能可以产出 + 补重试阈值」的建议已作废。）

01.3 第 76、77 行的定义是**客户端直接证实**：

```
|`client_retry_loop`|客户端直接证实的重试循环|
|`client_compaction`|客户端直接证实的上下文压缩|
```

同一 Request ID 下的渠道尝试可能是 **NewAPI 内部重试**；
Nginx 的 `upstream_attempts` 属于 **Nginx→NewAPI 这一跳**。
两者都不能直接证明客户端重试循环，**补一个「重试几次」的阈值也不能解决**。

### 三条具体确认

**1. 复用既有口径，但明确是「当前可见尝试数」**

复用 `logchain.js:869-899` 的口径：**先按 Request ID 归组，再在组内**以
`(channel_id, 秒级时间)` 去重。

两条跨请求合并的坑（2026-10-09 review 指出）：
- 只用「渠道 + 秒」会把**两个不同请求在同一秒命中同一渠道**算成一次尝试。
  去重键必须带 Request ID。
- **无 Request ID 的记录不得擅自合并**（`logchain.js:877` 每行单独成组），
  也不得与有 ID 的行合并：按记录各计一次。
`CompletenessNote()` **即使在 Exact 为真时也**声明
「不含 Monitor 观察点之前的客户端尝试，不等于实际网络尝试数」——
因为客户端到 Nginx 之前的尝试 Monitor 永远看不到。

**2. 撤掉 `attempt_count_is_rows`，但不隐藏退化**

该字段建立在「重试链无法归并」的错误前提上，已撤掉。
但 `logchain.js:889` 明确「取不到渠道时退化为按记录计数」
（`:896` 的 `extra++`），这种退化**必须保留说明**：

```
RowsWithoutChannel  int64   取不到渠道、退化为按记录计数的行数
UnlinkableRows      int64   无 Request ID、无法归并的行数（对应 logchain.js:877 unlinkable）
Exact               bool    无任何退化或不可关联时为真
```

`CompletenessNote()` 在退化发生时给出具体行数，不静默。

**3. B 期不产出这两类，显式标记观察能力缺失**

没有经授权的客户端直接遥测时不产出，标记 `needs_client_telemetry`。
**仅接入 Eval 也不能代替真实客户端遥测**——
`CanProduceFaultClass(fc, hasExternalEvidence=true, hasClientTelemetry=false)`
对这两类返回 false，由
`TestClientFaultsRequireClientTelemetryNotEval` 断言。

`ProvesClientRetryLoop()` 恒返回 false，让这条纪律在代码里有可被测试引用的落点，
而不是只写在注释里。

---

## 三个责任「待判」值的区分

| 值 | 含义 | 指向责任方 |
|---|---|---|
| `unknown` | 观察齐全，但确实判不出（01.3 的「有异常事实但仍无法分类」） | 否 |
| `insufficient_evidence` | 证据不足，无权归责 | 否 |
| `telemetry_gap` 的归属 | 恒为 `insufficient_evidence`——采集坏了不是上游的错 | 否 |

`unknown` 与 `telemetry_gap` **不可互换**，页面文案也必须不同
（`TestFaultUnknownNeverCollapsesIntoTelemetryGap` 双向断言，含文案差异）。

---

## 附｜严重度判定：场景优先（2026-10-09 review 修正）

原实现 `IncidentSeverityFloor` 只按窗口数统一升级，有两个缺陷：
先判「≥2 窗口 → SEV1」导致后面「≥3 → SEV2」**永远不可达**；
而且简单交换两个判断会让**第三个窗口反而降级**。

根因是把严重度当成窗口数的函数。02.1 §174-177 的严重度**首先取决于故障场景**：

| 场景 | 候选严重度 | 窗口/佐证条件（§174-177） |
|---|---|---|
| `site_wide_or_security` 全站不可用、安全事件、严重错路由、大面积重复计费 | SEV0 | 硬信号立即触发，否则需双源 |
| `primary_no_fallback` 主渠道/主模型多用户持续失败、无备用 | SEV1 | 两个连续窗口，或一源 + Eval/另一源 |
| `degraded_with_fallback` 单渠道退化但有备用、持续慢或 429 | SEV2 | 最小样本 **且** 至少三个窗口 |
| `isolated_or_drift` 单请求、低样本、采集延迟或配置漂移 | SEV3 | 无额外门槛 |

现改为 `DecideIncidentSeverity(IncidentSeverityInput)`：
**先定候选严重度，再验证该场景自己的窗口条件**。
验证不过时降到 SEV3（仍是事故、仍要展示），**不跨场景升级**——
第三个窗口不该让「有备用的单渠道退化」变成「无备用的主渠道失败」。

测试：
- `TestIncidentSev2ReachableWithThreeWindowsAndSample` —— SEV2 正向可达
- `TestIncidentSeverityDependsOnScenarioNotOnlyWindows` —— 同一窗口状态在
  两个场景下得到 SEV1/SEV2 不同结果，证明场景参与了判定
- `TestIncidentThirdWindowNeverDowngrades` —— 三个场景各推进 5 个窗口，
  断言严重度单调不降
- `TestIncidentSev2SceneLowSampleNotSev2` —— 低样本不产 SEV2，但仍为 SEV3
- `TestIncidentSev0RequiresHardSignalOrDualSource`
- `TestIncidentUnknownScenarioYieldsNoSeverity`

---

## 附二｜当前段证据与历史证据的分离（2026-10-10 review 修正）

`HardSignal` / `MultiSourceConfirmed` 原先只置 true，正常窗口不清，
导致恢复后的新一段异常凭旧标记绕过自己的确认门槛：
双源（或硬信号）异常 → 正常窗口 → 单源非硬信号异常窗口，
连续数已重算为 1，却仍判 SEV1。

根因是一个字段承担了两种相反语义。现已分离：

| 字段 | 语义 | 正常窗口打断时 | 参与严重度判定 |
|---|---|---|---|
| `HardSignal` | 当前连续异常段的硬信号 | **清零** | 是 |
| `MultiSourceConfirmed` | 当前段的多源确认 | **清零** | 是 |
| `EverHardSignal` | 历史见过硬信号 | 保留 | **否** |
| `EverMultiSourceConfirmed` | 历史见过多源确认 | 保留 | **否** |
| `PeakSeverity` | 历史最高严重度 | 保留 | **否**（不回灌） |

当前段标记的置位时机也从「正常分支之前」挪到「之后」，
确保恢复后由新窗口自己重新确认。

**缺口不算恢复**：采集缺口分支在置位前就 return，不清标记（原有正确行为，
已加测试防回归）。

**重置不等于永久压低**：新一段自己带硬信号、或自己攒够两个连续窗口，
仍正常升到 SEV1。

## 附三｜归因函数的证据等级校验（2026-10-10 review 修正）

上一轮只修了 `SupportsQuarantineSuggestion`，`DecideResponsibility` 仍是排除法，
`""`、`"high"`、未知串都能穿过天花板，只要上游两个标记为 true 就返回 `upstream`。

现改为白名单 + 契约校验：

- `DecideResponsibilityChecked()` 返回 `(resp, error)`，
  用 `EvidenceLevel.Validate()` / `FaultClass.Validate()`
  把缺失与非法值送入**契约错误处理**，不静默降级。
- 天花板白名单：只有 `exact`/`correlated` 继续检查定责条件。
- `DecideResponsibility()` 作为便捷形式，出错返回 `insufficient_evidence`，
  忽略 error 也拿不到确定责任。

特别注意 `high`/`medium`/`low`：01.3 §92 明示那是 API-evaluator 的测试置信度，
不得代替 evidence level，因此必须被校验拒绝而不是当成「强证据」。
