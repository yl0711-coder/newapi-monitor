# 上下游全链路日志与日毛利核算 · 开发说明书

- 文档状态：**P1 后端已实现（前端未做）；P2/P3 未开始**
- 初版日期：2026-08-19（Asia/Shanghai）
- 最后修订：2026-08-19（第二轮会话：修正三处事实错误 + 补入 P1 实现记录与成本分摊口径）
- 编写者：第一、二轮 AI 会话（基于代码阅读 + 本地脱敏快照实测）
- 交付对象：接手开发的 AI / 开发人员
- 前置阅读：[QUALITY_REVIEW_AND_TEST_SOP.md](QUALITY_REVIEW_AND_TEST_SOP.md)、[usage-facts-v2-architecture.md](usage-facts-v2-architecture.md)

> **第二轮修订摘要**（细节见各节与第 13、14 节）：
> 1. 修正 7.1 节"`LogRow` 已有全部字段"——**错误**，`LogRow` 无 `ChannelID`，且是故意不给的（客户面）。
> 2. 修正 3.3 节上游端点 `/api/log/self` → 实际是 `/api/log/`。
> 3. 11 节问题 1（生产上游同步是否开启）**代码里已有答案**：默认 `true`。
> 4. 新增：事实表 `type IN (2,6)` 排除错误日志、`scrubContent` 会清空上游错误原文、
>    前置拒绝无 `user_id`——三件都直接决定排障能做到什么程度。
> 5. 新增 4.4 节：客户级成本分摊口径（用户已决策：按 token 占比，但必须在 域名×模型×日 摊）。
> 6. 新增第 13 节：P1 排障链路已实现内容、验证状态、前端方案。第 14 节：本轮修正与失误。

> **本文档的证据分级**，务必遵守：
> - 标 `【已验证】` = 上一轮会话直接读过代码或在本地 8202 快照实测过，可直接依赖。
> - 标 `【待验证】` = 推断或从注释间接得出，**动手前必须自行确认**，不得当既定事实。
> - 标 `【未知】` = 完全没有证据，需要用户或真实环境提供答案。
>
> 本文档所有结论均**未接触生产数据库**。本地快照是 2026-08-19 10:34 线上备份的脱敏派生，
> 上游凭据已被剥离，因此任何"上游数据为空"的观测都是测试环境产物，不能推断生产状态。

## 1. 业务需求（用户原话转述）

用户经营一个 AI API 中转站，NewAPI 是网关。需求有三层，**排障优先级不低于财务**：

1. **拿到下游客户日志**——客户侧每一条请求的用量、模型、错误。
2. **拿到上游供应商日志**——中转站向模型提供方发出的请求与消耗。
3. **两条链路按"每一个客户使用的上游模型"对应起来**，用于：
   - **客户排障**：客户报"AI 用不了/报错"时，能定位到这条请求走了哪个上游、上游返回了什么。
   - **日毛利核算**：每天的收入、成本、毛利，以及"核算出的毛利额是否正确"。
   - **日收入 / 充值金额查询**。

### 1.1 用户已明确的口径决策

用户自己提出并经确认的结论：**日毛利的成本侧必须用"按量成本"，不能用"充值成本"**。
理由是客户当天充值但没用完，充值额不是当天的实际成本。这个判断正确，
但**"充值成本"并未因此废弃，它的角色变成了单价换算系数**，详见第 4 节。

## 2. 现状盘点

### 2.1 下游客户日志：已完备，不需要重建 【已验证】

| 表 | 位置 | 维度 | 度量 |
|---|---|---|---|
| `UsageDailyFact` | [store.go:320](../monitor/store.go#L320) | `date_ts × user_id × channel_id × grp × model_name × token_id` | `requests`/`refund_records`/`prompt_tokens`/`completion_tokens`/`consume_quota`/`refund_quota` |
| `UsageHourFact` | [store.go:300](../monitor/store.go#L300) | 同上，小时粒度 | 同上；仅保留近期约 8 天 |

来源口径（已冻结，见 usage-facts-v2 文档 3.1 节）：
`logs.type IN (2,6)`（2=消费 6=退款）`AND traffic_class = user`，CST 自然日左闭右开。

单条日志明细走 `LogRow`（[usage.go:1169](../monitor/usage.go#L1169)），字段含
`RequestID`、`ModelName`、`Group`、`TokenName`、`UseTime`、`PromptTokens`、`CompletionTokens`、
`CostUSD`、`FirstByteMs`、`CacheReadTokens`、`CacheWriteTokens`，以及 `Content`（错误原文，经脱敏）。

**关键：`LogRow` 里没有 `UpstreamCostUSD` / `MarginUSD` 字段。**
全库 grep 这两个名字零命中【已验证】。单请求级毛利目前不存在，需从零建。

#### 2.1.1 `LogRow` 也没有 `ChannelID`，而且是故意的 【已验证·第二轮修正】

初版 7.1 节写"现有 `LogRow` 已有全部字段，缺的只是 UI 与接口"——**这是错的**，
和第 12 节第 2 条（误报 `UpstreamCostUSD`）是同一类失误。实际情况：

- `LogRow`（[usage.go:1169-1202](../monitor/usage.go#L1169-L1202)）**没有 `ChannelID` 字段**。
- 取它的 SQL（[usage.go:1802](../monitor/usage.go#L1802)）**也没 SELECT `channel_id`**。
- [usage.go:1225](../monitor/usage.go#L1225) 注释说明这是设计意图：
  > 渠道等内部字段(channel_id/channel_name/admin_info…)不在此结构 → 天然不解析、不外传

`LogRow` 服务的是 **[portal.go](../monitor/portal.go) 客户自助面**（路由
[portal.go:779-781](../monitor/portal.go#L779-L781)），不是管理员面。往它加 `BaseDomain`
等于把"你用哪些上游供应商"告诉客户，属经营秘密，比第 9.3 节列的那些更该管。

**初版把"客户排障"（你排查客户报的故障）和"客户自助查日志"当成了一个东西。**
P1 要建的是管理员面新接口，不是改 portal。

#### 2.1.2 事实表排除了错误日志——排障不能建在事实层上 【已验证·第二轮新增】

`UsageDailyFact`/`UsageHourFact` 的来源口径是 `logs.type IN (2,6)`
（[usage.go:337](../monitor/usage.go#L337)、[579](../monitor/usage.go#L579)、[1001](../monitor/usage.go#L1001)），
**排除了 type=5 错误**。而排障主要看的就是 type=5。

明细查询没这个限制：[logFilterWhere](../monitor/usage.go#L1661) 放开 1-6 类型，
注释说明这是对齐 new-api 官方客户端 `model.GetUserLogs` 的口径。
采样器采了 type=5 但只按 `channel_id × model × grp` 聚合、不留明细
（[sampler.go:449](../monitor/sampler.go#L449)）。

**结论：排障接口必须直查生产 `logs`，不能走事实表。**

#### 2.1.3 `scrubContent` 会把上游错误原文整段清空 【已验证·第二轮新增】

[usage.go:1581-1586](../monitor/usage.go#L1581-L1586)：

```go
func scrubContent(content string) string {
	if strings.Contains(content, "渠道") { return "" }
	return content
}
```

new-api 的上游错误原文常形如 `渠道 xxx (#12) 返回错误：status_code=429 ...`，
过一遍 scrubContent 就**整段变空**。这对客户面是正确的纵深防御，但
**管理员排障面必须绕过它**，否则最有用的那批错误正好全是空白。

好消息：`buildLogDetail` 对 type=5 是原文直出、不过 scrubContent
（[usage.go:1516-1518](../monitor/usage.go#L1516-L1518)，注释"不归类/改写上游错误，保留原始排障信息"）。
但 `populateExpandFields` 里的 `OtherContent` 仅 type=2 才填且过 scrubContent
（[usage.go:1596-1598](../monitor/usage.go#L1596-L1598)）。**不能整段复用 portal 的组装路径。**

### 2.2 上游供应商日志：已有采集链路，但维度被丢弃 【已验证】

表 `ChannelUpstreamUsageHour`（[channel_upstream.go:100](../monitor/channel_upstream.go#L100)）：

```go
Domain    string  // 主键
HourTs    int64   // 主键
Requests  int64
Tokens    int64
Quota     float64
CostUSD   float64
FetchedAt int64
Provider  string  // newapi | sub2api
```

**主键只有 `domain + hour_ts`。没有 model、没有 channel、没有 user。**

采集器 [channel_upstream_usage.go](../monitor/channel_upstream_usage.go) 通过 HTTP 拉上游账户
API，**逐条明细取回来后按小时聚合就丢弃了**。解析时只提取 4 个字段
（[channel_upstream_usage.go:206-215](../monitor/channel_upstream_usage.go#L206-L215)）：

```go
type newAPIUsageItem struct {
    CreatedAt        int64
    Quota            float64
    PromptTokens     int64
    CompletionTokens int64
}
```

采集边界（改动时不得放宽）：单轮分页硬限 `maxUpstreamUsagePages = 50`、
`upstreamUsageMaxRequestsPerRun = 60`、请求间隔 200ms、tail 重叠 3 小时、
默认 30 分钟一轮、失败指数退避最长 240 分钟。

## 3. 请求级关联的可行性分析（本任务的技术核心）

用户要"对应到每一个客户使用的上游模型"。这是**请求级关联**，不是聚合对账。

### 3.1 结论：下游侧已经能独立回答大部分排障问题 【已验证】

`logs.channel_id` → `ChannelSnap.BaseDomain` 就是这条请求走的上游主域名。

`ChannelSnap.BaseDomain`（[store.go:153](../monitor/store.go#L153)）是**已落库的索引列**，
由 [normalizeChannelBaseDomain](../monitor/channel_domain.go#L42) 归一化为可注册主域名
（eTLD+1，小写）。且渠道删除后快照保留（`DeletedAt` 注释明确说明是为了让历史用量仍能显示主域名）。

**所以「某客户某条请求走了哪个上游、用了什么模型、报了什么错」不需要上游日志就能答**，
下游 `logs` 里 `user_id + channel_id + model_name + content` 已经全有。这条要先做，
成本最低、价值最高。

`channel_id` 确实可用【已验证】：采样器已在用它做分组
（[sampler.go:413](../monitor/sampler.go#L413)），`channel_snaps.base_domain` 也已被多处 join
（[channel_upstream_alert.go:95](../monitor/channel_upstream_alert.go#L95)、
[channel_finance.go:814](../monitor/channel_finance.go#L814)、
[stability.go:390](../monitor/stability.go#L390)）。

> **措辞澄清**（避免误读）：本节说的"不需要上游日志"**只针对"给单条客户请求补上游信息"这一件事**。
> 上游日志对**日毛利的成本侧是必需的**，不是可选项——见第 4 节与 4.4 节。用户已确认两个交付物都要。

#### 3.1.1 排障接口结构性答不了的三类问题 【已验证·第二轮新增】

这三条必须在 UI 上常驻可见。排障工具最危险的失效方式是**"查不到"被读成"没发生过"**：

1. **未打到渠道即被拒的请求**（限流 / 无可用渠道 / 分组无权限）**不写 `logs`**，
   只在 `StabilityRejectHour`（[stability_store.go:93-100](../monitor/stability_store.go#L93-L100)）里，
   主键 `hour_ts × node × reason × model × grp`——**没有 `user_id`**，且是小时聚合。
   [stability_problem.go:123](../monitor/stability_problem.go#L123) 注释也点明"前置拒绝是用户交付问题，
   但没有 channel_id"。
   → 客户报"我的请求根本发不出去"，**排障接口查不到属预期**，只能看到"这小时该分组有 N 次 rate_limit"，
   定位不到是不是他。要修得给该表加用户维度（改 schema，bump plan ID），不在 P1 范围。
2. **重试链无法归并。** new-api 换渠道重试会落多条 type=5，没有把多次尝试归到同一次客户请求的标识。
   看到 3 条错误，判断不了是 3 次失败还是 1 次失败重试 3 次。
3. **从不采集请求/响应正文**（也不该采）。"回答质量不对"这类问题答不了。

另注：`StabilityProblemSample`（[stability_store.go:105](../monitor/stability_store.go#L105)）
有 `ChannelID` + `Message`（content 原文）但**无 `user_id`**；`LogRow` 有客户维度但无 `ChannelID`。
**改动前没有任何单一结构能同时回答"哪个客户 + 哪个上游 + 什么错"**，这是 P1 必须新建查询的原因。

### 3.2 上游日志无法与下游逐条 join 【已验证】

[channel_upstream_usage.go:238-240](../monitor/channel_upstream_usage.go#L238-L240) 注释原文：

> NewAPI 未修改版本不暴露真实日志 ID：响应里的 id 会被重写为页内序号。

因此**没有稳定的上游日志主键可用于 join**。可能的关联方式只有
`(domain, 时间窗, model, tokens)` 模糊匹配，会有歧义，不能作为账务依据。

**结论：上游日志的定位是"独立对账证据"，不是"下游日志的补充字段"。**
它用来回答"上游收了钱但下游没记录"这类差异，不用来给单条客户请求补上游信息。

### 3.3 上游 API 是否返回 model 维度 【未知】——阶段二的唯一前置

现有代码不解析 model，但**不代表上游不返回**。判定方法：
`canonicalUsagePageFingerprint` 会解码完整条目，抓一次真实响应即可确认。

**端点修正【已验证·第二轮】**：实际请求的是 **`/api/log/`**，不是初版写的 `/api/log/self`
（[channel_upstream_usage.go:175](../monitor/channel_upstream_usage.go#L175)）：

```go
query.Set("p", ...); query.Set("page_size", ...); query.Set("type", "2")
query.Set("start_timestamp", ...); query.Set("end_timestamp", ...)
headers := {"Authorization": "Bearer "+token, "New-Api-User": userID}
upstreamEndpoint(row.BaseURL, "/api/log/") + "?" + query.Encode()
```

这就是 NewAPI 自己日志页用的接口，其列表本来要渲染"模型"列，因此
**`model_name` 很可能已在响应里**【待验证】。而
[canonicalUsagePageFingerprint](../monitor/channel_upstream_usage.go#L231) 已把**整条 item 原样解码**，
说明字段全在响应体内，只是 [206-209 行](../monitor/channel_upstream_usage.go#L206-L209)
那个 struct 只挑了 4 个。

- NewAPI 的 `/api/log/` 【待验证】很可能含 `model_name`
- Sub2API 【未知】

**确认成本很低**：登任一上游站 → F12 看日志接口响应字段，或在生产日志里打一条首页响应的字段名列表。
**不需要改表、不需要抓包工具。** 测试里的响应是手写的
（[channel_upstream_test.go:275](../monitor/channel_upstream_test.go#L275)），证不了这件事。

**必须先确认，再决定阶段二做不做。** 不要凭猜测改表。

## 4. 成本口径：必须做兑换比换算（否则毛利算错一个数量级）

### 4.1 上游 `CostUSD` 是额度面值，不是现金 【已验证】

[channel_upstream_usage.go:355-367](../monitor/channel_upstream_usage.go#L355-L367)：

```go
unit := row.BalanceUnit
if unit <= 0 { unit = defaultNewAPIQuotaPerUSD }  // = 500000.0
bucket.CostUSD = bucket.Quota / unit
```

这是**上游站内额度折算的美元面值**。各上游充值优惠不同，同样"1 美元面值"
对应的真实现金差异巨大。本地快照实测【已验证】：

| 域名 | `recharge_paid : recharge_credit` | 花 $1 得到 |
|---|---|---|
| `987xyz.com` | 1 : 10 | 10 额度 |
| `codeyu.shop` | 1 : 7 | 7 额度 |
| `blackaicoding.com` | 1 : 10 | 10 额度 |
| `aicodewith.com` | 1 : 1 | 1 额度 |

### 4.2 正确公式

```
真实现金成本 = upstream_quota / 500000 × (recharge_paid / recharge_credit)

日毛利 = 下游净消费(权责发生制) − 上游按量消耗 × 兑换比
```

**不做这次换算，`987xyz.com` 的成本会虚高 10 倍，毛利变负数。**

`ChannelDomainCost`（[channel_finance.go:50](../monitor/channel_finance.go#L50)）的
`recharge_paid`/`recharge_credit` **不是成本金额，是汇率**。它不进日成本，
只作为按量成本的换算系数。客户充值（`logs.type=1`）只进现金流报表，不进毛利。

### 4.3 兑换比缺少版本历史——**上线前必须解决的隐患** 【已验证】

本地快照中 `ChannelDomainCost` 的 `version` 字段恒为 `0`，视图无历史数组。
如果上游调整过优惠比例，**用今天的兑换比算上个月的成本，账是错的且不会报错**。

已存在 `ChannelFinanceVersion` 表和 `migrateLegacyChannelFinanceVersions`
（[store.go:801](../monitor/store.go#L801)），**先查清它是否已覆盖 domain cost 的版本化**，
不要重复造。若未覆盖，需按 `EffectiveAt` 取当时生效值，
改 schema 则必须 bump plan ID（见第 7 节）。

### 4.4 客户级成本分摊 【用户已决策·第二轮新增】

用户要"每个客户的每日金额"。上游日志无可 join 主键（见 3.2），所以客户级成本
**只能分摊，不是实测**。用户已明确选定口径：**按该客户的 token 占比摊**。

#### 4.4.1 必须在 域名×模型×日 摊，不能在 域名×日 摊

这不是精度问题，是数量级问题。同一域名上 Haiku 与 Opus 单价差几十倍；
在"域名×日"摊等于假设所有客户模型结构相同，
**一个只用便宜模型的大 token 客户会把贵模型客户的成本吸走，两边毛利都错且不报错。**

`UsageDailyFact` 主键含 `ChannelID` + `ModelName`（[store.go:320-335](../monitor/store.go#L320-L335)），
下游侧这个分组现成可做。

> **这条把"上游 model 维度"从 P3 的可选增强升级为客户级利润的硬前置。** 见 3.3 节。

#### 4.4.2 公式

```
对每个 (域名 d, 模型 m, 日 t):
  上游现金成本 C = upstream_quota(d,m,t) / 500000 × (recharge_paid / recharge_credit)
  客户 i 权重   w_i = 加权token(i,d,m,t) / Σ 加权token(·,d,m,t)
  客户 i 成本   = C × w_i
  客户 i 收入   = (consume_quota − refund_quota)(i,d,m,t) / quotaPerUSD

客户 i 当日毛利 = Σ_{d,m} (收入 − 成本)
```

#### 4.4.3 权重不能用裸 token 相加

两处系统性偏差：

1. **输出 token 比输入贵**（常见 4–5 倍）。裸加会高估"输入多"的客户的成本。
   权重应为 `prompt + r × completion`，`r` 取该模型的 completion 倍率
   （下游 `other.completion_ratio` 可作代理，见 `logOther.CompletionRatio`）。
2. **缓存读 token 只要约一折。** 事实表**没有缓存 token 列**（grep 零命中），
   只有 `PromptTokens`/`CompletionTokens`；`LogRow` 是从 `other.cache_tokens` 现算的
   （[usage.go:1185](../monitor/usage.go#L1185)）。
   > 【待验证】new-api 是否把 `cache_tokens` 计入 `prompt_tokens` 之内。**动手前查一条真实日志确认。**
   > 若是，则重度用 Claude Code 这类高缓存命中的客户会被严重高估成本——而这在本站客户里可能是常态。

   要修必须给事实表加缓存 token 列 → **改 schema，bump plan ID**。

#### 4.4.4 关键：不要改成"按收入占比摊"

看似更自然，但那样算出来**每个客户的毛利率完全相同**：

```
成本_i = C × 收入_i/R  ⟹  毛利_i/收入_i = 1 − C/R   (与 i 无关)
```

金额有差别、率是常数，于是**永远看不出哪个客户不赚钱**——而这正是做客户级利润的目的。
按 token 摊则毛利率随"该客户下游单价 ÷ 其 token 量"变化，
低价分组、大折扣、超卖套餐的客户会自己浮出来。**这是用户所选口径优于按收入摊的真正理由。**

#### 4.4.5 三条实现纪律

1. **客户级数字是分摊估算，不是账实。** 字段名与 UI 都要带 `allocated`/`estimated`，
   不得与域名级账目混在一张表里当同等证据。
2. **对账只在域名级做**（见第 8 节）。分摊在客户级**无独立控制量**，无法自证。
   域名级差异超阈值时，其下客户级数字一并压掉不显示。
3. **无上游账号的域名摊不出成本。** 如 `aicodewith.com` 收入 $4,346 但
   `upstream.configured = false`，其下所有客户必须标 `cost_missing`，
   **不得按 0 成本算出 100% 毛利率。**

#### 4.4.6 模型映射会让 join 对不上 【已验证】

事实表 `ModelName` 是**计费模型名**。发生模型映射时上游收到的是另一个名字——
`LogRow` 有 `IsModelMapped`/`UpstreamModelName`（[usage.go:1194-1195](../monitor/usage.go#L1194-L1195)，
源自 `other`），但**事实表没存**。有映射的渠道按计费模型名去对上游会对不上。
设计 `UpstreamCostDailyFact` 时必须一并解决。

## 5. 客户充值金额：可取，但解析脆弱 【已验证】

`logs.type=1` 是充值（类型语义见 [usage.go:1173](../monitor/usage.go#L1173) 注释：
`1充值 2消费 3管理 4系统 5错误 6退款`）。`logs` 已在只读授权内，不需要加表权限。

**但 [usage.go:1820-1821](../monitor/usage.go#L1820-L1821) 注释明确指出**：

> 费用仅消费(type=2)有意义：充值/管理/系统在 new-api 里 quota 恒为 0（金额只写在 content），
> 折美元会得 $0.00 误导客户对账

所以充值金额必须**从 `content` 自由文本解析**，随 new-api 版本措辞变化会失效。
若实现：解析失败必须标记 `unparsed` 并计数告警，**不得静默当 0**。

> 【待验证】上一轮会话未在真实脱敏数据中抽样确认 `content` 的实际文本格式。
> 动手前先在本地快照查若干 `type=1` 记录的 `content` 原文，再写解析规则。

## 6. 本地快照实测数据（8202 环境，2026-08-13 ~ 08-19）【已验证】

用 `GET /channels/report` 取得。**收入侧真实，成本侧全空。**

```
16 个域名 / 69 渠道（29 启用）/ 149,604 请求
下游收入合计 = $15,987.37
上游成本合计 = $0.00   ← 全部 16 个域名 upstream_usage.available = false
```

成本侧为空的原因【已验证】：测试包剥离了上游凭据，且
`docker-compose.intern-local.yml` 设 `MONITOR_UPSTREAM_SYNC_ENABLED: "false"`，
上游同步从未运行。**这是测试环境产物，不能推断生产状态。**

已知数据缺陷，实现时必须处理：

- **10/16 域名配了上游账号，6 个没配。** `aicodewith.com` 下游收入 $4,346 却
  `upstream.configured = false` —— 这个域名注定算不出成本。
  必须标 `cost_missing`，**不得按 0 成本算出 100% 毛利率**。
- `bigsnake.xyz` 等域名 `recharge_paid/credit` 为空（表中显示 `-`），换算系数缺失同样要标记。
- 站点级 `finance.fx_benchmark = 7`（人民币汇率），`site_recharge_paid/credit = 1/1`。

## 7. 实现分层与落点

### 7.1 优先级建议（与用户需求排序一致）

**P1 · 客户排障链路**——价值最高、风险最低、不依赖上游数据。**后端已实现，见第 13 节。**
纯下游数据即可完成：客户 → 请求 → 渠道 → 上游主域名 → 模型 → 错误原文下钻。

> ~~现有 `LogRow` 已有全部字段，缺的只是把 `channel_id → BaseDomain` 呈现出来的 UI 与接口。~~
> **这句是错的，已作废。** `LogRow` 无 `ChannelID` 且故意不给（客户面），
> 管理员面也**原本没有任何逐条日志接口**。详见 2.1.1 与第 13 节。

**P2 · 域名级日毛利**——需要上游按量数据存在。
`MONITOR_UPSTREAM_SYNC_ENABLED` 默认 `true`【已验证，见 11 节问题 1】，
仅需确认 `ChannelUpstreamUsageHour` 实际是否已攒到数据。
必须实现 4.2 的兑换比换算与 4.3 的版本化。

**P3 · 模型级毛利 + 客户级分摊**——前置是 3.3 的上游 model 维度确认。
改 `ChannelUpstreamUsageHour` 主键 → 必须 bump plan ID。
**注意：用户已确认要客户级利润，而 4.4.1 证明它同样依赖上游 model 维度**，
因此这一层不再是"可选增强"。

### 7.2 各层落点

| 层 | 位置 | 说明 |
|---|---|---|
| 配置 | `Settings` @ [settings.go](../monitor/settings.go) | 环境变量 + 启动校验；参考 `LocalSnapshotOnly` 写法 |
| 表结构 | [store.go:786](../monitor/store.go#L786) `AutoMigrate` | 加 model 后**必须** bump `preMigrationPlanID` |
| 迁移门禁 | [store_migration_backup.go:30](../monitor/store_migration_backup.go#L30) | 当前 `main-facts-schema-20260818-v12` → 改 schema 时递增 |
| 只读接口 | [server.go:168](../monitor/server.go#L168) `view` 组 | 管理员可读 |
| 写接口 | [server.go:234](../monitor/server.go#L234) `rootChannels` 组 | 仅超管 |
| 采集 | 后台 goroutine + 持久游标 + 退避 | 参考 [stability_sampler.go](../monitor/stability_sampler.go) |
| 前端导航 | [page.html:276-283](../monitor/page.html#L276-L283) | 现有 6 个 tab，新增 tab 加在此处 |
| 前端逻辑 | 新建 `finance.js` / `logchain.js` | 照 [channel_management.js](../monitor/channel_management.js) 模式；`go:embed` + `server.go` 挂路由 |

前端是**零构建**：React 18.2 / Semi UI 2.72.2 / ECharts 全部 `go:embed` 内嵌自服务，
不走 CDN、不需要 npm。ECharts 可直接画趋势图。

### 7.3 新增事实表建议

- `UpstreamCostDailyFact` —— 上游成本日汇总（**已换算为现金**，保留原始额度值与所用兑换比以便复核）
- `MarginDailyFact` —— `date_ts × domain → 收入 / 成本 / 毛利 / 毛利率 / coverage 状态`
- `CustomerTopupDailyFact` —— 客户充值日汇总（含 `unparsed` 计数）

**金额用整数存微美元**。现有 `CostUSD float64` 日累加会有浮点漂移，
SOP 3.2 节明确要求"金额、Token、时间等关键字段使用合适的数据类型，避免浮点误差"。

每张表都要带 coverage 证明字段，遵循 usage-facts 既有模式：
**能区分"真实为零"与"尚未采集"，没有 coverage 证明的日期不得显示 0。**

## 8. 对账层：回答"核算出的毛利额是否正确"

用户明确问了这一点，不能只算不验。核算正确性**不能靠自证**，要用独立控制量：

```
上游账户余额变化（balance_usd 前后差）  vs  同期小时成本累加 × 兑换比
```

差异超阈值 → 标 `unreconciled`，**页面不显示毛利数字**，而不是显示一个错的。
这套"独立控制查询 + 差异归零才发布"的模式在 usage-facts-v2 第 8 节已有完整先例，照抄。

## 9. 硬约束（违反即 SOP 的 P0/P1 阻断项）

1. **生产 MySQL 只读**，仅 `logs`/`channels`/`users`/`tokens`/`options` 五表 SELECT。
   不建索引、不改表、不写测试数据。采样器是唯一访问它的组件。
2. **页面永不直连生产库**，只读本地 SQLite。新 SQL 必须有界：时间窗 + 分页 + 预算 + 可取消。
3. **公开面**（`/status`、`monitor/public` 包）绝不输出渠道名/ID/IP、成本/配额、
   令牌/用户、请求量/QPS、错误详情。**毛利数据属于最敏感一类，绝不能进公开面。**
4. **改 schema 必须 bump `preMigrationPlanID`**，否则踩迁移前双库快照门禁。
5. 上游 API 调用不得放宽现有限流（50 页 / 60 请求 / 200ms / 全局串行 / 指数退避）。
6. 可选组件（Redis / 上游 API）故障不得拖垮主功能，且不得缓存错误结果。

## 10. 开发环境

- **Windows 本地 `go build ./...` 会失败**，这是预期的：[portal.go](../monitor/portal.go)、
  `store_reliability.go`、`usage_facts_capacity.go`、`cmd/hostagent` 用了 Linux 专属
  syscall（`Statfs`/`Flock`/`Stat_t`）。自验必须用：

  ```bash
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...
  go test ./...
  ```

- **`gofmt -l .` 会列出几乎所有文件**，那是 `core.autocrlf=true` 造成的 CRLF，
  不是格式错误。判断真实格式问题要先 `tr -d '\r'`。
- **本地环境重建**：`./.local-test-kit/stop.sh && ./.local-test-kit/start.sh`，
  地址 `http://127.0.0.1:8202/`，免登录已由测试包的两只 Nginx 完成
  （`local-auth` 假装 NewAPI 返回 role=100，`local-entry` 拦截 `GET /login`）。
  **不要为免登录修改源码鉴权** —— 现方案源码零改动，改了会有误提交泄漏风险。
- `.local-test-kit/` 不提交（已在 `.git/info/exclude`）。
- `dev/run-local-dev.sh` 是另一套 zsh 脚本、端口 8100，**不要和 8202 混**。
- 本机无 `jq`，`python` 是 WindowsApps 占位符不可用。解析 JSON 用 `node`，
  且 node 是 Windows 原生程序**不认 MSYS 的 `/tmp` 路径**，要用仓库内相对路径。

## 11. 必须先向用户确认的问题

按阻塞程度排序：

1. ~~**生产的 `MONITOR_UPSTREAM_SYNC_ENABLED` 是开的吗？**~~ **代码里已有答案【已验证·第二轮】**：
   [settings.go:255](../monitor/settings.go#L255) 默认 `true`，
   只有三个本地/验收 compose 显式设 `false`（`.local-test-kit/docker-compose.intern-local.yml`、
   `docker-compose.local-acceptance.yml`、`docker-compose.local-production-readonly.yml`）。
   生产未显式关闭即为开启。
   **剩余待确认：`ChannelUpstreamUsageHour` 实际是否已攒到数据**——
   看生产 `GET /channels/report` 的 `upstream_usage.available` 与 `cost_usd`。
2. **上游各站的兑换比历史上变过吗？**【未知】——决定 4.3 是否必须先做版本化。
   这条比第 1 条更容易埋雷：用错的汇率算历史账不会报错。
3. **NewAPI / Sub2API 的上游 API 是否返回 model 维度？**【未知】——
   决定 P3 可行性。需要真实接口响应或抓包。
4. **`aicodewith.com` 这类有收入无上游账号的域名，成本怎么算？**
   是漏配还是自有渠道？影响 `cost_missing` 的处理策略。

## 12. 上一轮会话的已知失误（避免重复踩）

诚实记录，供接手者判断本文档可信度：

1. 曾提议在 `monitor/settings.go` 加 `DevDisableAuth` 环境变量绕过登录 ——
   用户拒绝，且方案确实劣于测试包的 Nginx 方案（改源码有误提交泄漏风险）。**已作废。**
2. 曾误报 `LogRow` 含 `UpstreamCostUSD`/`MarginUSD` 字段 —— **实际不存在**，grep 零命中。
3. 曾编造"987xyz 上游报 $1174" —— **实际 `upstream_usage.cost_usd = 0`**。
   任何数字必须来自实际工具输出，不得凭印象写。
4. 工具曾连续返回空结果（`grep -c ""` 对已知存在的文件也返回空），
   那是**工具故障不是"没找到"**。遇到反常空结果先用一个必然有输出的命令自检，
   不要把空结果当结论。

---

## 13. P1 排障链路：已实现内容 【第二轮会话产出】

### 13.1 交付物

| 文件 | 状态 | 说明 |
|---|---|---|
| [monitor/logchain.go](../monitor/logchain.go) | 新增 | 全部后端逻辑 |
| [monitor/logchain_test.go](../monitor/logchain_test.go) | 新增 | 8 个测试，钉住三条易被后续改动破坏的约束 |
| [monitor/server.go](../monitor/server.go#L181) | 改 1 行 | `view` 组挂路由 |

**前端未做**（`logchain.js` + `page.html` 改动尚未开始，方案见 13.6）。

### 13.2 接口

```
GET /logchain/requests        # view 组，requireRole(roleAdmin)
```

参数：`days`(默认1) | `from`+`to`(YYYY-MM-DD)、`user_id`、`channel_id`、`domain`、
`model`、`group`、`token_name`、`request_id`、`keyword`、`error_only`、`type`(1-6)、
`before_id`(游标)、`limit`(默认50/上限200)。

响应：`{ok, rows[], has_more, next_before_id, scope{}, blind_spots[]}`。
`LogChainRow` 含客户侧（`user_id`/`member`/`group`/`token_name`）、
上游侧（`channel_id`/`channel_name`/`channel_vendor`/`upstream_domain`/`channel_status`/
`channel_deleted`/`channel_unresolved`）、请求侧（模型/映射后上游模型/tokens/耗时/首字/路径）、
`cost_usd`、`content`（**原文**）。

### 13.3 三个关键实现决定（改动前请先读懂再动）

1. **`content` 不过 `scrubContent`。** 这是本接口存在的理由（见 2.1.3）。
   `TestScrubContentWouldBlankUpstreamErrors` 断言
   `scrubContent("渠道 gpt-relay (#12) 返回错误：status_code=429...") == ""`。
   **谁把 logchain 的 content 接回 scrubContent，该测试会红**——
   而不是让排障静默失效（有行、有时间、错误原文全空白）。
2. **默认 `type IN (2,5)`，不是事实表的 `(2,6)`。** 照抄事实层口径会滤掉全部错误。
   `TestLogChainWhereIncludesErrorLogs` 显式断言 SQL 中**不出现** `type IN (2,6)`。
   显式传 `type` 或 `error_only=true` 可覆盖；`error_only` 与 `type≠5` 同传视为矛盾，返回 400。
3. **快照查不到渠道时标 `channel_unresolved`，不留空。**
   留空会被读成"这条请求没有上游域名"，真实含义是"我们的快照没覆盖到"。
   `channel_id=0`（未打到渠道）两者都不标——它本就没有渠道。三态在 UI 上必须分开显示。

### 13.4 结构性约束

- **生产库与本地库是两个连接，不能在一条 SQL 里 join。** 故分两步：
  先查生产 `logs`（含 `channel_id`），再用 `attachChannelSnaps` 批量查本地 `channel_snaps` 补全。
- **按 `domain` 筛是"先本地反查渠道 ID，再进生产库"**（`resolveDomainChannelIDs`）。
  域名无对应渠道时直接返回空集，不打生产库。
  命中数超 `logChainMaxDomainChans=500` 时返回 `domain_channels_truncated=true`——**不静默截断**。
- **遵守 9.2 节有界要求**：时间窗（跨度硬上限 31 天，超出砍 from 端保 to 端）、
  游标分页（`id < ?` 倒序，不用深 OFFSET）、`MAX_EXECUTION_TIME(8000)`、
  25s context 超时、复用 `acquireInteractiveUsageDetailGate` 并发闸门、
  多取一行判 `has_more` 而不做 `COUNT(*)`。
- **`scope` 回显生效范围。** 用户传的值可能被收敛（跨度截断、limit 上限），不回显会让前端
  以为筛选按原样生效了。
- **`blind_spots` 随每次响应返回**（3.1.1 的三条）。写进接口而非只写文档，
  因为最可能造成的实际损害是：客户说"请求发不出去"，你查不到，得出"他在瞎说"。
- **`LocalSnapshotOnly` 下 `prodDB` 为 nil**，返回"生产库未连接：本地快照只读模式无法查询请求明细"
  而不是 panic。8202 验收环境正是此模式，**因此那里只能验接口存在与报错文案，验不了真实数据。**
- 排除渠道测试流量（复用 `channelTestLogPredicateSQL()`），与既有口径一致。
- 全部用户可控值参数化；`LIKE` 值走 `escapeLike` + `ESCAPE '!'`。
  `TestLogChainWhereParameterizesUserInput` 断言占位符数与参数数相等、通配符已转义。

### 13.5 验证状态（诚实记录）

| 项 | 结果 |
|---|---|
| `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...` | **通过** |
| `go vet ./monitor/` | logchain 相关无告警 |
| `gofmt`（`tr -d '\r'` 去 CRLF 后比对） | 三个文件均干净 |
| logchain 定向测试 8 个 | **全部 PASS**（容器内实跑） |
| `go test ./...` 全量回归 | **未完成**——Docker 守护进程磁盘中途变只读 |

全量回归跑到 279s 时容器 `/tmp` 变只读，随后一批 `TempDir` 失败均为
`read-only file system`，**不是断言失败**；最终 `docker run` 自身也报
`/var/lib/desktop-containerd/.../meta.db: read-only file system`。
**接手者请重启 Docker Desktop 后补跑全量回归。**

#### 13.5.1 在 Windows 上真跑测试的办法 【已验证·可复用】

第 10 节说"Windows 本地 `go test` 会失败"，这是对的（Linux 专属 syscall），
但**可以用一次性容器跑，不必重建部署容器**：

```bash
MSYS_NO_PATHCONV=1 docker run --rm \
  -v "//d/monitorcode/newapi-monitor://src" \
  -v "//c/Users/86177/go/pkg/mod://go/pkg/mod" \
  -w //src \
  -e GOFLAGS=-mod=mod -e GOCACHE=/tmp/gocache -e GOPROXY=off \
  golang:1.25 go test ./monitor/ -run 'LogChain' -v
```

两个要点：
- **必须挂宿主机 `GOMODCACHE`**（`go env GOMODCACHE`）并设 `GOPROXY=off`——
  容器内连不上 `proxy.golang.org`（connection refused），但宿主缓存是全的。
- `MSYS_NO_PATHCONV=1` + 双斜杠路径，绕开 MSYS 的路径改写。

### 13.6 前端方案（未实现，待用户确认一个默认值）

- **不引入 React。** 现有 tab（[channel_management.js](../monitor/channel_management.js)、
  [stability.js](../monitor/stability.js)）都是**原生 JS + IIFE + 字符串拼 HTML**；
  React/Semi 只为日期控件存在（`range_picker.js` 适配层）。
  新建 `logchain.js`，`go:embed` 挂 `/logchain.js`，暴露 `window.logChainActivate()`。
- **主入口是跨页跳转，tab 只作落地页。** 排障真实起点是"客户报障"——手上有的是**客户**，
  不是 request_id。新建空白 tab 让人从零填筛选，等于把最常见路径做成最长路径。
  走现成机制：[monitorNavigate](../monitor/page.html#L1440) +
  [monitorNavigationContext](../monitor/page.html#L1439)（`channelManagementOpen` 是范例）。
  路径：用户用量 → [usageOpenDetail](../monitor/page.html#L1629) 客户详情 → 「排障」按钮
  → 带 `user_id` 跳转。反向也要通：排障行点上游域名 → 跳渠道管理。
- **`page.html` 需改四处**：两处导航栏 tab 按钮
  （[277-282](../monitor/page.html#L277-L282) 与 [299-304](../monitor/page.html#L299-L304)）、
  `tab-logchain` 容器、[switchTab](../monitor/page.html#L1442) 加 hidden 切换与激活调用、
  [2904 行](../monitor/page.html#L2904)的 tab 名白名单正则加 `logchain`。
- **表格**：一行一请求，**错误行整行标红底**。列
  `时间 | 客户 | 模型 | 上游域名 | 渠道 | 耗时 | 费用 | 状态`。
  `上游域名` 是本功能全部意义所在，列宽给足。
  行展开放错误原文全文（`<pre>` 保原样，**不折行不美化**——要拿它去问上游客服）
  \+ `request_id` + token 明细 + 请求路径。
- **`blind_spots` 固定显示在筛选栏下方**，不得收进折叠面板（理由见 13.4）。
- **待用户拍板**：`error_only` 默认开还是关。
  默认开＝最快定位，但看不到"该客户大部分请求其实成功"的背景，易把偶发错误当系统性故障；
  默认关＝一眼看出错误占比（建议，配合整行标红 + "本页 N 条中 M 条错误"计数）。

### 13.7 还原方式

```bash
rm monitor/logchain.go monitor/logchain_test.go
git checkout monitor/server.go   # 只加了一行，撤它安全
```

## 14. 第二轮会话的修正与失误

### 14.1 对初版文档的修正（三处事实错误）

1. **7.1 节"`LogRow` 已有全部字段"——错。** `LogRow` 无 `ChannelID`，SQL 也没 SELECT，
   且注释表明是故意不给（客户面）。与第 12 节第 2 条同类失误：**声称字段存在前必须 grep 确认。**
2. **3.3 节端点 `/api/log/self`——错**，实际 `/api/log/`。
3. **11 节问题 1 被列为"最高阻塞的未知"——过度保守。** 答案就在
   `settings.go` 默认值里，不需要问用户。

### 14.2 本轮自身的失误

1. **曾说"客户排障不需要上游日志"，措辞过窄。** 该判断仅对"给单条请求补上游信息"成立；
   用户实际要的是"上下游日志都要拿到"（排障 + 日利润两个交付物），
   而**利润的成本侧必须有上游日志**。已在 3.1 节补措辞澄清。
   教训：把"技术上此路不通"表述成"你不需要它"，会被读成否定需求本身。
2. **`grep_search` 工具多次只返回文件名、不给行号内容**，一度看起来像"没找到"。
   与第 12 节第 4 条同源：**改用 `bash grep -n` 才拿到真实结果。**
   反常的空/残缺结果先自检工具，不要当结论。

