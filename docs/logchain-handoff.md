# 客户排障（logchain）· 接手说明书

- 用途：让接手的 AI / 开发者**在不重读全部代码的前提下**继续这项工作
- 覆盖范围：P1 客户排障链路（后端 + 前端 + 异常日志）
- 最后更新：2026-08-21
- 相关文档：[upstream-downstream-log-chain-dev-spec.md](upstream-downstream-log-chain-dev-spec.md)（需求与现状调研）、
  [QUALITY_REVIEW_AND_TEST_SOP.md](QUALITY_REVIEW_AND_TEST_SOP.md)、
  [../使用手册.md](../使用手册.md)（本地环境安装）

> 本文只记**已跑通并验证过的做法**。踩过的坑写成"为什么这样做"，
> 因为不知道理由的人会把防御当冗余顺手删掉。

---

## 第一部分：用户的工作要求（**先读这段，它比技术细节更容易踩雷**）

这几条是用户明确提过的，违反会被直接打断。按重要性排：

### 1. 绝不主动提交代码

本地开发阶段只改文件，**不跑 `git add` / `git commit` / `git branch` / `git reset`**。

- 改完照常自验（构建 + 全量测试 + 必要时 fixture 实测），**报结果就停**
- 不要问"要不要提交"，等用户明说"提交吧"
- 用户说"保存"**不等于**"提交" —— 本地开发时"保存"就是写到磁盘

### 2. 只在被要求时更新文档

默认**不动** `docs/`。做完功能就报结果，不附带文档提交。

- 发现文档与代码不一致：**口头告诉用户**，不要顺手改
- 代码注释与 commit message **照常写**（仓库风格是中文详注），那随代码走，不算"更新文档"
- 本文件是用户明确要求写的，属例外

### 3. 一律用中文回复

- 面向用户的全部文字：正文、结论、报告、提问、选项、表格
- **长任务中途最容易漂回英文**（大量工具调用后、写完代码报结果时），那些节点要盯住
- 技术标识符不翻译：函数名、字段名、SQL 关键字、环境变量、错误码原文
- 上游返回的错误原文不翻译（排障要拿原文去问上游客服）

### 4. 新增功能不得妨碍既有功能

"妨碍"不只指改坏既有代码，**也包括抢占既有功能依赖的共享资源** —— 这类回归编译能过、测试也不报。

加功能时必须查三件：

1. **共享泳道/信号量**：新查询用了哪个闸门？容量多少？还有谁在用？
2. **新功能的超时/预算 <= 既有调用方**，内部功能不得比客户功能占用更久
3. **改动是否纯增量**：新增文件 + 路由加一行最安全

实例见本文 §3.1（15 秒闸门那条）。

### 5. 优先级：先把 P1 做完美，P2/P3 暂缓

P2（域名级日毛利）/ P3（模型级毛利 + 客户级分摊）**不要主动开工**，即使前置条件已具备。

用户已确认上游 `/api/log/` 自带 `model_name`（2026-08-20 实测 987xyz），
**地基具备 ≠ 该做**。等用户说开工。

### 6. 上游只考虑 NewAPI，不考虑 Sub2API

用户 2026-08-21 明确。这条只影响 P2/P3。

---

## 第二部分：这个功能是什么

管理员专用的只读查询页，回答客户报障时的核心问题：
**这条请求走了哪个上游、上游返回了什么、有没有异常。**

入口在侧边栏「数据同步状态」**下方**（用户指定位置），名为「客户排障」。

### 2.1 数据来源：**只用下游日志**

这点最容易误解。上游供应商自己的日志（`ChannelUpstreamUsageHour` 那套）**一次都没用到**。

| 页面上显示的 | 实际来源 |
|---|---|
| 上游主域名 | `logs.channel_id` → 本地 SQLite `channel_snaps.base_domain` |
| 上游返回原文 | `logs.content`（new-api 把上游报错抄在这里） |
| 流结束状态 | `logs.other` 的 `$.stream_status.end_reason` |
| 客户/令牌/分组/模型/tokens/耗时 | `logs` 表自己的列 |

准确说法：**下游日志本来就记着"这条请求发给了哪个渠道"和"上游回了什么话"**，
本功能把 `channel_id` 这个内部编号翻译成人看得懂的域名。

**为什么不用上游日志**：NewAPI 未修改版本会把响应里的 `id` 重写成页内序号，
上下游都没有稳定主键，逐条 join 不成立。上游日志的定位是 P2 的独立对账证据。

### 2.2 两个接口

```
GET /logchain/requests    逐条明细（查生产 logs）
GET /logchain/filters     筛选下拉取值（只读本地 channel_snaps）
```

都挂在 `view` 组（管理员及以上），见 [server.go](../monitor/server.go) 的路由表。

`/logchain/requests` 参数：

| 参数 | 说明 |
|---|---|
| `days` / `from`+`to` | 时间窗。单日查询传 `from=to=当天`，后端把 to 当天整日纳入 |
| `user_id` / `token_name` | 客户。前端按"纯数字→user_id，否则→token_name 模糊"分派 |
| `channel_id` / `domain` / `model` / `group` | 维度筛选 |
| `request_id` / `keyword` | 精确查一条 / 错误原文关键词 |
| `error_only=true` | 只看错误（type=5） |
| `anomaly=<类型>` | 异常筛选，见 §4 |
| `order=asc` | 时间正序（缺省倒序） |
| `before_ts`+`before_id` | 复合游标，**必须成对** |
| `limit` | 默认 50，上限 200 |

响应：`{ok, rows[], has_more, next_before_ts, next_before_id, scope{}, blind_spots[]}`

`/logchain/filters` 响应：`{ok, groups[], domains[], channels[{id,name,domain,deleted}]}`
单独一个接口而非塞进 requests：下拉选项与所选日期无关，换一天不该重算，
也不该因当天没有错误就让下拉变空。已删除的渠道也列出（历史请求仍要能按它筛）。

### 2.3 表格

列序（用户指定）：

```
客户 | 令牌 | 分组 | 模型 | 渠道 → 上游主域名 | 上游返回原文/异常详情 | 时间
```

- 时间在**最后一列**、精确到**时分**，hover 显示完整日期到秒
- 错误行**红底**，异常行**黄底** —— 错误=明确失败，异常=成功了但有问题，
  视觉上混为一谈会让人分不清"要立刻处理"和"要核查"
- 表头两行：主名称 + 灰色副说明，省掉一堆要悬停才看得到的解释
- 点表头「时间」或用按钮组切换正/倒序
- 行展开：全部字段 + 错误原文全文（`<pre>` 原样，**不折行不美化** ——
  要能原样拿去问上游客服）+ 复制按钮 + 跳渠道管理按钮

筛选范围（互斥单选，**没有"全部请求"档** —— 本页只看问题，正常请求去「用户用量」看）：

```
[错误] [流异常] [消费异常] [全部异常] [错误+异常]
```

默认停在「错误」。

---

## 第三部分：**改动前必须读懂的八条**

这七条都有测试钉住。测试红了先读这里，不要直接放宽断言。

### 3.1 闸门超时必须 <= 15 秒（不得挤占客户 Portal）

`usageDetailGate` **容量只有 1**（[monitor.go](../monitor/monitor.go) 的字段注释），
客户自助面查自己日志的 `countGroupLogs` / `queryGroupLogs`（均 15s 超时）**走同一条泳道**。

初版实现设了 25s，意味着一次内部排障查询能让**客户查自己日志多排队 10 秒**。
编译能过、测试不报，属隐性回归。

规则：**排障是内部功能，不得比客户功能占用更久。**
`MAX_EXECUTION_TIME(8000)` 也必须小于闸门超时 —— 否则闸门先超时释放、
SQL 仍在生产库上跑，等于绕过并发控制。

钉住它的测试：`TestLogChainGateTimeoutDoesNotStarveExistingFeatures`

### 3.2 `content` 绕过 `scrubContent`

`scrubContent` 的逻辑是：content 含"渠道"二字就**整段清空**。
而 new-api 的上游错误原文经常正好长这样：

```
渠道 LA-claude-max (#31) 返回错误：status_code=429, message: rate_limit_error...
```

不绕过它，最有用的那批错误全是空白。这个函数对客户面是正确的纵深防御，
对管理员排障面是致命的。

钉住它的测试：`TestScrubContentWouldBlankUpstreamErrors`
（它断言 `scrubContent("渠道 ...")` 返回空串 —— 谁把 logchain 的 content
接回 scrubContent，测试会红，而不是让排障静默失效：有行、有时间、错误原文全空白）

### 3.3 直查生产 `logs`，不走本地事实表

事实表口径是 `type IN (2,6)`（消费 + 退款），**把 type=5 错误全滤掉了**。
而排障主要看的就是错误。照抄事实层口径，这个功能从根上失效。

钉住它的测试：`TestLogChainWhereIncludesErrorLogs`（显式断言 SQL 里**不出现** `type IN (2,6)`）

### 3.4 不改 `LogRow`，新建接口

`LogRow` 服务客户自助面 portal，**故意不含渠道字段** ——
[usage.go](../monitor/usage.go) 有注释："渠道等内部字段天然不解析、不外传"。
往它加 `base_domain` 等于把"你用哪些上游供应商"告诉客户，属经营秘密。

初版调研文档曾写"`LogRow` 已有全部字段，只缺 UI" —— **那是错的**，
它把"客户排障"（你排查客户报的故障）和"客户自助查日志"混成了一件事。

### 3.5 按 `created_at` 排序，不按 `id`

**这条是造假数据实测才抓出来的，读代码发现不了。**

原实现按 `id DESC`，注释还写着"id 近似时间序"。fixture 一跑就露馅：
15:40、14:02、09:13 排在 13:22、11:47 前面。

根因：**new-api 在请求完成时才写日志**。一个耗时 60 秒的超时请求，
会比后发起、快速失败的请求更晚拿到 id。所以 id 序 ≠ 发生时间序，
**生产上同样会乱**。而用户要的就是按发生时间排。

教训：**"近似"成立的前提要验，不能采信注释里的断言。**

排序键在 `logChainOrderBySQL(asc bool)`，抽成函数是为了让实现与测试
共用同一份字面量，避免"改了 SQL 但测试还断言旧写法"。

钉住它的测试：`TestLogChainOrdersByOccurrenceTime`

### 3.6 复合游标 `(created_at, id)`，且方向跟随排序

排序键变成 `created_at` 后，单用 `id` 已无法定位续查位置。

```sql
-- 倒序取更早的
created_at < ? OR (created_at = ? AND id < ?)
-- 正序取更晚的
created_at > ? OR (created_at = ? AND id > ?)
```

三个要点：

- **写成 `OR` 形式而非行值比较**（`(created_at,id) < (?,?)`）：前者能用上
  `created_at` 索引，后者在 MySQL 上未必走索引
- **同秒多条用 `id` 破平**，否则翻页会漏行或重复
- **比较方向必须跟随排序方向**。方向写死会让"加载更多"在正序下往回翻、
  重复吐出已看过的行 —— 首页看不出来，**只在翻第二页时才暴露**
- **只给半个游标显式返回 400**，不静默忽略 —— 静默会让"加载更多"从头再来

钉住它的测试：`TestLogChainCursorFollowsSortDirection`、`TestLogChainCursorRequiresBothParts`

### 3.7 渠道三态必须分开显示

| 显示 | 含义 |
|---|---|
| `AWS-CH1 → 208.98.41.154` | 正常 |
| `⚠ 渠道快照缺失` | 本地快照查不到该渠道，**不等于没有上游** |
| `未打到渠道` | `channel_id=0`，请求在选渠道前就失败了 |

留空会被读成"这条请求没有上游域名"，而真实含义完全不同。

钉住它的测试：`TestAttachChannelSnapsMarksUnresolved`

### 3.8 生产库与本地库是两个连接

**不能在一条 SQL 里 join。** 所以分两步：先查生产 `logs` 拿 `channel_id`，
再用 `attachChannelSnaps` 批量查本地 `channel_snaps` 补全。

按 `domain` 筛也是反过来做：先本地反查渠道 ID（`resolveDomainChannelIDs`），
再进生产库。域名无对应渠道时直接返回空集，不打生产库。
命中数超 500 时返回 `domain_channels_truncated=true` —— **不静默截断**。

---

## 第四部分：异常日志（两类，用户 2026-08-21 确认的范围）

### 4.1 参数

```
anomaly=stream           流状态异常
anomaly=billing          消费异常（两个方向）
anomaly=billing_unpaid   只看扣费未交付
anomaly=billing_free     只看交付未扣费
anomaly=all              流 + 消费
anomaly=err_anom         错误 + 流 + 消费（本页能查到的全部问题）
```

三类冲突**全部显式返回 400**，不静默忽略：

- 与 `error_only` 同传 —— 错误是 type=5、异常是 type=2 里的问题请求，交集为空，必然 0 行
- 与 `type≠2` 同传 —— 异常判据都限定 type=2（`err_anom` 例外，它本身跨 type）
- 取值拼错 —— 会退化成"全部请求"，而人以为在看异常清单

钉住它的测试：`TestLogChainAnomalyRejectsConflicts`

### 4.2 流状态异常：**排除法，不是枚举法**

```sql
end_reason NOT IN ('eof','') OR error_count > 0
```

字段路径是 `other.$.stream_status.end_reason`。已知取值：

| 值 | 含义 |
|---|---|
| `eof` | 正常结束 |
| `client_gone` | **下游客户端断连**（客户的程序/浏览器关了连接） |
| `timeout` | 超时 |
| `scanner_error` | 流解析出错 |
| `panic` | 崩了 |
| `ping_fail` | 心跳失败 |

**为什么用排除法**：枚举漏掉的新取值会被静默吞掉。new-api 升级新增 `end_reason` 时，
枚举写法会假装它不存在 —— 而排障最怕"没见过的情况被藏起来"。
fixture 里专门造了一条 `brand_new_reason_v2` 验这个。

排除空串是因为**非流式请求的 `other` 里根本没有 `stream_status`**，取不到值。

钉住它的测试：`TestLogChainStreamAnomalyUsesExclusion`

### 4.3 消费异常：两个方向

| 方向 | 判据 | 含义 | 谁亏钱 |
|---|---|---|---|
| `billing_unpaid` | `quota > 0 AND completion_tokens = 0` | 客户付了钱，一个 token 都没拿到 | 客户 |
| `billing_free` | `quota = 0 AND completion_tokens > 0` | 内容给了，钱没收 | **我方** |

两个方向都限定 `type = 2`，且都排除天然无输出模型：

```
model_name NOT REGEXP 'embed|rerank|bge-|m3e|image|seedream|seedance'
```

不排除的话每条 embedding 请求都会被判成"扣费未交付"，**整类误报**。

`billing_free` 还必须排除订阅计费：

```
billing_source <> 'subscription'
```

订阅走订阅额度而非钱包 quota，`quota` 恒为 0 属正常。
不排除会把**所有订阅客户的请求整批误报成漏计费**。

钉住它的测试：`TestLogChainBillingAnomalyBothDirections`

### 4.4 判"是否真交付"只能用 `completion_tokens`

这个选择沿用仓库既有口径（[sampler.go](../monitor/sampler.go) 的注释论证过），
另两个候选都不行：

- **`frt`（首字延迟）不行** —— stream_scanner 在任何 `data:` 行（含 Claude 的
  `message_start`）都会置位，只证明"上游开了口"，不证明客户拿到内容。
  哪怕下一秒断掉 `frt` 也有值。
- **`prompt_tokens` 不行** —— 上游不返 usage 时 new-api 会本地估算输入并照此扣费，
  有输入 token 不代表上游真的处理了。

### 4.5 `end_error` 只展示，绝不参与判定

`end_error` 是自由文本（如 `context canceled`），内容**可能含 `panic` 等词**。
拿它做字符串匹配会误命中 —— 仓库注释记录过这正是旧口径误报的来源之一。

钉住它的测试：`TestLogChainAnomalyNeverReadsEndError`

### 4.6 不复用 `expandAnomalyPredicates`（这是有意的）

那套服务**稳定性报表**，判据是：

```go
end_reason IN ('timeout','scanner_error','panic','ping_fail') OR error_count > 0
```

**四个值里没有 `client_gone`，这是故意的** —— 客户自己断开既不是我方也不一定是
上游的错。要是算进渠道稳定性，客户关个标签页就会拉低渠道评分，那个指标会失真。

而排障页要的**恰恰是** `client_gone` —— 客户的实际体验是"回答没出来"。

| | 稳定性报表 | 客户排障 |
|---|---|---|
| 问题 | 这个渠道/模型稳不稳 | 这个客户遇到了什么 |
| `client_gone` | 排除（非我方故障） | **要看** |

所以两者**目标不同、判据不同**。改 `expandAnomalyPredicates` 会让历史稳定性数据的
判定标准变化，属破坏既有功能。排障页复用它的**取值 SQL**
（`anomalyEndReasonSQL` / `anomalyErrCountSQL`，它们已处理三个坑：
`JSON_VALID` 兜底、`COALESCE` 防 NULL 传染、用 `REPLACE(CAST(...))` 而非
MySQL 专有的 `JSON_UNQUOTE` 以保持本地 SQLite 假库也能执行），但**判定组合自己写**。

钉住它的测试：`TestLogChainDoesNotAlterStabilityPredicates`

### 4.7 `COALESCE` 兜 NULL 是必须的（很隐蔽的坑）

非流式请求的 `other` 里没有 `stream_status`，`JSON_EXTRACT` 返回 NULL。而：

```
NULL IN (...)  = NULL
FALSE OR NULL  = NULL
NOT NULL       = NULL
SUM 跳过该行
```

结果**只丢成功数、异常数反而正常**（因为 `TRUE OR NULL = TRUE`），极难察觉。

### 4.8 `anomaly_tags`：每行自证

后端给每行打标签（`stream` / `billing_unpaid` / `billing_free`），
一行可同时命中多类（如 `client_gone` 且扣费未交付）。

**标签由后端判定，前端不自己算** —— 两处各判一次一旦口径不一致，
会出现"筛出来了但没标签"这种自相矛盾的结果。

但 SQL 侧和标签侧**确实各写一份**（SQL 在库里筛，标签给已捞回的行打标记），
改动时必须同改两处。`TestLogChainAnomalyTagsMatchSQL` 用 12 个用例钉住一致性，
`TestLogChainNoOutputModelListMatchesSQL` 钉住两侧的模型名单同步。

### 4.9 `client_gone` 有个结论**给不了**

它只记录"谁断的"，不记录"为什么断"。这两种情况在数据上完全一样：

- 客户主动取消（关标签页、Ctrl+C）→ 正常行为
- 上游太慢，客户等不住走了 → **真问题，根因在上游**

所以**不替用户归类**，只把 `end_reason` 原值和耗时并排显示。
耗时 47 秒的 `client_gone` 大概率是后者，2 秒的大概率是前者 ——
这是唯一的旁证，判断权在用户。

### 4.10 明细列随范围切换（第三点的由来）

`logs.content` 在不同类型里装的是**完全不同的东西**：

| 类型 | `content` 装什么 |
|---|---|
| `type=5` 错误 | 上游返回的错误原文 ← 排障要看的 |
| `type=2` 消费 | **计费摘要**（"模型倍率 3.00, 分组倍率 1.00"） |

而异常行全是 `type=2`。表头固定写"上游返回原文"，
最显眼的位置就会摆一句无用的计费摘要，真正有用的信息挤在下面一行小字。

修法：表头与内容按**行的性质**切换（不按当前筛选 —— `err_anom` 混排时
两种行并存，各自显示对自己有意义的东西）。计费摘要退到展开区，
标题如实写成「计费摘要（logs.content，非上游返回）」。

钉住它的测试：`TestLogChainDetailColumnSwitchesByScope`

### 4.11 已知的误报风险（**需要真实数据核对**）

`billing_free`（交付未扣费）**很可能有误报**。`quota = 0` 的合法情况不止订阅一种 ——
免费额度、内部测试令牌、促销策略都可能，而这些从代码里查不出来。

**给接手者的动作**：用户第一次在真实数据上看到「交付未扣费」时，
让他抽几条核对是否真漏收。发现某类是正常的，就把它的特征加进排除条件。

三个标签的可信度分层（这点要对用户讲清，别让他把推断当事实）：

| 标签 | 性质 | 可信度 |
|---|---|---|
| 流未正常结束 | **转述** new-api 写的 `end_reason` 字段 | 高 |
| 扣费未交付 | 我方判据（两列做减法） | 较高 |
| 交付未扣费 | 我方判据 | **最低**，见上 |

---

## 第五部分：环境与验证（**这段能省接手者几小时**）

本机是 Windows + MSYS bash + Docker Desktop，网络受限（Docker Hub 与
`proxy.golang.org` 均不可达）。以下全部实测可用。

### 5.1 改完代码 → 在 8202 看到效果

```bash
./.local-test-kit/rebuild-8202.sh            # 常规
./.local-test-kit/rebuild-8202.sh --offline  # 完全不联网
```

一条命令走完：交叉编译自验 → 构建镜像 → 重启 → 校验 `/login` 与隔离日志。

**不要用 `.local-test-kit/start.sh`**：它跑 `up -d --build`，构建阶段会拉
`golang:1.26-alpine3.23` —— 本机未缓存且拉不到，必然失败；
而且它的 EXIT trap 会在失败时 `down` 一次，白折腾。

`rebuild-8202.sh` 绕开的办法（三个 build arg，全用本机已缓存镜像，**不访问任何镜像仓库**）：

| 参数 | 值 | 作用 |
|---|---|---|
| `BUILDER_IMAGE` | `golang:1.25` | 产物是 `CGO_ENABLED=0` 静态二进制，构建镜像的发行版不影响运行结果 |
| `RUNTIME_IMAGE` | `newapi-monitor:intern-main` | 已装好 ca-certificates + tzdata |
| `OFFLINE_RUNTIME` | `true` | 跳过 `apk upgrade/add`（**那才是真正要联网的一步**），但仍校验两份运行资产存在 |

`BUILDER_IMAGE` 是本轮给 [Dockerfile](../Dockerfile) 加的一行 ARG（默认值不变，
原构建方式不受影响）；`RUNTIME_IMAGE` / `OFFLINE_RUNTIME` 是仓库原有设计。

### 5.2 跑测试（Windows 本地跑不了）

本机 `go test` 会因 Linux 专属 syscall（`Statfs`/`Flock`）失败，这是预期。
用一次性容器：

```bash
MSYS_NO_PATHCONV=1 docker run --rm --tmpfs /tmp:rw,exec,size=6g \
  -v "//d/monitorcode/newapi-monitor://src" \
  -v "//c/Users/86177/go/pkg/mod://go/pkg/mod" \
  -w //src -e GOFLAGS=-mod=mod -e GOCACHE=/tmp/gocache -e GOPROXY=off \
  golang:1.25 go test ./... 
```

两个必需项：

- **挂宿主 `GOMODCACHE`（`go env GOMODCACHE`）并设 `GOPROXY=off`** ——
  容器内连不上 `proxy.golang.org`，但宿主缓存是全的
- **`MSYS_NO_PATHCONV=1` + 双斜杠路径**，绕开 MSYS 的路径改写

全量回归约 27 秒（`monitor` 包）。

**遇到成批 `read-only file system` 先怀疑环境**：Docker Desktop 的 VM 磁盘会变只读
（守护进程报 `meta.db: read-only file system`），表现为大量 `TempDir` 失败。
那不是断言失败，重启 Docker Desktop 即可（同一份代码 280s → 27s）。

### 5.3 gofmt 判断真实格式问题

`gofmt -l .` 会列出几乎所有文件，那是 `core.autocrlf=true` 造成的 CRLF，不是格式错误：

```bash
tr -d '\r' < 文件.go > /tmp/f.go && gofmt -d /tmp/f.go
```

### 5.4 fixture：造假数据验表格（**快照库没有 `logs` 表**）

脱敏快照有 42 张表，**没有 `logs`** —— 它只存在于生产 MySQL。
所以 8202 上排障接口永远返回"生产库未连接"，表格恒为空，
**表格渲染 / 异常判定 / 筛选 / 分页全都验不了**。

fixture 目录 `.local-test-kit/logchain-fixture/`（不入库）：

| 文件 | 用途 |
|---|---|
| `init.sql` | 建 5 张表 |
| `seed.sql` | 25 行编造数据，覆盖全部用例 |
| `docker-compose.logchain-fixture.yml` | 独立栈 |
| `Dockerfile.prebuilt` | 用已缓存镜像装预编译二进制 |
| `dumpchans/` | 从快照读真实渠道 ID 的一次性工具 |

启动：

```bash
# 1. 编二进制
MSYS_NO_PATHCONV=1 docker run --rm --tmpfs /tmp:rw,exec,size=4g \
  -v "//d/monitorcode/newapi-monitor://src" -v "//c/Users/86177/go/pkg/mod://go/pkg/mod" \
  -w //src -e GOFLAGS=-mod=mod -e GOCACHE=/tmp/gocache -e GOPROXY=off \
  golang:1.25 sh -c 'CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o .local-test-kit/logchain-fixture/monitor-bin .'

# 2. 装进运行基座（**不要 cd**，否则相对路径失效）
MSYS_NO_PATHCONV=1 docker build -q -f .local-test-kit/logchain-fixture/Dockerfile.prebuilt \
  -t newapi-monitor:logchain-fixture .local-test-kit/logchain-fixture

# 3. 起栈
MSYS_NO_PATHCONV=1 docker compose -f .local-test-kit/logchain-fixture/docker-compose.logchain-fixture.yml up -d --no-build

# 4. 取 cookie 后调接口
curl -s -c ck.txt -X POST 'http://127.0.0.2:8203/login' \
  -H 'Content-Type: application/json' -d '{"username":"local","password":"local"}'
curl -s -b ck.txt "http://127.0.0.2:8203/logchain/requests?anomaly=all"

# 用完清干净
MSYS_NO_PATHCONV=1 docker compose -f .local-test-kit/logchain-fixture/docker-compose.logchain-fixture.yml down -v
```

### 5.5 ★ fixture 必须绑 `127.0.0.2`，不能绑 `127.0.0.1` ★

**浏览器 cookie 按主机名隔离，不区分端口。**

第一版 fixture 绑在 `127.0.0.1:8203`，与 8202 共用同一个
`newapi_monitor_session` cookie（cookie 名是硬编码常量，改不了），
而两边 `MONITOR_SESSION_SECRET` 不同，于是**互相覆盖**：

```
登 8203 → 8203 签发 cookie，覆盖 8202 的
回 8202 → 用自己的密钥验签失败 → 401
       → 前端跳 /login → 自动登录页重新签发 → 又把 8203 的顶掉
       → 30 秒后自动刷新定时器再来一轮 → 死循环
```

用户看到的现象是「页面自己刷新回用户用量」，因为 `auto-login.html`
里 `location.replace('/')` 丢了 hash，重载后落回默认 tab。

**换主机名后浏览器视为两个站点，cookie 各存一份。** 实测：同一个 cookie jar
里存了两份（`127.0.0.1` 与 `127.0.0.2` 各一），两站可同时开着。

### 5.6 fixture 的两个坑（都在 fixture 侧，不是产品代码）

1. **`init.sql` / `seed.sql` 开头必须 `SET NAMES utf8mb4;`** ——
   MySQL 入口脚本默认按 latin1 读文件，会把 UTF-8 字节当 latin1 再编码进
   utf8mb4 列（双重编码），"渠道"存成 `C3A6C2B8C2A0…`（正确是 `E6B8A0`），
   页面看到乱码。
   判定：`SELECT HEX(LEFT(content,3)), CHAR_LENGTH(content), LENGTH(content)`
   —— 双重编码时字节数异常膨胀。
   > 反过来说这证明透传是对的：Monitor 把库里字节原样返回，一个字节没改。
2. **时区**：容器 `TZ=Asia/Shanghai`，`NOW()` 已是 CST，
   直接 `UNIX_TIMESTAMP(DATE(NOW()))` 即可。再 `CONVERT_TZ` 或减 `8*3600`
   会整体早 8 小时。

### 5.7 fixture 启动会被两道校验拦住（都是 fail-closed，设计正确）

1. `MONITOR_USAGE_FACTS_READ_ENABLED=true` 但 `..._ENABLED=false` →
   *"已拒绝静默回扫生产 logs"*。排障直查 `logs` 不经事实层，两个都设 `false`。
2. **五张表必须齐全**（`sourcePreflightQueries`，[source_lifecycle.go](../monitor/source_lifecycle.go)）：
   `logs` / `channels` / `users` / `tokens` / `options`。少一张就
   `Table 'newapi.channels' doesn't exist` 起不来。列名必须与预检 SELECT 完全一致。

### 5.8 `fixture-db` 必须 `restart: unless-stopped`

初版误设 `restart: "no"`（照抄了 seed 的写法）。Docker Desktop 重启后
monitor / local-entry 自己回来了，**只有假库没回来**：
页面能打开、`/live` 也健康，但每次查询卡在 DNS 解析 `fixture-db` 失败上约 16 秒，
表现就是"进不去网站"。一次性初始化任务（`seed-store`）才该用 `no`。

### 5.9 工具故障的识别（这个仓库反复出现）

`execute_bash` 会**报成功但实际不执行**（连续约 25 次）。
`grep_search` 会只返回文件名不给内容。

判定方法：`cmd > file` 后读文件，文件不存在即工具故障。
遇到反常的空结果，**先用一个必然有输出的命令自检**（`echo ALIVE`），
不要把空结果当结论。恢复办法是重启 IDE。

同理，**改完关键处要重新读文件确认**，不能只信工具回执 ——
本轮出现过多次"报告已修好、实际编辑没落盘"。

---

## 第六部分：写测试的方法（**本轮最重要的一条经验**）

### 6.1 断言写完必须做反向验证

**把 bug 原样改回去，确认测试会红。** 不做这一步，可能写出永远不会红的断言。

本轮真实发生过：`TestLogChainJSAvoidsChangeOnTextInputs` 初版用
`strings.Index` 只取**第一处** `addEventListener('change'`，而那处是 `lcDate`
（绑 change 是对的）。于是把 bug 原样塞回去，**测试照样绿** —— 断言完全无效。

做法：

```bash
cp monitor/logchain.go /tmp/lc.bak
# 用 sed 把 bug 塞回去
sed -i "s|正确写法|错误写法|" monitor/logchain.go
# 跑测试，确认变红
docker run ... go test ./monitor/ -run 'TestXxx'
# 还原并校验
cp /tmp/lc.bak monitor/logchain.go && rm -f /tmp/lc.bak
```

已做过反向验证的：流状态排除法、订阅计费排除、`lcModel` 绑 change、
loading 互斥。

### 6.2 断言不要拿整串去搜片段

三次踩过：

- 搜 `created_at < ?` 命中的是**时间窗上界**（`created_at >= ? AND created_at < ?`），
  那个与排序方向无关且恒存在 → 必须只比对"游标带来的增量"
- 搜 `if(lc.loading)return` 命中了**解释"为什么不用它"的注释** →
  写"某写法不得出现"类断言前先剔掉注释（`stripJSLineComments`）
- 断言 `"前置拒绝"` 而实际文案是 `"未打到渠道即被拒"` →
  **按实际措辞断言，不按脑子里的叫法**

### 6.3 改破既有测试时先分清性质

本轮改破过 `sync_status_ui_test.go`：它硬编码 tab 白名单正则字面量，
加 `logchain` 后匹配不上。

**这不是行为回归**（白名单只增不减，`#tab=sync` 照常工作），是测试过度绑定字面量。
所以更新字面量并注明"新增 tab 时须同步改这里"是对的。

但**必须先查清是哪种**：行为坏了就修代码，断言过紧才改测试。
不能因为测试红了就直接放宽 —— 那正是用户"不得妨碍已有功能"要防的事。

### 6.4 当前测试清单（38 个）

后端 [logchain_test.go](../monitor/logchain_test.go)：解析层收敛与冲突、
SQL 口径（type、参数化、排序、游标）、异常判据（排除法、两方向、
不读 end_error、未知值 fail-closed、不改稳定性口径、标签与 SQL 一致）、
闸门超时、`LocalSnapshotOnly` 保护。

前端 [logchain_ui_test.go](../monitor/logchain_ui_test.go)：页面接线、
导航位置、文本框不绑 change、世代计数、按钮判断顺序、绝对路径、
盲区存在与可折叠、标题注册、无"全部请求"档、`err_anom` 在 SQL 层筛、
明细列随范围切换、排序控件、表头副说明、不引入 scrubContent。

---

## 第七部分：当前状态与剩余工作

### 7.1 提交状态

`feat/logchain-p1` 分支，5 个提交，**未推远端**，`main` 干净（在 `a07db5b`）：

```
790be9b fix: 排障按发生时间排序，游标改为 (created_at,id) 复合键
5d58bf6 feat: 客户排障前端(侧边栏 tab + 单日表格 + 筛选) 与筛选取值接口
12d51a6 fix: 排障接口不得挤占客户 Portal 的日志查询泳道
9bb4399 feat: 客户排障链路 P1 后端 + 修正开发说明书三处事实错误
5ea32bf docs: 同步排障闸门约束(25s→15s)并补 13.4.1 节
```

**异常日志、排序按钮、表头优化、盲区折叠这批改动尚未提交**（按用户要求）。

工作区另有两处用户未拍板的改动：`Dockerfile` 的 `BUILDER_IMAGE` 那行
（重建 8202 必需）、`docs/upstream-downstream-log-chain-dev-spec.md`。

### 7.2 交付物

| 文件 | 状态 | 行数 |
|---|---|---|
| [monitor/logchain.go](../monitor/logchain.go) | 新增 | 801 |
| [monitor/logchain.js](../monitor/logchain.js) | 新增 | 662 |
| [monitor/logchain_test.go](../monitor/logchain_test.go) | 新增 | 560 |
| [monitor/logchain_ui_test.go](../monitor/logchain_ui_test.go) | 新增 | 372 |
| [monitor/page.html](../monitor/page.html) | 改 | tab / 容器 / CSS / switchTab / hash 白名单 |
| [monitor/server.go](../monitor/server.go) | 改 | 两条路由 + embed + 静态路由 |
| [monitor/stability.js](../monitor/stability.js) | 改 | `ST_HEADERS` 加 logchain 条目（否则标题错显"用户用量"） |
| [monitor/usage.go](../monitor/usage.go) | 改 | `logOther` 加 `StreamStatus` 嵌套字段 |
| [monitor/sync_status_ui_test.go](../monitor/sync_status_ui_test.go) | 改 | tab 白名单正则同步 |

### 7.3 P1 剩余

**① 前置拒绝定位不到客户**（要改 schema）

限流 / 无可用渠道 / 分组无权限这类**根本不写 `logs`**，只在
`StabilityRejectHour`（主键 `hour_ts × node × reason × model × grp`，
**无 `user_id`**，小时聚合）里。

现状用 `blind_spots` 明确告知，未解决。要修得给该表加用户维度 →
**改 schema，必须 bump `preMigrationPlanID`**，且只有新数据有用户维度。

**更上游还有个未知**：那个采集器在独立仓库
（`github.com/yl0711-coder/newapi-reject-collector`），是 tail new-api 日志抓的。
**如果 new-api 那行日志本身没写用户，整条链路凭空造不出来。**
推测"选渠道失败"这类能拿到（发生在鉴权之后），"token 无效"这类拿不到。
要确认得在节点上看一行真实日志。

**② 重试链无法归并**

没有任何字段能把多次尝试关联成一次客户请求。看到 3 条错误判断不了是
3 次失败还是 1 次失败重试 3 次。本轮加的 `end_reason`/`end_error` 帮不上。

**③ 跨页跳转按钮** —— 用户明确说不做。`logChainOpen()` 与
`applyNavigationContext()` 接口已留好，将来要做只需在「用户用量」客户详情页加个按钮。

**④ 视觉细节** —— 用户 2026-08-21 确认"视觉上目前没有问题"。

### 7.4 P2/P3（**暂缓，不要主动开工**）

地基已具备：用户实测确认 987xyz 上游 `/api/log/` 自带 `model_name`。
上游采集器（[channel_upstream_usage.go](../monitor/channel_upstream_usage.go)）
每 30 分钟拉一轮，**明细取回来了但落库时只留 4 个字段**
（`created_at` / `quota` / `prompt_tokens` / `completion_tokens`），
按"域名×小时"聚合，丢掉了 model 维度。

要做的顺序（前一步是后一步的正确性前提）：

1. **兑换比版本化** —— 必须排第一。用今天的汇率算上个月的账，**错了不报错**
2. 采集器加 model 维度 → 改 `ChannelUpstreamUsageHour` 主键 → bump plan ID
3. 事实表加缓存 token 列 → 再次 bump plan ID
4. 域名级日毛利 + 对账层（差异不归零就不显示数字）
5. 客户级 token 占比分摊

**四个待用户确认的问题**（卡在最前面，详见
[dev-spec 第 11 节](upstream-downstream-log-chain-dev-spec.md)）：
兑换比历史是否变过、`cache_tokens` 是否计入 `prompt_tokens`、
`aicodewith.com` 这类有收入无上游账号的域名怎么算、Sub2API 是否也返 model_name
（用户已说**只考虑 NewAPI**，此条可能不再需要）。

客户级分摊的口径用户已定：**按 token 占比，但必须在 域名×模型×日 摊**
（在域名×日摊会差一个数量级，因为同域名下 Haiku 与 Opus 单价差几十倍）。
**不可改成按收入摊** —— 那样每个客户毛利率恒等，永远看不出谁不赚钱。
