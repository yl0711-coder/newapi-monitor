# NexusAPI CloudWatch 监控与日志使用手册

> 适用环境：NexusAPI 生产环境
> AWS 账号：`842806122225`
> 最后核验时间：2026-09-13
> 文档用途：日常巡检、故障发现、客户报障定位、日志查询、交接培训
> 变更边界：本文主要说明“怎么看、怎么查、怎么判断”，不授权读者修改或删除生产资源。

## 1. 阅读导航

如果第一次接触这套系统，建议按下面顺序阅读：

1. 先看第 2～4 节，理解请求链路、区域和日志组。
2. 再看第 5 节，学会使用总览 Dashboard。
3. 遇到客户报障时，直接按第 8 节执行。
4. 需要精确统计时，使用第 7 节的 Logs Insights 查询模板。
5. 需要在本机或程序中读取时，看第 10～11 节。
6. 涉及费用、敏感信息或配置变更前，必须看第 12～14 节。

只想快速确认线上是否正常，可以直接执行第 6 节的“10 分钟巡检”。

## 2. 当前监控范围与能力边界

### 2.1 当前已经完成的配置

- 已将 CloudFront 标准访问日志 v2 投递到 CloudWatch Logs。
- 已配置一份核心访问日志和一份客户诊断日志。
- 已配置日志保留期和删除保护。
- 已将 CloudFront、WAF、ALB、ECS、RDS、告警和关键日志查询集中到 `NexusAPI-Prod-Overview`。
- Dashboard 已支持跨区域查看，日常使用不需要频繁切换区域。
- 已验证两条 CloudFront 日志投递的事件数量、字段完整性和延迟数据一致性。
- 已验证生产环境为 1 个 Master、3 个 Worker，服务正常。

### 2.2 当前没有做的事项

- 本轮没有新增基于日志内容的 CloudWatch 告警。
- Monitor 尚未全部改为从 CloudWatch 主动读取非 Lightsail 业务日志。
- Worker 中现有的 `nginxcollector`、`reject-collector` 和日志桥仍未下线。
- 误建在 `us-west-2` 的空 CloudFront 日志组、已停用实验日志组尚未清理。
- 尚未建立贯穿 CloudFront、Nginx、NewAPI 和数据库的统一请求 ID。

因此，当前 CloudWatch 已经可以完成基础设施监控、入口请求分析和大部分故障定位，但不能仅凭一条 CloudFront 日志直接得到 NewAPI 的用户 ID、分组、模型、渠道和上游返回详情。

## 3. 先理解完整请求链路

生产请求的主要链路如下：

```text
客户客户端
  ↓
DNS / 客户本地网络 / 运营商网络
  ↓
CloudFront（全球边缘节点）
  ↓
WAF / CloudFront Function
  ↓
ALB（us-west-2）
  ↓
ECS Fargate Worker × 3
  ↓
NewAPI
  ├─→ 上游模型渠道
  └─→ RDS MySQL

ECS Fargate Master × 1
  └─→ 定时任务、渠道测试、订阅重置、邮件等后台任务
```

不同日志分别回答不同问题：

| 位置 | 能回答的问题 | 不能单独回答的问题 |
|---|---|---|
| CloudFront | 客户请求是否到达边缘、状态码、路径、总耗时、源站首字节耗时、客户网络特征 | NewAPI 用户 ID、分组、模型、渠道、请求体内容 |
| WAF | 请求是否被安全规则拦截或计数 | NewAPI 内部为什么失败 |
| ALB | 请求是否到达负载均衡、目标是否健康、ALB/目标响应错误 | 具体上游渠道错误内容 |
| Nginx | 请求是否到达 Worker、HTTP 状态、入口层警告和连接中断 | 不一定有渠道、模型和上游详情 |
| NewAPI | 应用处理、渠道选择、上游错误、数据库错误、无可用渠道等 | 客户在到达 CloudFront 前的本地网络故障 |
| RDS | 数据库 CPU、连接、内存、存储、IO、慢查询和数据库错误 | 客户到 CloudFront 的网络状况 |

排障时要沿链路逐层缩小范围，不能看到一个 `5xx` 就直接认定是上游问题，也不能因为 NewAPI 使用日志没有记录就认定客户没有发出请求。

## 4. 区域、资源和日志组清单

### 4.1 区域划分

| 区域 | 中文名称 | 主要资源 |
|---|---|---|
| `us-east-1` | 美国东部（弗吉尼亚北部） | CloudFront 指标、WAF 指标、CloudFront 标准访问日志 |
| `us-west-2` | 美国西部（俄勒冈） | ALB、ECS/Fargate、RDS、应用日志、数据库日志、日志桥 |

CloudFront 是全球服务，但 CloudFront 指标以及本次标准日志 v2 的控制和日志组位于 `us-east-1`。ECS、ALB、RDS 位于 `us-west-2`。

### 4.2 CloudFront 生产日志组

| 日志组 | 区域 | 格式 | 保留期 | 删除保护 | 用途 |
|---|---|---|---:|---|---|
| `nexusapi-cloudfront-access` | `us-east-1` | JSON | 30 天 | 开启 | 日常请求量、状态码、路径、错误和延迟分析 |
| `nexusapi-cloudfront-client-diagnostic` | `us-east-1` | JSON | 14 天 | 开启 | 客户 IP、网络、国家、ASN、客户端和 TLS 排障 |

两组日志来自同一个 CloudFront 分配：

```text
CloudFront Distribution ID：E25FZDB19ACOZ2
域名：us.nexusapi.link
Delivery Source：nexusapi-cloudfront-e25fzdb19acoz2
```

### 4.3 ECS 与 RDS 生产日志组

| 日志组 | 区域 | 用途 |
|---|---|---|
| `/ecs/nexusapi-prod-worker` | `us-west-2` | Worker 中 NewAPI、Nginx 及现有采集器的容器日志 |
| `/ecs/nexusapi-prod-master` | `us-west-2` | Master 的 NewAPI 后台任务日志 |
| `/aws/rds/instance/nexusapi-mysql-prod/error` | `us-west-2` | RDS MySQL 错误日志 |
| `/aws/rds/instance/nexusapi-mysql-prod/slowquery` | `us-west-2` | RDS MySQL 慢查询日志 |
| `/aws/lambda/nexusapi-monitor-ecs-log-production-bridge` | `us-west-2` | ECS 日志注册/桥接链路自身运行日志 |

生产 Worker 的 ECS 服务名目前是：

```text
nexusapi-prod-worker-canary
```

生产 Worker 的日志组仍然叫：

```text
/ecs/nexusapi-prod-worker
```

二者名称不同，不要把“ECS 服务名”和“CloudWatch 日志组名”混为一谈。

### 4.4 实验资源

以下资源不是当前正式业务查询入口：

```text
ECS 服务：nexusapi-prod-worker-buffer-canary（期望任务数 0）
日志组：/ecs/nexusapi-prod-worker-buffer-canary
日志组：/ecs/nexusapi-buffer-lab-6cm1iv
```

它们用于之前的请求缓冲灰度实验。日常排障不要把实验日志混入生产统计。

## 5. 如何使用总览 Dashboard

### 5.1 打开方式

1. 登录 AWS 管理控制台。
2. 搜索并进入 `CloudWatch`。
3. 左侧进入 `控制面板（Dashboards）`。
4. 打开自定义控制面板 `NexusAPI-Prod-Overview`。
5. 时间范围默认是最近 3 小时。
6. 建议将页面刷新频率手动设置为 15 分钟。

Dashboard 已在每个组件中指定数据区域，因此日常查看时不需要为了不同组件反复切换 `us-east-1` 和 `us-west-2`。

注意：自动刷新频率是浏览器会话设置，不会永久写入 Dashboard。重新打开页面后应检查当前刷新频率。

### 5.2 推荐阅读顺序

建议从上到下按以下顺序看：

1. ALB 请求量、连接数、延迟和错误。
2. 目标组健康目标与异常目标。
3. Fargate Worker/Master CPU 和内存。
4. RDS CPU、连接数、内存、存储、IO 和延迟。
5. 主动告警状态。
6. CloudFront 请求量、错误率和 WAF 放行/拦截。
7. 日志桥 Lambda 和注册 API Gateway。
8. CloudFront 状态码、延迟、异常请求、投递一致性和客户网络分布。

这样可以先确认基础设施是否健康，再深入具体请求。

### 5.3 ALB 组件怎么看

#### ALB · 请求量与连接

主要观察：

- 请求量是否突然上升或下降。
- 新连接和活跃连接是否异常增长。
- 请求量下降是否与错误上升、目标不健康同时出现。

请求量上升本身不是故障。只有同时出现响应时间上升、目标错误或 ECS 资源紧张时，才说明容量可能不足。

#### ALB · 延迟与错误

重点区分：

- `ELB 5xx`：ALB 自己无法正常处理或连接目标，优先检查目标健康、连接和网络。
- `Target 5xx`：后端 Worker 返回 5xx，继续查 Nginx 和 NewAPI。
- `TargetResponseTime`：目标响应耗时。模型 API 本身可能是长请求，应结合流式/非流式和模型耗时判断。

单次尖峰不一定是事故；持续升高并伴随错误率升高才需要立即处理。

### 5.4 目标组组件怎么看

#### 目标组 · 每目标请求量

用于判断流量是否大致均匀分配到多个 Worker。如果某个目标长期没有流量，应检查：

- 目标是否健康。
- 是否刚完成扩容或滚动发布。
- 是否处于 draining。
- ALB 是否仍有旧目标残留。

#### 目标组 · 健康/异常目标

正常生产基线是：

```text
Worker：3 个运行、0 个待处理
Master：1 个运行、0 个待处理
```

Master 不接用户流量，因此目标组主要看 Worker。出现异常目标时不要立即删除任务，先检查 ECS 事件、部署状态和容器日志。

### 5.5 Fargate 组件怎么看

#### Fargate · Worker / Master CPU

- Worker CPU 随请求量升高是正常现象。
- Master CPU 主要受定时任务、渠道测试等影响。
- 持续高位比瞬时尖峰更重要。
- CPU 高且请求延迟、队列或 5xx 同时升高，说明需要关注扩容是否及时。

人工巡检可采用以下参考线，但它不是当前自动扩缩容规则本身：

| 状态 | CPU 参考 |
|---|---:|
| 正常 | 持续低于 70% |
| 关注 | 连续多个周期高于 70% |
| 紧急排查 | 持续高于 85%，并伴随错误或延迟升高 |

#### Fargate · Worker / Master 内存

- 内存持续上涨但不回落，需要关注泄漏或大请求积压。
- 内存接近上限可能触发容器 OOM，表现为任务被替换、连接中断或部分请求失败。
- Worker 和 Master 必须分开看；Master 的异常不能通过增加 Worker 数量解决。

### 5.6 RDS 组件怎么看

#### RDS · CPU 与连接数

- CPU 高但连接数稳定：可能是慢查询、排序或大范围扫描。
- 连接数突然上升：可能是流量峰值、连接池配置或任务集中启动。
- CPU、连接数和慢查询同时上升：优先查慢查询日志与应用查询。

#### RDS · 内存与存储余量

- `FreeableMemory` 下降不等于立即故障，MySQL 会利用内存做缓存。
- 持续接近最低水平并伴随 swap、延迟或连接异常时需要处理。
- 存储余量必须看长期趋势，不能等接近耗尽再扩容。

#### RDS · 读写延迟与队列

- 读写延迟突然上升可能来自慢查询、大量日志写入或存储 IO 压力。
- `DiskQueueDepth` 持续升高说明存储请求开始排队。
- 单个滚动替换时刻的一次 IO timeout 需要记录，但不能仅凭一次事件认定数据库故障。

#### RDS · IO 与网络吞吐

用于判断瓶颈是查询计算、磁盘 IO，还是网络传输。必须与 CPU、连接数、慢查询和业务流量一起判断。

### 5.7 主动告警组件怎么看

`NexusAPI · 主动告警状态` 汇总已经配置的 CloudWatch Alarm：

- `OK`：指标当前没有越过阈值。
- `ALARM`：指标已达到告警条件，需要立即进入对应资源检查。
- `INSUFFICIENT_DATA`：没有足够数据，可能是资源刚建立、指标停止上报或配置不匹配。

刚创建告警时收到一批“OK”邮件，通常是告警从无数据转为正常状态，不代表此前发生了生产事故。

### 5.8 CloudFront 与 WAF 指标组件怎么看

#### CloudFront · 请求量与错误率

- 请求量：客户到达 CloudFront 的请求规模。
- 4xx：包括无效令牌、访问限制、WAF/CloudFront 拒绝、路径错误等，不能全部视为网站故障。
- 5xx：优先检查 CloudFront 详细结果、ALB、ECS 和上游链路。

#### WAF · 放行与拦截

- 放行数量应与正常流量趋势相符。
- 拦截突然增长时，应检查是否存在扫描、攻击或规则误伤。
- Count 模式只记录匹配，不会拦截；Block 模式会返回 403。

### 5.9 日志桥组件怎么看

#### ECS 日志桥接 Lambda

用于查看 Lambda 调用量、错误和耗时。它服务于现有 ECS 业务日志注册/桥接链路，不是 CloudFront 标准访问日志投递。

#### ECS 日志注册 API Gateway

用于查看日志注册接口的请求、错误和延迟。如果 Worker 自动扩缩容后 Monitor 数据断流，应同时检查这两个组件和采集器日志。

### 5.10 新增的五个 CloudFront 日志组件

#### CloudFront · 5分钟状态码趋势

按 5 分钟展示各状态码数量。适合快速回答：

- 当前是否突然出现大量 403、429、502、503、504。
- 错误是短时尖峰还是持续存在。
- 错误出现时间是否与客户反馈一致。

#### CloudFront · 入口与源站延迟

展示：

- 总耗时 P50、P95。
- 首字节时间 P95。
- CloudFront 到源站的首字节延迟 P95。

如果总首字节时间与源站首字节时间非常接近，主要耗时通常在源站或源站之后；如果两者差距明显，再考虑边缘、连接或客户网络因素。

#### CloudFront · 最近4xx/5xx请求（不含IP与User-Agent）

展示最近异常请求的：

- 时间。
- CloudFront Request ID。
- HTTP 状态码。
- 方法和路径。
- Edge Result / Detailed Result。
- 总耗时与源站首字节耗时。

表格为空通常代表所选时间范围内没有 4xx/5xx，不代表组件损坏。

#### CloudFront · 核心/诊断投递一致性

两组日志来自同一请求源，事件数和唯一请求数应基本一致。

- 差 1～数条且很快恢复：多半是两个查询执行时刻或投递延迟不同。
- 持续 15 分钟以上明显不一致：检查两条 delivery、日志组权限和日志到达时间。
- 不要仅凭某一秒的绝对相等判断健康。

#### CloudFront · 客户国家与ASN分布

用于观察客户来源国家和网络自治系统分布。适合判断：

- 某个区域是否集中出现异常。
- 某家运营商或云网络是否出现集中失败。
- 是否出现明显的扫描或异常流量来源。

主 Dashboard 只展示聚合后的国家和 ASN，不展示原始客户 IP 或 User-Agent。

## 6. 日常 10 分钟巡检流程

建议每天至少一次，在流量高峰前后各做一次更好。

### 第一步：确认告警和目标健康

1. 打开 `NexusAPI-Prod-Overview`。
2. 时间选择最近 3 小时。
3. 检查主动告警是否全部为 `OK`。
4. 检查目标组异常目标是否为 0。
5. 检查 Worker 是否保持 3 个运行任务。

### 第二步：确认容量

1. 看 Worker CPU 和内存是否持续高位。
2. 看 ALB 请求量是否出现异常峰值。
3. 看延迟是否跟随请求量明显上升。
4. 看 RDS CPU、连接数、内存和 IO 队列是否同步升高。

### 第三步：确认错误

1. 看 CloudFront 状态码趋势。
2. 看 ALB 的 ELB 5xx 和 Target 5xx。
3. 看最近 4xx/5xx 表格。
4. 如果错误上升，缩小到 15～30 分钟继续查日志。

### 第四步：确认日志链路

1. 看核心/诊断投递一致性。
2. 看日志桥 Lambda 和 API Gateway 是否有错误。
3. 如果 CloudFront 有流量但日志没有新事件，记录时间并检查 delivery。

### 第五步：记录结论

巡检记录至少包含：

```text
检查时间与时区：
检查时间范围：
请求量：
CloudFront 4xx/5xx：
ALB ELB/Target 5xx：
Worker CPU/内存：
RDS CPU/连接/存储：
异常目标：
告警状态：
结论与后续动作：
```

## 7. CloudWatch Logs 与 Logs Insights 使用方法

### 7.1 三种查看方式

| 方式 | 适合场景 | 费用特点 |
|---|---|---|
| 搜索所有日志流 | 已知时间、错误短语或 Request ID，直接找原始日志 | 不按 Logs Insights 扫描量计费 |
| Logs Insights | 聚合、分组、百分位、排序、跨日志流统计 | 按扫描数据量计费 |
| Live Tail | 现场短时间观察刚产生的日志 | 超过免费额度后按使用时间计费 |

日常查一条具体错误，优先“搜索所有日志流”；需要趋势、数量、延迟百分位或多字段组合时，才使用 Logs Insights。

### 7.2 打开 CloudFront 日志

1. 将控制台区域切换为 `us-east-1`。
2. 进入 `CloudWatch → 日志 → 日志组`。
3. 日常查询打开 `nexusapi-cloudfront-access`。
4. 客户网络排障打开 `nexusapi-cloudfront-client-diagnostic`。
5. 点击“搜索所有日志流”查看原始事件，或点击“在 Logs Insights 中查看”进行分析。
6. 先选择 15 分钟、30 分钟或 1 小时，确认范围后再扩大。

### 7.3 Logs Insights 查询注意事项

- 在控制台已经选择日志组时，下面的查询不需要写 `SOURCE`。
- Dashboard 查询需要用 `SOURCE '日志组名'` 指定日志组。
- 字段名包含连字符或括号时要用反引号，例如 `` `sc-status` ``、`` `cs(User-Agent)` ``。
- 时间范围由控制台右上角选择，不要在查询语句里重复写固定日期。
- 运行前检查区域和日志组，避免误扫其他环境。
- 查询完立即记录时间范围、查询条件和 Request ID，不要只保留截图。

### 7.4 查询模板：状态码趋势

选择 `nexusapi-cloudfront-access`：

```sql
stats count(*) as Requests by bin(5m), `sc-status`
```

### 7.5 查询模板：最近异常请求

```sql
filter `sc-status` like /^[45]/
| fields @timestamp,
         `x-edge-request-id` as RequestId,
         `sc-status` as Status,
         `cs-method` as Method,
         `cs-uri-stem` as Path,
         `x-edge-detailed-result-type` as EdgeResult,
         `time-taken` as TotalSeconds,
         `origin-fbl` as OriginFBL
| sort @timestamp desc
| limit 100
```

### 7.6 查询模板：按 CloudFront Request ID 查找

将示例 ID 替换为真实值：

```sql
filter `x-edge-request-id` = "REPLACE_WITH_REQUEST_ID"
| fields @timestamp,
         `x-edge-request-id`,
         `sc-status`,
         `cs-method`,
         `cs-uri-stem`,
         `x-edge-result-type`,
         `x-edge-detailed-result-type`,
         `time-taken`,
         `time-to-first-byte`,
         `origin-fbl`,
         `origin-lbl`
| sort @timestamp asc
```

### 7.7 查询模板：按接口路径查找

```sql
filter `cs-uri-stem` = "/v1/responses"
| stats count(*) as Requests,
        pct(`time-taken`, 50) as P50,
        pct(`time-taken`, 95) as P95,
        max(`time-taken`) as Max
  by bin(5m), `sc-status`
```

如果要查 `/v1/chat/completions` 或 `/v1/messages`，替换路径即可。

### 7.8 查询模板：查慢请求

下面示例查总耗时超过 30 秒的请求：

```sql
filter `time-taken` >= 30
| fields @timestamp,
         `x-edge-request-id` as RequestId,
         `sc-status` as Status,
         `cs-method` as Method,
         `cs-uri-stem` as Path,
         `time-taken` as TotalSeconds,
         `time-to-first-byte` as TTFB,
         `origin-fbl` as OriginFBL,
         `origin-lbl` as OriginLBL,
         `x-edge-detailed-result-type` as EdgeResult
| sort TotalSeconds desc
| limit 100
```

模型请求本身可能持续几十秒或更久。对流式请求，应更关注首字节时间和连接是否中途断开；对非流式请求，总耗时更容易持续较长时间。

### 7.9 查询模板：入口与源站延迟对比

```sql
stats pct(`time-taken`, 50) as TotalP50,
      pct(`time-taken`, 95) as TotalP95,
      pct(`time-to-first-byte`, 95) as TTFB_P95,
      pct(`origin-fbl`, 95) as OriginFBL_P95,
      pct(`origin-lbl`, 95) as OriginLBL_P95
  by bin(5m)
```

解释：

- `time-taken`：CloudFront 处理这次请求的整体耗时。
- `time-to-first-byte`：从 CloudFront 接收请求到开始向客户返回首字节的时间。
- `origin-fbl`：CloudFront 到源站的首字节延迟。
- `origin-lbl`：CloudFront 从源站取得最后字节的延迟。

### 7.10 查询模板：客户 IP 排障

只能在 `nexusapi-cloudfront-client-diagnostic` 中运行：

```sql
filter `c-ip` = "203.0.113.10"
| fields @timestamp,
         `x-edge-request-id` as RequestId,
         `sc-status` as Status,
         `cs-method` as Method,
         `cs-uri-stem` as Path,
         `c-country` as Country,
         asn,
         `time-taken` as TotalSeconds,
         `origin-fbl` as OriginFBL,
         `x-edge-detailed-result-type` as EdgeResult
| sort @timestamp desc
| limit 200
```

文档中的 `203.0.113.10` 是保留的示例地址，不是真实客户 IP。

### 7.11 查询模板：客户网络与客户端分布

按国家和 ASN：

```sql
stats count(*) as Requests by `c-country`, asn
| sort Requests desc
| limit 50
```

按 User-Agent 聚合时不要直接导出全部原文：

```sql
stats count(*) as Requests by `cs(User-Agent)`
| sort Requests desc
| limit 30
```

User-Agent 可能包含软件版本和设备信息，只能用于内部排障，截图或对外沟通前必须脱敏。

### 7.12 查询模板：检查两条投递是否一致

在 Dashboard 中使用：

```sql
SOURCE 'nexusapi-cloudfront-access'
| SOURCE 'nexusapi-cloudfront-client-diagnostic'
| stats count(*) as Events,
        count_distinct(`x-edge-request-id`) as UniqueRequests
  by @log
```

如果在 Logs Insights 控制台中运行，也可以同时勾选两个日志组后去掉两行 `SOURCE`。

## 8. 客户报障标准排查流程

### 8.1 先收集信息

至少收集以下可获得的信息：

```text
客户名称：
用户名或用户 ID：
准确时间范围：
时区：
访问域名：
接口路径：
模型：
分组：
令牌名称或末尾几位（不要提交完整 Key）：
Request ID：
HTTP 状态码：
客户端错误原文：
是否流式：
客户端名称和版本：
客户出口 IP（如愿意提供）：
是否使用 VPN/代理：
是否可稳定复现：
```

不能只记录“晚上 9 点左右”。应记录类似：

```text
2026-09-13 21:00:00～21:10:00，Asia/Shanghai（UTC+8）
```

### 8.2 第一步：确认是否为全局故障

打开 Dashboard，对照客户时间检查：

- CloudFront 请求量和 4xx/5xx。
- ALB 请求量、延迟、ELB 5xx、Target 5xx。
- 目标组健康状态。
- Worker CPU、内存和任务数量。
- RDS CPU、连接和 IO。
- 主动告警。

如果多个客户、多个模型同时失败，并且基础设施指标同步异常，优先按平台事故处理；如果只有单个客户或单个网络来源异常，继续做单请求定位。

### 8.3 第二步：确认请求是否到达 CloudFront

优先查询核心日志：

- 已知 Request ID：按第 7.6 节查询。
- 已知路径和时间：按第 7.7 节查询。
- 已知客户 IP：在诊断日志中按第 7.10 节查询。

判断：

| CloudFront 结果 | 初步结论 |
|---|---|
| 找到请求且为 2xx | 请求已通过入口，继续查应用或响应中途断开 |
| 找到请求且为 403 | 查 WAF、地域限制、源站验证或鉴权 |
| 找到请求且为 429 | 查限流来源，是 CloudFront/WAF、Nginx、NewAPI 还是上游 |
| 找到请求且为 502/503/504 | 继续查 ALB、ECS、NewAPI 和上游 |
| 完全找不到 | 可能未到达 CloudFront、访问了其他域名、时间不准，或命中日志记录边界 |

CloudFront 标准日志不是严格实时日志。实际观察中通常能在较短时间内看到事件，但 AWS 官方说明通常可能在一小时内到达，少数记录可能延迟更久。因此，刚发生的问题不要因为第一分钟查不到就下结论，应保留查询条件并稍后复查。

### 8.4 第三步：确认是否到达 ALB 和 Worker

如果 CloudFront 有记录：

1. 对照 `origin-fbl`、状态码和详细结果类型。
2. 看同一时段 ALB 的 ELB 5xx、Target 5xx 和目标响应时间。
3. 进入 `/ecs/nexusapi-prod-worker`，搜索所有日志流。
4. 用时间、接口路径、NewAPI Request ID、模型或错误短语过滤。
5. 先看 `nginx/`，再看 `new-api/`。

CloudFront Request ID 和 NewAPI Request ID 当前不保证是同一个值。无法直接匹配时，应结合精确时间、路径、状态码和耗时关联，不要强行认定两个 ID 相同。

### 8.5 第四步：判断 NewAPI、数据库或上游

常见判断：

| 证据 | 更可能的问题位置 |
|---|---|
| Nginx 有请求，NewAPI 完全没有进入记录 | Nginx 拒绝、连接提前断开、应用未接到请求 |
| NewAPI 有 `No available channel` | 分组下没有可用渠道、渠道被禁用或模型映射不满足 |
| NewAPI 有上游 401/404 | 上游凭证、模型权限、地址或协议不匹配 |
| NewAPI 有上游 408/429/5xx | 上游超时、限流或服务异常 |
| RDS 连接/IO/慢查询同步异常 | 数据库可能参与故障 |
| CloudFront/ALB/ECS 正常，仅单客户反复重连 | 客户网络、代理、客户端超时或长连接稳定性问题 |

### 8.6 第五步：形成结论

结论必须区分“事实”和“推断”：

```text
事实：CloudFront 在 21:03:12 收到 POST /v1/responses，返回 504。
事实：同一时间 ALB Target 5xx 上升，Worker 任务健康。
事实：NewAPI 日志记录上游连接超时。
结论：请求已到达平台，平台基础设施可用，失败发生在 NewAPI 访问上游阶段。
处置：降低/关闭异常渠道权重，观察同分组其他渠道。
```

不要写成：

```text
应该是网络问题。
可能是上游不稳定。
```

没有证据时必须明确写“当前证据不足”，并列出还缺什么。

## 9. 常见故障场景与判断

### 9.1 客户显示“正在重新连接”，NewAPI 使用日志没有错误

可能原因包括：

- 客户请求未到达 CloudFront。
- 请求到达 CloudFront，但在 WAF、CloudFront、ALB 或 Nginx 层失败。
- 客户在服务端写回响应前主动断开。
- 流式连接被客户网络、VPN、代理或客户端超时中断。
- 请求进入应用前鉴权或参数校验失败，未写入 NewAPI 业务 `logs` 表。
- 应用处理成功，但客户端没有完整收到流式响应。

排查顺序：CloudFront → ALB → Nginx → NewAPI → 上游，不要从数据库没有日志直接跳到“用户网络问题”。

### 9.2 CloudFront 403

依次检查：

- WAF Block 规则是否命中。
- CloudFront Function 的地域/网站访问限制。
- 源站验证配置。
- 请求域名、Host 和路径是否正确。
- 是否由源站应用主动返回 403。

403 既可能发生在边缘，也可能由 NewAPI 返回，需要结合 `x-edge-detailed-result-type`、ALB 和应用日志判断。

### 9.3 502、503、504

- CloudFront 5xx + ALB 无对应请求：优先查 CloudFront 到源站连接。
- ALB `ELB 5xx`：查目标健康、连接失败、TLS/网络。
- ALB `Target 5xx`：查 Nginx/NewAPI。
- NewAPI 明确记录上游 5xx/超时：问题主要在上游渠道。

### 9.4 Nginx 499、broken pipe

通常表示客户或中间代理在服务端完成响应前关闭连接。少量出现并不代表服务器故障；如果集中出现，应检查：

- 客户端超时设置。
- 非流式长请求。
- VPN、代理或运营商长连接质量。
- CloudFront/ALB/Nginx 超时配置。
- 上游首字节时间是否过长。

### 9.5 `request body is buffered to a temporary file`

这是一条 Nginx 警告，表示请求体超过内存缓冲区，被暂存到磁盘；它不是请求失败。需要关注的是出现频率、临时磁盘 IO 和是否伴随延迟。

当前已对 `/v1/responses` 做过小范围缓冲优化。不要为了消除警告直接关闭请求缓冲；关闭会改变请求向上游转发的行为，需要独立压测和灰度。

### 9.6 Dashboard 没有数据

按以下顺序检查：

1. 时间范围是否太短或选错日期。
2. 浏览器是否选择了错误 AWS 账号。
3. 日志组件是否仍指向 `us-east-1`。
4. 当前时间是否确实没有对应类型请求。
5. CloudFront 标准日志是否存在投递延迟。
6. IAM 是否缺少 `logs:StartQuery`。
7. 核心/诊断两组日志是否都有新事件。

异常请求表为空但状态码趋势正常，通常只是没有 4xx/5xx。

## 10. ECS、Master 和 RDS 日志怎么看

### 10.1 Worker 日志

1. 区域切换为 `us-west-2`。
2. 打开 `CloudWatch → 日志 → 日志组`。
3. 打开 `/ecs/nexusapi-prod-worker`。
4. 选择“搜索所有日志流”，不要先猜 Task ID。
5. 按时间和错误内容搜索。

日志流前缀：

| 前缀 | 内容 |
|---|---|
| `nginx/` | Nginx 入口访问与错误输出 |
| `new-api/` | NewAPI 应用日志 |
| `nginxcollector/` | Nginx 采集器自身运行日志，不是原始访问日志 |
| `reject-collector/` | 前置拒绝采集器自身运行日志 |

日志流名称最后一段通常是 ECS Task ID。自动扩缩容、滚动发布和故障替换都会创建新 Task ID，因此日常排障要搜索整个日志组，而不是长期收藏某一条日志流。

### 10.2 Master 日志

打开：

```text
/ecs/nexusapi-prod-master
```

Master 不接用户 API 流量，主要包含：

- 渠道自动测试。
- 订阅或额度重置任务。
- 邮件发送。
- 后台定时任务。
- Master 启动、数据库连接和异常信息。

如果看到用户高并发请求集中进入 Master，需要检查架构和目标组，不应把它当作正常情况。

### 10.3 RDS 错误日志

打开：

```text
/aws/rds/instance/nexusapi-mysql-prod/error
```

重点搜索：

```text
ERROR
timeout
Too many connections
InnoDB
crash
deadlock
```

反向 DNS 解析失败警告一般不是攻击，也不等于数据库连接失败；应结合连接错误和应用表现判断。

### 10.4 RDS 慢查询日志

打开：

```text
/aws/rds/instance/nexusapi-mysql-prod/slowquery
```

分析时至少记录：

- 执行时间。
- 查询耗时和锁等待。
- 扫描行数与返回行数。
- 涉及表和过滤条件。
- 是否来自 NewAPI、Monitor、Usage 或其他内部服务。
- 同类查询发生频率。

不能看到一条慢查询就立即创建索引。应结合 `EXPLAIN`、查询频率、写入成本和现有索引整体评估。

## 11. 在本机使用 AWS CLI 只读查看

### 11.1 账号原则

- 每位开发或运维人员使用独立 IAM 用户或企业统一身份。
- 不共享 `yanglei` 管理账号。
- 不创建和传播长期 Access Key。
- 使用临时登录凭证并启用 MFA。
- 本地 profile 名称只是别名，实际权限由 AWS 身份决定。

### 11.2 登录

```bash
aws --version
aws login --profile monitor-dev
aws sts get-caller-identity --profile monitor-dev
```

返回的账号必须是：

```text
842806122225
```

如果 ARN 显示其他用户或其他账号，立即停止查询并退出：

```bash
aws logout --profile monitor-dev
```

### 11.3 查看 CloudFront 日志

```bash
aws logs describe-log-groups \
  --log-group-name-prefix nexusapi-cloudfront- \
  --region us-east-1 \
  --profile monitor-dev
```

查看最近 15 分钟核心日志：

```bash
aws logs tail nexusapi-cloudfront-access \
  --since 15m \
  --format short \
  --region us-east-1 \
  --profile monitor-dev
```

查看最近 15 分钟诊断日志：

```bash
aws logs tail nexusapi-cloudfront-client-diagnostic \
  --since 15m \
  --format short \
  --region us-east-1 \
  --profile monitor-dev
```

### 11.4 查看 Worker 日志

```bash
aws logs tail /ecs/nexusapi-prod-worker \
  --since 15m \
  --format short \
  --region us-west-2 \
  --profile monitor-dev
```

只看 NewAPI：

```bash
aws logs tail /ecs/nexusapi-prod-worker \
  --log-stream-name-prefix new-api/ \
  --since 30m \
  --format short \
  --region us-west-2 \
  --profile monitor-dev
```

只看 Nginx：

```bash
aws logs tail /ecs/nexusapi-prod-worker \
  --log-stream-name-prefix nginx/ \
  --since 30m \
  --format short \
  --region us-west-2 \
  --profile monitor-dev
```

### 11.5 查看 ECS 状态

```bash
aws ecs describe-services \
  --cluster nexusapi-prod-cluster \
  --services nexusapi-prod-worker-canary nexusapi-prod-master \
  --query 'services[].{service:serviceName,desired:desiredCount,running:runningCount,pending:pendingCount,status:status}' \
  --region us-west-2 \
  --profile monitor-dev
```

正常基线：

```text
nexusapi-prod-worker-canary：desired=3，running=3，pending=0
nexusapi-prod-master：desired=1，running=1，pending=0
```

### 11.6 CLI 常见错误

#### 登录过期

出现 OAuth token、authorization grant invalid、expired 或 revoked：

```bash
aws logout --profile monitor-dev
aws login --profile monitor-dev
aws sts get-caller-identity --profile monitor-dev
```

#### 区域错误

如果错误 ARN 中出现 `eu-north-1` 或其他区域，说明页面/CLI 区域选错。CloudFront 日志用 `us-east-1`；ECS/RDS 日志用 `us-west-2`。

#### `logs:StartQuery` 被拒绝

说明当前人员只读策略没有授权 Logs Insights。仍可使用“搜索所有日志流”、`aws logs tail` 和 `FilterLogEvents`。如业务确实需要 Logs Insights，应由管理员评估后增加最小权限，不能直接加入管理员组。

## 12. 字段、隐私和数据安全

### 12.1 核心日志包含的 26 个字段

核心日志覆盖以下类型：

| 类型 | 字段 |
|---|---|
| 时间与标识 | `timestamp(ms)`、`date`、`time`、`distributionid`、`x-edge-request-id`、`x-edge-location` |
| 请求 | `cs-method`、`cs(Host)`、`x-host-header`、`cs-uri-stem` |
| 响应 | `sc-status`、`x-edge-result-type`、`x-edge-response-result-type`、`x-edge-detailed-result-type` |
| 协议与流量 | `cs-protocol`、`cs-protocol-version`、`cs-bytes`、`sc-bytes` |
| 延迟 | `time-taken`、`time-to-first-byte`、`origin-fbl`、`origin-lbl` |
| 内容与路由 | `sc-content-type`、`sc-content-len`、`cache-behavior-path-pattern`、`ssl-protocol` |

核心日志故意不包含原始客户 IP、X-Forwarded-For 和 User-Agent，适合长期日常分析。

### 12.2 诊断日志额外增加的 8 个字段

```text
c-ip
c-port
x-forwarded-for
cs(User-Agent)
asn
c-country
ssl-cipher
connection-id
```

这些字段可以帮助判断客户来源、网络运营商、客户端和 TLS 连接问题，因此只保留 14 天，并且不在主 Dashboard 展示原始值。

### 12.3 两组日志都没有记录的内容

```text
cs-uri-query
cs(Cookie)
cs(Referer)
viewer-request-log-data
viewer-response-log-data
请求体
响应体
Authorization Header
API Key / Token
```

这可以降低敏感信息泄露风险和日志体积，但也意味着 CloudFront 日志无法直接看到请求体中的模型、提示词、分组和业务参数。

### 12.4 使用要求

- 不得将诊断日志原文发到公开群、工单或代码仓库。
- 对外截图必须遮盖 IP、User-Agent、Request ID 和可能识别客户的信息。
- 不得把完整 API Key、Authorization、Cookie、请求体或响应体复制到日志查询中。
- 导出日志后应按最小必要范围保存并及时删除。
- 诊断日志只能用于稳定性排查、安全分析和必要审计。

## 13. 费用控制

### 13.1 日志保留

- 核心日志自动保留 30 天。
- 诊断日志自动保留 14 天。
- 到期事件由 CloudWatch 按保留策略清理，不需要人工逐条删除。
- 删除保护用于防止误删日志组，不会阻止到期日志按保留策略清理。

### 13.2 Dashboard 查询费用

当前 Dashboard 中有 5 个 Logs Insights 日志组件。AWS 会在以下情况重新运行查询：

- 打开 Dashboard。
- 刷新 Dashboard。
- 修改时间范围。
- 手动刷新组件。

因此建议：

- 默认使用最近 3 小时。
- 自动刷新设为 15 分钟，不要设置为 1 分钟。
- 深度排障先缩小到 15～30 分钟。
- 不要长期打开 7 天、30 天时间范围并持续自动刷新。

上线前验证时，5 条查询扫描 30 分钟日志合计约 `1.37 MB`。按流量线性估算，3 小时完整刷新约扫描 `8.2 MB`；如果全天每 15 分钟刷新，约为 `24 GB/月`。按常见的 Logs Insights `0.005 美元/GB` 估算，查询费约 `0.12 美元/月`，实际以区域价格、流量和 AWS 账单为准。

这个估算只包含这些日志查询的扫描费，不代表 CloudWatch 的全部费用。日志写入、存储、告警、Live Tail、其他 Dashboard 或其他服务会分别计费或占用套餐额度。

### 13.3 控制费用的原则

- 用时间缩小扫描范围，比写复杂过滤条件更能直接减少扫描数据量。
- Dashboard 用于总览，不替代长期全量分析平台。
- 高频程序采集优先使用经过限制的 `FilterLogEvents`，不要每几分钟扫描数小时 Logs Insights。
- 每月检查 CloudWatch Logs、Logs Insights、Alarm、Lambda 和数据传输账单。
- 日志保留期、字段和刷新频率的调整必须先评估成本和排障价值。

## 14. 当前限制与容易误判的地方

### 14.1 CloudFront 日志不是完整业务日志

CloudFront 不记录请求体，所以无法直接按用户 ID、模型、分组或渠道查询。完整业务判断仍要结合 NewAPI 日志、使用日志和数据库中的业务数据。

### 14.2 CloudFront 日志可能延迟

标准访问日志不是严格实时链路。实际观察中日志可以很快到达；AWS 官方说明日志通常在一小时内投递，少数日志记录可能延迟最长约 24 小时。刚发生的问题查不到时，要保留查询条件并稍后复查，不能把它作为秒级实时告警的唯一数据源。

### 14.3 极端大 Header 或超长 URL 可能无法完整记录

如果请求 URL 或 Header 超过 CloudFront 可解析范围，CloudFront 可能无法生成正常访问日志。因而“CloudFront 没日志”不能 100% 证明请求从未到达任何边缘节点。

### 14.4 请求 ID 尚未全链路统一

CloudFront 有 `x-edge-request-id`，NewAPI 也有自己的 request id；当前不能假设它们天然相同。定位时仍需结合时间、路径、状态码、耗时和客户信息。

### 14.5 4xx 不等于平台故障

4xx 可能来自：

- 无效令牌。
- 额度不足。
- WAF 拦截。
- 地域限制。
- 请求参数错误。
- 模型或分组权限不匹配。
- 恶意探针和扫描。

必须按来源和错误类型拆分。

### 14.6 长请求不等于超时

模型生成请求可能持续很久，尤其是非流式、大上下文、工具调用和复杂推理。应区分：

- 首字节是否过慢。
- 流式连接是否中途断开。
- 总耗时是否符合模型特征。
- 是否最终返回 2xx。

## 15. 权限建议

只读人员至少需要：

- 查看 CloudWatch Dashboard。
- 查看 CloudWatch 指标。
- 查看 CloudWatch Alarm。
- 查看指定 CloudWatch Logs 日志组和日志流。
- 使用 `FilterLogEvents`。
- 如确需统计分析，再单独授予 `StartQuery`、`GetQueryResults` 和 `StopQuery`。

不应授予：

- 修改或删除 Dashboard。
- 修改告警。
- 删除日志组或修改保留期。
- 修改 ECS 服务、停止任务。
- 修改 ALB、CloudFront、WAF 或 RDS。
- 读取 Secrets Manager 密钥。
- `logs:Unmask`。

人员控制台登录和生产程序访问必须使用不同身份。ECS 上的生产 Monitor 应使用独立的
ECS Task Role，不能复用员工 IAM 用户或把长期 Access Key 写进任务环境。HMAC 证据密钥
另由受控 Secrets Manager 注入，Task Role 只授予 CloudWatch Logs 最小读取权限。

## 16. 周期性维护建议

### 每日

- 检查 Dashboard 告警、目标健康、Worker 数量、错误率和延迟。
- 检查 CloudFront 核心/诊断投递一致性。
- 检查是否出现持续 5xx、连接错误或数据库异常。

### 每周

- 查看 7 天流量、错误率、P95 延迟和资源趋势。
- 分析主要 4xx/5xx 类型。
- 检查 RDS 慢查询和存储增长。
- 检查自动扩缩容是否按预期发生。
- 检查告警是否存在误报、漏报或长期无数据。

### 每月

- 检查 CloudWatch、CloudFront、WAF、Fargate、RDS、Lambda 和数据传输账单。
- 复核日志保留期和敏感字段是否仍合理。
- 复核人员和程序 IAM 权限。
- 清理已确认无用的实验资源和空日志组。
- 检查 Dashboard 查询是否仍匹配当前服务名、日志组和架构。

## 17. 配置变更与回滚原则

查看日志不需要变更生产配置。以下操作属于生产变更，必须单独确认、备份和验证：

- 修改 CloudFront 日志字段。
- 修改 delivery source、destination 或 delivery。
- 修改日志组保留期或删除保护。
- 修改 Dashboard 查询和布局。
- 修改 WAF 规则。
- 修改 ECS 日志驱动或任务定义。
- 删除实验日志组、Lambda Bridge 或采集器。

本次 Dashboard 上线记录保存在：

```text
Log/NexusAPI/cloudwatch-logging-change-20260912/
```

关键文件：

```text
step-10-dashboard-backup.json   上线前 Dashboard 备份
step-10-dashboard-draft.json    已上线的 Dashboard 草案
step-10-dashboard-review.json   上线和复核记录
step-09-diagnostic-two-observations.json  两轮日志投递观察记录
```

回滚 Dashboard 时只能使用确认过的备份文件，不得手工删除部分组件后直接保存。

## 18. 一页式排障速查

```text
客户报错
  ↓
记录准确时间、时区、域名、路径、模型、用户、Request ID、错误原文
  ↓
打开 NexusAPI-Prod-Overview，确认是否全局异常
  ↓
查 CloudFront 核心日志：请求是否到达、状态码、路径、耗时
  ├─ 没找到 → 核对时间/域名；查客户 IP 诊断日志；考虑客户网络或日志延迟
  └─ 找到
       ↓
     查 WAF / ALB：边缘拦截、ELB 5xx、Target 5xx、目标健康
       ↓
     查 /ecs/nexusapi-prod-worker
       ├─ nginx/：是否到达 Worker、状态码、连接中断
       └─ new-api/：渠道、上游、数据库和程序错误
       ↓
     必要时查 RDS error / slowquery
       ↓
     输出“事实、判断、影响、处置、待观察项”
```

## 19. 官方参考资料

- [CloudFront 标准日志 v2 配置](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/standard-logging.html)
- [CloudFront 标准日志字段与投递说明](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/standard-logs-reference.html)
- [CloudWatch Dashboard 使用说明](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Dashboards.html)
- [CloudWatch Dashboard 日志组件结构](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Dashboard-Body-Structure.html)
- [将 Logs Insights 查询加入 Dashboard](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_ExportQueryResults.html)
- [CloudWatch 价格](https://aws.amazon.com/cloudwatch/pricing/)
- [AWS CLI 使用控制台凭证登录](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sign-in.html)

## 20. 相关内部文档

- `脚本/newapi-monitor/docs/cloudwatch-container-log-access.md`：面向 Monitor 开发人员的 ECS 日志访问和代码调用说明。
- `文档/NexusAPI/13-运维手册/01-线上问题定位标准流程SOP.md`：通用线上问题定位流程。
- `文档/NexusAPI/13-运维手册/02-报障信息提交模板.md`：客户和内部报障信息模板。
- `文档/NexusAPI/13-运维手册/08-AWS监控报警配置方案.md`：AWS 监控与告警方案。
- `文档/NexusAPI/42-渠道稳定性治理体系/02.1-Monitor全链路监控需求与产品方案.md`：Monitor 全链路排障需求。

---

本文档描述的是 2026-09-13 已核验的生产配置。以后若 ECS 服务名、日志组、区域、字段、保留期或 Dashboard 发生变化，变更负责人必须同步更新本文档和变更记录。
