[English](README.en.md) | 简体中文

# newapi-monitor

> **new-api 上游监控** —— 零侵入、只读采样的旁路稳定性监控与邮件报警。

[![CI](https://github.com/yl0711-coder/newapi-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/yl0711-coder/newapi-monitor/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/yl0711-coder/newapi-monitor)](https://goreportcard.com/report/github.com/yl0711-coder/newapi-monitor)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

给 [new-api](https://github.com/Calcium-Ion/new-api) 网关加一个独立的「上游稳定性」看板：用一个**只读账号**执行有界、串行、带退避的来源采样/事实同步，在本地 SQLite 留存并展示 **分组 / 渠道 / 模型** 维度的成功率、异常、响应耗时(TTFB/TTFT)，异常时邮件报警。**不写 new-api 数据库。**

本机 Docker 启动、离线 Smoke Test、隔离合成数据验收和受控只读真实数据验收见 [本地 Docker 启动与测试](docs/local-docker-testing.md)。

新开发环境可直接运行：`./dev/run-local-dev.sh up`。它只启动隔离的本机容器，不读取线上数据库或上游；常用的查看、日志和停止命令也在该脚本中。

## 特性
- **零侵入**:只读、小窗口、有预算的后台采样；页面只读本地 SQLite，不随访问量查询生产库。
- **历史稳定性**:分组 / 渠道 / 模型长期趋势、同比环比、问题原文聚合与渠道使用排行。
- **渠道配置留痕**:渠道删除后保留最后快照；倍率更新追加版本，不覆盖历史版本。
- **上游账户同步**:按主域名配置 NewAPI / Sub2API / AICodeWith 账户；余额与消费账单是两个独立状态，凭据本地加密保存，失败保留最后成功值并指数退避。
- **动态充值预警**:用最近完整自然日的本地小时汇总和双方倍率估算余额可用天数；余额、倍率或数据覆盖不可靠时停止评估，不用残缺数据误报。
- **三态稳定性**:成功 / 异常(`client_gone` 等客户端中断)/ 失败(上游错误),按 分组 × 渠道 × 模型 聚合。
- **响应耗时**:P50/P95 时延、TTFB/TTFT 首字延迟分布、出字速度(tok/s)。
- **登录鉴权**:复用 new-api 用户身份(调其 `/api/user/login` 验证),按角色分权,无需自建账号。
- **邮件报警**:错误率 / 错误突发 / 异常成簇 / 采样掉线 等规则,阈值可调。
- **轻量**:纯 Go + 内嵌 SQLite(`CGO_ENABLED=0` 静态编译),单容器即可运行；Redis 仅为可选用量缓存。

## 工作原理
```
new-api 日志库 (MySQL) ──有界只读采样/同步──► newapi-monitor ──► 本地 SQLite ──► 看板 / 邮件报警
```
采样器是**唯一**访问 new-api 库的组件;页面只读本地 SQLite,与生产库隔离。

## 快速开始(Docker)
```bash
docker run -d --name newapi-monitor \
  -p 8090:8090 \
  -e NEWAPI_LOG_DSN='ro_user:pass@tcp(db-host:3306)/newapi?charset=utf8mb4&timeout=5s&readTimeout=10s' \
  -e MONITOR_NEWAPI_BASE_URL='https://your-newapi.example.com' \
  -e MONITOR_SESSION_SECRET="$(openssl rand -hex 32)" \
  -v newapi_monitor_data:/data \
  ghcr.io/yl0711-coder/newapi-monitor:REPLACE_WITH_ACCEPTED_TAG_OR_DIGEST
```

打开 `http://<host>:8090`,用 new-api 管理员账号登录。完整 compose 见 [`docker-compose.example.yml`](docker-compose.example.yml)。生产建议前面放一层反向代理(nginx / Caddy)做 HTTPS。

## 配置(环境变量)
| 变量 | 说明 | 默认 |
|---|---|---|
| `NEWAPI_LOG_DSN` | new-api 库的**只读** DSN(MySQL) | 必填 |
| `MONITOR_NEWAPI_BASE_URL` | new-api 地址,用于登录鉴权 | 必填 |
| `MONITOR_SESSION_SECRET` | 会话签名密钥(`openssl rand -hex 32`) | 留空则启动随机生成 |
| `MONITOR_UPSTREAM_CREDENTIAL_SECRET` | 渠道管理中上游令牌的 AES-256-GCM 加密密钥；生产应长期固定并与会话密钥分离 | 显式会话密钥；两者都未配置时拒绝保存凭据 |
| `MONITOR_UPSTREAM_SYNC_ENABLED` | 上游余额后台同步开关；上游故障/风控维护窗口可临时关闭 | `true` |
| `MONITOR_UPSTREAM_SYNC_MINUTES` | NewAPI / Sub2API 上游账户余额正常同步间隔；失败自动指数退避 | `5` |
| `MONITOR_UPSTREAM_SYNC_TIMEOUT_SECONDS` | 单个上游同步请求超时 | `15` |
| `MONITOR_UPSTREAM_USAGE_SYNC_ENABLED` | 上游消费日志后台同步的独立灰度闸门；首次发布不会访问上游，验证后再显式开启 | `false` |
| `MONITOR_UPSTREAM_USAGE_SYNC_MINUTES` | 上游消费账单当天尾部刷新间隔；支持 NewAPI 分页日志、Sub2API 小时汇总（旧版回退单日汇总）和 AICodeWith 按 Key 日账单；只有全局灰度闸门和账户开关都开启时运行，历史补全独立退避，所有上游请求全局串行 | `30` |
| `MONITOR_UPSTREAM_USAGE_BACKFILL_DAYS` | 首次低频补全的上游账户使用日志天数 | `90` |
| `MONITOR_ADDR` | 监听地址 | `:8090` |
| `MONITOR_PORTAL_ADDR` | 客户用量门户独立监听地址；留空则不启用 | 留空 |
| `MONITOR_USAGE_REDIS_ADDR` | 客户用量聚合结果 Redis 私网地址；留空时自动使用有界本机短缓存 | 留空 |
| `MONITOR_USAGE_REDIS_USERNAME` | Redis ACL 用户；生产建议仅允许 `nxmon:*` | 留空 |
| `MONITOR_USAGE_REDIS_PASSWORD` | Redis 密码；只通过环境变量注入 | 留空 |
| `MONITOR_USAGE_REDIS_DB` | Redis DB 编号；安全隔离仍依赖 ACL 与 key prefix | `0` |
| `MONITOR_USAGE_REDIS_PREFIX` | 用量缓存键前缀 | `nxmon:usage:v1` |
| `MONITOR_TRUSTED_PROXIES` | 可提供真实客户端 IP 的可信反代 IP/CIDR，逗号分隔；留空则不信任转发头 | 留空 |
| `MONITOR_STORE_PATH` | 本地采样库路径 | `/data/monitor.db` |
| `MONITOR_USAGE_FACTS_STORE_PATH` | 独立用量事实库路径；与主库分文件，避免补数/WAL/损坏影响控制数据 | `<MONITOR_STORE_PATH 目录>/usage-facts.db` |
| `MONITOR_STORE_BACKUP_ENABLED` | 运行期在线日备份开关；不影响启动时强制迁移前快照 | `true` |
| `MONITOR_STORE_BACKUP_DIR` | 在线备份和迁移前成套快照目录 | `<MONITOR_STORE_PATH 目录>/backups` |
| `MONITOR_STORE_BACKUP_RETENTION` | 已验证 main+facts `backup-set` 成套备份保留数 | `7` |
| `MONITOR_STORE_MIGRATION_BACKUP_RETENTION` | main+facts 同一 manifest 的迁移前快照批次保留数 | `3` |
| `MONITOR_USAGE_FACTS_ENABLED` | 后台按成员、按小时构建本地用量事实；生产部署示例固定开启 | `false` |
| `MONITOR_USAGE_FACTS_READ_ENABLED` | 聚合页面只读取完整发布的本地事实；未完成时 fail-closed，不回扫 logs | `false` |
| `MONITOR_USAGE_FACTS_FULL_HISTORY_ENABLED` | 按成员真实注册/首日志边界构建全历史；生产示例显式开启 | `false` |
| `MONITOR_USAGE_FACTS_HISTORY_SOURCE_MODE` | 全历史来源完整性声明；只有 DBA 签收后的 `complete` 可启动 | `unverified` |
| `MONITOR_USAGE_FACTS_HISTORY_SOURCE_EPOCH` | 来源归档/路由契约稳定标识；变化会强制全域重签 | 留空 |
| `MONITOR_USAGE_FACTS_HISTORY_SOURCE_DUTY_PERCENT` | cold history 来源查询最大 duty；Tail 优先 | `20` |
| `MONITOR_USAGE_FACTS_CLASSIFICATION_MIGRATION_ENABLED` | 显式分类迁移维护开关；开启时 READ 必须关闭 | `false` |
| `MONITOR_SAMPLE_SECONDS` | 采样间隔(秒) | `60` |
| `MONITOR_RETENTION_DAYS` | 分钟级本地留存天数 | `7` |
| `MONITOR_HOUR_RETENTION_DAYS` | 小时级汇总留存天数(长期趋势 + 同比环比) | `90` |
| `MONITOR_BACKFILL_HOURS` | 来源 epoch 启动缺口配置；自动路径按持久水位且硬限 1 小时 | `1` |
| `MONITOR_STABILITY_ENABLED` | 历史稳定性报表与原始问题采集开关；关闭不影响原模型/用量/服务端监控 | `true` |
| `MONITOR_STABILITY_QUERY_MAX_DAYS` | 稳定性报表页面单次最大查询范围 | `90` |
| `MONITOR_STABILITY_RETENTION_DAYS` | 稳定性本地数据留存；运行时至少取 `2×QUERY_MAX+1`，保证上一周期对比完整 | `181` |
| `MONITOR_STABILITY_PROBLEM_SAMPLE_SECONDS` | 原始错误签名后台采样间隔；高峰时自动按本地游标续采 | `300` |
| `MONITOR_STABILITY_BACKFILL_ENABLED` | 历史补数总开关；关闭后人工、自动修洞和重启续跑都不访问生产历史库 | `true` |
| `MONITOR_NGINX_ENABLED` | Nginx 入口层脱敏分钟聚合开关；启用前必须配置 ingest token 和节点白名单 | `false` |
| `MONITOR_NGINX_RETENTION_DAYS` | Nginx 入口分钟聚合本地留存天数；需与节点采集器一致 | `7` |
| `MONITOR_NGINX_ALLOWED_NODES` | 允许上报 Nginx 聚合的节点名白名单，逗号分隔 | 留空 |
| `MONITOR_HEARTBEAT_URL` | dead-man 心跳 URL(如 healthchecks.io);留空=不启用 | 留空 |
| `MONITOR_SITE_NAME` | 对外看板站点名**兜底值**;站点名/favicon 默认部署时从主站 new-api 的 `system_name`/`logo` 同步,此项仅主站不可达时兜底 | 留空 |
| `MONITOR_INGEST_TOKEN` | 「被拒请求」接收口 `POST /internal/rejections` 的鉴权 token,供各节点 [newapi-reject-collector](https://github.com/yl0711-coder/newapi-reject-collector) 推送前置拒绝;**留空=关闭该接口** | 留空 |

Nginx 入口层只接收节点端已脱敏、已按分钟聚合的事实，不读取原始请求日志。
安全边界、Nginx 专用日志格式、轮转要求和滚动接入流程见
[`docs/nginx-collector.md`](docs/nginx-collector.md)。

## 被拒请求(前置拒绝 · logs 盲区)

new-api 的「无可用渠道」等**前置拒绝**不写 `logs` 表,读 logs 的监控天然看不到。配套的旁路采集器 [newapi-reject-collector](https://github.com/yl0711-coder/newapi-reject-collector) 在每个节点 tail new-api 日志、抽出这类拒绝,`POST /internal/rejections`(带 `MONITOR_INGEST_TOKEN` 鉴权)推来,监控落 `rejection_samples` 表并在「被拒请求」面板按 模型 × 分组 展示。每次推送必须携带稳定的 `batch_id`；中心在同一事务内登记批次并累加样本，响应丢失后的重试不会翻倍。升级时先部署新版采集器（旧 Monitor 会忽略新字段），再部署新版 Monitor。

该面板由**超管开关**控制(报警设置页「被拒请求」,**默认关**):开启后才显示,开关旁附说明需在各节点安装采集器;开启但尚无数据时显示"暂无数据,请部署采集器"空状态。未配置 `MONITOR_INGEST_TOKEN` 时接收口关闭(503)。开关关、无 token 或无数据,都不影响监控其它功能。

## 对外状态看板(公开、无登录)
除内部监控外,同一进程还提供一个**面向客户的公开状态页**(脱敏、无需登录),适合放在独立子域名(如 `status.example.com`):

- `GET /status` —— 浅色卡片状态页(内嵌、自包含)。
- `GET /public/status` —— 脱敏 JSON,供页面轮询。

维度是**分组(线路)× 模型**:渠道对用户透明。可见分组取自 new-api 的 `/api/pricing`(`usable_group`,即令牌创建页能选到的分组),显示名与主站一致。状态按「近期可用率 + 是否有可用上游」合成:正常(≥99%)· 性能下降(50–99%,仍在服务)· 不可用(<50% 或无可用上游)。分组状态按「线路还能不能用」判——有正常模型即最多「性能下降」,无任一正常才「不可用」(不取最差模型,避免个别降级把整条线标成不可用)。

> **禁用渠道不计入稳定性**(看板 + 内部监控一致):稳定性聚合(总览/分组/模型/趋势)只统计**当前启用且在其启用时刻之后**的渠道流量——手动禁用 / 自动熔断渠道的历史失败不再拖低模型,渠道重新启用(含新部署)从启用时刻重新计(`channel_snaps.enabled_since`)。内部监控「按渠道」明细表仍保留禁用渠道供排障。

> **只展示 / 统计用户可选的模型**:看板只显示、内部监控只统计「可见分组(`/api/pricing`)∩ 有启用渠道配置」的 (分组,模型)。不可选的(全禁用 / 只在不可选分组 / 误路由到没配它的渠道)在看板、监控、报警中一律排除——不能选的报警没意义。内部监控「按渠道」明细不过滤,排障仍能看误路由等异常。

**强隔离**:看板是独立的 `monitor/public` 包,只读本地采样库,绝不引用内部结构;**公开面绝不输出**渠道名/ID/IP、成本/配额、令牌/用户、请求量/QPS、错误详情。

反代示例(Caddy,按子域名分流):
```
status.example.com {
    reverse_proxy monitor:8090
    rewrite / /status
}
```

## 权限
登录复用 new-api 用户身份(仅调用其 `/api/user/login` 验证):
- `role >= 10`(管理员):可登录查看;
- `role = 100`(超级管理员):可修改报警配置。

## 只读账号
给 new-api 库单独建一个只读账号，仅授予 `logs`、`channels`、`users`、`tokens`、`options` 五表的 `SELECT`
(后两张是「用户用量 / 客户报表」功能读余额与令牌名所需),用于 `NEWAPI_LOG_DSN`:
```sql
CREATE USER 'ro_user'@'%' IDENTIFIED BY '<strong-password>';
GRANT SELECT ON newapi.logs     TO 'ro_user'@'%';
GRANT SELECT ON newapi.channels TO 'ro_user'@'%';
GRANT SELECT ON newapi.users    TO 'ro_user'@'%';
GRANT SELECT ON newapi.tokens   TO 'ro_user'@'%';
GRANT SELECT ON newapi.options  TO 'ro_user'@'%';
```

## 客户用量门户(Usage Portal)

客户门户与管理端使用独立监听端口、独立 Cookie 和独立路由。正式 compose 已默认监听
`127.0.0.1:8091`，并通过 `MONITOR_PORTAL_ADDR=:8091` 启用；它**不是**给开发机或客户 IP
加白名单。请用 HTTPS 反代分别暴露管理端和客户门户，例如：

```caddy
monitor.example.com {
    reverse_proxy 127.0.0.1:8090
}
usage.example.com {
    reverse_proxy 127.0.0.1:8091
}
```

不要直接将 `8090`、`8091` 映射到公网；客户账号需由超级管理员在「用户用量」中为分组开通。
若 Caddy/Nginx 不在同一主机，需将其实际 IP/CIDR 配入 `MONITOR_TRUSTED_PROXIES`，否则登录限流会按反代地址计算。

可选 Redis 只缓存矩阵、按日/分组/模型及令牌日志聚合：包含今天的区间 TTL 为 60 秒，
已结束的历史区间 TTL 为 10 分钟，管理端重新选择日期时强制取新。用户名、邮箱、当前余额、
令牌当前元数据、会话、原始日志和 CSV 不写入 Redis。Redis 断连/超时/鉴权失败时接口自动回退到
最多 128 项、16 MiB 的本机最多 60 秒应急缓存，本机过期时间不会超过 Redis 记录的剩余 TTL。
Redis 故障后会快速回源并退避 30 秒，使故障稳态的同键回源频率不高于旧版 60 秒本机缓存。
每个聚合结果最多缓存 4 MiB；每日消费矩阵在“成员数×自然日数”超过 20,000 格时，
服务端会在执行聚合和缓存填充前拒绝并提示缩小范围。事实层冷查询另有容量为 2 的本地并发闸门，
避免不同缓存键同时读取大范围 SQLite 时放大 CPU 和内存。
不能把 Redis 当作业务正确性的依赖。生产必须使用私网地址、
独立 ACL 用户及带 TTL 的 `nxmon:*` 权限，禁止把 Redis 6379 直接暴露公网。
管理员登录后可读取 `GET /usage/cache-stats` 查看命中、回源、远端错误、退避状态、本机容量及
`source_budget`/`facts_read_budget` 查询闸门计数；
该接口不会主动探测 Redis，也不返回缓存键、筛选条件或客户数据。CSV 导出先做一次带快照的预检，
随后由浏览器直接流式写入文件；5 万行上限保持不变，页面内存不再随导出行数增长。

## 安全
- 镜像内不含任何密钥;DSN、会话密钥、SMTP 凭证均通过环境变量注入。
- SMTP 凭证等敏感信息前端永不回显。

## 稳定性采集健康与数据保护

- `GET /health` 只用于容器存活检查，不会因报表数据暂时延迟而触发错误重启。
- 管理员登录后可读取 `GET /stability/health`，查看主采样新鲜度、错误采集完整覆盖时间、积压分钟和本地库状态；该接口不查询生产库。
- Monitor SQLite 已包含稳定性历史、渠道最后快照和倍率版本，必须随 `/data` 数据卷备份。安全备份、恢复、功能关闭和镜像回退步骤见 [`docs/monitor-operations.md`](docs/monitor-operations.md)。

## 构建
```bash
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o newapi-monitor .   # 二进制
docker build -t newapi-monitor .                                        # 镜像
```
推 `main` 或打 `v*` tag 时,GitHub Actions 先跑 `go vet` + `go test`,通过后自动构建并发布镜像到 GHCR(见 [`.github/workflows/ci.yml`](.github/workflows/ci.yml))。

## 第三方组件
- [Apache ECharts](https://echarts.apache.org/)(Apache-2.0)——看板图表,已内嵌、自服务、不走 CDN。
- [go-mail](https://github.com/wneessen/go-mail)(MIT)——报警邮件发送。
- [gin](https://github.com/gin-gonic/gin) / [GORM](https://gorm.io) / [glebarez/sqlite](https://github.com/glebarez/sqlite) / [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) / [godotenv](https://github.com/joho/godotenv)。

## License
[MIT](LICENSE)
