[English](README.en.md) | 简体中文

# newapi-monitor

> **new-api 上游监控** —— 零侵入、只读采样的旁路稳定性监控与邮件报警。

[![CI](https://github.com/yl0711-coder/newapi-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/yl0711-coder/newapi-monitor/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/yl0711-coder/newapi-monitor)](https://goreportcard.com/report/github.com/yl0711-coder/newapi-monitor)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

给 [new-api](https://github.com/Calcium-Ion/new-api) 网关加一个独立的「上游稳定性」看板:用一个**只读账号**每分钟对其日志库做一次小聚合查询,在本地 SQLite 留存,展示 **分组 / 渠道 / 模型** 维度的成功率、异常、响应耗时(TTFB/TTFT),异常时邮件报警。**不改 new-api、不写它的库。**

## 特性
- **零侵入**:只读采样,每周期一条小聚合查询,不给生产库添负担。
- **三态稳定性**:成功 / 异常(`client_gone` 等客户端中断)/ 失败(上游错误),按 分组 × 渠道 × 模型 聚合。
- **响应耗时**:P50/P95 时延、TTFB/TTFT 首字延迟分布、出字速度(tok/s)。
- **登录鉴权**:复用 new-api 用户身份(调其 `/api/user/login` 验证),按角色分权,无需自建账号。
- **邮件报警**:错误率 / 错误突发 / 异常成簇 / 采样掉线 等规则,阈值可调。
- **轻量**:纯 Go + 内嵌 SQLite(`CGO_ENABLED=0` 静态编译),单容器、无外部依赖。

## 工作原理
```
new-api 日志库 (MySQL) ──每 60s 只读聚合查询──► newapi-monitor ──► 本地 SQLite ──► 看板 / 邮件报警
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
  ghcr.io/yl0711-coder/newapi-monitor:latest
```

打开 `http://<host>:8090`,用 new-api 管理员账号登录。完整 compose 见 [`docker-compose.example.yml`](docker-compose.example.yml)。生产建议前面放一层反向代理(nginx / Caddy)做 HTTPS。

## 配置(环境变量)
| 变量 | 说明 | 默认 |
|---|---|---|
| `NEWAPI_LOG_DSN` | new-api 库的**只读** DSN(MySQL) | 必填 |
| `MONITOR_NEWAPI_BASE_URL` | new-api 地址,用于登录鉴权 | 必填 |
| `MONITOR_SESSION_SECRET` | 会话签名密钥(`openssl rand -hex 32`) | 留空则启动随机生成 |
| `MONITOR_ADDR` | 监听地址 | `:8090` |
| `MONITOR_STORE_PATH` | 本地采样库路径 | `/data/monitor.db` |
| `MONITOR_SAMPLE_SECONDS` | 采样间隔(秒) | `60` |
| `MONITOR_RETENTION_DAYS` | 分钟级本地留存天数 | `7` |
| `MONITOR_HOUR_RETENTION_DAYS` | 小时级汇总留存天数(长期趋势 + 同比环比) | `90` |
| `MONITOR_BACKFILL_HOURS` | 启动时回填的历史小时数 | `24` |
| `MONITOR_HEARTBEAT_URL` | dead-man 心跳 URL(如 healthchecks.io);留空=不启用 | 留空 |
| `MONITOR_SITE_NAME` | 对外看板站点名**兜底值**;站点名/favicon 默认部署时从主站 new-api 的 `system_name`/`logo` 同步,此项仅主站不可达时兜底 | 留空 |

## 对外状态看板(公开、无登录)
除内部监控外,同一进程还提供一个**面向客户的公开状态页**(脱敏、无需登录),适合放在独立子域名(如 `status.example.com`):

- `GET /status` —— 浅色卡片状态页(内嵌、自包含)。
- `GET /public/status` —— 脱敏 JSON,供页面轮询。

维度是**分组(线路)× 模型**:渠道对用户透明。可见分组取自 new-api 的 `/api/pricing`(`usable_group`,即令牌创建页能选到的分组),显示名与主站一致。状态由「拓扑健康(某分组×模型有无可用渠道)」+「近 7 天流量」合成:配置在册但无可用渠道直接显示「不可用」。

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
给 new-api 库单独建一个只读账号,仅授予 `logs`、`channels` 表的 `SELECT`,用于 `NEWAPI_LOG_DSN`:
```sql
CREATE USER 'ro_user'@'%' IDENTIFIED BY '<strong-password>';
GRANT SELECT ON newapi.logs     TO 'ro_user'@'%';
GRANT SELECT ON newapi.channels TO 'ro_user'@'%';
```

## 安全
- 镜像内不含任何密钥;DSN、会话密钥、SMTP 凭证均通过环境变量注入。
- SMTP 凭证等敏感信息前端永不回显。

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
