# Changelog

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/) 与 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

## [1.1.0] - 2026-06-10

### Added
- **对外公开服务状态看板**(独立 `monitor/public` 包):`GET /status` 浅色卡片状态页 + `GET /public/status` 脱敏 JSON,无需登录,适合独立子域名。
  - 维度 = **分组(线路)× 模型**,渠道对用户透明;可见分组取自 new-api `/api/pricing` 的 `usable_group`,显示名与令牌创建页一致。
  - 状态由「拓扑健康(某分组×模型有无可用渠道)」+「近 7 天流量可用率(排除用户 4xx)」合成;配置在册但无可用渠道直接显示「不可用」。
  - 落地页为分组卡片(展示该线路最稳定推荐模型),点击弹层看组内全部模型;支持跨线路搜索某模型对比稳定性;Uptime Kuma 风格心跳条;每 60s 自动刷新;30s 服务端缓存。
  - **中英双语**:按浏览器语言默认 + 切换记忆(localStorage);标识符(分组/模型名)不翻译。
  - **强隔离**:`public` 包不引用任何内部结构,公开面绝不输出渠道/成本/令牌/请求量/错误详情(有脱敏单测)。
- 采样器周期性快照渠道健康(状态/分组/模型)到本地 `channel_snaps`,供看板派生「无可用渠道」(只读非密字段,生产库零额外负担)。
- 配置项 `MONITOR_SITE_NAME`(看板站点名)。
- 开源标准完善:`SECURITY.md`、`CODE_OF_CONDUCT.md`、英文 README 看板节;golangci-lint 加严(misspell/errorlint/unconvert/bodyclose/revive/gocritic)。

### Changed
- 地道命名:`ChannelId→ChannelID`、`ChannelSnap.Id→ID`;`public` 导出类型去 stutter(`Model`/`Group`/`Snapshot`)。

### Fixed
- `public` 包本地查询失败补 `slog.Warn`,优雅降级为空且不再静默吞错。

## [1.0.0] - 2026-06-09

首个正式版。零侵入、只读采样的旁路稳定性监控。

### Added
- **三态稳定性**:成功 / 异常(`client_gone` 等客户端中断)/ 失败(上游错误),按 **分组 × 渠道 × 模型** 聚合。
- **响应耗时**:P50/P95/P99 时延、TTFT 首字延迟、出字速度(tok/s)、延迟分布直方图。
- **错误分类**:4xx / 5xx / 超时 / 其他。
- **per-Key/令牌维度**:按令牌聚合成功率/异常/错误/用量/成本(独立隔离采样,Top 100)。
- **成本/配额** 与 **SLO + 错误预算 + 燃烧告警**(SLI=非错误率,快烧/慢烧两档,全部可配)。
- **同比环比**(近 24h vs 前 24h vs 上周同期)与**小时级 rollup 长期趋势**(长留存)。
- **行内逐分钟下钻**、**快照缓存**(短 TTL,减负)。
- **邮件报警**:错误率 / 错误突发 / 异常成簇 / 采样掉线 规则,阈值可配;SMTP 可手填或一键同步 new-api 发件配置(go-mail,HTML 邮件)。
- **dead-man 心跳**:每周期成功采样后 ping 外部服务(如 healthchecks.io)。
- **登录鉴权**复用 new-api 用户身份,按角色分权(管理员看、超管改)。
- 动态站点名(默认取 new-api `system_name`)、ECharts 自服务图表(不走 CDN)、`log/slog` 结构化日志。
- 纯 Go + 内嵌 SQLite(`CGO_ENABLED=0` 静态编译),单容器、零外部依赖。
- Docker 镜像;GitHub Actions 自动 `go vet` + `go test` + `golangci-lint`,通过后发布镜像到 GHCR。

[Unreleased]: https://github.com/yl0711-coder/newapi-monitor/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/yl0711-coder/newapi-monitor/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/yl0711-coder/newapi-monitor/releases/tag/v1.0.0
