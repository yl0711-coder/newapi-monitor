# Changelog

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/) 与 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Added
- **客户报表「使用日志」视图**:逐条日志(时间/成员/令牌/分组/类型/模型/用时·首字/输入/输出/费用/详情),游标分页(50/页,首页返回总条数),筛选(成员/模型/分组/类型/令牌名);类型含 充值/消费/管理/系统,**错误与退款不对客户展示**(显式请求返回 400)。
- **CSV 导出**:UTF-8 BOM;单次封顶 5 万条(超出先确认导最新 5 万);每组织 5 分钟 1 次(原子预占,仅成功下载计次,探测/失败回退);导出进度弹窗可取消;文本列做公式注入消毒。
- **详情列**:对齐 new-api 线上多行格式(倍率/输入价/缓存读等,自 `other` 白名单解析,渠道等内部字段不落客户端);表头「输入tokens」加 Anthropic 缓存口径提示;导出按钮旁加导出规则说明(? 悬停,自适应上下方向)。
- 客户报表「用量总览」工具条对齐管理端(预设/自定义起止+查询/搜索用户);new-api 风格紧凑日期选择器(最长 90 天)。

### Fixed
- 管理端切换监控时间窗口不再整页刷新跳回默认 Tab;当前 Tab 记入 URL hash,从报警设置返回/手动刷新均保留。
- 客户日志令牌名搜索转义 LIKE 通配符并限长 64,防泛匹配拖慢生产库查询。

### Changed
- 代码质量收尾(无行为变化):portal 越权判定改 sentinel error + `errors.Is`(弃字符串比较);ingest 鉴权抽公共闸 `checkIngest`;服务端监控「页面色标」与「邮件告警」的红线判定收敛为共用谓词(永远同口径);`computeFollowUps` 拆出纯函数 `classifyMember`;本地库读路径 fail-open 时补 `slog.Warn` 留痕;维度列名白名单守卫;客户/分组管理域从 usage.go 拆到 customers.go。
- 数据库连接数/磁盘队列/LB 响应毫秒三个告警阈值从硬编码提为可配:`MONITOR_INFRA_DB_CONN_WARN`(默认 70)/`MONITOR_INFRA_DB_DISK_QUEUE_WARN`(默认 5)/`MONITOR_INFRA_LB_RESP_WARN_MS`(默认 2000)。
- CI 测试改跑 `go test -race`(常开竞态检测)。

## [1.4.0] - 2026-06-24

### Added
- **服务端健康监控(新增「服务端监控」Tab)**:在原「模型监控」之外加一个 Tab,监控实例 / 数据库 / 负载均衡健康。数据来自 AWS Lightsail 指标接口(只读,对实例零影响)。
  - **分组指标图**:DB / 实例统一「指标组下拉 + 图」,相关指标同组(内存=已用内存+Swap、算力=CPU+突发/连接、存储=已用存储+磁盘队列、网络、负载=load1/5/15、容器);图顶部带当前值+颜色。实例默认一行、点开看图;近 6h 序列按需经 `GET /infra/series` 拉取。容量类(内存/存储)按惯例显示「已用」。
  - **端到端可用性探活**:对各前端域名 HTTPS 探活(状态码/延时/TLS 证书剩余天数),并校验「是否经 CloudFront(Via 头)」,脱离 CDN 记黄。
  - **源站锁看门狗(F-5)**:周期性直连各源站不带密钥头,期望被拦 403;变非 403 即红告警(可绕 CDN 直连后端)。
  - 实例按重要程度排序(Master>Slave>Redis>其余)。
  - 基础设施告警(复用现有邮件+冷却):DB 可用内存/存储不足、实例 StatusCheckFailed、LB 不健康节点、探活失败/证书将过期、源站锁失效。
  - **`MONITOR_INFRA_ENABLED` 开关,默认关**;关闭时不调 AWS、不影响模型监控与现网行为。
  - 新增环境变量:`MONITOR_INFRA_*`、`AWS_REGION`、`MONITOR_PROBE_*`、`MONITOR_ORIGIN_LOCK_*`(AWS 凭证用 SDK 默认链)。
- **节点主机指标采集器 `hostagent`(`cmd/hostagent`,同仓 matrix 构建)**:采本机内存/Swap/磁盘/load/容器存活,`POST /internal/host`(Bearer = `MONITOR_INGEST_TOKEN`)。纯 stdlib、只读、fail-open;某项采集失败即不上报该项(避免「缺失=0」被下游算成异常)。

## [1.3.2] - 2026-06-12

### Changed
- **监控也只统计用户可选的模型**(报警同理——"都不能选了报什么警"):新增 `selectable_pairs` 表,采样器每分钟据 `/api/pricing` 可见分组 ∩ 启用渠道配置 重算;监控的稳定性聚合(总览/分组/模型/趋势 + 报警 summary)只算可选 (分组,模型),排除误路由 / 全禁用 / 只在不可选分组的流量(表为空 fail-open 不过滤,避免空窗)。「按渠道」明细不过滤,排障仍能看误路由等异常。
- 看板**只显示用户真能选到的模型**:某分组×模型在该(可见)分组下至少有一个【启用】渠道才显示;改用 channel_snaps 的启用渠道配置判定,不再因历史/偶发流量带出"不可选"模型。自动隐藏:所有含它的渠道都被禁用、只配置在不可选分组、或仅因误路由产生流量(例:gpt-image-1 只配在内部分组 internal_test,却因 1 次误路由出现在 codex-1.2x 显示"不可用",现已隐藏)。

## [1.3.1] - 2026-06-11

### Changed
- 看板状态语义重新平衡(诚实但不夸大、体现可用一面):**模型**——"不可用"只留给真·故障(无可用上游 或近期可用率 <50%),在服务但有失败(50–99%)→「性能下降」(原 <85% 即判不可用过狠);**分组**——按"线路还能不能用"判,有正常模型即最多「性能下降」、无任一正常才「不可用」(原取最差模型,个别降级就把整条线标不可用,对外夸大);「当前事件」只列真·outage 不广播降级;分组卡改用后端分组状态(前端不再重算 worst)。
- 渠道开关状态改为**每个采样周期(1 分钟)同步**(原约 10 分钟):"禁用渠道不计入 / 重启用从启用时刻起算"近乎实时生效;代价可忽略(小查询,远轻于每分钟的日志聚合)。

### Fixed
- 看板心跳条 tooltip 改为显示该桶的**时间区间**(末桶结束 = 现在),消除"误以为数据只到某点"的歧义(每桶约 7天÷桶数 ≈ 3.4 小时)。
- 看板心跳条:**单桶请求数过少(<5)不再画红/黄**,避免少量请求里的偶发失败在对外页面被呈现为"服务差"。

## [1.3.0] - 2026-06-11

### Added
- 「被拒请求」面板增加**超管开关**(报警设置页「被拒请求」):默认关,开启后内部监控才显示该面板;开关旁说明需在各 new-api 节点安装采集器 `newapi-reject-collector` 才有数据。开启但尚无采集器数据时,面板显示"暂无数据,请部署采集器"空状态(而非隐藏)。

### Fixed
- **禁用渠道不再拖低稳定性**:看板与监控的稳定性聚合(总览/分组/模型/趋势 + 看板)只统计"**当前启用且在其启用时刻之后**"的渠道流量。手动禁用 / 自动熔断渠道的历史失败不再计入,避免"渠道已禁用、模型却仍显示不可用";**新建渠道 / 从禁用重新启用的渠道从启用时刻起算**,而**既有启用渠道在监控升级后保留原历史**(不因一次部署把所有渠道稳定性清零)。`channel_snaps` 增 `enabled_since` 列;按渠道明细表仍显示禁用渠道供排障;无渠道快照的流量默认保留(fail-open),不影响告警。

## [1.2.0] - 2026-06-10

### Added
- **被拒请求(前置拒绝)监控**:接收旁路采集器 [newapi-reject-collector](https://github.com/yl0711-coder/newapi-reject-collector) 推送的「无可用渠道」等前置拒绝(这类拒绝不进 new-api `logs` 表,是 logs 维度监控的盲区)。
  - 新增接收接口 `POST /internal/rejections`(token 鉴权,`MONITOR_INGEST_TOKEN` 配置;留空则关闭)。
  - 新增 `rejection_samples` 表(按 节点×分钟桶×原因×模型×分组 累加,重复推送幂等)+ 随留存定期清理。
  - 内部监控新增「被拒请求」面板(按 模型×分组 计数,Top 100;无数据则隐藏)。

## [1.1.5] - 2026-06-10

### Changed
- 看板:GLM 模型识别为智谱(Zhipu)徽章(原归为"其他")。

### Removed
- 清理去横幅后遗留的死代码(JS 中 ICON 图标常量 + banner_* 文案,均已无引用)。

## [1.1.4] - 2026-06-10

### Changed
- 看板副标题文案改为「服务可用性看板」(原"上游模型实时可用性")。

## [1.1.3] - 2026-06-10

### Changed
- 看板**去掉顶部整体状态横幅**:不向客户广播"部分服务降级"这类总览;各模型明细仍在卡片如实呈现。
- 头部 logo 改用**主站同步的 logo 图**(与 favicon 一致),无 logo 时回退站点名首字母。

## [1.1.2] - 2026-06-10

### Changed
- 对外看板**站点名与 favicon 改为部署时从主站 new-api 同步**(`system_name` + `logo`),不再硬编码任何品牌名;主站不可达时用 `MONITOR_SITE_NAME` 兜底,再空则显通用名。保持开源通用——各部署自动显示各自的站点名/图标。

## [1.1.1] - 2026-06-10

### Changed
- 看板状态判定改为**反映「当下」**:状态由「近期窗口(24h)流量 + 渠道健康」决定,不再被一周前的旧数据钉死。
  - 有健康渠道但近期无流量 → 正常(不拿陈旧数据判死);可用率/延迟在数据陈旧(最新 >48h)时显「—」不展示。
- 档位阈值调整:**正常 ≥99% · 性能下降 85–99% · 不可用 <85%**(原 <95% 一刀切过狠,会把"严重降级"误判为"不可用")。

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

[Unreleased]: https://github.com/yl0711-coder/newapi-monitor/compare/v1.3.2...HEAD
[1.3.2]: https://github.com/yl0711-coder/newapi-monitor/compare/v1.3.1...v1.3.2
[1.3.1]: https://github.com/yl0711-coder/newapi-monitor/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/yl0711-coder/newapi-monitor/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/yl0711-coder/newapi-monitor/compare/v1.1.5...v1.2.0
[1.1.5]: https://github.com/yl0711-coder/newapi-monitor/compare/v1.1.4...v1.1.5
[1.1.4]: https://github.com/yl0711-coder/newapi-monitor/compare/v1.1.3...v1.1.4
[1.1.3]: https://github.com/yl0711-coder/newapi-monitor/compare/v1.1.2...v1.1.3
[1.1.2]: https://github.com/yl0711-coder/newapi-monitor/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/yl0711-coder/newapi-monitor/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/yl0711-coder/newapi-monitor/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/yl0711-coder/newapi-monitor/releases/tag/v1.0.0
