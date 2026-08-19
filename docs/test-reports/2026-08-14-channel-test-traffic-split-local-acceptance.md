# 渠道内部测试流量拆分：本机真实数据验收

日期：2026-08-14（Asia/Shanghai）

## 结论

本机候选 Monitor 通过 `nexus_ro` 和回环 SSH 隧道只读生产 MySQL，服务运行在 `127.0.0.1:8100/8101`。早期 v3 候选已完成近 7 天 168/168 小时真实数据重分类；当前 v4 候选彻底移除了对 NewAPI 新协议的依赖，只使用未修改 NewAPI 已有日志字段。可识别的渠道测试请求、Token 和 quota 成本独立保存在本机 SQLite，不进入 Monitor 用户统计。NewAPI 仓库保持干净，本次不构建、不部署 NewAPI 镜像，也没有写入线上服务/数据库。

## 分类口径

- 成功测试必须同时命中 `type=2`、`token_name=模型测试`、`content=模型测试`。
- 失败测试必须同时命中现有 NewAPI 的 root 合成请求特征：`type=5`、`user_id=1`、无 token、无 request_id。
- 不以 `internal` 分组本身作为测试条件，避免把该分组中的真实用户流量误排除。现有日志不能证明手动/定时或单渠道/全渠道，来源/范围只标记为 `legacy`。
- 普通测试成本使用 `legacy_assumed_base`；旧日志已有 `billing_mode=tiered_expr` 时使用 `legacy_after_group`，先除网站倍率再乘上游倍率。
- 用户流量写 `stability_hour_samples`，内部测试成本写 `channel_test_hour_samples`；小时台账分别保存两类控制总数。

## 真实数据结果

- `/channels/report?hours=168`：覆盖率 100%，168/168 小时，缺口 0；接口本机响应日志约 13 ms。
- 截图对应渠道 #37/#38/#43/#47 均只剩真实用户分组；全报表 `observed_internal` 用户请求为 0。
- 独立测试成本表：近 7 天 9,157 次测试、13,833,349 Tokens、quota 成本基数 18,725,716；测试表合计与小时控制台账完全一致。
- 真实故障注入样本：渠道 #38 在 2026-08-13 21:19 的定时测试收到 429。旧口径曾把它显示成 1 次 `internal` 用户失败；重分类后用户表为 0，测试成本表该小时为 6 次（5 次成功 + 1 次失败）。
- 历史补数期间正常 60 秒采样持续运行，`/data?window=60` 每分钟成功；1 小时渠道报表保持 100% 覆盖并继续更新。

## 本地自动化验收

- Monitor：`go test ./... -count=1` 全通过。
- Monitor 分类、渠道管理、独立成本与上游燃烧专项测试通过。
- Monitor `git diff --check` 通过；NewAPI `git status --short` 为空，没有需要发布的 NewAPI 制品。

## 上线迁移

不删除、不重建 Monitor SQLite。新镜像启动时原地追加列和 `channel_test_hour_samples`；旧分类版本 fail-closed。保持实时采样运行，在维护窗口调用 `POST /admin/stability/backfill?days=N` 重分类所需历史范围，并通过 `GET /admin/stability/backfill` 验证完成率。发布对象只有 Monitor，不更换 NewAPI 镜像。

## 最终候选补验

- 早期只读基线镜像 `sha256:98948e99438dd5158d52dedb0e58859436b927a36b1d949348946580ab650954` 曾验证旧格式模型测试 `215` 次与用户稳定性桶未混桶；该镜像已被 v4 候选取代，不是可发布制品。
- v4 独立表保存 `scope/cost_basis/success/anomaly/failed/traffic_class_version`；`origin` 只用 `legacy_base/legacy_tiered` 作为旧复合主键的成本分桶，不声称它是手动/定时来源。
- Monitor 用户收入汇总的排除谓词对旧库 NULL 使用 `COALESCE`，避免 SQL 三值逻辑误排除普通用户日志；不依赖任何结构化测试新协议。
- 只要现有 NewAPI 写出上述稳定旧标记，测试就不会重新算成 Monitor 用户收入；无法区分的来源维度保持 `legacy`。
- 最新本机 Monitor 镜像为 `sha256:99d063ea19799e008ae24e620aea34c21c445bca8067470a06b96ef36320e9cf`：双库迁移快照复核成功、restart=0、OOM=false。v4 真实闭合小时共分出 1,815 次用户请求和 209 次内部测试（测试 quota 427,210），小时控制台账与两张本地事实表各自一致。
- v4 分类升级对旧稳定性小时 fail-closed；当前容器已有 1 个 v4 完整小时，7 天历史须在维护窗口运行受控稳定性回填后才能重新签收，不应把旧 v3 行直接冒充 v4。
