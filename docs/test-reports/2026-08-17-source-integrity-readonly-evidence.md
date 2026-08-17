# 2026-08-17 全历史来源完整性只读证据

状态：**技术负责人已确认来源历史契约；生产调度执行待候选验收**

采集时间：2026-08-17（Asia/Shanghai）

执行方式：本机经受控 SSH 隧道，使用生产 `nexus_ro`，单连接、仅 SELECT；共 18 次
查询，相邻来源查询启动至少间隔 2,000 ms。检查结束后验收容器已优雅停止，SSH
control socket 与本地 13316 监听均已关闭。

## 已取得的数据库证据

| 项目 | 结果 |
|---|---:|
| 目标数据库 | `nexusapi` |
| 账号 | `nexus_ro@%` |
| SELECT 权限 | 是 |
| 可见写权限 | 否 |
| `idx_user_created_type` 列顺序 | `user_id, created_at, type` |
| 边界查询 EXPLAIN 使用预期索引 | 是 |
| `logs` 使用 MySQL table partition | 否 |
| 当前 users 数 | 144 |
| 最早 `users.created_at` | 1778485853 |
| 最新 `users.created_at` | 1786931771 |
| 抽样边界用户数 | 10 |
| 抽样中存在可见日志的用户 | 8 |
| 抽样最大“注册→首条可见日志”间隔 | 330 小时 |

本报告不保存 DSN、密码、主机地址、用户明细或日志内容。

## 证据边界

以上结果证明本次检查时：只读账号、目标库、关键索引、EXPLAIN 和抽样边界符合
全历史 worker 的最低查询契约；`logs` 不是按 MySQL partition 隐藏旧分区。

它**不能仅靠 SQL 自证**过去从未执行过 DELETE、归档、冷热迁移或视图过滤；也不能
用 10 个用户抽样替代业务侧的数据保留制度。因此仍须由技术负责人签署下方声明。
如果声明不能成立，`SOURCE_MODE` 不得填写 `complete`，也不得把当前来源 epoch 用于
全历史首发。

## 技术负责人签署项

- [x] 我确认所有 active 用户从注册日起的 `logs` 在当前 `nexus_ro` 可见范围内完整
  保留，没有历史归档、删除、冷热分层、租户路由或权限过滤造成的缺口。
- [x] 我确认未来任何归档、清理、路由、查询语义或可见性变化，都会先部署覆盖新
  来源的 adapter/manifest，再更换 `MONITOR_USAGE_FACTS_HISTORY_SOURCE_EPOCH` 并全域
  重签；不会先删 hot logs。
- [ ] 我确认 `nexus_ro` 仅用于只读来源查询，生产发布时保持全局单 worker、查询启动
  间隔至少 2 秒、cold duty 不高于 20%，并按 pilot 停止线值班。

签署人：技术负责人（本次会话用户确认）

签署时间：2026-08-17（Asia/Shanghai）

签署的 source epoch：`newapi-hotlogs-complete-20260817-v1`

第三项仍保留为生产运行门禁：代码与 Compose 已配置租约、spacing 和 duty，
但只有最终候选在目标环境的 2h/24h 证据才能证明实际执行没有突破这些限制。
