# 2026-08-17 Usage Full History / Stability v5 生产验证清单

> 状态：本地代码/配置门禁已通过，生产外部门禁待执行。勾选项只代表
> 2026-08-17 已留有证据的本地工作，不是已完成生产签字。

## 1. 冻结候选制品

- [x] 冻结 Go 文件并完成 fresh 快照 ordinary/race/vet/lint/gofmt/diff-check；
  Go 清单 SHA-256 为 `bfe8bf7fd24d18a80cc006c3f991858e507015f4ac32d804eead0ad31200eb23`。
- [x] 本地候选镜像完成 UID1000、只读根、独立卷、zero-network、live/ready
  和优雅退出冒烟。
- [x] 实际生产 Compose 的 Phase A/maintenance、Phase B/live 通过 config-only
  预检；两份 Caddyfile 通过固定 digest 的 `caddy validate`。
- [ ] 提交当前工作树，生成 registry 不可变 RepoDigest/SBOM，记录旧/新 digest。
- [ ] 在目标机对真实 `.env.cluster` 运行完整 preflight，归档 commit、
  migration plan `v11`、`SOURCE_EPOCH`、Compose/Caddy/preflight/config hash。

## 2. 技术负责人（承担 DBA 职责）来源完整性签收

- [ ] 以只读 SQL 有界核对 active 用户注册时间、可见首/末日志及依赖索引。
- [ ] 书面核对分区删除、保留周期、归档/冷迁移和历史事故；不得只用 `MIN(created_at)` 证明无缺口。
- [ ] 只有 hot `logs` 自每个 active 用户注册起无缺口时，才签 `SOURCE_MODE=complete`。
- [ ] 定义 epoch 变更规则：归档、路由、可见性或查询语义变化必须换 epoch 并全域重签。

## 3. 迁移前灾备门禁

- [x] runtime backup-set 恢复工具已实现并测试 `IN_PROGRESS → READY → ACTIVATED`、
  并发所有权、崩溃点、路径旁路、哈希/`quick_check`、历史 plan 和换钥。
- [x] 恢复测试以真实 runbook 环境变量启动 Local Snapshot，验证
  `/live`、`/ready`、独立 backup 目标、旧上游密文换钥和 zero-source worker。
- [ ] 用真实生产大库产生 main+facts 成对 manifest，验证哈希、行数和两库
  `quick_check`。
- [ ] 加密复制到另一故障域，启用不可变保留。
- [ ] 恢复到全新生产同级卷，以 Full History + Local Snapshot Read Only、隔离
  network/无 source worker 启动。
- [ ] 验证 `/live`、`/ready`、published revision/fingerprint/generation、Portal ACL/聚合与重启一致性；来源连接数为零。
- [ ] Phase A 生成 v11 迁移前快照后，在客户仍处于 maintenance 时用**真实旧不可变
  digest**恢复到另一全新卷，跑完 `restored → 旧镜像 /health/ACL/数据探针 → cutover`
  回滚预检；演练结束后优雅停止旧镜像，再回到保留的迁移后原卷进入 Phase B。
- [ ] 控制面具备不高于 5 分钟的卷快照 RPO；每日应用备份不能替代该项。

## 4. Usage 分阶段 pilot

- [x] 生产 Compose/preflight 已固定来源全局租约、查询启动间隔至少 2 秒、
  cold duty 不高于 20%、history delay 30 秒、启动补缺 1 小时。
- [ ] 在真实目标环境证明来源实际全局并发 1、spacing/duty 不突破，
  且旧 Monitor/本机验收容器已退出并释放租约。
- [ ] 实际 Caddy maintenance/live 两份配置、固定代理 IP、TLS、X-Forwarded-For
  登录限流、collector `/internal/*` 连续性和 Portal/管理员权限均在目标机实测。
- [ ] 渠道余额、渠道使用同步和基础设施快照通过 cutover readiness；不能只用
  `enabled=true` 代替数据新鲜度与完整账户集合签收。
- [ ] 2 小时小流量 pilot：查询形状、超时/降档/退避、Tail 优先、页面零回源。
- [ ] 24 小时混合负载：日切、备份、WAL、重启、来源短断、repair/audit 恢复。
- [ ] 上线后 7 天连续观察：每日记录 rows/day、SQLite bytes/row、WAL 峰值、backup/VACUUM 时长和 signed member-days/s；该项不得在第 1 天冒充已完成。
- [ ] 软线触发即停止候选并保留卷/游标（当前没有独立cold pause）：DB CPU ≥60%连续两个5分钟或+10pp、连接≥70%、DiskQueue≥5、复制延迟≥2s连续三周期、重复3024/锁等待、NewAPI p95同时+10%且≥50ms；稳定15分钟后才半开。
- [ ] 硬线触发即回滚：CPU≥75%、连接≥85%、p95+25%、5xx+0.5pp、数据/ACL/hash错误、磁盘≥85%或绝对余量破线、OOM/restart、来源并发>1或spacing<2s。

## 5. 独立正确性对账

- [ ] 覆盖 no-history、注册到首日志空前缀、普通活跃、单日 >20k 重用户、remove/rejoin、跨夜和边界变化成员。
- [ ] 独立只读 detail/day-control 与 SQLite daily/hour/proof 一致。
- [ ] 空前缀/no-history 范围本地事实为零；重用户降小时时不拖住健康成员。
- [ ] 断源时已发布成员继续读 SQLite，partial/repair 成员 fail-closed；重启后游标、世代和权限不倒退。
- [ ] Portal、stats、matrix、followups 和 CSV 不读未发布水位，不直接扫生产 `logs`。

## 6. 容量签字

- [ ] 数据卷按 `live DB + WAL 峰值 + tmp/CSV reserve + 至少 20% 余量` 反推。
- [ ] 备份卷按 7 组 runtime 双库 + 3 组 migration 双库 + 1 组临时双库、manifest 和余量反推；若降低保留数须显式签字。
- [ ] `32 GiB` 仅作 pilot 起点，不作未实测的生产容量结论。

## 7. Stability v5 独立窗口

- [ ] 明日 Usage 受控首发不启动 Stability 大迁移；其完成不是 Usage 首发的虚假前置条件。
- [ ] Usage 扩窗完成后才开 Stability 迁移，两者不并行加压。
- [ ] live problem Tail 在 cold migration 期间持续推进；断源 20 分钟、重启、高峰多页不跳分钟。
- [ ] hourly migration 和 raw problem migration 两域都 `complete` 后才允许关迁移开关。
- [ ] 未完成却关开关时 health 必须 degraded，不能隐藏缺口。

## 最终 GO 条件

明日 Usage 受控首发的 GO 条件是：技术负责人来源签收、卷外成套备份
与新卷恢复、2h/24h pilot、独立对账、容量/止损签字和固定 digest 验收
缺一不可。7 天是上线后强制观察，不要求明日前等完；Stability 双域大迁移
转为独立维护窗口。未满足客户切流条件时，只能发布影子候选并继续
收集证据；不得把本地绿色、空卷冒烟或未跑满的 pilot 写成生产已通过。
