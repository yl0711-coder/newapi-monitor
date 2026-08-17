# 2026-08-17 Usage 全历史同步完整性与一致性终审

## 结论

当前冻结 Go 树的同步设计满足本次代码层完整性/一致性要求：来源读取采用
可重放的持久游标，小时事实与证明在同一 SQLite 事务提交，完整自然日经独立
control 校验后才原子替换 daily/proof，发布面又以成员 revision、来源 epoch、
分类版本、查询语义、成员 floor、左右水位、指纹和 `ServingGeneration` 二次隔离。
崩溃、重试、迟到日志、单成员坏日、并发 repair/publish 或缓存中的旧结果均不能
把未签事实当成已发布事实。

这里的“一致”不是声称 MySQL 与 SQLite 存在跨库分布式事务。实现采用的是
**至少一次读取 + 幂等本地提交 + 独立 control/hash 校验 + 发布签名**：来源在两次
查询之间发生变化时，本轮不发布或进入小时级复核；后续 Tail/rolling audit 会重新
读取并收敛。这个模型在只读来源约束下比伪造 exactly-once 更可验证。

## 不变量与实现证据

| 不变量 | 实现与失败语义 | 回归证据 |
|---|---|---|
| 一个成员/小时只有完整提交或完全不提交 | `claimUsageFactHour` 领取短租约；`syncUsageFactHourWithOptions` 在 `usageFactsSyncMu` 与单个 SQLite 事务内替换 facts、重读核 hash/metrics、写 complete proof 与成员水位。revision/lease/manifest 变化使事务失败回滚。 | `TestUsageFactHourSyncCorrectsLateLogsAndIsIdempotent`、`TestUsageFactHourStateCannotHideMissingLocalRows`、`TestUsageFactHourLeaseIsShortAndExpiredClaimIsRecoverable` |
| 小时 staging 不能与旧 daily 混读 | 读 CTE 对完整旧日优先 daily；当前尾日严格读取 `hour_ts < publishedThrough`；有 daily/proof 的成员日不混入小时 staging。 | `TestUsageFactsLastGoodDailySuppressesPartialHourly`、`TestUsageFactsReadKeepsMemberDayAtomicDuringRepair`、`TestUsageFactsReadIsBoundedByPublishedHourAndPerMemberFloor` |
| 自然日只有 24 小时与独立来源 control 一致才签收 | 日终先验证 24 个当前 epoch 小时 proof，再重建 daily，逐成员对比独立 control；daily、严格 day proof、generation 在同一事务提交。失败保留最后正确日版本。 | `TestUsageFactHistoryMidnightIsolatesLocalBadMemberWithOneControlQuery`、`TestUsageFactDailyRebuildPreservesLastGoodVersionWhenHoursAreIncomplete`、`TestCommitUsageFactHistoryRangeRollsBackDeleteOnInsertFailure` |
| 普通 Tail 跨午夜不能越过成员验证水位 | publisher 跨日同时要求 `PublishedMember.VerifiedThroughHour >= dayStart(target)` 和完整尾小时 proof；history Tail 可消费高优先 Tail 已写的 current-epoch proof，不重复查来源，随后持久推进 job/member 水位。 | `TestUsageFactHistoryTailConsumesCurrentEpochHourWithoutSourceQuery`、`TestFullHistorySnapshotReadinessUsesDurableCheckpointWithoutWorkerOrWrites` |
| 首次 v5 发布不能先展示部分员工 | 无既有完整发布基线时，只有 `allTrackedReady` 才建立第一份 publication；已有签名基线后，新成员未完成时保持 admin-only，不缩小组织既有全集。 | `publishUsageFactFullHistorySnapshot` 的 clean-install/all-active 原子切换；`TestPortalPartialMemberIsHiddenFromOverviewDetailLogsAndExport`、`TestPortalOverviewShowsPublishedOverlapWhileBackfillContinues` |
| 每个成员只可读取自己的已证明历史 | published member 行携带 `SourceFloorHour`；daily/hour CTE 都按成员 floor 过滤，同时严格受全局 `PublishedThrough` 限制。旧迁移残留的 floor 前事实不可见。 | `TestUsageFactsReadIsBoundedByPublishedHourAndPerMemberFloor`（stats/matrix/token） |
| repair 期间坏成员撤权、好成员继续服务 | enqueue 与 publisher 共用写串行锁；repair job/幂等 request、删除坏成员 publication、重算指纹/左右界、generation 递增在一个事务。已证明 mismatch 先立即关内存闸，再持久化 repair-hold intent；durable 撤权后重验并只恢复无关成员。 | `TestUsageFactHistoryManualDayRepairIsRootOnlyClosedAndDurablyIdempotent`、`TestNoHistoryInvalidationPublishesRemainingMemberBounds`、`TestProvenMismatchReopensUnaffectedPublishedMembers`、独立 QA repair/publisher barrier 与长锁持久 hold 用例 |
| 来源慢/超时不是“已证明错数” | 日级 source audit 的 20k/3024/控制竞态降为非撤权小时核验；SQLite `First` 的 busy/I/O 错误仅重试，不制造 mismatch。只有确切内容/control 不符才撤权 repair。 | `TestUsageFactSourceAuditWorkloadFallbackKeepsSignedMemberVisible`、`TestUsageFactSourceAuditHourlyVerificationAtomicallyRepairsAndReturnsToCycle` |
| 一个重用户不能饿死同批健康成员 | adaptive audit 有固定查询预算；共享查询失败后隔离具体成员，其余以无损 sentinel 释放，下一 durable claim 旋转推进。lease busy/cancel/source gate 不消耗成员 attempts。 | `TestUsageFactSourceAuditAdaptiveBudgetIsolatesWithoutPenalizingUnattemptedMembers`、`TestUsageFactHistoryLeaseBusyNeverConsumesDurableAttempts` |
| 来源边界收缩/消失必须撤销旧授权 | discovery 保留历史 floor 单调性；已签成员边界 unknown、最早日志后移或末日志前移时，在同一事务暂停 job/state、撤其 publication、重算全局范围并切 ServingGeneration。 | discovery 状态机静态复核；`TestNoHistoryInvalidationPublishesRemainingMemberBounds` 覆盖 no-history 首日志变化 |
| 重启不能相信仅有“complete”状态的损坏事实 | startup checkpoint 校 current member control、revision/epoch/classification/query semantics、repair holds、published fingerprint/range；对当前未闭合日额外重算 trailing hour 内容/hash。 | `TestFullHistorySnapshotReadinessUsesDurableCheckpointWithoutWorkerOrWrites`、`TestUsageFactSemanticAuditDetectsDeletedPublishedDailyRowAndFailsClosed` |
| 旧缓存/在途响应不能跨发布世代提交 | 本地 serving snapshot 要求 SQLite `ServingGeneration`、指纹和左右界与 atomics 完全相同；管理员与 Portal 聚合在计算前后比较快照，handoff/ABA 时丢弃或 503。 | `TestUsageAggregateGuardRejectsServingGenerationHandoff`、`TestStaleReadinessRefreshCannotOverwriteNewerPublicationBounds`、`TestPortalAggregateGuardRejectsOldResultAcrossServingGenerationABA` |
| prune 不能破坏可恢复游标 | full-history daily 与 day proof 永久保留；小时 staging 仅在存在当前 epoch/版本严格日 proof 且不落入活跃 repair-hour/audit-hour/Tail 范围时删除。 | `TestUsageFactPruneKeepsActiveHourlyAuditStagingUntilFinalized`、`TestUsageFactsBackfillSkipsPrunedHoursWithVerifiedDayProof`、`TestUsageFactsPruneKeepsHistoricalWatermarks` |
| 语义/来源版本升级不能复用旧授权 | `COALESCE(source_epoch,'')` 取消旧 epoch 活跃任务；classification/query-semantics 变化重置成员签名并取消 ancillary audit/repair 租约；cancelled audit 会显式重建。启动的大表存在性检查使用 `SELECT EXISTS`，不做 `COUNT(*)` 全表扫描。 | `TestUsageFactSemanticResetCancelsAncillaryMaintenanceJobs`、`TestUsageFactsTrafficClassificationV5RequiresExplicitMaintenanceAndPreservesDerivedRows` |

## 本次复跑

- 当前 Go 树与独立 QA 最终冻结快照
  `/private/tmp/newapi-monitor-qa-freeze-final.O7Y4QV/repo`：全部 Go 文件
  `rsync --checksum` 0 差异。
- 25 个核心同步/发布/repair/read-fence 用例：ordinary `-count=20` 通过，
  总耗时 14.306s。
- 同一组：race `-count=10` 通过，总耗时 79.392s。
- 最终 staged Go 内容的全仓 `go test ./... -count=1` 通过（Monitor 20.358s）；
  `go test -race ./... -count=1` 通过（Monitor 138.878s）。
- `go vet ./...`、独立缓存 `golangci-lint run ./...`、gofmt 和 diff-check
  均通过；lint 0 告警。
- 实际部署仓四个 config-only stage（phase-a/pilot/cutover/rollback）通过；
  phase/stage 错配按预期拒绝。maintenance/live 两份 Caddyfile 使用固定
  Caddy digest、`network=none` 验证均为 `Valid configuration`。

## 仍需候选环境证明的边界

1. 技术负责人已经确认 hot `logs` 自 active 用户注册日起完整，并定义
   source epoch `newapi-hotlogs-complete-20260817-v1`；候选仍必须证明实际使用
   该 epoch、只读账号、单 worker、全局 spacing 与 duty。
2. 假来源/SQLite 测试能证明状态机与故障语义，不能替代最终 MySQL 数据的独立
   detail/control 对账。2h pilot 必须覆盖 no-history、普通成员、重用户、跨夜、
   remove/rejoin；24h pilot 必须覆盖日切、backup、短断与恢复。
3. 客户切流必须等待全体 tracked revision 已发布、`facts.read_active=true`、
   左右水位完整且独立对账通过。候选镜像构建成功只允许进入影子/maintenance，
   不自动构成生产 GO。
