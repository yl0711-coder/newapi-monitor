# Monitor 自动恢复：本机复查与 Linux 故障测试

日期：2026-11-25。分支：`fix/resource-recovery-20261125`。
基线：`ed05c0cc8088b73b22b7cd7313ea9e689d8a8c14` 加本分支未提交修改；不是已发布候选。

## 本轮代码复查

额外发现并修复：`retryStoreBackupRetention` 原先使用阻塞式互斥锁，取消/超时检查在锁之后，可能排在慢快照后无限延长等待。现在先检查取消，再用 TryLock 让出；不堆积清理任务、不占发布屏障，不把已验证备份当失败。新增 `TestBackupRetentionRecoveryYieldsToHeldLockAndCancellation`，Linux 下通过。

另逐项复核了：核验与提交仍由原锁/事务串行；资源等待不移动数据水位；旧暂停只在窄条件下重排；多天补齐不借当天 live 证明越过历史日；归档全量预检后仅追加精确匹配的派生文件尾部；未知/冲突/链接拒绝；已完整但可能未 fsync 的副本也重新刷盘再释放热资料。未改金额算法、权限、生产容量阈值或 source 连接。

## 测试环境和身份

- 本机已有 Go 1.26.11 工具链，`GOTOOLCHAIN=local GOPROXY=off GOOS=linux GOARCH=arm64 CGO_ENABLED=0` 交叉编译测试二进制；没有下载模块/镜像。
- 复用 `newapi-monitor:release-candidate-local-20261110c` 运行 monitor 测试；这是旧运行镜像承载新测试二进制，不是正式候选应用镜像。
- 月单组复用 `golang-node-ci:1.26.10` 的 Node/运行依赖，执行的仍是上面 Go 1.26.11 编译的静态测试二进制；没有使用其中的旧 Go 编译器。
- 容器禁网、只读根目录、UID/GID 1000、内存 768MiB、CPU 2；日志轮转 2MiB × 2；显式覆盖镜像的 data/backup/evidence 卷，避免匿名卷。
- 临时目录 128MiB；故障目标单独 `/fault`，16MiB tmpfs。测试只允许该固定目录，且核对文件系统总容量在 4–32MiB 内；不填满宿主机磁盘。
- WAL/归档另用本任务专用磁盘卷 `monitor-recovery-20261125-wal-01`，仅合成数据，和 tmpfs 结论分开。

二进制 SHA-256：

```text
monitor.test      08207175ff53f3dc367ea59841d5127a3decc5aebc0c52e292bd7dd87809fe4f
monthlydraft.test b0e7e4cd62258d0576f8870f47a1245ff55309c4383beed08ed26366c3be208f
```

路径：`.run/resource-recovery-linux-20261125/`，已忽略，不提交二进制。

## Linux 结果

| 故障/安全条件 | 实际执行及断言 | 结果 |
|---|---|---|
| 文件系统 ENOSPC | 写满专用 16MiB 文件系统，确认容量闸门关闭；有界耗尽 SQLite 预留页后真实写入失败；释放 filler 后同一连接可继续提交，数量等于先前成功提交数＋恢复后一笔 | 通过 |
| SQLite FULL | 页限制触发真实 SQLITE_FULL，增大测试限额后恢复；失败写入不残留 | 通过 |
| 数据库锁竞争 | `TestStabilityLocalRetryRecoversRealSQLiteLockUpgrade`，真实 SQLite 锁升级失败后有界重试恢复 | 通过 |
| 多天积压 | 原小时断点 Tail→Backfill→Verify，金额 5000 quota 不丢不重，发布水位自动达到目标 | 通过 |
| Nginx 证据库满 | 原窗口重试，覆盖不虚增，重复重放不新增重复数据 | 通过 |
| 清理排队与取消 | 持有备份锁时清理让出；已取消任务不等锁 | 通过 |
| SQLite WAL 进程崩溃 | actor 在 FULL/WAL 模式先提交一笔、再打开未提交事务并以退出码 23 结束；第二个容器核对仅提交数据存在、integrity_check=ok，再写入成功 | 通过；23 是预期崩溃，不是 actor 的成功测试退出码 |
| 月单持久盘故障组合 | `^Test(PublishedArchive|CapacityMaintenance|PreparationInbox|MonthlyResource)`；四个文件部分 ENOSPC 后续传、重放、归档释放/还原、撤回、完整性和链接反例均在磁盘卷执行 | 补齐测试依赖后整组通过 |
| 不可误恢复反例 | 错 epoch、缺证明、坏摘要、空白区间矛盾、别的 owner、游标冲突、人工暂停、权限/I/O 错误、真正来源失败上限继续拦截；本地故障不消耗正常失败预算 | 通过 |

WAL actor 选择：`^TestLocalResourcePersistentCrashWriter$`，仅在 `MONITOR_TEST_PERSISTENCE_PHASE=crash-write` 且目标为 `/data/recovery` 时运行；已有文件拒绝覆盖。第二个容器使用 `verify` 阶段运行 `^TestLocalResourcePersistentCrashVerify$`。普通本机/CI 无环境开关会跳过这两个破坏性 actor，不能用常规包测试冒充该演练已经跑过。

## 失败复盘

1. 首次容器未启动：local 日志驱动不接受压缩与 max-file=1 的组合。改为保留 2 份轮转日志；没有修改生产代码。
2. 首次文件系统满盘用例误认为第一笔 SQLite 写入必须失败。SQLite 可复用 freelist/预分配 WAL；改成有界耗尽该余量，记录已成功提交笔数，再断言后续失败与恢复无重复。容量闸门在整个满盘时段始终正确关闭。
3. 月单 Linux 组首次因缺少 `../../monitor/portal_monthly_generation_local.js` 失败；真实前端脚本用例还需要 Node。增加一份只读源码挂载，复用已有 Node 镜像，整组重跑通过。没有跳过或删弱该用例。

## 其他门禁及范围限制

- Go 1.26.11：`go vet` 涉及的 monitor/monthlydraft/localresource 三包通过；复杂度 ratchet、`git diff --check` 通过。没有放宽基线。
- 此前扩大 native 回归记录见 `docs/bugfix-resource-recovery-20261125.md`；月单 580 项全包曾总超时，不能称为完整通过。这里运行的是明确列出的受影响组，而非全部 3900+ Monitor 测试。
- 本轮是 Linux arm64 测试二进制的隔离诊断。未完成正式不可变候选服务镜像、目标 amd64 发布环境、全量 CI/race、完整应用切换及回退验收；没有上线。
- tmpfs ENOSPC 只证明 Linux 容量错误/重试语义；磁盘卷上的 WAL/归档测试证明这些具体持久化路径。均不等于物理断电、真实块设备慢盘、磁盘硬件损坏或所有生产故障已获保证。

## 资源保留

测试容器均 `--rm`，未触碰其他会话容器。合成数据卷已在报告保存、核对任务标签并确认无容器引用后删除，未使用 force；可按本报告和测试源代码重建，不保留合成数据库副本。未清镜像、共享缓存或用户数据。测试二进制暂保留用于后续候选对照。清理前后 Docker 卷数 84→83，逻辑总占用四舍五入均为 16.06GB；不能换算为宿主 SSD 物理写入或释放量。

## 续验：错误优先级和竞态（同日，未提交）

### 新发现及修复

对 `monthlyResourceWait` 补充复合错误反例，先复现了 `errors.Join(ErrGenerationCapacity, syscall.ENOSPC)` 被误分为可重试的问题。这会让逻辑容量限制变成持续重试，而不是人工复核；不代表已经放行了错误账单。

修复后，逻辑容量限制与完整性、选择冲突、版本变化一样，优先于可重试资源错误。新增分类断言和真实 preparation inbox 调度反例：第一次暂停后，即使推进 24 小时也不再次调用接入；没有放宽生成容量或修改金额规则。

共享分类新增真实 SQLite 写锁测试：另一连接占用写锁时识别为临时失败，释放后原连接可继续写入；真实主键约束、query_only 只读错误、EIO、权限和类似错误文本不归为临时容量。新增库只有小型合成数据。

### 已完成结果

统一用本机缓存 Go 1.26.11，`GOTOOLCHAIN=local GOPROXY=off`。race 沿用项目对两个第三方 SQLite 库的 checkptr 参数，未禁用任何库的 race；编译并发 2，race 的 GOMAXPROCS=2。

| 执行范围 | 结果 |
|---|---|
| Monitor 扩大原生回归，`-shuffle=20261125` | 47.086 秒通过 |
| 月单恢复、归档、inbox、容量维护原生组合 | 13.555 秒通过；本次在 monthlytasks 的筛选无匹配，不算该包通过 |
| Monitor 恢复及锁竞争相关 race | 28.068 秒通过，无竞态报告 |
| 月单恢复、inbox、容量维护及受控归档 race | 45.961 秒通过，无竞态报告，包含新增实际调度反例 |
| localresource 全包 | 原生 0.484 秒，race 1.355 秒通过 |
| monthlytasks 全包 | 原生 2.954 秒，race 4.604 秒通过 |

Monitor 原生范围：`^Test(UsageFact|UsageVerify|RecentServiceAudit|UsageHistoryResource|LocalResourceRecovery|IncidentLocal|Stability|BackupRetention|StoreBackup|UpstreamFailurePolicy|NginxEvidenceFull)`。

Monitor race 范围：`^Test(UsageVerifyLiveTail|UsageFactMultiDay|RecentServiceAudit|UsageHistoryResource|LocalResourceRecovery|IncidentLocalResource|StabilityBackfillResource|StabilityResourceWait|StabilityProblemMigrationLocalResource|BackupRetention|StabilityLocalRetry)`。

月单 race 范围：`^Test(MonthlyResource|PreparationInbox|CapacityMaintenance|PublishedArchiveManaged)`。

新增本机 stage CLI 二进制仅用于完整包的显式 CLI 合成测试，保留在本任务 `.run` 目录；没有把运行目标指向线上。本节分类收紧之后，前文旧 Linux 月单二进制不代表最新月单候选，正式镜像仍须重新验收。Monitor 原有 Linux 诊断代码未发生业务改动。

### 第二处复现：本地用量核验超时

继续追查本地慢盘/查询超时发现：local-only Verify 在本地小时证明读取返回 `context.DeadlineExceeded` 时，原实现会把任务置 paused、Attempts 从 1 增至 2。新增 GORM 本地查询故障用例实际复现了这一路径。

仅在该纯本地核验入口将 deadline 归为资源等待，不扩大共享 localresource 的含义，不修改来源查询的超时策略。复测注入一次本地读取超时，断言 queued、Attempts=1、小时游标不动、退避生效；移除故障后同一任务重新严格核验，正常发布，5000 quota 可读且无重复。坏摘要反例仍暂停。额外检查上游 backfill 的真实 deadline 仍将失败预算从 4 增至 5，取消、EIO、权限和成员控制错误不归为本地核验等待。

- 新补修后原生组合通过：2.511 秒。
- 新补修后恢复相关 Monitor race 通过：31.213 秒；比上一轮范围增加 `UsageLocalAudit`。无竞态报告。
- 因 Monitor 代码此后有新补修，前文 Linux 诊断二进制不能作为最终 Monitor 候选验收，仍待正式镜像重验。

### 全包与 CLI 验证的实际边界

用明确的 `MONTHLY_STAGE_TEST_BINARY` 运行月单完整原生包，整体 10 分钟预算耗尽，返回失败（600.550 秒）；摘要记录 276 个顶层通过、3 个跳过，没有记录到具体用例断言失败。**整体失败就是失败，不算全包通过。** 第一版流式摘要只保留了计数及用例失败尾部，未完整保存包级超时的活动用例堆栈，属于本轮诊断缺口，不据此断言没有阻塞。

编译后的完整原生清单共 584 项；源码无 `t.Parallel`，按原清单顺序安排续验，从第 261 项重叠开始（324 项，覆盖原超时边界），不整包重复构建/运行。续验增加包级失败诊断输出，单批仍限 10 分钟，结果另补。

3 个跳过项中，`TestHandoffExportCLIToReviewCommand` 因缺少第二个 CLI 未执行；已复用缓存补建 `cmd/monthlybillprepare`，显式提供 stage/review 两个本机二进制后，该交接至独立复核的合成全链路通过（1.761 秒）。其余分别需明确的真实留存输入及浏览器会话，不自行启用，也不冒称已验收。

静态复查：四包 go vet、函数长度 ratchet、格式及 diff 检查通过，未抬高复杂度基线。没有提交或发布。

### 月单续验收口

续验的 324 项于 572.552 秒结束，包级通过：311 顶层通过、13 显式跳过、0 失败。两个批次重叠的 19 项均通过。按固定的 584 项原生测试清单合并去重，先得到 568 通过、16 跳过；再将独立补跑通过的 CLI 交接测试替换其跳过结果，最终为 **569 项通过、15 项显式跳过**。这些是分批覆盖结果，不能改写为第一次 10 分钟整包命令成功。

续验包含两个强制原生最大规模用例：200 账号/6000 对完整月份生成，以及 32 条完整大负载的保留资料检查，原有正式时限和断言未放宽。未修改 CI 分配规则。

仍跳过的 15 项需明确的真实留存输入、原件清单、浏览器会话或独立合成导出目录；没有自行提供真实凭据或触发线上动作。它们不是失败，也不是已通过。本轮普通合成 CLI 接入、准备和导出交接的工具依赖已补足。

当前代码相较第五批旧 Linux 二进制已有两处补修，因此仍须以最终固定提交及正式 Linux 候选重验。全量仓库 race、目标 amd64 应用镜像、完整应用启停和回退未在本轮完成。未部署、未提交、未推送，测试进程均已退出，没有创建新 Docker 资源。

写入控制：复用依赖/编译缓存，合成测试按 t.TempDir 清理；流式输出保留计数和必要失败诊断，没有持久化整包大量正常日志；续验仅重叠边界的 19 项，未重新整包跑前 276 项。必要源码、失败复盘、测试报告和任务自己的 CLI/诊断二进制保留；二进制并非正式发布镜像，也不用于替代其验收。

## 第七批：本机组合故障矩阵补齐

用户要求先把本机可想到的情况尽量测清。本批只增补/加强测试和记录，没有继续修改生产逻辑，没有提交或上线。

### 当前源码的 Linux 诊断身份

仍复用前述两个已有 arm64 镜像、禁网、只读根目录、768MiB/2 CPU 限额；不拉新镜像、不构建镜像、不带线上凭据。Go 1.26.11 当前源码测试二进制：

```text
monitor-v2.test      521652fd603159c74fee7c36654a6b4b7320021c9a0a1b8650a107310ae796be
monthlydraft-v2.test 08e47bb326d8c6c22fe04972dfe5a12732cf574140a87a6ac3cccab86ed6c368
```

大小分别约 121MiB、63MiB，为文件逻辑大小，不是 SSD 物理写入量。必要磁盘路径用本任务 `monitor-recovery-20261125-wal-02` 卷；只有独立 `/fault` 使用 32MiB tmpfs 制造 ENOSPC。没有把磁盘卷持久性结论归到 tmpfs。

### 覆盖矩阵及结果

| 场景 | 本机实际验证与边界 | 结果 |
|---|---|---|
| 容量阈值与未知文件系统 | 既有分级/启动探针反例保留；本批三轮真实文件系统写满，少量释放 64KiB 仍阻断，充分释放后同一连接再次提交，逐轮核对之前及本轮所有提交笔数 | Linux 通过 |
| 恢复后再次满盘 | 同一 32MiB 故障文件系统、同一数据库和连接连续三轮；不清空之前成功提交的探针记录 | Linux 通过 |
| 连续本地超时 | 同一核验任务连续三次本地查询超时，Attempts 保持 1、原小时游标不动、有退避；解除故障后重新核准并发布 | 原生及 Linux 通过 |
| 数据文件关闭重开 | 三次超时后 Close 并用新 Monitor 对象重开同一库，原 queued 断点继续；来源换成空合成库，读取仍得到原 5000 quota，不能靠回扫空来源蒙混过关 | 原生及 Linux 通过；不是整个应用发布停启演练 |
| 过期租约与并发胜负 | 既有 `UsageFactRawPageConcurrentWinnerMakesLoserYieldAndResume`、小时租约过期、取消排队及已取消上下文不抢名额 | 当前 Linux 通过 |
| 跨日与缺失证明 | 两天积压正常分段补齐；当前日 live 等待；缺小时、错 epoch、坏摘要、矛盾空白、其他 owner/游标冲突仍阻断 | 当前 Linux 组合通过 |
| 真实 SQLite 资源错误 | FULL、真实读写锁升级，保留原有有界重试；上游超时仍扣原失败预算，不扩大共享 deadline 分类 | 当前 Linux 及前述分类/race 通过 |
| 备份成功、清理失败 | 坏旧 manifest 不抹掉新备份成功、不重复快照；取消/持锁时清理让出；保留、成套恢复相关组合 | 当前 Linux 磁盘卷通过 |
| 同一归档连续四次中断 | plan/preparation/artifact/manifest.pending 依次写一半 ENOSPC，每次关闭并重建维护 owner；最后同目录续跑，只有全部校验后释放热资料 | 原生及 Linux 磁盘卷通过 |
| 取消及归档后的客户体验 | 已取消复制不写文件；连续中断恢复后客户页面、下载金额及原发布记录不变，未新增归档目录 | 原生及 Linux 磁盘卷通过 |
| 中断期间权限/来源/发布变化 | 已有半文件时撤销 authority、改来源原件、撤回发布；下一轮拒绝，半文件字节保持不变，不写新文件、不释放原资料 | 新增三反例，原生及 Linux 通过 |
| 归档目录安全与并发关闭 | 未知文件、权限、符号/硬链接、另一个目录 owner、关闭超时保留资源等既有反例，随本轮组合复验 | Linux 通过 |
| WAL 进程异常终止 | 当前二进制在 FULL/WAL 下提交 123456、另开未提交 999999 后退出 23；新容器同卷只读回已提交金额，integrity_check=ok，再提交 42 后总额 123498 | 23 是预期崩溃，独立新进程 verifier PASS；不是物理断电 |

Monitor 原生恢复子组 1.480 秒通过；新增归档组合 1.141 秒通过。随机顺序（`-shuffle=20261126`）竞态复测：Monitor 15.043 秒、月单 10.382 秒，均通过，无竞态报告。vet/ratchet/diff 格式门禁通过，未抬基线。

Linux Monitor 选择：`^Test(LocalResourceFilesystemFullStopsAndRecovers|BackupRetention|LocalResourceRecovery|UsageVerifyLiveTail|UsageLocalAudit|UsageFactMultiDay|RecentServiceAudit|StabilityLocalRetry|NginxEvidenceFullThenExpanded|UsageFactRawPageConcurrentWinnerMakesLoserYieldAndResume|UsageFactHourLeaseIsShortAndExpiredClaimIsRecoverable|BackgroundSourceCanceledWaitersDoNotLeakOrDeadlock|BackgroundSourceAlreadyCanceledContextNeverClaims)`。

Linux 月单选择：`^Test(PublishedArchive|CapacityMaintenance|PreparationInbox|MonthlyResource)`。两组完成码均为 0 / PASS，另有独立 WAL actor/verifier。

### 本批失败复盘（未借故降低生产保护）

1. 首次关闭重开库的夹具未装合成 prodDB，导致读意图条件不成立、测试误走 nil 来源。按真实条件补一份新的空合成来源，保留生产读取函数和判断不变；若误回扫会读不到原金额并失败。
2. 首次新增归档用例在读取 permit 表之前未执行维护初始化，报缺表。调整为真实维护初始化顺序；未在生产逻辑中忽略缺表。
3. Linux 首轮把备份及重开库用例放到 128MiB TMPDIR，被原 2GiB 备份空间保护拒绝。这是环境不满足约束，移到隔离磁盘卷复验；没有调低生产 reserve。
4. 三轮探针在 16MiB 故障盘内积累了约 10.6MB 主库及 4.36MB WAL，填充文件已为零但仍占用 89.37%。容量闸门继续阻断正确。改为独立 32MiB 故障盘容纳本测试保留的有效探针数据，三轮全部通过；没有清掉已提交探针来伪造恢复。

### 原跳过项复核与保留资料

此前缺显式空目录的 `TestDeploymentExportSyntheticVersionFixture` 已提供新的私有目录补跑，1.283 秒通过。它验证合成资料导出，不代表二进制版本切换已演练。留存目录 `.run/resource-recovery-linux-20261125/version-fixture-FUvZeu` 约 4.2MiB，供后续正式候选使用；独立合成数据、没有客户数据，验收结束不再需要时删除。

此前 15 项未执行中剩余 14 项：13 项要求明确的真实留存资料，1 项为需操作者参与、最长 45 分钟的浏览器会话。没有为了消除 skip 启动空转浏览器服务或重新拷线上资料。客户页面与下载本轮已经过合成 HTTP/内容对照，但不能冒称完成真实浏览器人工验收。本批另新增 2 个月单顶层测试均通过；没有把旧全包总数冒充当前测试清单。

### 仍不能由本机诊断代替的结论

矩阵是当前识别的故障集合，不是对所有组合的数学穷举。真实块设备延迟、硬件损坏和断电不在本轮模拟结论内；deadline/锁等待不等于物理慢盘性能。目标 amd64 正式不可变应用镜像、完整服务升级/回退、全量仓库 CI/race 和线上资源变化仍须正式验收。

容器均按 `--rm` 退出，独立 WAL writer 的退出 23 也已移除容器。清理前已核对合成卷标签及无引用；报告保存后仅清理本轮合成卷，二进制、源码、诊断及 4.2MiB 合成演练资料保留供下一阶段复用。不清镜像、构建缓存或其他会话资源。

## 第八批：外部 SIGKILL、真实 cgroup OOM、完整服务恢复

### 环境与证据绑定

本批仍无生产操作，只新增显式 opt-in 测试 actor、归档 I/O 错误测试和文档；生产逻辑没有继续变化。复用已有 arm64 运行镜像，无镜像拉取/构建。本任务磁盘卷为 `monitor-recovery-20261125-termination-03`。

```text
monitor-v3.test             b7a2f912ab113e3f89c13fb61c3be2cc4bfc79509927a2de974f27bb27c7cc34
monthlydraft-v3.test        e84f58d53fdad3b898837d3b93e3aedf006ec71892f4fcc66bbb084a77cca10e
monitor-service-under-test a3f06971e89995a7c5b85e7efb75690b4a46532eb28e5398b8b34ee231b05545
```

完整服务是当前源码构建的 Linux 静态程序、只读绑定到已有运行镜像，不是正式不可变候选镜像；`/ready` 的 git_sha/image_version 为 unknown，因此不能拿它代替正式候选身份验收。程序文件约 94MiB，测试二进制约 121MiB、63MiB；这是逻辑文件大小，不是物理 SSD 写入。

容器禁网、不映射宿主端口、只读根目录、无真实凭据、非 root、cap-drop=ALL/no-new-privileges、日志 2MiB×2、restart=no。仅 bootstrap 对新建空卷根目录执行 chown，没有递归更改其他内容。actor 限 192MiB 内存，memory-swap 同为 192MiB，不更改宿主或全局交换配置。OOM actor 最多触碰并保留 256MiB，其他恢复 verifier 提高到 768MiB；完整服务限 512MiB。

### 实际执行结果

| 场景 | 证据及断言 | 结果 |
|---|---|---|
| 外部 SIGKILL，而非进程主动退出 | 新 actor 先持久化合成金额 123456、真实 UsageFactJob 的 queued/NextHour=123/VerifyNextHour=120/Attempts=4，再开事务写未提交金额 999999、状态 complete/NextHour=999。落盘 ready 屏障后由 runner 核对容器 ID/标签，外部 `docker kill --signal KILL` | ExitCode=137、OOMKilled=false |
| SIGKILL 后独立恢复 | 新进程读同一磁盘卷，未提交金额和虚假完成断点均不可见；原 queued/Attempts/VerifyNextHour 保持；显式合成幂等续写 42 后仅两条、总额 123498；integrity_check=ok | PASS |
| 真正 cgroup OOM | 相同的已提交/未提交边界，actor 自身分配并触碰内存直至被内核杀死；没有以普通 exit 冒充 OOM | ExitCode=137、OOMKilled=true；ready 文件存在，证明已到指定事务边界 |
| OOM 后独立恢复 | 新进程以相同断言核对钱数、真实任务记录和数据库完整性，续写总额 123498 | PASS |
| 真正只读挂载 | 合成卷 readonly 挂载。创建新文件实际返回 EROFS；SQLite 写入失败、不归为临时容量，原总额仍为 123498 | PASS |
| 完整 Monitor 程序 SIGKILL/重启 | 以同一合成主库启动完整 main，显式 LocalSnapshotOnly=true、来源/通知/账单生成关闭。管理端 /health 正常、Usage /login 可访问。外部 SIGKILL 后同一容器、同一卷 start；/health=ok、/ready=ready、Usage /login 恢复，随后正常 stop | 强杀时 137/OOMKilled=false，最后优雅退出 0 |
| 完整服务重启后再重放核验 | 服务停止后再次启动独立 verifier，原任务游标没有虚假完成，已续写 42 没有重复累计，金额仍为 123498 | PASS |
| 写入完成处 EIO | 文件已写出后注入 EIO：返回空 receipt，不创建完成 manifest，无删除许可，原热资料存在；不归为自动容量恢复；故障去除后仅显式调用可重新核准已有字节 | 原生 0.810 秒、Linux 组合 PASS；不是底层真实 fsync 错误 |

用于退出状态核对的本任务容器：SIGKILL writer `c44a09f55ec455b5c10c56e6fcb52f1a6bf999ffe9d43b4dd0361aadb8648e93`；OOM writer `3c45a63992c79d85c14909db87326461fae4aadb06749b88d8879ad9d5c35a19`；完整服务 `84460b10c269bc8007fcacc1ce748b48f9268154f429c2ad51c75e79a3f9303f`。均核对 ai.task=monitor-resource-recovery，未操作其他容器。

`local_resource_termination_test.go` 中三个 actor 必须显式设置 `MONITOR_TEST_TERMINATION_ROOT=/data`、MODE=sigkill/oom、PHASE=write/verify/readonly。写入者拒绝已存在的目标目录；普通包测试无开关就跳过。SIGKILL writer 最多等待 40 秒，不允许长时空转；OOM 有内存和分配双重上界。verifier 的合成幂等续写不是正式 worker 或财务发布，不把数据库持久性验证等同于真实来源 worker 已自动完成任务。

### 回归与结论边界

归档 I/O 完成失败、连续归档中断和权限/来源变化组合 race 通过（5.087 秒）；Linux 当前月单二进制相同组合 PASS。当前 monitor/monthlydraft 的 vet、复杂度和 diff 检查通过，未放宽基线。本批没有出现需新增修改生产逻辑的问题。

完整服务测试是离线生命周期 smoke，不含真实登录态、RDS/上游同步或完整业务在途负载；活跃未提交事务、真实任务记录的故障边界由独立 actor 验证。没有模拟磁盘控制器缓存丢失、真实电源切断、物理扇区损坏或底层 fsync 返回 EIO。停止容器、SIGKILL 和 cgroup OOM 均不能称作硬件断电测试。正式 amd64 不可变镜像、版本回退、全量 CI/race 及线上验证仍须按发布门禁完成。

本轮测试进程已退出。为记录 OOM 状态，两个 writer 和完整服务容器未使用 --rm；报告保存后只删除上述三个已停止容器，再删除本任务合成卷，不使用 force，不停止 Docker 引擎或 OrbStack。必要测试代码、指纹和报告保留。

清理已执行：上述三个容器已无 force 删除，再核对卷无引用后删除了唯一目标合成卷。Docker 逻辑统计为容器 19→16、卷 84→83，卷占用四舍五入 16.08GB→16.06GB；镜像 44、Build Cache 541.9MB 未动。这不是宿主物理释放量或 NAND 写入测量。

## rc.1 CI 静态检查复盘与 rc.2 修复

- `v1.18.73-rc.1` / `df0e1c8bf49d17a9f26d8d5e12112c7014a0829a` 的 CI `45559648412` 在 `test / golangci-lint` 失败：测试文件中三处未处理返回值、两处多余转换和一处日志格式建议。其他 race 分片成功，不把整轮 CI 记为通过。
- 本次只修五个 `_test.go` 文件并记录复盘：回滚清理检查真实错误、允许已显式回滚后的 `sql.ErrTxDone`；GORM 故障注入明确丢弃返回值，仍由 `tx.Error` 传播；去掉多余转换；使用 `t.Logf`。没有修改生产源码、金额、权限、故障断言、CI 配置或检查规则。
- Go 1.26.11 下，相关 Monitor 场景 race 回归通过（20.320 秒），`internal/localresource` 整包 race 通过（1.371 秒）。第一条筛选命令没有匹配该错误分类包，因此不能计为该包通过；随后已独立运行完整包补齐。
- 格式、补丁和函数复杂度检查通过。macOS 上原有 Go 1.26.4 构建的 lint 工具另报无关旧测试的 SA5011；未因此扩改旧文件。已从缓存的 golangci-lint v1.64.8 源码用 Go 1.26.11 重建工具，使用 `GOOS=linux GOARCH=amd64 CGO_ENABLED=0` 及现行 CI 的 GOFLAGS 完整运行 `run ./...`，退出码 0。该检查不是 Linux 运行时测试。
- SIGKILL/OOM/真实满盘 opt-in 场景本次未重新执行；只改测试语法与清理错误检查，生产源码和 Docker 构建配置与 rc.1 无差异。此前第八批故障证据仍按其原范围解释，不将普通包测试中的 opt-in 跳过写成重新通过。
- 新候选使用 `v1.18.73-rc.2`，不覆盖 rc.1、不合 main、不部署。必须由新候选自己的 CI 和后续正式镜像/回退验收放行。本轮暂停 S3 恢复实测和磁盘优化，未下载线上数据库、未修改或清理线上备份。
