# ECS 生产接入前只读核对与分阶段修改清单

## 结论

2026-09-10，通过当前已登录的 yanglei 身份只读核对 ECS、任务定义、Task Role、ALB 规则及目标健康，结合当前候选代码检查。

**隔离协议验收已通过，但当前候选不能直接替换生产采集器。** 尚缺真实日志文件合同、生产/影子接入范围、停止依赖、发布镜像与切换边界的适配。不要把 `isolated` 改成任意值、把生产 DSN 清空、重命名业务日志或放开白名单来绕过门禁。

本轮没有调用 UpdateService、RegisterTaskDefinition、IAM 写接口、ECS Exec 或生产数据库，也没有改动生产流量。新增的只有这份接入检查文档。运行证据脱敏后保存在 `/private/tmp/ecs-prod-preflight-vhl9q0wu/`；未保存 Secret 值、业务环境变量值或 ALB 请求头条件值。

## 当前 AWS 快照

| 服务 | desired / running / pending | 任务定义 | 说明 |
| --- | --- | --- | --- |
| nexusapi-prod-master | 1 / 1 / 0 | master:2 | 仅 new-api 容器，没有两个文件采集 sidecar |
| nexusapi-prod-worker-canary | 3 / 3 / 0 | worker:6 | 三个 ALB 目标均 healthy，实际承接正式流量 |
| nexusapi-prod-worker-buffer-canary | 0 / 0 / 0 | buffer-canary:3 | 目标组为空；规则 110 中权重 0，正式 worker 权重 100 |

所有服务部署状态 COMPLETED。Master 为 1 vCPU / 2 GiB，Worker 和 buffer 的任务规格为 2 vCPU / 4 GiB。服务名里的 canary 不能当成无生产流量的证据。

ALB 443 规则 90 和默认规则均向正式 worker 转发；buffer 在本次所列规则中只有权重 0 的转发入口。**这不构成未来启动 buffer 的授权，也不证明启动后对数据库无影响。** 它仍引用生产 NewAPI 镜像及 SQL_DSN 等秘密；零 ALB 权重不隔离初始化、后台同步或数据库访问。

## 必须处理的适配差异

| 项目 | 已核实的差异 | 风险与处理方式 |
| --- | --- | --- |
| 来源身份 | 正式及 buffer 的两个采集器均配置 `ecs-canary` | 新代理改为服务授权、task/runtime/lane 身份；旧来源保留历史，不让新任务复用旧游标或批次身份 |
| 访问日志 | 生产 `/logs/nexusapi_access.jsonl`；候选最终核验固定 `access.jsonl` | 在代理与归档协议内显式配置实际来源文件，绑定逻辑 lane、真实文件身份和校验内容；不修改 nginx 写日志路径或用软链接规避检查 |
| NewAPI 日志 | 生产 `/app/logs/oneapi-*.log`；候选仅支持单个 `new-api.log` 且 reject 游标要求恰好一个文件 | 增加保留文件集合/多文件游标核验与文件代次处理，覆盖新增、轮转、跨日期、同名替换及缺失文件；不能仅改一个字符串。此次尚未进入容器验证实际保留文件数量、大小和轮转行为 |
| 停止顺序 | nginxcollector 启动依赖 nginx START，reject 启动依赖 new-api START | ECS 停止依赖反向执行，现定义会让采集器先停，可能遗漏尾部；需要对采集相关任务依赖做明确评审，不能只换镜像 |
| 收尾时限 | nginx/new-api 显式 120 秒；两个采集器没有显式 stopTimeout | 当前代理收尾分阶段预算合计最多约 36 秒，应给采集器明确且受限的停止预算，例如 60 秒，经真实任务契约验收；不调整业务容器的既有 120 秒设置 |
| 状态存储 | nginx 有 `/data` 和 `/evidence` 临时任务卷；reject 仅挂只读日志，没有独立状态卷 | 两类代理都需独立、受权限保护的任务状态卷，容器内重启复用身份和冻结批次；任务消失后的恢复靠外部归档，不把临时卷称为持久化恢复源 |
| 权限与注册桥 | 三个服务共享 `nexusapi-ecs-task-role`，该角色无 inline/attached identity policies；信任 ECS 但没有 SourceAccount/SourceArn 限制 | 为新采集阶段设计独立最小权限角色、AWS_IAM 注册入口和私有归档授权，评估同任务共享角色边界；不直接给共享角色叠加权限。此次未验证生产注册桥/S3 资源策略是否已经另行配置 |
| 接收端隔离保护 | `parseECSLogPolicies` 要求 isolated、业务 DSN 为空、source worker 关闭；代理/桥/归档也限定 isolated | 当前生产 Monitor 不能直接打开该开关。应先明确独立影子接收存储与范围，再设计显式受控模式，不取消身份认证或把测试范围冒充生产范围 |
| 发布产物 | Monitor CI 只构建普通 Monitor、hostagent、nginxcollector；新代理另在 `Dockerfile.ecs-isolated` | 普通发布流程不会自动生成 ECS 新代理镜像；需新增独立候选构建/测试/安全扫描与 immutable digest 锁定，reject 仓库同步核对。不能借用已经清理的临时 ECR 地址 |
| 业务统计 | 最终文件证明与稳定性/模型完整性仍是独立层次 | 影子数据必须隔离，不因同时采集而重复影响请求数、错误数、金额。正式接入需声明切换起点、旧新来源职责区间及去重依据 |

AWS 对依赖反向停止、START/HEALTHY 条件的定义见 [ContainerDependency](https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_ContainerDependency.html)；采集器停止时限配置见 [ContainerDefinition](https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_ContainerDefinition.html)。默认停止时限和启动重试预算不能作为尾部必然交付的保证。

## 停止依赖方案：仅提案，未应用

保留 NewAPI 镜像、命令、环境、秘密、健康检查和业务端口。保留 nginx 对 new-api HEALTHY 的业务依赖；将采集相关依赖改为：

```text
初始化状态卷 → reject 采集代理 START → new-api HEALTHY ─┐
初始化状态卷 → nginx 采集代理 START ──────────────────┴→ nginx
```

精确要求：reject 代理只等待状态卷初始化；new-api 等待 reject 代理 START。nginx 代理只等待初始化；nginx 等待其代理 START 及 new-api HEALTHY。停止反向执行，分别先停日志生产者，再排空对应采集器。

- 不允许采集代理 HEALTHY 又等待生产者运行，避免循环等待。
- 采集 sidecar 保持 nonessential，不能把网络注册失败或采集退出直接升级为整个业务 task 被停。START 依赖仍是任务启动配置变化，必须在真实生产形态的隔离任务中测试后另行批准。
- nginx 绑定生产者 `nginx`；reject 绑定生产者 `new-api`，不能照搬合成测试中所有 lane 均绑定 `synthetic` 的策略。
- 采集器日志卷只读；状态目录每种采集器独立。记录来源文件发生变化、未能读取或预算超限的情况，不能生成“已完整”的证明。

## 执行顺序与交付门槛

进展补充：第 1 步的显式生产文件规则、多文件游标核验和本地真实采集器验收已完成，详见 [本地适配验收记录](ecs-real-file-contract-local-20260910.md)。这不覆盖任意 nginx 轮转，也不替代真实生产文件清单核对及下列后续门槛。

2026-09-10 本机完整验收补充：用户执行的 `/private/tmp/ecs-local-acceptance.VPC81G` 证据目录已复核。拒收采集器竞争测试、ECS 采集器 race 测试、Monitor ECS race 测试、桥接/切换/镜像收尾/任务契约/IAM 模板/分片准备、前端回归均通过；测试分片合计 1306 项，失败数为 0。该证据只证明本机隔离合同和候选代码回归通过，不代表生产发布已获授权，也不替代真实 AWS 生产形态验收。

本机质量门禁补充：针对 ECS 相关 Go 包的 `go vet` 已通过，`git diff --check` 已通过。随后在隔离临时模块缓存中补齐依赖，完成全仓 `go vet ./...`（通过）及依赖策略扫描（通过，584 个 Linux 生产/测试包；未发现被禁止的 OpenPGP 依赖）。临时缓存位于 `/private/tmp`，未修改 `go.mod`、`go.sum` 或生产环境。

后续使用 CI 同版本 Go 1.26.6 复核：全仓 `go vet ./...` 通过；`golangci-lint v1.64.8` 首次发现一处测试错误包装不符合 `errorlint` 规则，已将查询错误改为 `%w` 并将数量校验拆开，复跑通过；`govulncheck v1.1.4 ./...` 结果为代码可达漏洞 0，依赖中存在但代码不可达的漏洞 1，具体为 `GO-2026-5932`（`golang.org/x/crypto/openpgp`，无修复版本）。仓库实际只使用同模块的 `bcrypt`，未导入 `openpgp`；依赖策略扫描已禁止该包被直接或传递使用。受影响 ECS 镜像归档/日志测试通过。该修复未改变业务逻辑或生产配置。

第 2 步已完成源码路径核对、独立接收边界的本地验证及切换设计，见 [隔离与统计切换方案](ecs-shadow-isolation-cutover-20260910.md)。新增 isolated 接收端拒绝旧 Token 混入、私有验收目录拒绝硬链接共库的保护；隔离候选任务责任账本已实现并本地验证，正式 shadow 范围及正式责任切换仍未开放，不应将设计文档当作可直接部署的配置。

本机后续进展：新增只读切换/回退清单校验，完整采集回归通过；两个 ECS 采集器及 Monitor 候选镜像已本机构建、扫描，没有推送或部署。三镜像 HIGH/CRITICAL 为 0，Monitor 保留一条未引用 OpenPGP 子包的 UNKNOWN 模块提示。详见 [离线校验与构建扫描记录](ecs-cutover-preflight-local-20260910.md)。部署差异、真实来源审批、候选镜像内的完整进程验收和 AWS 生产形态隔离验证仍不能跳过。

镜像内进程验收后续已完成本机部分：分别运行 nginx/new-api 合成生产者、两个候选默认入口，验证正确停止时尾部收齐、反向顺序时不会生成已验证最终边界，见 [镜像收尾验收](ecs-image-shutdown-local-20260910.md)。AWS 调度顺序、IAM 与正式部署审批仍未由本机测试替代。

后续本机新增双生产者任务契约、业务配置不变比较及测试模板最小权限回归，见 [任务定义静态复检](ecs-task-contract-local-20260910.md)。旧单生产者 AWS 模板不能作为双生产者定义验收通过；本轮没有生成/上传正式任务定义，真实 IAM 与调度验收仍待执行。

1. **本地适配真实日志合同**：支持生产访问日志名和 `oneapi-*.log` 文件集合；旧固定名协议继续通过。验证文件变更、轮转、跨日期、重启、缺失、超限、符号链接/路径逃逸拒绝，以及 frozen retry 内容不变。不得要求 NewAPI 改日志写法。
2. **定义影子模式与切换合同**：独立 audience、服务/角色授权、接收存储、归档及告警；不混入老板看到的消费或成功率。明确历史不回放、新任务从何时开始、哪些旧来源停止承担责任，防止一份请求计两次。
3. **准备候选资源与镜像计划**：只生成待审 Task Definition 差异和 IAM/存储/桥模板，核验不改业务镜像/DSN/端口/ALB/扩容设置。构建两个新代理的发布候选，完整 CI 与漏洞扫描，锁定同一已扫描 digest。
4. **用生产形态做隔离验证**：使用相同文件名、多文件模式、分开的 nginx/new-api 生产者及停止依赖，但不得使用生产 DSN 或上游 Key。既有合成测试只证明已声明合同，不替代此步。
5. **明确授权后生产侧影子灰度**：先复核当时 ALB 所有入口、数据库后台影响、资源预算和回滚。不得凭本次 0 权重快照自动启动旧 buffer。业务流量权重仍不由 Monitor 验收修改。
6. **验收后才切换统计责任**：影子读数、延迟、缺口、重复率和资源开销都达标后，再安排统计接入；失败则保留冻结批次与归档，旧 Lightsail 继续原链路。

如果需要先发布服务端资源自动发现、归档/恢复界面，可将其与 ECS 日志新入口分开发布，新日志开关维持关闭；不能对外宣称已完成生产日志自动扩缩容接入。

## 回滚边界

- 未开始生产部署时无需线上回滚；本轮就是这一状态。
- 后续任务灰度失败：恢复获批前的任务定义，权重不变，不删除唯一游标/身份/归档来消除告警。
- 已生成新协议对象时，保留能识别它们的接收端完成补传；不要让旧二进制直接打开唯一已迁移 SQLite。独立影子存储不替代现有生产 SQLite，降低回滚耦合。
- 无法确认停机尾部或历史删除文件时保留缺口。正常业务使用不能以“监控界面全部标绿”为代价牺牲事实准确性。
