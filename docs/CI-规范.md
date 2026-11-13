# Monitor CI 规范

> 版本：v1.0（随 v1.18.48 生效）　维护：monitor 开发　更新：2026-11-10
>
> 适用范围：本仓库 `.github/workflows/ci.yml`。所有开发者新增或修改测试、调整 CI 前先读本文。

## 1. 目标

- 每次 CI 在约 20 分钟内给出结果（v1.18.46 时约 36 分钟）。
- 不降低检查强度：竞态检测（race）全量保留，任何测试都不允许因为"跑得慢"被跳过。
- 新增测试**不需要改 CI 配置**，会自动被分配并且只跑一次。

## 2. 流水线结构

触发条件：

| 事件 | 是否运行 | 是否发布镜像 |
|---|---|---|
| 推送到 `main` | 是 | 是（`:main`、`:latest`，以及提交短哈希） |
| 推送到 `dev` | 是 | 是（`:dev`，以及提交短哈希） |
| 推送 `v*` 标签 | 是 | 是（版本号镜像） |
| 向 `main`、`dev` 发起或更新 PR | 是 | 否 |
| 推送到其他分支（`fix/*`、`release/*` 等） | 否 | 否 |

同一个 PR 再次推送时，会取消上一轮还没跑完的 CI；`main`、`dev` 和标签的 CI 总是跑完。

任务关系：

```
secrets ──┬─ image-security（5 个镜像，扫描）──┐
          │                                   ├─ build-push（发布镜像）
test ─────┤                                   ├─ build-nginx-ops-tools
race-monitor（11 个任务）──────────────────────┤
race-monthly（7 个任务）───────────────────────┘
```

- `test`：vet、lint、前端测试、各类离线检查，以及 monitor 和 monthlydraft 以外所有包的 race 测试。
- `race-monitor`、`race-monthly`：这两个包测试量最大，单独分配（见第 3 节）。
- `image-security` 只依赖 `secrets`，和测试同时开始；**镜像发布仍然要求测试和扫描全部通过**。

### 并发上限

本仓库是公开仓库，GitHub 免费额度下同时最多运行约 20 个任务，超出的任务要排队。所以：

- 测试阶段同时运行的任务总数不超过 20：`secrets` + `test` + race 18 个（monitor 7 并行 + 4 串行，monthly 6 并行 + 1 串行）= 20 个。
- 镜像扫描是 5 个 1–2 分钟的短任务，利用剩余的空位依次完成，不影响总时长。
- **调整任务数时，必须重新核算这个总数。**

## 3. race 测试是怎么分配的

monitor 包约 3900 个测试，monthlydraft 包约 580 个，全部串行跑要 7 个多小时（race 模式）。分配方式：

1. **按实测耗时均分**：`dev/monitor-race-timings.json`、`dev/monthly-race-timings.json` 记录每个测试在 CI 上的实测耗时（秒）。`dev/race-plan.mjs` 按"最长的先放"把测试分给 `任务数 × 3` 个进程，使每个进程的预估总时长接近。
2. **每台机器同时跑 3 个进程**：测试本身是串行的，一个进程只用得上 4 核里的 1 核。`dev/run-race-job.sh` 在每台机器上把 race 测试程序编译一次，再同时启动 3 个进程。
3. **测试清单以实际编译结果为准**：分配用的是 race 编译出的程序自己列出的测试清单（`-test.list`），不是耗时表。耗时表里没有的新测试按默认估计（monitor 30 秒，monthly 90 秒）分配，**照样会跑**。
4. **覆盖校验**：`test` 任务里的 "Verify every race test belongs to exactly one planned process" 步骤会证明每个测试恰好属于一个进程，漏了或重复都会失败。

当前配置（`dev/race-plan.mjs` 的 `racePackages`）：

| 包 | 并行任务 | 每任务进程数 | 串行任务（每任务 1 个进程） | 单进程预估上限 |
|---|---|---|---|---|
| monitor | 7 个（编号 0–6），共 21 个进程 | 3 | 4 个（编号 7–10）：全部 `TestToken*` 测试 | 14 分钟（目标） |
| monthly | 6 个（编号 0–5），共 18 个进程 | 3 | 1 个（编号 6）：`TestRetentionFullReceiverPayloadPerformance` | 14 分钟（目标） |

### 串行任务（对 CPU 争用敏感的测试）

有一类测试验证的是"受真实时间预算限制"的行为（例如一轮处理只能用 4 秒、提交成本要在预算内分摊、清理要在限定延迟内完成）。和别的进程抢 CPU 时，这类测试会失败。这类测试列在 `dev/monitor-race-serial.json`、`dev/monthly-race-serial.json`，放在串行任务里跑：**每台机器只跑 1 个进程**，不和其他测试并行。

清单格式：

```json
{"format_version": 2, "jobs": 4, "prefixes": ["TestToken"], "tests": ["TestSomeBudgetCase"]}
```

- `prefixes` 按测试名前缀整批归入（目前 monitor 的全部 `TestToken*`，约 420 个），`tests` 逐个列出；`jobs` 是串行任务的数量，这些测试按耗时均分给它们。
- **依据**：v1.18.48 在真实 CI 上试过每台机器 3 个进程，两轮里有 19 个不同的 `TestToken*` 测试和 1 个 monthly 性能测试失败，其余约 3900 个测试全部通过。所以只把这些测试单独运行，其余测试保持 3 个进程并行。
- **只有证据充分才加入**：在并行负载下重复失败、单独运行稳定通过，才列入清单；不要把普通的慢测试放进来。
- 清单里的测试被改名、删除，或者某个前缀已经匹配不到任何测试时，CI 会报错，需要同步更新清单。
- 串行任务的预估总时长同样受 14 分钟目标约束；不够时增加 `jobs`，同时核算总任务数（第 2 节）。

### 串行任务的运行方式（`mode`）

| mode | 做法 | 当前用于 |
|---|---|---|
| `race` | race 模式运行，任何失败都让任务失败（允许单独重跑一次，见第 7 节） | monthly 的性能测试 |
| `native-gate-race-advisory` | 先在**正常模式**（不开 race）运行，所有断言都必须通过，包括性能指标；再在 race 模式运行，只有数据竞争、panic、测试程序卡死、找不到失败测试会让任务失败，其他失败只在 Summary 列为"Race-mode advisory failures" | monitor 的全部 `TestToken*` |

为什么令牌测试用第二种方式：这批测试里有大约 100 个检验"在多少毫秒内至少处理多少条"这类性能指标。race 模式会让程序慢好几倍，GitHub 的机器在 race 模式下达不到这些指标，即使一台机器只跑一个进程、单独重跑也一样。v1.18.48 验证期间有 20 个不同的令牌测试这样失败过，v1.18.44、v1.18.45 当时的 CI 也因此失败。这些指标是按线上（正常模式）速度定的，所以在正常模式下必须通过；race 模式继续检查数据竞争。

这是过渡做法：等这些测试改成不依赖机器速度（第 7 节"待修"），令牌测试恢复为 `race` 模式，和其他测试一样必须在 race 模式下通过。**新写的性能类测试不要依赖这个豁免**，按第 4 节的规则写。

预估值按"一台机器只跑 1 个进程"时的实测耗时计算；3 个进程同时跑会互相抢 CPU，实际会慢一些，所以目标上限定在 14 分钟，单进程超时 25 分钟，任务超时 35 分钟。

## 4. 开发者写测试的规则

1. **单个测试在 race 模式下尽量控制在 60 秒内**；超过 3 分钟的测试要在 PR 里说明原因。CI 的 Summary 页会列出每个进程最慢的 15 个测试。
2. **不要用真实时间等待**（长时间 `time.Sleep`、轮询等待真实时钟）。用可注入的时钟或缩短的测试间隔。
3. **不要假设测试独占机器**：同一台机器上会同时跑 3 个测试进程（串行任务除外）。所以：
   - 不要写固定路径的文件（用 `t.TempDir()`）；
   - 不要监听固定端口（用 `127.0.0.1:0`）；
   - 对执行时间的断言要留足余量。
4. **race 模式比正常慢好几倍**：如果测试必须验证某个正式时限（例如"最大规模的数据要在 5 秒内处理完"），可以在 race 模式下放宽**测试使用的时限**，正式代码里的时限值不变。做法参考 `internal/monthlydraft/archive_test.go` 中的 `TestArchiveFullRecordBoundRoundtrip`：用 `raceDetector` 常量（`race_enabled_test.go` / `race_disabled_test.go`）判断，把包内的时限变量临时调大。
5. **大量造数用批量 SQL**：上千行的测试数据，不要用 ORM 逐条插入（race 模式下非常慢）。用一条 `INSERT ... SELECT`（递归 CTE）或批量插入。
6. **新写的测试如果检验"在多少秒内完成"这类真实时间预算**：先在本地模拟并行负载跑一下（例如同时跑 3 个 `dev/run-race-job.sh` 任务）。如果会失败，优先改成不依赖真实时间；做不到的，在 PR 里说明，并加入串行清单（`TestToken*` 前缀的测试会自动进入串行任务）。
7. **新增需要外部程序的测试**（例如需要预先编译的采集器）：在 `dev/run-race-job.sh` 里统一准备，并通过环境变量传给测试；不要假设测试会落在某一个特定任务里。

## 5. 维护：刷新耗时表与调整任务数

### 什么时候刷新耗时表

- 新增或明显改变了一批慢测试之后；
- CI 的 Summary 显示各进程耗时明显不均（最慢和最快相差超过 3 分钟）；
- `race-plan.mjs` 报错 "predicted race process exceeds the 14-minute target"。

### 怎么刷新

```bash
# 1. 下载一次（或几次）完整通过的 CI 的耗时记录
gh run download <run-id> -p 'monitor-race-*' -D /tmp/race-monitor
gh run download <run-id> -p 'monthly-race-*' -D /tmp/race-monthly

# 2. 更新耗时表（多个 run 时每个测试取最慢的一次；失败或未完成的测试保留原值）
go test -race -list . ./monitor > /tmp/monitor-list.txt
node dev/update-race-timings.mjs monitor --run <run-id> --listing /tmp/monitor-list.txt /tmp/race-monitor
go test -race -list . ./internal/monthlydraft > /tmp/monthly-list.txt
node dev/update-race-timings.mjs monthly --run <run-id> --listing /tmp/monthly-list.txt /tmp/race-monthly

# 3. 检查分配结果
go test -race -list . ./monitor | node dev/race-plan.mjs monitor --verify
go test -race -list . ./internal/monthlydraft | node dev/race-plan.mjs monthly --verify
```

耗时表只包含测试名和秒数，不包含日志或任何业务数据，可以直接提交。

### 怎么调整任务数

测试总量增长、刷新耗时表后仍超过 14 分钟目标时：

1. 改 `dev/race-plan.mjs` 里 `racePackages` 的 `jobs`；
2. 同步改 `.github/workflows/ci.yml` 里 `race-monitor` / `race-monthly` 的 `matrix.job` 列表：并行任务编号 0 到 `jobs-1`，有串行清单的包再加一个编号 `jobs`（`dev/tests/race-plan.test.mjs` 会校验两者一致）；
3. 重新核算并发总数（第 2 节），保持测试阶段的任务数不超过 20。超过时优先考虑把 `processes` 从 3 调到 4（runner 为 4 核），但要先观察时间敏感测试是否稳定。

## 6. 排查失败

- **哪个测试失败**：在失败的 race 任务日志末尾，`dev/run-race-job.sh` 会列出 `failed: <测试名>`，以及是否做了重跑、不重跑的原因（`not re-run: …`）；Summary 页列出失败和未完成的测试，以及偶发失败清单。
- **完整输出**：每个进程的 Go 测试事件保存在任务的 artifact（`monitor-race-<n>` / `monthly-race-<n>`）里，保留 7 天。
- **本地复现某个任务**（和 CI 完全相同的分配和并行方式）：

  ```bash
  dev/run-race-job.sh monitor 3 /tmp/race-out     # monitor 第 3 个任务
  dev/run-race-job.sh monthly 0 /tmp/race-out     # monthly 第 0 个任务
  ```

- **只跑某一个测试**：`go test -race -count=1 -run '^TestName$' ./monitor`。

## 7. 偶发失败（flaky）的处理

### 自动重跑一次

race 任务里有测试失败时，`dev/run-race-job.sh` 会等这台机器上的所有进程结束，再把失败的测试**单独重跑一次**（同一台机器上不再有其他测试争用 CPU）：

- 重跑通过：任务通过，但会在 PR 页面标出黄色警告（"Flaky race test"），并在 Summary 里列出"Flaky race tests"清单；
- 重跑仍然失败：任务失败；
- 以下情况**不重跑，直接失败**，因为它们不是机器快慢造成的：
  - 报告了数据竞争（`WARNING: DATA RACE`）；
  - 测试 panic，或测试程序整体超时；
  - 进程失败但找不到是哪个测试失败；
  - 同一个任务里失败的测试超过 5 个（`dev/race-failures.mjs` 的 `raceRerunMaxTests`），这说明是系统性问题。

判断逻辑在 `dev/race-failures.mjs`，单元测试在 `dev/tests/race-failures.test.mjs`。

### 偶发失败必须修

重跑只是为了不让偶发失败卡住发版，**不能一直靠重跑**：

- 每次 CI 列出的偶发失败清单就是待修清单。由负责该模块的开发修复，原则上在下一个版本修掉；一时修不了的，记录原因和计划。
- 修法见第 4 节：不依赖真实时间；做不到的，放宽 race 模式下测试用的时限，或者加入串行清单。
- 同一个测试多次被列为偶发失败，又一直没修，要在发版评审时提出来。
- 重跑掩盖不了真问题：稳定会失败的测试，重跑一样失败；数据竞争不会重跑。

### 已知情况

- 已修复：`TestArchiveFullRecordBoundRoundtrip`、`TestRetentionFullReceiverPayloadPerformance`（race 下盘点的 5 秒时限过紧，已在 race 模式下放宽测试用的时限，正式时限不变）。
- 已列入串行任务：全部 `TestToken*` 测试（`native-gate-race-advisory` 方式，见第 3 节），以及 monthly 的 `TestRetentionFullReceiverPayloadPerformance`（`race` 方式）。
- 已修复：`TestTokenMeteringPaginationCrashResumeAndFencing`（原来要求恢复后一轮内写完，慢机器上稳定失败；改为最多 4 轮，仍检查结果正确）。
- 待修（v1.18.48 验证期间在真实 CI 上出现过）：
  - 在 race 模式下达不到性能指标的令牌测试（共 20 个，集中在 `token_metering_budget_test.go`、`token_commit_latency_test.go` 等）：改成不依赖机器速度后，令牌测试恢复 `race` 模式；CI 每次的"Race-mode advisory failures"清单就是这份待修清单；
  - `TestTokenReviewReplayFailureAndAtomicCompleteness`：单进程也偶发失败；
  - `TestFinanceGiftSeriesBudgetsAndLocks/timeout`：并行时偶发"deadline not respected"；
  - `TestTokenDetectionFairnessMinuteBacklogRestartRecovery`：race 下约 6 分钟，每轮有 5 秒时限，需要观察。
- 历史上 v1.18.44、v1.18.45 的 CI（每台机器 1 个进程）也有 `TestTokenDetection*` 等令牌测试偶发失败，说明这批测试本身对机器快慢敏感，和并行方式无关。
- 待观察：`internal/usagebill/exportjobs` 的 `TestExpiryAndCancellationPreserveOpenLeaseOnly`，机器很忙时偶发"产物未清理"。

## 8. 发版时的 CI

- 发版流程：在发版分支上提交 → 推送 `v*` 标签（触发一次完整 CI 并发布版本镜像）→ 上线 → 稳定后开 PR 同步到 `main`。
- PR 会跑一次 CI，合并到 `main` 后还会再跑一次（发布 `:main`、`:latest` 镜像）。
- **避免同时推送多个标签或同时触发多个 CI**：它们共用 20 个并发名额，会互相排队，每一轮都变慢。
