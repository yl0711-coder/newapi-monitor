# 五项修复与验证记录（2026-10-08）

范围仅为客户维护连续前缀展示、独立排障错误事实保留、零/缺失耗时关联、CloudFront 最终复查和问题预警可信窗口。保留工作区此前的未提交修改，不提交、不部署、不改生产数据。

## 修改文件

- 客户维护：`monitor/customer_health.go`、`customer_health_source.go`、`customer_health_query.go`、`customer_health.js`、`customer_health_test.go`、新增 `customer_health_watermark_test.go`。
- 预警：`monitor/alerts_page.go`、`alerts.js`、新增 `alerts_watermark_test.go`。
- 排障证据与摘要：`monitor/logchain.go`、`logchain.js`、`logchain_investigation_linkage.go`、`logchain_investigation_run.go`、`observability_investigation.go`、`observability_investigation_test.go`。
- 关联与完成状态测试：`monitor/logchain_investigation_test.go`、`logchain_investigation_linkage_test.go`、`logchain_investigation_reverse_lookup_test.go`、新增 `logchain_investigation_duration_test.go`、`logchain_investigation_delivery_test.go`。
- 资源版本与前端回归：`monitor/page.html`、`monitor/logchain_ui_test.go`；`dev/tests/customer-health-watermark.test.mjs`、`alerts-watermark.test.mjs`、`logchain-investigation-entry.test.mjs`。
- 记录：`docs/交接文档.md` 第 58 节及本文。部分文件本轮开始前已经修改或未跟踪；本清单不代表整个工作区差异只属于本轮。

## 结果

| 检查 | 结果 |
| --- | --- |
| Linux/amd64，CGO_ENABLED=0，monitor 测试二进制编译 | 通过 |
| 完整 monitor 测试集（每批 100 个顶层用例，21 个隔离容器） | 2,035 通过，22 跳过，0 失败 |
| `internal/observability` Linux 测试 | 6 通过 |
| `node --test dev/tests/*.test.mjs`（PowerShell 展开文件名） | 165 通过，1 跳过，0 失败 |
| Linux/amd64，CGO_ENABLED=0 主程序构建 | 通过，退出码 0 |
| Linux 目标 `go vet ./monitor` | 通过 |
| 本轮 Go 文件 gofmt；三个修改 JS 的 `node --check` | 通过 |
| `git diff --check` | 通过 |

Go 版本为 1.26.6，测试在本机 Docker Linux 隔离容器执行，不是远程 CI。容器使用 `--network none --read-only`，只读挂载当前工作区，临时 SQLite 写入独立 `/tmp`，运行结束自动删除容器。本轮没有连接或变更生产，也没有重建 8204。

跳过项包括需要真实服务凭证、真实 CloudWatch 对账配置、私有财务/云端样本、独立采集器二进制和专门离线验收开关的测试；不能将这些项目当作已验证。主程序构建出现既有 C 盘 Go 模块 stat cache 写入权限警告，但产物生成且退出码为 0。本轮未运行全量 golangci-lint，也未声称原工作区不存在其他 lint 告警。

首次定向测试 `target-1.log` 包含一次新增测试夹具失败：固定 9 月日期的失败时间早于 GORM 自动写入的真实 10 月成功时间。只修正测试夹具时间，未放宽业务失败判断；最终 `full-8.log` 对应测试通过，完整 21 批均通过。

## 验收边界

- 客户维护只展示有持久连续覆盖证明的 `[from_ts,to_ts)`；`ready` 仍表示追平，不能用它隐藏已可用前缀。旧口径或真实缺口仍禁用指标。金额是原有独立口径。
- 错误和消费是两条事实：消费不抵消错误，未知错误不等于最终请求失败。已有明确归因不因共享错误细类为 unknown 而丢失。
- 0 秒是可校验数值，NULL 不是 0；缺少耗时的竞争候选不能被无声排除。
- 入口证据缺失不会因为超过一小时变成完整，最终复查保留缺口。
- 预警 `statistics_complete` 只证明实际窗口；`coverage_complete` 仍说明原始查询范围。今日实时尾部可以未完整，但不再隐藏已知数量。真实混合或缺口只能展示“已采集部分”。

日志目录：`.local-test-kit/five-fixes-20261008/`。最终结果以 `full-1.log`～`full-21.log`、`node-tests.log`、`observability-tests.log`、`build.log`、`vet.log` 为准。测试/构建二进制验证后清理，仅保留小型日志。

## 后续复查：秒级分页窗口（2026-10-08）

仅修复 `alerts.js` 保存截止参数的逻辑，`to_ts` 不再作为 `cutoff_ts` 的兜底；后端没有明确返回 cutoff 时清空。新增恢复水位后继续分页与筛选的完整操作链、完整响应无 cutoff、旧 cutoff 撤销三项回归；测试桩会拒绝非法秒级 cutoff 并返回 400。`page.html` 更新为 `alerts.js?v=3`，不改其余四项业务逻辑。

修复前新增三项失败；修复后预警 13/13 通过，前端全套 168 通过、1 跳过、0 失败。`node --check monitor/alerts.js` 和 `git diff --check` 通过。本次未修改 Go 逻辑，未重跑 Go 测试、未提交、未部署。后续日志：`.local-test-kit/alerts-cutoff-20261008/alerts-regression.log`、`node-tests.log`。
