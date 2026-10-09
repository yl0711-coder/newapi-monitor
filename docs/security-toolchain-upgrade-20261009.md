# Go 安全补丁本机验收（2026-10-09）

## 原因与边界

- `v1.18.17` 的 CI run `37902607114` 在可达 Go 漏洞检查失败，旧构建链为 Go 1.26.6 / x/net 0.57.0。
- 本次在该版本提交 `a985b6432b7423f6d43d6ad193fc156dd6859fd7` 上升级安全依赖，不修改业务处理器、SQL、数据库结构、生产配置或部署脚本行为。
- Go 最低版本、CI 四处工具链和六份 Dockerfile 默认构建链统一为 1.26.9。x/net 升为 0.60.0，按其依赖要求同步 x/crypto 0.57.0、x/sys 0.48.0、x/text 0.42.0；go mod tidy 将已有直接使用的 x/sys 归类为直接依赖。
- 复用并增强既有 security-workflow 测试，检查 go.mod、CI 和所有构建阶段版本一致；没有新增重复测试或绕过扫描规则。
- 本机验收期间未修改线上，未重打或移动 v1.18.17。验收完成后用户授权提交和触发 CI，拟使用尚未占用的新标签 v1.18.18；本记录不代表远端 CI 已通过。

## 验证结果

1. `go mod verify` 和 `go vet ./...` 通过。
2. govulncheck v1.1.4 全仓扫描通过；分别检查本机目标和 Linux/amd64、CGO_ENABLED=0 生产目标，均为 0 项可达漏洞、0 项已导入包漏洞。
3. 仍有 GO-2026-5932 的模块级提示：x/crypto 包含未维护的 OpenPGP 包，但应用没有导入或调用。既有依赖门禁核查 662 个 Linux 生产/测试包通过，没有加入忽略规则。不能将此结果表述为所有依赖不存在任何漏洞提示。
4. `go test -race ./cmd/... ./internal/... ./monitor/public -count=1 -timeout=10m` 通过。
5. `go test ./monitor -run '^Test(Usage|Finance|Channel|Stability|Upstream|LocalSnapshot|HistoricalCost|Auth|Login)' -count=1 -timeout=10m` 通过，173.801 秒。
6. `go test -race ./monitor -run '^Test(UpstreamGuard|Upstream.*RetryAfter|UsageServing|HistoricalCostRange)' -count=2 -timeout=8m` 通过，61.651 秒。
7. `node --test dev/tests/*.test.mjs`：238 项，232 通过、0 失败、6 项条件跳过；跳过项不计为已执行。本次不替代上一轮已完成的浏览器及实际报表产物验收。
8. 正式多阶段 Dockerfile 构建成功：`newapi-monitor:go1269-security-local`。没有替换二进制或使用 OFFLINE_RUNTIME。Docker Hub 直连鉴权超时后仅本次构建命令使用本机既有代理重试成功，未修改全局网络配置。
9. 本机新建隔离数据卷，旧版 v1.18.16 → 安全升级候选 → 候选重启 → 旧版回滚：四阶段健康检查、四模块报表及并发读取均通过，共 80 次请求；比较指定业务字段一致，排除随墙钟变化的 usage_lag_seconds。
10. 四阶段数据库 quick_check 通过，原始快照哈希不变，用量事实库不变；没有 OOM 或异常重启。容器 network=none，不访问生产或上游。验收私有产物留在 `/private/tmp/monitor-go1269-acceptance.SMvkTj/`，不进入 Git。
11. `git diff --check` 通过。

## 发布前仍需完成

- 本次升级可以进入提交及新 tag 的 CI；旧失败 tag 不覆盖。
- 必须等待远端完整测试、全部竞态分片、密钥检查和镜像 HIGH/CRITICAL 漏洞门禁通过。此次本机主镜像构建不能替代 CI 对所有发布镜像的扫描。
- 历史数据覆盖缺口仍按原功能验收记录单列，安全升级不补齐历史数据，也不证明线上所有功能无未知缺陷。
