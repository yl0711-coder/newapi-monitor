# Nginx 入口旁路采集上线前验收报告

- 验收日期：2026-08-07
- 分支：`dev`
- 状态：本地代码与容器验收通过；尚未 commit、push 或改动生产 Nginx
- 范围：Nginx access 脱敏分钟聚合、断点续传、日志轮转恢复、Monitor 接收与状态展示

## 1. 交付内容

1. 采集器按 inode + byte offset 持久化游标，接收失败不推进游标。
2. 进程中断或 Monitor 暂时不可用期间，可依次追读多个仍保留的未压缩轮转文件。
3. 原 inode 丢失、copytruncate、轮转文件残行等无法证明连续的情况会客观累计“游标不连续”。
4. 无效、数值越界、过旧或未来日志行会被跳过并计数，不会堵住队列。
5. Monitor 页面显示节点心跳、待读字节、游标不连续和跳过行；不自动推断责任方或丢失量。
6. `/stability/health` 已包含 Nginx 允许节点数、异常节点数、积压和累计异常指标。
7. `MONITOR_NGINX_ENABLED=true` 但缺 token、白名单或存在重复/非法节点名时，Monitor 拒绝启动，避免半配置。

## 2. 安全边界

- 专用日志不写入 IP、X-Forwarded-For、Authorization、Key、Cookie、query、请求/响应体、User-Agent、Referer、Request ID 原值或 upstream 地址。
- 采集器在节点本地归一化路径并按分钟聚合，Monitor 不接收请求级原始日志。
- 镜像内用户为 `collector`（uid 100），运行时只读根文件系统、`cap_drop=ALL`、`no-new-privileges`，不挂载 Docker socket。
- 容器边界：64 MiB 内存、0.25 CPU、64 PIDs，容器日志 10 MiB × 3。

## 3. 测试矩阵与结果

| 类别 | 验证点 | 结果 |
|---|---|---|
| 单元测试 | 脱敏归一化、半行、长/坏行、超留存、多次轮转、积压、断点续传、幂等入库 | 通过 |
| 全包测试 | `go test ./...` | 通过 |
| 竞态检测 | `go test -race ./cmd/nginxcollector ./monitor` | 通过 |
| 静态检查 | `go vet ./...`、`golangci-lint run ./...`、`node --check` | 通过 |
| 测试覆盖率 | nginxcollector 67.9%；Monitor 包 64.9% | 通过 |
| 可达漏洞 | `govulncheck ./...` | 0 个可达漏洞 |
| Compose | 必填变量展开、资源/安全参数 | 通过 |
| 镜像 | Go 静态构建、Alpine 运行层、非 root | 通过；本地镜像约 6.8 MB |
| 容器黑盒 | 首次 POST 故意返回 503，确认同 batch ID 重试、游标只在成功后推进 | 通过 |
| 轮转黑盒 | 断线期间 rename 旧文件并新建当前日志，确认旧尾部先于新文件上报 | 通过 |
| 资源黑盒 | 受限容器空载/小日志 | 4.5–9.8 MiB，7 PIDs，CPU <0.1% |
| 清理 | 临时容器、游标卷、本机模拟接收端 | 已停止/删除 |

## 4. 验收中发现并已修复

1. 目录扫描与 `open` 之间恰好发生日志轮转时，旧实现可能直接切到新 inode。现改为短暂重试并重新扫描，不跳过旧日志尾部。
2. 首次启动尚未保存游标、接收端又首次失败时，心跳会把历史轮转文件误算为积压。现已与首次游标选择逻辑统一，只统计当前文件，并加入回归测试。
3. 坏数字字符串原先可能被容错为 0。现在非空/非 `-` 的无效数字会整行拒绝并计入跳过行。

## 5. 生产变更闸门

代码本身已达到可 commit/CI 的状态，但“入口与平台”仍不能视为已生产接入。
现网 Nginx 容器没有专用脱敏日志卷，需要按 Slave → Master 滚动重建；该步骤会接触流量入口，
必须在明确确认后单独执行，不包含在本次本地开发授权中。

精确接入顺序、轮转配置、验证点与回滚边界见
[`docs/nginx-collector.md`](../nginx-collector.md)。Nginx error 日志和“可能原因/沟通与验证”知识库不在本次
access 客观事实采集中，需按独立数据源与人工审核规则继续建设。
