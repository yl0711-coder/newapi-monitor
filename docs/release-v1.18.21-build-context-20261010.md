# v1.18.21 构建上下文修复与提交前验证

## 范围

- 日期：2026-10-10；基线：58432248076e13da19d644ce0ab70ac3d7c91431；分支：release/v1.18.1。
- 用户授权修复构建、提交推送，并确认改用新 tag v1.18.21。v1.18.20 已被另一提交占用，本轮不覆盖；同时保留已推送的 v1.18.19，不合并 main、不部署、不改动运行中的服务。
- 仅修改 .dockerignore、CI 构建上下文检查和对应 Node 回归；不改业务 Go/前端代码、数据库、运行配置或生产基础镜像版本。

## 已确认根因

v1.18.19 的普通测试、12 个 Monitor race 分组和密钥扫描通过，但主镜像在 go build 阶段失败：

    no required module provides package github.com/yl0711-coder/newapi-monitor/internal/observability

原因是生产 .dockerignore 默认拒绝未列出的输入，新增 internal/observability 未加入白名单。该包还通过 go:embed 依赖 schema/observability.v1.schema.json，必须一起放行。

原始失败证据：https://github.com/yl0711-coder/newapi-monitor/actions/runs/38024485416/job/114135125288

## 修复

- 放行 internal/observability 下的生产 Go 源码以及固定的 observability.v1.schema.json；不开放任意 JSON 文件。
- 保留默认拒绝规则、测试源码排除和所有凭据/运行数据排除规则。
- CI 的真实 Docker 上下文检查增加 5 个必需文件断言及 3 个排除断言；新增排除探针覆盖测试 Go 文件、包目录临时 JSON、Schema 目录临时 JSON。
- 增加 2 个 Node 回归，防止白名单和对应 CI 正反向检查再次遗漏。

## 本地验证

| 检查 | 结果 | 证据或限制 |
|---|---|---|
| 原有安全工作流回归 | 通过 | 修改前 4/4 通过 |
| 新增回归的失败验证 | 通过 | 修复前新增 2 项按预期失败，分别指出白名单缺失和 CI 包含检查缺失 |
| 修复后安全工作流回归 | 通过 | 6/6 通过 |
| 真实 Docker 上下文正反向检查 | 通过 | 临时 Git 快照中复现旧规则缺包；修复后 CI 同一组 39 项文件断言全部通过 |
| Docker 筛选后源码编译 | 通过 | BuildKit 导出实际过滤后的上下文，再用 Go 1.26.9 离线编译 linux/amd64、CGO_ENABLED=0，go build -trimpath 成功 |
| 前端全套原始执行 | 有限通过 | 284 项：272 通过、6 跳过、6 项既有 Windows CRLF/LF 匹配失败 |
| 换行环境诊断 | 通过 | 仅在内存中把 CI YAML 转为 LF 后，上述 6 项全部通过；未修改测试来绕过失败 |
| 差异格式 | 通过 | git diff --check |

筛选后 Linux 二进制 SHA256：981de81effc772f4f891a8b84fd570b43b34c54e9b32c1e1db6f4a93439fd88f。

本机配置的 Docker 镜像代理域名无法解析，官方 registry 直连也超时，因此本地不能宣称标准生产 Dockerfile 构建完成。上下文测试仅使用已有 Alpine 3.20 执行文件断言，不发布该审计镜像；生产配置仍为 Go 1.26.9 / Alpine 3.23。未修改 Docker 全局配置。

## 三个视角与交付条件

- 技术：本地检查通过。修复限定在构建输入和回归，不更改业务逻辑或放宽安全门禁。
- 运维：本轮变更边界通过。全部探针在独立临时快照中运行；没有读取真实凭据文件作为构建输入，没有操作线上或 8204 服务。
- 测试：本地有限通过。Windows 换行限制已定位，标准生产镜像构建及完整安全门禁仍须由新提交的远端 Linux CI 验证。
- 本记录只描述提交前证据。最终交付以 v1.18.21 对应提交的完整 CI 实际成功为准，包括全部测试、race、密钥扫描、生产镜像构建及镜像安全检查；不能只以推送成功作为完成标准。合并 main 和上线由用户同事负责。
