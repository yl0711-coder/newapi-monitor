# 依赖与交付安全治理（2026-09-09，本地验收）

## 范围和结论

本轮仅修改 Monitor 仓库的依赖、构建版本、安全检查及 CI 发布门禁。未修改 NewAPI、生产数据库、AWS 权限、流量、生产实例或线上服务；未提交、推送、部署。已有未提交的资源生命周期功能变更保留。

原有 4 项模块级告警中，3 项 SSH 漏洞已通过依赖升级消除；1 项无修复版本的 OpenPGP 告警保留并作明确适用性判断，不宣称“所有依赖漏洞清零”。本地业务回归及下述检查通过不等于零风险，也不代替 GitHub runner 实际执行或 ECS 扩缩容灰度验收。

## 最小必要依赖变更

| 项目 | 原版本 | 新版本 | 原因 |
| --- | --- | --- | --- |
| go.mod 最低 Go | 1.25.0 | 1.26.0 | x/crypto 修复版的最低要求 |
| golang.org/x/crypto | v0.53.0 | v0.56.0 | 修复 GO-2026-6355、6354、6303 |
| golang.org/x/net | v0.56.0 | v0.57.0 | x/crypto 的最低依赖要求 |
| golang.org/x/sys | v0.46.0 | v0.47.0 | 同上 |
| golang.org/x/text | v0.39.0 | v0.41.0 | 同上 |
| hostagent 构建镜像 | golang:1.26-alpine3.23 | golang:1.26.6-alpine3.23 | 与 Monitor、nginxcollector、CI 对齐，禁止浮动补丁版本 |

没有更新 AWS SDK、Gin、GORM、SQLite 等业务依赖。Go mod tidy 后 go.mod/go.sum 仅有上表相关变化。旧/新 x/crypto 的 bcrypt.go 与 blowfish/cipher.go 对比无差异；密码登录、双密码、改密后会话失效及上游凭证加密由现有竞态回归覆盖。

## 剩余 OpenPGP 告警如何处理

- GO-2026-5932：上游明确不再维护、无修复版本。x/crypto 模块仍因 bcrypt 等合法用途需要保留，不能删除整个模块。
- `dev/check-dependency-policy.mjs` 对 Linux amd64 生产及测试依赖执行 `go list -mod=readonly -deps -test`，包括间接引入；禁止 `golang.org/x/crypto/openpgp` 及全部子包。清单为空或 Go 查询失败时拒绝放行。
- 已验证实际依赖清单 543 项，无 OpenPGP。测试覆盖直接包、子包、空清单、执行错误和部分输出，避免门禁假通过。
- 不添加该漏洞的全局忽略规则；后续若引入 OpenPGP，应改用维护中的实现，而不是放宽门禁。

### 二进制扫描的精度边界（必须保留）

实际 Monitor 镜像使用 `-ldflags='-s -w'`，没有符号表。govulncheck v1.1.4 对此退化为模块级推断，报告 GO-2026-5932 并退出非零；该原始失败结果保留，没有删除或改写为成功。

核对依据：扫描器 `internal/vulncheck/binary.go` 的 `len(bin.PkgSymbols) == 0` 分支会调用 `allKnownVulnerableSymbols`。见 [官方源码](https://github.com/golang/vuln/blob/v1.1.4/internal/vulncheck/binary.go)。

同源码、同 Go 1.26.6、Linux amd64、CGO=0 的带符号验收二进制复扫：0 符号级、0 引入包级漏洞，仍有 1 项模块级 OpenPGP 告警；与源码及依赖图一致。带符号产物仅用于交叉核对，不替换运行镜像，也不能声称是同一字节产物。两个实际采集器二进制扫描均为 `No vulnerabilities found`。

## 新增交付门禁

### 密钥扫描

- Gitleaks CLI v8.30.0 固定版本，安装在 runner 临时目录，不加入应用依赖；无需 Gitleaks Action 组织许可证或生产凭证。
- CI 拉取完整历史，扫描 `--all`，全部脱敏，并禁用源码中的 `gitleaks:allow` 绕过。异常或扫描器失败阻止发布。
- `.gitleaks.toml` 保留全部默认规则，仅豁免已人工核对的“指定文件 AND 精确模拟值”：HMAC 测试向量、内存数据库 token、请求关联 ID 和一个镜像标签。
- 不排除整个测试/文档目录、不豁免整次提交、不用历史报告作为整体基线忽略。
- `dev/check-secret-policy.mjs` 使用真实扫描器做正反控制：允许确定的测试样例；同一文件的新模拟密钥必须被拦截；把豁免的测试值放进生产文件也必须被拦截。
- 本地完整历史扫描 172 次提交：初始 23 个命中均为上述测试数据，精确豁免后 0 命中。本地可提交工作树快照初始 22 个命中，核对后 0 命中。被 gitignore 排除的本机私密运行目录不属于此次待提交源码扫描范围。
- 扫描报告只存本机私有临时目录，不提交 Git、不上传 CI secret 报告。

### 镜像 OS 漏洞扫描与发布顺序

- Trivy CLI v0.74.0，Action 固定提交 `57a97c7e7821a5776cebc9bb87c984fa69cba8f1`（0.35.0）。本机下载包与官方 SHA-256 清单匹配。
- Monitor、hostagent、nginxcollector 三个镜像均构建扫描；PR 同样执行，不在 PR 发布。
- HIGH/CRITICAL OS 漏洞阻止发布，未修复的也不忽略。UNKNOWN/LOW/MEDIUM 继续输出供审查，不能把这些告警称为零风险；如需放行例外，另行批准带原因、范围、到期时间的记录，当前未配置漏洞例外。
- Go 业务依赖继续由 govulncheck 可达性检查及 OpenPGP 禁用门禁覆盖。OS 扫描不是 Go 库扫描的替代品。
- 所有测试、密钥及三镜像安全检查全部通过，才允许任何镜像推送或运维工具发布。
- 构建扫描作业没有包写权限；发布作业加载此前扫描的镜像 tar 后打标签、推送，不二次构建。保留原有版本/分支/SHA/latest 标签和 OCI 元数据。镜像 tar 的 CI 保留期为 1 天。
- 扫描器、漏洞库下载或构建失败均保持失败，不设置 continue-on-error。网络不可达时可以换官方源重试，不能跳过扫描发布。

## 本地验收结果

| 检查 | 结果 |
| --- | --- |
| Monitor 全量 race 分片 | 1233 项，347/302/327/257，全部通过 |
| 分片耗时 | A–F 277.648s；G–N 194.370s；O–S 297.857s；T–Z 159.550s |
| 其他包 race | 按 go list 实际清单执行，全部通过，包括各采集器、monitor/public 和本地工具包 |
| Node 回归及安全门禁契约测试 | 68/68 通过 |
| go vet / golangci-lint v1.64.8 | Go 1.26.6 下通过 |
| actionlint v1.7.7 | 工作流语法/表达式检查通过；未启用其外部 shellcheck/pyflakes 集成 |
| Linux amd64 CGO=0 全包构建 | 通过 |
| 3 个默认 Dockerfile 镜像构建 | 本地成功，无 OFFLINE_RUNTIME 替代 |
| 扫描产物保存/加载 | 3 个候选镜像 docker save/load 后镜像 ID 一致；临时 tar 已清理，没有推送 |
| Trivy OS 全严重级别扫描 | 3 个镜像均为 Alpine 3.23.5；包数 18/18/17，已知漏洞均为 0 |
| 源码及带符号产物 Go 漏洞检查 | 显式 Linux amd64 源码扫描也通过：0 可达/引入包漏洞，1 项未使用 OpenPGP 模块告警；剥离符号实际 Monitor 产物的非零结果单独保留，见上文 |
| 真实密钥扫描正反控制 | 通过，无整目录放行 |

验收期间遇到 Docker Hub 获取浮动 hostagent 标签超时、Trivy 镜像源超时。hostagent 锁定与其他镜像一致的补丁版本后构建成功；Trivy 使用官方 `ghcr.io/aquasecurity/trivy-db:2` 完成新库下载及全部扫描，没有以空库/旧库结果替代。最初手写非 Monitor 测试路径 `./public` 错误（正确为 `./monitor/public`）；已改按实际包清单完整重跑通过，原失败日志仍保留。

证据目录：`/private/tmp/monitor-security-20260909.SFlv7A/`。该目录及数据库、备份、运行配置、扫描器下载包均不应提交。临时提取容器未启动，提取后二者（容器和空匿名卷）已清理；现有运行容器未变更。

## 发布前剩余步骤与回滚

1. 本地检查不等于 GitHub CI 已运行；提交前只审查并提交代码/测试/文档，检查 git diff 与 staged 文件清单。
2. GitHub CI 需实际完成新增的历史扫描、三个镜像检查和扫描产物跨作业传递，任何失败都不进入发布。
3. 资源生命周期/ECS 日志适配的功能范围仍沿用各专项报告；本轮安全修复不代表已完成可信 ECS 日志身份、动态 expected sources、全链路游标隔离或真实 3→5→3 测试。
4. 如后续上线需要回滚，恢复此前已经验收的镜像版本即可；本轮没有数据库迁移。不得通过关闭安全门禁来处理新发现漏洞。单独回退依赖时必须同时恢复成对的 go.mod/go.sum，并明确重新引入的 SSH 风险；不建议这么做。
