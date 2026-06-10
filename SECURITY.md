# 安全策略 / Security Policy

## 支持的版本 / Supported Versions

| 版本 | 安全更新 |
|---|---|
| 1.x | ✅ |
| < 1.0 | ❌ |

## 报告漏洞 / Reporting a Vulnerability

请**不要**通过公开 issue 报告安全漏洞。请发送邮件至仓库所有者(见 GitHub 主页),或使用 GitHub 的私密漏洞报告(Security → Report a vulnerability)。我们会尽快响应并在修复后致谢。

Please do **not** report security vulnerabilities through public issues. Email the repository owner (see GitHub profile) or use GitHub private vulnerability reporting. We aim to respond promptly and will credit reporters after a fix.

## 设计上的安全边界 / Security Model

本项目是一个**只读旁路监控**,内置多重边界:

- **只读、最小权限**:采样器只对 new-api 库做 `SELECT`。建议为其单独建只读账号,**仅授予 `logs`、`channels` 表的 `SELECT`**(见 README「只读账号」)。不写、不改 new-api,也不改其源码。
- **镜像不含任何密钥**:DSN、会话密钥、SMTP 凭证均通过环境变量注入;SMTP 密码等敏感信息前端永不回显。
- **对外看板强脱敏**:公开看板(`/status`、`/public/status`)由独立 `monitor/public` 包提供,**绝不引用内部结构**,只输出 模型名/状态/可用率/延迟/状态条;**绝不输出**渠道名/ID/IP、成本/配额、令牌/用户、请求量/QPS、错误详情。看板拉取可见分组走 new-api 的匿名 `/api/pricing`,**不读含密钥的 `options` 表**。
- **生产前置反代**:生产建议在前面放一层 HTTPS 反向代理(nginx / Caddy),并将内部监控(需登录)与对外看板(无登录)用不同子域名隔离。

This project is a **read-only, side-channel monitor**: the sampler only issues `SELECT` against the new-api database (grant a read-only account `SELECT` on `logs`/`channels` only), never writes to or modifies new-api, ships no secrets in the image, and the public status board is served by an isolated package that emits only sanitized fields.
