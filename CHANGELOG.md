# Changelog

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/) 与 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Added
- new-api 上游稳定性监控:**分组 / 渠道 / 模型** 维度的成功率、三态(成功 / 异常 / 失败)、响应耗时(P50/P95、TTFB/TTFT)、出字速度(tok/s)。
- 邮件报警:错误率 / 错误突发 / 异常成簇 / 采样掉线 规则,阈值可配;SMTP 可手填或一键同步 new-api 的发件配置。
- 登录鉴权复用 new-api 用户身份,按角色分权(管理员看、超管改)。
- 动态站点名:默认取 new-api 的 `system_name`(取一次存下、可手改),📈 favicon。
- Docker 镜像;GitHub Actions 自动 `go vet` + `go test` + `golangci-lint`,通过后发布镜像到 GHCR。
