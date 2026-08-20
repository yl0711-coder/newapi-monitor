# 客户端技术结果证据（Monitor-only）

## 不可突破的边界

- NewAPI 源码不做任何修改。
- 默认不要求修改 NewAPI 的 Nginx 配置。
- Monitor 既有的生产 MySQL 连接仍只读；客户端证据只写 Monitor 本地 SQLite。
- 没有客户端证据时，页面必须显示“不可判断”，不得用 NewAPI 日志、HTTP 200 或服务端结果填成100%。

## 两条独立口径

1. **历史日志推断**：继续复用现有 Monitor 对 NewAPI `logs` 的有界只读采样。它可用于分组、渠道、模型的趋势和错误签名，但不声称代表客户端已接收完整结果。
2. **受控客户端技术结果**：可选的 SDK/客户端直接向 Monitor 报告 `request_started` 和 `request_outcome`。只在已签收的 `family@version` cohort 内计算，不外推全站。

## 配置

三项全部留空即关闭，不影响 Monitor 任何既有功能：

```text
MONITOR_CLIENT_EVIDENCE_TOKEN=<至少32字节的独立随机值>
MONITOR_CLIENT_EVIDENCE_HMAC_SECRET=<至少32字节的另一独立随机值>
MONITOR_CLIENT_EVIDENCE_ALLOWED_CLIENTS=controlled-sdk@1.0.0
```

## 上报契约

`POST /internal/client-outcomes`，`Authorization: Bearer <token>`。最多500个事件/批：

```json
{
  "client_family": "controlled-sdk",
  "batch_id": "batch-unique-id",
  "events": [
    {
      "version": 1,
      "event_id": "event-unique-id",
      "event_type": "request_started",
      "occurred_at_ms": 1787220000000,
      "request_id": "raw-request-id",
      "logical_request_key": "optional-retry-chain-key",
      "client_version": "1.0.0",
      "retry_index": 0,
      "protocol": "openai_responses",
      "model": "optional-model"
    }
  ]
}
```

`request_outcome` 的 `outcome` 只允许：

- `succeeded`
- `client_timeout`
- `transport_failure`
- `protocol_failure`
- `semantic_failure`
- `user_cancelled`

`error_signature` 只允许不含空格的有界规范化枚举，不接受原始报错、输入、输出或用户内容。

Monitor 在请求内存中将原始 `request_id` 和 `logical_request_key` 立即 HMAC，SQLite 中只保存64位派生值。批次和事件均幂等；同 ID 不同内容直接冲突，不覆盖原证据。

## 展示与判定

- 回传覆盖 = 已有结果的 request / 已报告开始的 request。
- 技术成功率 = `succeeded` / 技术结果。`user_cancelled` 单列，不进技术成功率分母。
- 只有开始数≥20、技术结果≥20、覆盖≥95%、无互斥终态冲突时才标记“可判断”。
- 客户端指标不自动给渠道着色，因为在不修改 NewAPI 的前提下，Monitor 无法可靠获得每个客户端请求的内部渠道归属。
- 原始客户端事件保留90天，幂等批次回执保留100天。

## 能力限制

在“NewAPI 一个字不改”的约束下，Monitor 无法自动获得 NewAPI 内部的 `response.completed`、最后一次 Flush、上游 attempt 时间线和精确渠道归属。界面不会暗示这些事实已经存在。
