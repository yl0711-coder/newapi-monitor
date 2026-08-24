# 上游日志 SQLite 表结构

当前 Monitor 保存的是上游消费的时间桶汇总，不保存请求体、响应体、API Key 或用户内容。

## 表：`channel_upstream_usage_hours`

主键：`domain + hour_ts`

| 字段 | SQLite 类型 | 说明 |
|---|---|---|
| `domain` | `TEXT` | 上游主域名，例如 `4sapi.com`、`aicodewith.com` |
| `hour_ts` | `INTEGER` | 时间桶起点，Unix 时间戳（秒） |
| `bucket_seconds` | `INTEGER` | 时间桶长度；小时数据为 `3600`，日数据为 `86400` |
| `requests` | `INTEGER` | 时间桶内的上游请求数 |
| `tokens` | `INTEGER` | 时间桶内的 Token 总数 |
| `quota` | `REAL` | 上游返回的原始额度或消费值 |
| `cost_usd` | `REAL` | 换算后的美元消费金额 |
| `fetched_at` | `INTEGER` | 本次数据成功获取并写入的时间，Unix 时间戳（秒） |
| `provider` | `TEXT` | 上游类型，例如 `newapi`、`sub2api`、`aicodewith` |

逻辑建表结构：

```sql
CREATE TABLE channel_upstream_usage_hours (
    domain         TEXT    NOT NULL,
    hour_ts        INTEGER NOT NULL,
    bucket_seconds INTEGER NOT NULL,
    requests       INTEGER NOT NULL,
    tokens         INTEGER NOT NULL,
    quota          REAL    NOT NULL,
    cost_usd       REAL    NOT NULL,
    fetched_at     INTEGER NOT NULL,
    provider       TEXT    NOT NULL,
    PRIMARY KEY (domain, hour_ts)
);

CREATE INDEX idx_channel_upstream_usage_hours_fetched_at
    ON channel_upstream_usage_hours (fetched_at);
```

示例：

```json
{
  "domain": "4sapi.com",
  "hour_ts": 1787475600,
  "bucket_seconds": 3600,
  "requests": 126,
  "tokens": 3528100,
  "quota": 482351.0,
  "cost_usd": 0.964702,
  "fetched_at": 1787479226,
  "provider": "newapi"
}
```

## 当前边界

这张表可以供“上游消费统计、成本与毛利分析”共用，但目前没有逐请求日志字段，例如 `request_id`、HTTP 状态码、错误信息、模型、渠道 ID、延迟和重试次数。后续若要做请求链路和错误排查，应另建逐请求事件表，不应改变这张汇总表的粒度。
