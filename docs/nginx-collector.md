# Nginx 入口层旁路采集

该能力默认关闭。它用于补充“请求是否到达入口、HTTP 状态、入口与 upstream 耗时”这层客观事实，不替代 NewAPI 使用日志，也不自动判断责任方。

## 安全边界

采集器只读取一份专用 JSON access log，在节点本地先脱敏并按分钟聚合。Monitor 只接收：节点、归一化路径、HTTP 方法、HTTP/upstream 状态、请求数、耗时汇总、响应字节数和“是否携带 Request ID”的计数。

即使没有新请求，采集器也会每分钟发送一个不含业务数据的空心跳；因此页面上的“采集器正常”不会把真实零流量误报为采集中断。

禁止写入专用日志或发送到 Monitor 的字段包括：客户端 IP、X-Forwarded-For、Authorization、API Key、Cookie、完整 query、请求体、响应体、User-Agent、Referer、Request ID 原值及 upstream 地址。

## Nginx 专用日志格式

下面的日志文件必须与现有 access/error log 分开；`$uri` 不带 query，`$request_id` 只由采集器转换成“是否存在”，原值不会离开节点。

```nginx
log_format nexus_monitor escape=json '{'
  '"msec":"$msec",'
  '"request_method":"$request_method",'
  '"uri":"$uri",'
  '"status":"$status",'
  '"request_time":"$request_time",'
  '"upstream_status":"$upstream_status",'
  '"upstream_response_time":"$upstream_response_time",'
  '"bytes_sent":"$bytes_sent",'
  '"request_id":"$request_id"'
'}';

access_log /var/log/nginx/nexusapi_access.jsonl nexus_monitor;
```

先执行 `nginx -t`，再平滑 reload。不要改动现有业务路由、超时、upstream 或原日志。

## 启用顺序

1. 先在本地用模拟日志验证采集器、幂等重试、轮转和 Monitor 页面。
2. 节点创建专用日志并确认不含敏感字段；确认采集器处理速度持续高于日志增长速度。
3. 部署 `docker-compose.nginxcollector.yml`，日志目录只读挂载，游标用独立小卷持久化。
4. Monitor 配置相同的 `MONITOR_INGEST_TOKEN`，显式设置节点白名单。
5. 最后设置 `MONITOR_NGINX_ENABLED=true` 并重建 Monitor。

任一步失败都可把 `MONITOR_NGINX_ENABLED=false`；模型监控、用户用量、渠道管理和 NewAPI 请求链路不依赖该采集器。

## 日志轮转前提

当前采集器通过 inode 和字节偏移识别文件轮转。只有在旧文件内容已经读取完毕后切换到新 inode，才能保证连续采集；它不会追读已经改名的旧 inode。上线前必须为专用日志采用“延迟压缩/延迟删除”策略（例如 logrotate 的 `delaycompress`），并监控采集延迟，确保轮转时游标已经追平文件尾部。若无法满足这一前提，不应启用采集器。

该限制只影响尚未启用的 Nginx 旁路数据，不影响 NewAPI 请求、使用日志、模型监控、用户用量或渠道管理。后续若要容忍采集器长时间落后，需要把“轮转后继续追读旧 inode”作为独立功能开发和验收，不能仅依赖当前轮转测试。
