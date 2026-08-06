# Monitor 生产运维手册

本文只涉及 `newapi-monitor` 容器及其独立数据卷，不修改 NewAPI 数据库，也不操作其他容器。

## 数据边界

- `NEWAPI_LOG_DSN` 必须使用只读账号。
- Monitor 只向自己的 `MONITOR_STORE_PATH` SQLite 写入数据。
- SQLite 包含稳定性历史、已删除渠道的最后快照、渠道倍率版本和管理端配置，应视为业务数据。
- Redis 只用于可选用量缓存；不可用时自动降级，不能作为恢复来源。

## 上线前检查

1. 确认 `/data` 使用具名数据卷，而不是容器可写层。
2. 确认 `.env` 中固定配置 `MONITOR_SESSION_SECRET`，且不进入仓库或镜像。
3. 确认只读 DSN 仅有 `SELECT` 权限。
4. 确认 `GET /health` 返回 200。
5. 管理员登录后检查 `GET /stability/health`：本地库可达、主采样有成功时间、问题采集无长期积压。

## 一致性备份

不要在 Monitor 正常写入 SQLite 时只复制 `monitor.db`，否则可能遗漏 WAL 中的数据。

最稳妥的维护窗口方案是只短暂停止 Monitor 容器，然后备份整个数据卷：

```bash
docker compose stop monitor
docker run --rm \
  -v newapi-monitor_monitor_data:/source:ro \
  -v "$PWD/backups:/backup" \
  alpine:3.23 \
  tar -C /source -czf "/backup/monitor-$(date +%Y%m%d-%H%M%S).tar.gz" .
docker compose start monitor
```

具名卷实际名称可能带 Compose 项目前缀，执行前用以下命令只读确认：

```bash
docker volume ls
docker inspect newapi-monitor --format '{{range .Mounts}}{{println .Name .Destination}}{{end}}'
```

建议每天备份，至少保留 7 个日备份和 4 个周备份。备份文件应放在实例外或对象存储中，并限制访问权限。

## 恢复演练

恢复前先保留当前数据卷副本。恢复操作具有覆盖性，只能在明确维护窗口中执行：

1. 停止 `monitor` 容器。
2. 创建新的空数据卷，不直接覆盖原卷。
3. 将备份解压到新卷。
4. 用临时 Compose 覆盖将 Monitor 指向新卷。
5. 启动后检查 `/health`、`/stability/health`、倍率版本和历史趋势。
6. 验证通过后再切换正式 Compose；原卷保留到验收完成。

## 功能降级与回滚

若稳定性新功能异常，但原模型监控、用户用量和服务端监控仍需继续使用：

```dotenv
MONITOR_STABILITY_ENABLED=false
```

修改后只重建 `monitor` 容器。该开关停止稳定性长期汇总和原始错误采集，不删除已有表和历史数据。

镜像回滚时：

1. 先完成数据卷备份。
2. 只修改 Monitor 镜像标签并重建 Monitor 容器。
3. 不删除数据卷；本次新增迁移均为追加表或追加列，旧版本会忽略不认识的表。
4. 验证用户用量、模型监控、服务端监控和登录。

## 日常观测

- 主采样超过 3 个采样周期没有成功：按降级处理并检查只读数据库连通性。
- 原始问题采集出现积压：页面问题排行只包含已完整分钟；采集器会按固定预算续跑，不应通过提高单次上限盲目加压生产库。
- 原始错误签名延迟约 10 分钟定稿，用于容纳 360 秒长请求和日志落库抖动；这不影响稳定性主报表和原模型监控的新鲜度。
- 监控数据卷持续增长：检查问题签名数量、保留天数和备份是否成功，不直接删除 SQLite 文件。
- 90 天报表采用首屏轻量、分组详情按需加载；若接口变慢，先检查本地 SQLite 和容器内存，不应在页面请求中增加生产库查询。
