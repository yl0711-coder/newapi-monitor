# AI 线上只读 SSH 隧道测试指南

## 1. 用途和边界

本文档用于让 AI 在本机运行 `newapi-monitor` 的受控测试，通过 SSH 隧道以生产 MySQL 的 `nexus_ro` 账号读取真实数据。

这是“本机代码 + 本机容器/工具 + 线上只读数据源”，不是部署，不会替换线上 Monitor，也不得修改 NewAPI、Nginx、线上容器或生产数据。

```text
本机 Go 检查工具  -> 127.0.0.1:13316             \
                                                      SSH -> Ubuntu-1 -> 托管 MySQL
本机 Monitor 容器 -> host.docker.internal:13316       /
                              仅允许 nexus_ro / nexusapi
```

生产库是只读也不等于可以无限查询。所有测试都必须小窗口、低并发、可中断，不得将生产库用作压测库。

## 2. AI 必须遵守的执行契约

1. 优先使用仓库内的 `dev/ai-production-readonly-access.sh`；它会调用 `dev/run-local-production-readonly.sh` 管理隧道。不自行拼接 `ssh -L`。
2. 只能使用 `nexus_ro` 账号连接 `nexusapi`；脚本检查不通过时必须停止，不得绕过。
3. 优先使用 `tools/readonly-inspect`，不得用通用 MySQL 客户端临时执行未审核 SQL。
4. 禁止任何写操作，包括 `INSERT` / `UPDATE` / `DELETE` / `REPLACE` / DDL / `ANALYZE TABLE` / `OPTIMIZE TABLE` / `LOCK TABLES` / `KILL` / `SET GLOBAL`。
5. 禁止压测、高并发、全表导出、长时间范围聚合、无 `LIMIT` 的原始日志查询和反复刷新大范围页面。
6. `EXPLAIN ANALYZE` 会真正执行 SQL；除非先确认索引命中、时间窗口足够小且设置了查询超时，否则不得使用。
7. 禁止读取或输出令牌、API Key、密码、完整 DSN、SSH 私钥、原始提示词或用户隐私数据。
8. 禁止使用 `cat` / `sed` / `printenv` / `env` 展示凭据配置文件，也不得开启 `set -x`。
9. 测试结束或命令失败后都必须关闭隧道；不得让其长期留在后台。
10. 任何需要建索引、改生产配置、重启容器或扩大查询范围的结论，只能作为建议报告，不得执行。

## 3. 本机已有的受控文件

默认使用以下文件，它们均不应进入 Git：

- `~/.config/newapi-monitor/local-acceptance.env`：保存本机验收配置和指向 `host.docker.internal:13316` 的 `nexus_ro` DSN。
- `../NexusAPI/deploy/release-rc19/release.env`：保存 `MONITOR_SSH_TARGET` 和 `SSH_IDENTITY_FILE` 的本机引用。
- `dev/run-local-production-readonly.sh`：校验账号、数据库和本地端口，再建立/关闭回环 SSH 隧道。
- `dev/ai-production-readonly-access.sh`：供 AI 使用的一键入口，只暴露开启、关闭和有界查询，不接受任意 SQL。
- `tools/readonly-inspect`：单连接、有界、只读的真实数据检查工具。

如果换到另一台电脑，仅有本文档和 Git 代码是不够的；必须由管理员另行配置上述本机文件和 SSH 权限。不要通过 AI 对话、Git、聊天工具或本文档传递凭据。

## 4. 建立隧道并做最小预检

在仓库根目录执行 AI 专用入口：

```bash
cd '/Users/yl/Documents/work/AI孵化器/脚本/newapi-monitor'
dev/ai-production-readonly-access.sh start
```

`start` 会通过底层 `preflight` 完成：

1. 拒绝非 `nexus_ro`、非 `nexusapi` 或非 `host.docker.internal:13316` 的本机 DSN。
2. 经 SSH 只在内存中解析线上 Monitor 的数据库 `host:port`，不输出密码或完整 DSN。
3. 将本机 `127.0.0.1:13316` 转发到线上 MySQL。
4. 以 `nexus_ro` 执行一条极小的 `SELECT` 探针。

正常结果应包含：

```text
readonly tunnel: started on 127.0.0.1:13316
database preflight: nexus_ro SELECT succeeded
```

如果隧道已存在，第一行可能是 `healthy`。任何其他错误都应立即停止测试，不修改脚本以绕过门禁。

如果只需要验证权限和连通性，不希望隧道留在后台，执行：

```bash
dev/ai-production-readonly-access.sh probe
```

该命令会自动建立、验证并关闭隧道。

## 5. 使用受控只读工具

对 AI 而言，优先使用下面的一键命令。它们会自动建立隧道、通过只读预检、执行有界查询，无论成功还是失败都关闭隧道：

```bash
dev/ai-production-readonly-access.sh channel '75'
dev/ai-production-readonly-access.sh request '<REQUEST_ID>'
dev/ai-production-readonly-access.sh usage '<USER_ID>' '<UTC_FROM_UNIX>' '<UTC_TO_UNIX>'
dev/ai-production-readonly-access.sh raw-user '<USER_ID>' '<UTC_HOUR_UNIX>'
dev/ai-production-readonly-access.sh raw-email '<USER_EMAIL>' '<UTC_HOUR_UNIX>'
```

该入口故意不接受 SQL 字符串或任意 shell 命令。除非正在开发/审核只读工具本身，AI 不需要直接使用下面的底层写法。

不要 `source ~/.config/newapi-monitor/local-acceptance.env`。DSN 通常包含 `&`，直接 `source` 既可能解析失败，也会扩大 shell 执行风险。下面的写法只提取 `NEWAPI_LOG_DSN` 的值传给单个进程，不会打印它。

### 5.1 按渠道 ID 查配置

```bash
NEWAPI_LOG_DSN="$(awk 'index($0, "NEWAPI_LOG_DSN=") == 1 {sub(/^NEWAPI_LOG_DSN=/, ""); sub(/\r$/, ""); print; exit}' ~/.config/newapi-monitor/local-acceptance.env)" \
GOCACHE=/private/tmp/newapi-monitor-readonly-gocache \
go run ./tools/readonly-inspect -channels '75'
```

可以传递逗号分隔的多个 ID，但一次不要扩大到不相关渠道。工具不输出渠道 Key。

### 5.2 按 Request ID 查日志链

```bash
NEWAPI_LOG_DSN="$(awk 'index($0, "NEWAPI_LOG_DSN=") == 1 {sub(/^NEWAPI_LOG_DSN=/, ""); sub(/\r$/, ""); print; exit}' ~/.config/newapi-monitor/local-acceptance.env)" \
GOCACHE=/private/tmp/newapi-monitor-readonly-gocache \
go run ./tools/readonly-inspect -requests '<REQUEST_ID>'
```

Request ID 可以一次传入多个，但不得超过 100 个。优先从 1 个开始。

### 5.3 按小时核对指定用户的用量

`from` 和 `to` 必须是 UTC 整点 Unix 时间戳，区间为 `[from, to)`，最多 24 小时，最多 200 个用户。工具内部按小时分割，单查询最长 5 秒，每小时之间主动间隔 15 秒。

```bash
# macOS 上先生成 UTC 整点时间戳；该命令不访问数据库。
date -u -j -f '%Y-%m-%d %H:%M:%S' '2026-08-21 00:00:00' '+%s'
date -u -j -f '%Y-%m-%d %H:%M:%S' '2026-08-21 01:00:00' '+%s'

NEWAPI_LOG_DSN="$(awk 'index($0, "NEWAPI_LOG_DSN=") == 1 {sub(/^NEWAPI_LOG_DSN=/, ""); sub(/\r$/, ""); print; exit}' ~/.config/newapi-monitor/local-acceptance.env)" \
GOCACHE=/private/tmp/newapi-monitor-readonly-gocache \
go run ./tools/readonly-inspect \
  -usage-users '<USER_ID>' \
  -usage-from '<UTC_FROM_UNIX>' \
  -usage-to '<UTC_TO_UNIX>'
```

先测 1 个用户、1 小时；只有该次查询快速成功且没有增加线上负载，才能逐步放大。

### 5.4 验证 Usage 原始日志分页路径

该模式每次仅查 1 个用户、1 个 UTC 小时，最多 3 页×1,000 行，执行两遍哈希比对，查询之间至少间隔 2 秒。它用来验证分页完整性，不是导出工具。

```bash
NEWAPI_LOG_DSN="$(awk 'index($0, "NEWAPI_LOG_DSN=") == 1 {sub(/^NEWAPI_LOG_DSN=/, ""); sub(/\r$/, ""); print; exit}' ~/.config/newapi-monitor/local-acceptance.env)" \
GOCACHE=/private/tmp/newapi-monitor-readonly-gocache \
go run ./tools/readonly-inspect \
  -raw-page-email '<USER_EMAIL>' \
  -raw-page-hour '<UTC_HOUR_UNIX>'
```

也可用 `-raw-page-user '<USER_ID>'` 代替 `-raw-page-email`，两者只能选一个。输出中 `consistent=true` 表示两遍分页的行数、末尾游标和内容哈希一致；`complete=false` 只表示该小时超过了受控工具的 3,000 行上限，不能据此认定数据不完整。

## 6. 运行本机 Monitor 候选容器

如果不只是做精确查询，而是要用浏览器验收 Monitor/Usage 页面，必须使用已给定的不可变候选镜像 digest。不得把本地可变 tag 冒充候选镜像，也不得修改脚本放宽检查。

```bash
cd '/Users/yl/Documents/work/AI孵化器/脚本/newapi-monitor'

docker volume create newapi-monitor-local-data
docker volume create newapi-monitor-local-backup

MONITOR_PROD_READONLY_IMAGE='ghcr.io/yl0711-coder/newapi-monitor@sha256:<CANDIDATE_DIGEST>' \
  dev/run-local-production-readonly.sh up

dev/run-local-production-readonly.sh status
```

该模式的候选代码、SQLite、备份和 Redis 都在本机；只有来源 MySQL 读取经过 SSH 隧道。邮件、上游账户同步、基础设施探测和 Nginx 写入在此模式中强制关闭。

执行 `status` 要求本地 Monitor 容器已经启动；如果只建了隧道，不要用 `status` 判定隧道失败，直接重复执行 `tunnel-start` 即可做幂等健康检查。

## 7. 结束和清理

只建立了隧道时：

```bash
dev/ai-production-readonly-access.sh stop
```

已启动本地 Monitor 容器时：

```bash
dev/run-local-production-readonly.sh stop
```

`stop` 会停止与该仓库精确 Compose 组合匹配的本地 Monitor，然后关闭 SSH 隧道；不会停止线上 Monitor。禁止使用 `docker compose down -v` 或删除本地数据/备份卷。

清理后可做一次无敏感信息的端口检查：

```bash
nc -z 127.0.0.1 13316
```

清理成功时该命令应返回非 0。不要用 `ps e` 或其他会显示进程环境变量的命令检查。

## 8. 故障处理

| 现象 | 正确处理 | 禁止处理 |
|---|---|---|
| `required file is missing` | 停止，请管理员在本机补齐受控配置 | 向聊天中索要或输出密码/私钥 |
| `database user must be nexus_ro` | 立即停止，报告本机配置错误 | 改用线上写账号 |
| `127.0.0.1:13316 is already occupied` | 用 `lsof -nP -iTCP:13316 -sTCP:LISTEN` 只读确认进程，由管理员判断 | 盲目 `kill -9` 或改为局域网监听 |
| SSH 连接失败 | 检查当前网络/VPN 和授权，再重试一次 | 关闭 host key 检查或将私钥复制进仓库 |
| 查询超时 | 立即缩小为 1 用户×1 小时，仍失败则关闭隧道并报告 | 去掉超时、放大范围或并发重试 |
| `Monitor service is not running` | 这只表示本地容器未起；隧道状态用 `tunnel-start` 再检查 | 重启线上容器 |
| 页面可用但数据延时 | 先检查本地 SQLite/facts 水位和后台同步状态 | 直接反复请求线上原始日志 |

任何异常收尾时，先执行：

```bash
dev/ai-production-readonly-access.sh stop
```

## 9. AI 的建议执行顺序

1. 只读查看 `git status --short`，确认不覆盖现有改动。
2. 运行 `dev/ai-production-readonly-access.sh probe`，确认只读权限和连通性，该命令会自动关闭隧道。
3. 先用 `readonly-inspect` 做 1 个 ID 或 1 用户×1 小时的最小查询。
4. 记录命令用时、返回行数、是否超时和业务结论；不记录凭据。
5. 需要验收页面时，仅使用技术负责人提供的候选镜像 digest 启动本地容器。
6. 完成后执行 `dev/ai-production-readonly-access.sh stop`；如果启动过容器，执行 `dev/run-local-production-readonly.sh stop`。
7. 输出测试结论和证据范围，不做“一次小窗口成功即代表全量和生产性能通过”的外推。

## 10. 测试报告模板

```markdown
# 线上只读数据测试报告

- 代码版本/commit：
- 候选镜像 digest（如适用）：
- 测试时间（Asia/Shanghai）：
- 测试模式：精确 ID / 1 用户×1 小时 / 本机候选容器
- 查询范围：
- 查询数/页数/返回行数：
- 最长单查询用时：
- 是否超时：
- 一致性结果：
- 业务结论：
- 局限性：
- 隧道和本地容器是否已停止：
```

报告中不得包含 DSN、密码、私钥路径、令牌/API Key 或完整用户原始内容。

## 11. 与实现对应的仓库文件

- `dev/run-local-production-readonly.sh`
- `dev/ai-production-readonly-access.sh`
- `tools/readonly-inspect/main.go`
- `docker-compose.local-production-readonly.yml`
- `docker-compose.local-acceptance.yml`
- `docs/local-docker-testing.md`
- `docs/monitor-operations.md`
