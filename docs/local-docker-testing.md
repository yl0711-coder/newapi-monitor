# 本地 Docker 启动与测试

本文用于在开发机启动 `newapi-monitor`，验证镜像、配置、持久化存储和受控数据链路。默认使用本机 Docker 卷、合成数据和回环地址；不要将线上 DSN、管理员令牌、上游凭据或生产 Docker 卷带入本地测试。

本仓库提供三种模式。日常开发从“完全离线 Smoke Test”开始；需要验证用量事实、缓存和来源 SQL 时使用“隔离合成数据验收”。“本机只读真实数据”仅用于合成验收通过后的受控效果验证，不是常规开发入口。

| 模式 | 是否读取真实数据 | 可以验证 | 不可以验证 |
| --- | --- | --- | --- |
| 完全离线 Smoke Test | 否 | 容器启动、SQLite 数据卷、Redis、`/live`、`/ready` | 登录、稳定性数据、Usage 报表 |
| 隔离合成数据验收 | 否，仅使用临时 MySQL | 只读来源、事实同步、发布水位、缓存和健康状态 | 线上数据规模、反向代理、真实上游 |
| 本机只读真实数据 | 是，仅经回环 SSH 隧道与只读账号 | 候选版本在真实数据形态下的只读效果 | 生产发布、性能容量结论、反向代理语义 |

## 前置条件

- Docker Engine / Docker Desktop 与 Docker Compose v2 可用；建议先确认 `docker compose version`。
- 完全离线模式只需 Docker 和 `curl`。合成数据模式还需要本机 Go 工具链，用于运行仓库内的合成数据加载器。
- 本机端口 `8100`、`8101`、`17379` 和（合成数据模式）`13316` 未被占用。所有端口均绑定 `127.0.0.1`，不会向局域网公开。
- 本地运行库必须使用 Docker volume，不能把运行中的 SQLite/WAL 放在 macOS bind mount。跨宿主机与 Linux 容器的文件锁语义不稳定，可能损坏数据库。

首次运行前创建两份外部卷。数据卷与备份卷必须不同；后续停止服务时保留它们，避免误删测试数据和备份。

```bash
cd 脚本/newapi-monitor

docker volume create newapi-monitor-local-data
docker volume create newapi-monitor-local-backup
```

> 不要执行 `docker compose down -v`，也不要随意删除上述两个 volume。需要彻底重置时，应先确认没有需要保留的本地备份，再单独删除明确的卷名。

## 最快启动：日常本地开发

首次接手项目时，只需要在仓库根目录执行下面一条命令。脚本会自动创建所需 Docker volume、构建镜像、启动 Redis 和 Monitor，并等待 `/live`、`/ready` 两个健康检查成功。

```bash
cd 脚本/newapi-monitor
./dev/run-local-dev.sh up
```

常用操作也收敛在同一个入口中：

```bash
# 查看容器状态与健康检查
./dev/run-local-dev.sh status

# 持续查看 Monitor 日志（按 Ctrl+C 退出日志，不会停止服务）
./dev/run-local-dev.sh logs

# 停止容器，保留本地 SQLite 数据与备份卷
./dev/run-local-dev.sh stop
```

这个入口固定使用完全离线模式：不会读取生产 MySQL、NewAPI 主站、上游账户或任何凭据。第一次构建会因下载基础镜像和编译而较慢；镜像已构建后，后续启动通常只需数秒。离线模式的管理端与门户地址会打印在终端中，但由于没有 NewAPI 身份来源，登录和业务报表不可用，这是设计边界而不是故障。

需要修改页面、路由、SQLite 存储、Redis 缓存、健康检查或容器配置时，优先使用此模式。需要验证来源 SQL、用量事实同步和缓存读写时，再进入下一节的隔离合成数据验收；不要为了看到真实数据而把本地环境改连生产。

## 模式一：完全离线 Smoke Test

这是最快、最安全的启动方式。它通过 `MONITOR_LOCAL_SNAPSHOT_ONLY=true` 显式关闭来源 worker，因此即使没有 `NEWAPI_LOG_DSN` 也能启动；不会把“连接失败”悄悄降级为本地模式。

```bash
cd 脚本/newapi-monitor

MONITOR_ACCEPTANCE_IMAGE=newapi-monitor:local-snapshot \
  docker compose \
    -f docker-compose.local-acceptance.yml \
    -f docker-compose.local-snapshot.yml \
    up -d --build redis monitor
```

服务启动后检查容器和健康端点：

```bash
docker compose \
  -f docker-compose.local-acceptance.yml \
  -f docker-compose.local-snapshot.yml \
  ps

curl -fsS http://127.0.0.1:8100/live
curl -fsS http://127.0.0.1:8100/ready
```

`/live` 只说明 HTTP 进程存活；`/ready` 才用于判断本地存储和服务是否已准备好。该模式故意不配置 NewAPI 登录来源，所以不能登录管理端或门户，也不会产生模型、渠道或用量数据。这是预期行为，不是故障。

停止时只停止容器：

```bash
MONITOR_ACCEPTANCE_IMAGE=newapi-monitor:local-snapshot \
  docker compose \
    -f docker-compose.local-acceptance.yml \
    -f docker-compose.local-snapshot.yml \
    stop
```

## 模式二：隔离合成数据验收

该模式启动临时 MySQL、Redis 和 Monitor。MySQL 使用仓库内的初始化 SQL，网络标记为 `internal`，Monitor 没有外网出口；`NEWAPI_LOG_DSN` 被 Compose 固定为本地只读账号，不能通过环境变量替换为线上地址。

先构建本地镜像并启动 MySQL、Redis：

```bash
cd 脚本/newapi-monitor

MONITOR_ACCEPTANCE_IMAGE=newapi-monitor:local-acceptance \
  docker compose \
    -f docker-compose.local-acceptance.yml \
    -f docker-compose.local-facts-acceptance.yml \
    build monitor

MONITOR_ACCEPTANCE_IMAGE=newapi-monitor:local-acceptance \
  docker compose \
    -f docker-compose.local-acceptance.yml \
    -f docker-compose.local-facts-acceptance.yml \
    up -d local-newapi-mysql redis
```

等待 MySQL 健康后，导入小规模合成数据。下面的参数适合功能调试；它会清空并重建名为 `newapi_local_acceptance` 的本地临时库，加载器会拒绝非回环地址、非本地账号、非本地库名和缺少确认串的执行。

```bash
go run ./dev/local-facts-loader \
  --confirm-local=LOAD_SYNTHETIC_LOCAL_DB \
  --users=200 \
  --tracked=10 \
  --days=14 \
  --background-days=7 \
  --benchmark-index=false \
  --write-probe-rows=1000
```

然后启动 Monitor：

```bash
MONITOR_ACCEPTANCE_IMAGE=newapi-monitor:local-acceptance \
  docker compose \
    -f docker-compose.local-acceptance.yml \
    -f docker-compose.local-facts-acceptance.yml \
    up -d --no-deps --force-recreate monitor
```

先从容器网络命名空间检查健康状态：

```bash
docker exec newapi-monitor-local-acceptance \
  wget -qO- http://127.0.0.1:8090/live

docker exec newapi-monitor-local-acceptance \
  wget -qO- http://127.0.0.1:8090/ready
```

部分 Docker Desktop / OrbStack 环境不会把 `internal: true` 网络中容器的已发布端口转发给 macOS 宿主机。这是该模式的隔离设计，不要为了打开浏览器页面而移除 `internal` 网络。需要自动验证门户和报表时，使用仓库内的 `dev/run-local-facts-loadtest.sh`；它在 Monitor 容器内部调用本机回环地址，不依赖宿主机端口转发。

默认 Compose 为 MySQL 设置了 4 GiB tmpfs 上限，以兼容较大规模验收集；小规模调试不会预分配这些空间。若 Docker Desktop 可用内存不足，请先关闭该模式并提高 Docker 分配的内存，而不是修改数据源指向线上数据库。

停止合成环境时，保留 Monitor 的外部数据卷：

```bash
MONITOR_ACCEPTANCE_IMAGE=newapi-monitor:local-acceptance \
  docker compose \
    -f docker-compose.local-acceptance.yml \
    -f docker-compose.local-facts-acceptance.yml \
    stop
```

如需清空临时 MySQL，可使用同一组 Compose 文件执行 `down`，但**不要**附加 `-v`；外部的 `newapi-monitor-local-data` 与 `newapi-monitor-local-backup` 不应被删除。

## 模式三：本机只读真实数据（受控验收）

仅在合成数据验证通过、并已获授权时使用。该模式将候选容器、SQLite、备份和 Redis 保持在本机，只通过绑定到 `127.0.0.1` 的 SSH 隧道以 `nexus_ro` 账号读取生产 MySQL。脚本会拒绝写账号、非回环隧道、非目标数据库和未固定的候选镜像。

执行入口固定为：

```bash
cd 脚本/newapi-monitor

# 只建立隧道并验证只读权限，不启动容器。
dev/run-local-production-readonly.sh preflight

# 仅用于开发构建；此镜像不能作为最终候选验收物。
MONITOR_PROD_READONLY_IMAGE=newapi-monitor:local-debug \
  dev/run-local-production-readonly.sh build

# 正式候选验收只接受 repository@sha256:digest。
MONITOR_PROD_READONLY_IMAGE='repository@sha256:<candidate-digest>' \
  dev/run-local-production-readonly.sh up

dev/run-local-production-readonly.sh status
dev/run-local-production-readonly.sh stop
```

该模式会保留只读边界，但仍可能对生产日志库产生受控的查询负载。不要用它做压测、批量导出或反复刷新大范围页面；完成后务必运行 `stop`，同时停止本地容器和 SSH 隧道，避免残留来源 lease。

## 常见问题

**提示找不到外部 volume。** 先执行本文的两个 `docker volume create` 命令。不要改 Compose 把 external volume 改成匿名卷，否则重建时容易挂载空库。

**提示 `MONITOR_ACCEPTANCE_IMAGE` 未设置。** 这是防止 Compose 默默选择错误镜像的保护。按示例显式设置本地镜像标签；候选验收必须改用不可变 digest。

**`/live` 正常但 `/ready` 异常。** 不要立即重启。先查看 `docker compose logs monitor`，确认本地 SQLite、Redis、来源 worker 或事实库的状态。`/live` 不代表数据可用。

**离线模式无法登录。** 预期如此：该模式没有 NewAPI 身份校验来源，只用于启动与存储 Smoke Test。

**浏览器访问不到合成模式端口。** 保持内部网络隔离，改用 `docker exec` 验证健康端点或运行容器内测试工具；不要为了方便暴露 MySQL、Redis 或 Monitor 管理端到公网/局域网。

## 相关文件

- `docker-compose.local-acceptance.yml`：本机基础容器、端口、持久化卷与安全默认值。
- `docker-compose.local-snapshot.yml`：完全离线 Smoke Test 覆盖文件。
- `docker-compose.local-facts-acceptance.yml`：隔离的 MySQL 合成数据验收覆盖文件。
- `docker-compose.local-production-readonly.yml`：受控的本机只读真实数据覆盖文件。
- `dev/local-facts-loader/`：安全受限的合成数据加载器。
- `dev/run-local-facts-loadtest.sh`：在隔离容器网络中运行的用量负载/验收工具。
- `dev/run-local-production-readonly.sh`：本机只读真实数据的受控操作入口。
- `docs/monitor-operations.md`：生产运维、备份恢复和完整验收要求。
