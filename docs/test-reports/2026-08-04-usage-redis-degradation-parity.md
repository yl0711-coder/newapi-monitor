# Monitor / Usage Redis 降级性能对齐验收报告

- 日期：2026-08-04（Asia/Shanghai）
- 分支：`dev`
- 验收对象：当前未提交工作区
- 验收目标：Redis 正常时保留二级缓存收益；Redis 不可用时，Monitor / Usage
  功能持续可用，常规可缓存聚合的稳态回源频率不高于旧版 60 秒本机缓存。
- 边界：未连接生产 Redis，未修改数据库结构，未执行写操作，未提交、推送或部署。

## 1. 结论

**通过。** 本次缓存降级修订的功能、并发、故障切换、自动恢复、容量上限和本地
Docker 运行验证均通过，未发现本次范围内的上线阻断。

精确边界：Redis 由可用突然变为不可用时，第一个感知故障的冷查询最多多等待
150 ms；进入退避后不再反复等待 Redis。这是发现外部依赖故障的必要一次性成本，
不影响后续 60 秒本机降级口径。

## 2. 修改范围

| 项目 | 修订前 | 修订后 | 目的 |
|---|---:|---:|---|
| 本机 L1 TTL | 10 秒 | 60 秒 | 对齐线上旧版的 60 秒本机缓存 |
| 本机条目上限 | 32 | 128 | 覆盖更多活跃分组、成员和日期键 |
| 本机字节上限 | 8 MiB | 16 MiB | 减少故障时 LRU 抖动，仍受 256 MiB 容器限制保护 |
| Redis 错误退避 | 5 秒 | 30 秒 | 避免错误地址、断网或认证失败时频繁等待 |

保留不变：Redis 单次操作 150 ms 上限、禁用客户端内部重试、单项 2 MiB 上限、
同键 singleflight、可取消查询闸门、精确键删除、缓存绝对过期时间及权限指纹隔离。

## 3. 自动化测试

### 3.1 缓存专项

- `go test -race -count=10 ./monitor -run 'TestUsage(ResultCache|Redis|CacheStats|AggregateTTL|BoundedByteCache)'`：通过。
- 新增回归覆盖：
  - Redis 不可用时本机记录在 59 秒仍命中、61 秒后过期；
  - 30 秒退避期内，不同键不重复访问 Redis；
  - 退避后 Redis 可自动恢复读写；
  - 生产构造器确实限制为 128 项/16 MiB，超限淘汰最旧项。

### 3.2 真实 Redis 集成

- 本机专用 `redis:7-alpine`：`127.0.0.1:17379`。
- 真实 Redis 往返、跨缓存实例命中、错误密码降级、未监听端口降级，
  `-race -count=3`：通过。
- Redis 错误未向业务返回，连接不可用测试在规定快速降级窗口内完成。

### 3.3 全量质量门禁

- `go test -race -count=1 ./...`：通过。
- `go test -shuffle=on -count=3 ./...`：通过。
- `go vet ./...`：通过。
- `go build ./...`：通过。
- `golangci-lint v1.64.8 run ./...`：通过。
- `git diff --check`：通过。

## 4. 本地 Docker 故障演练

只重建 `newapi-monitor-local-acceptance`，保留原有只读 DSN、会话配置和
`.local-data/nexus_monitor.db`。只停止/恢复专用
`newapi-monitor-local-redis`，未操作其他容器。

| 场景 | 结果 |
|---|---|
| Redis 正常 | 单日 Usage 查询 200，运维计数为 `remote_misses=1, source_fills=1, remote_errors=0` |
| Redis 停止 | Monitor `/health` 200，Usage `/login` 200，新单日查询 200 |
| 故障后同键立即重查 | 首次约 2.8 s，第二次约 0.44 s；`source_fills` 只增加 1，`local_hits` 增加 1 |
| 故障后间隔 12 秒重查 | 两次均 200；首次约 2.94 s，12 秒后约 0.44 s；未重新回源，证明已超过旧 10 秒策略 |
| Redis 恢复 | 下一个新键查询 200，`remote_misses` 增加、`remote_errors` 不增加、退避关闭，Redis 内出现新 TTL 记录 |

数据查询只使用少量、串行的单日只读聚合，未对线上数据库执行并发或压力测试。

## 5. 容器与资源

- Monitor：约 `33.43 MiB / 256 MiB`，CPU 空闲时约 `0%`，`restart=0`，`oom=false`。
- 专用 Redis：约 `5.42 MiB / 192 MiB`，`restart=0`，`oom=false`，healthy。
- Monitor 容器 ID 因本次重建变更；Redis 容器 ID 在停止/恢复前后保持不变。
- 其他容器 ID 均未变，本次未重建、停止或修改其他容器。

## 6. 上线后观测项

1. 管理员读取 `/usage/cache-stats`，确认 `remote_configured=true`。
2. 触发一次小范围用量查询，确认 `remote_misses` 或 `remote_hits` 发生变化。
3. 观察 `remote_errors` 和 `remote_backoff_active`；若 Redis 配置错误，页面应继续可用，同时修复私网/ACL/密码。
4. 观察 `source_fills`、数据库连接和 Monitor 响应时间，确认无异常回源放大。

## 7. 技术/测试/运维总监独立自动化复验

用户无需手工观察缓存效果。本轮在不修改业务代码的前提下，按三个独立视角再次验收。

### 7.1 技术总监

- 审查范围：TTL/容量/退避常量、两级缓存读写顺序、绝对过期时间、序列化隔离、
  同键 singleflight、普通/强制刷新竞态、删除/填充锁顺序、LRU 条目和字节上限。
- Redis 客户端继续使用 150 ms 读写/建连上限、8 连接硬上限和 `MaxRetries=-1`。
- 生产库仍为最多 3 连接；重型用量聚合仍由可取消的单槽闸门串行，Redis 故障不会
  引入无上限并发回源。
- 安全边界未变：Redis 不保存会话、权限、用户资料、余额、原始日志或 CSV；
  运维计数不返回缓存键和业务结果。
- `go vet`、`go build`、`golangci-lint v1.64.8`、`git diff --check`：全部通过。
- 结论：**未发现新增 bug、竞态、无界资源增长或可维护性阻断。**

### 7.2 测试总监

- Monitor 包共 142 个自动化 `Test*` 用例，其中 Usage/Cache/Redis/Portal 相关 72 个。
- 缓存专项 `-race -count=20`：通过；覆盖 59/61 秒 TTL 边界、30 秒退避边界、
  容量淘汰、超大载荷不缓存、取消、同键并发、刷新/删除竞态和损坏 Redis 记录。
- 真实 Redis 集成 `-race -count=5`：通过；覆盖正常往返、跨实例命中、错误密码、
  未监听端口和快速降级。
- 全量 `go test -race -count=1 ./...`：通过。
- 全量 `go test -shuffle=on -count=5 ./...`：通过。
- Monitor 语句覆盖率：58.5%；关键故障和并发分支由定向用例额外覆盖。
- 结论：**所有自动化门禁通过，未出现偶发失败、数据倒退、重复回源或 Redis 错误透传。**

### 7.3 运维总监

- 容器黑盒演练已自动覆盖 Redis 正常、运行中停止、冷键回源、12 秒后本机命中、
  恢复后重新写入 Redis。
- 最终状态：Monitor `/health=200`，Usage `/login=200`，Redis `PONG` 且 healthy。
- 故障演练计数：`requests=6`、`local_hits=2`、`source_fills=4`、`remote_errors=2`；
  两条 Redis WARN 均对应人工断连，无 panic、fatal 或 ERROR。退避已关闭。
- 最终资源：Monitor 约 `23.7 MiB / 256 MiB`，Redis 约 `6.0 MiB / 192 MiB`；
  两者 `restart=0`、`oom=false`。
- 其他容器 ID 未变，未对其他容器执行重建、停止或修改。
- 结论：**Redis 不再是 Monitor/Usage 可用性的硬依赖；故障会被观测，但不会使站点退出服务。**

### 7.4 三方联合结论

**通过。** 在当前平台规模和常规可缓存聚合范围内，已用自动化证明：

1. Redis 可用时可提供跨请求/历史区间缓存收益。
2. Redis 不可用时，Monitor/Usage 仍可启动、登录和查询，60 秒本机缓存保持与线上旧版一致的稳态回源口径。
3. Redis 恢复后可自动重新接入，无需重启 Monitor。
4. 首个感知 Redis 故障的冷请求仍可有最多 150 ms 的一次性探测成本；这是已知且受控的客观边界，不属于阻断。
