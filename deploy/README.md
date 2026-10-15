# 生产部署辅助文件

`monitor-ecs-task-role-cloudwatch-policy.example.json` 仅是 Monitor 生产 ECS
Task Role 的最小 CloudWatch Logs 只读参考模板。生产环境已经有运维维护的
`nexusapi-monitor-ro`，不需要使用该文件新建 IAM 策略或角色。

运维部署时需要：

1. 在 Monitor 的 ECS Task Definition 将 `TaskRoleArn` 设置为已有的
   `nexusapi-monitor-ro`，无需新建 IAM 资源。
2. 确认该角色已有 us-east-1、us-west-2 指定 CloudWatch Logs 的只读权限，并用
   IAM Policy Simulator 及窄时间窗口分别验证 `FilterLogEvents`、`StartQuery`、
   `GetQueryResults`、`StopQuery`。不要用参考模板另建策略覆盖现有权限。
3. 不要设置
   `AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY` 或 `AWS_SESSION_TOKEN`；程序会通过
   ECS Task Metadata 使用临时凭证。
4. `MONITOR_CLOUDWATCH_EVIDENCE_HMAC_KEY` 和
   `MONITOR_CLOUDWATCH_EVIDENCE_HMAC_KEY_ID` 不是 AWS 凭证，仍需由受控的
   Secrets Manager 注入；不要写进镜像、Compose 或仓库。

该参考文件只描述权限，不会调用 AWS，也不会自动修改现有 Role、策略或任务定义。

## 本地 8204 验收

本地 `docker-compose.local-production-readonly.yml` 仍从 gitignored `.env` 读取 AWS
临时凭证，仅用于 8204 调试。它不代表生产部署方式；生产 ECS 不注入任何
`AWS_ACCESS_KEY_ID` 等长期密钥，直接使用上面的 `nexusapi-monitor-ro` Task Role。

## 发布冷备份保留（本机演练已实现，尚未接入生产发布）

`release_backup_retention.py` 是宿主机发布辅助工具，不随 Monitor 镜像进入运行容器，不需要 AWS 凭证，不连接业务数据库。它只读取 Docker 容器状态、发布验收记录和冷备份，默认**只输出计划**。

规则：

- 按 `acceptance.json` 的验收时间保留最近两份已验收发布恢复点，而不是按版本号或目录修改时间猜测。
- 当前容器必须健康，镜像 digest 和 revision 必须等于最新验收版本，验收不得早于当前容器启动。回滚或重启后必须重新验收，否则拒绝清理。
- 未验收的发布目录、任意容器（包括停止的容器）挂载引用的目录、存在 `KEEP_BACKUP` 标记的目录一律保护。记录损坏则报错，不猜测。
- 仅淘汰旧发布 `cold-backup/data/nexus_monitor.db`、`cold-backup/data/usage-facts.db`、`cold-backup/evidence/nginx-evidence.db` 三个显式文件。配置、审计、缓存、其他数据库、赠送修复证据均保留；不会递归删目录，也不扫描 `/opt/nexus-tools/backups` 或 Docker 数据卷。
- 执行前复核两份保留恢复点的全部清单文件 hash 和 SQLite `quick_check`，核验候选文件 hash、inode、大小、修改时间、链接、SQLite 边文件和 Linux 打开文件描述符。无法检查权限也拒绝删除。
- 一次最多清理 4 GiB、24 个文件；超限需要单独处理历史积压，不自动扩大范围。保留的是恢复点数量，不保证备份字节总量不增长，磁盘告警仍保留。
- 清单摘要改变就取消执行。每次删除前写入并落盘审计意图；中断后保留恢复点不受影响，须重新生成计划，不能继续执行过期清单。已删除的旧备份不能凭“关闭清理”找回；不可把恢复业务库作为脚本回滚方式。

### 本机测试

```bash
python3 -B -m unittest discover -s deploy -p 'test_release_backup_retention.py' -v
python3 -B -m unittest discover -s deploy -p 'test_monitor_release_guard.py' -v
```

测试使用临时目录、合成 SQLite 和模拟容器清单，不调用真实 Docker 服务。Linux `/proc` 打开文件检测和 CLI 实际删除路径另在断网、非 root Docker 中演练；macOS 或没有 `/proc` 检查权限的宿主环境会跳过该集成部分，生产工具不会跳过安全闸门。

### 将来接入发布的必要条件

必须先让**发布切换、回滚、备份挂载操作**与本工具共用 `/opt/nexus-tools/releases/.deployment.lock`，再在发布验收成功后单独调用清理。本机已经验证锁冲突会阻止清理，但当前线上发布脚本尚未改为共用此锁，**不能现在直接启用自动删除**。

示例读取计划（不删除、不生成审计文件）：

```bash
sudo python3 release_backup_retention.py --releases-dir /opt/nexus-tools/releases
```

审核输出的保留点和显式文件后，使用原清单摘要执行（需要单独的生产变更授权）：

```bash
sudo python3 release_backup_retention.py --releases-dir /opt/nexus-tools/releases \
  --apply-plan-sha256 <刚审核的plan_sha256>
```

工具自行取得公共锁，不要在外层持有同一把锁时再嵌套调用它。验收结束释放部署锁后，清理重新取得锁并重新核对容器状态；如果中间发生了版本切换，清单校验会拒绝执行。

清理失败只能记录维护异常，不能自动停止 Monitor、重启其他服务或恢复旧库。退出自动治理只需停止调用本工具；既有服务和数据不需回滚。不得提前淘汰旧恢复点后才做新版本验收。

### 统一发布入口（本机串联演练，尚未安装到生产）

`monitor_release_guard.py` 包装现有版本专用发布脚本，不替代其备份、就绪检查或镜像回滚逻辑。每个阶段必须经此入口执行，包括 `prepare`、`switch`、`observe`、`accept`、`rollback-image`；不能一部分同事绕过入口直接运行旧脚本。锁仅覆盖单次阶段执行，不锁住人工验收等待期。

```bash
sudo python3 monitor_release_guard.py \
  --releases-dir /opt/nexus-tools/releases \
  --script /opt/nexus-tools/<已审核发布脚本目录>/deploy.py \
  --release-name monitor-vX.Y.Z-YYYYMMDD \
  --authenticated-reports-verified --cleanup plan accept
```

上述路径为占位示例，不可直接执行。`switch` 要求 `--baseline-reports-verified`；`accept` 要求 `--authenticated-reports-verified`，由操作者完成真实页面核验后传入，开关本身不代替验收。验收脚本必须本次生成对应发布的新鲜成功记录，仅退出码为零不够。

- 默认 `--cleanup plan` 仅生成计划。`--cleanup off` 停止清理；`--cleanup apply` 仅允许验收成功后使用，生产启用需另行授权。
- 子进程继承锁文件描述符；入口进程异常退出但发布子进程仍运行时，锁不会提前释放。锁冲突退出码为 75，不自动重试。
- 发布阶段失败或验收记录不可信时，退出非零且不清理、不自动回滚。需要回滚时走已审核的 `rollback-image` 阶段，不恢复旧业务库。
- 验收成功但清理失败时，发布仍成功、退出码为零，同时返回 `cleanup.status=maintenance_warning`。调用方必须展示维护警告，不能据此触发服务回滚。
- 演练包含真实子进程、父进程被终止、双向锁冲突、过期验收凭据、清理期间版本变化，以及合成 SQLite 恢复点验证；不调用线上服务。
