# Nginx 入口层旁路采集

该能力默认关闭。它用于补充“请求是否到达入口、HTTP 状态、入口与 upstream 耗时”这层客观事实，不替代 NewAPI 使用日志，也不自动判断责任方。

## 安全边界

采集器只读取一份专用 JSON access log。核心分钟字段只解析一次；证据开启时再独立解析可选证据字段，后者格式异常不会污染分钟事实。输出分成两条完全独立的 lane：

1. 原有分钟汇总：节点、归一化路径、HTTP 方法、HTTP/upstream 状态、请求数、耗时汇总、响应字节数和 Request ID 存在计数。
2. 可选请求证据：每个推理请求的最终 HTTP 状态、多次 upstream 尝试状态序列、connect/header/总耗时、响应字节和入口完成状态。Request ID 在节点上立即使用 HMAC-SHA256 脱敏，Monitor 只存 HMAC，不存原值。

两条 lane 有不同的接收接口、批次幂等账本、本地队列和 SQLite。证据接口故障不会阻止原有分钟汇总推进；队列满时必须先持久化缺口计数，不允许静默丢数。

当 Nginx 因重试或内部跳转在 `$upstream_status` /
`$upstream_response_time` 中记录逗号或冒号分隔的多个值时，
单值报表取最后一次 upstream 响应；客户最终看到的状态仍以
`$status` 为准。这两个口径分开保留，不把中间重试误当成多个用户请求。

即使没有新请求，采集器也会每分钟发送一个不含业务数据的空心跳；因此页面上的“采集器正常”不会把真实零流量误报为采集中断。

禁止写入专用日志的字段包括：客户端 IP、X-Forwarded-For、Authorization、API Key、Cookie、完整 query、请求体、响应体、User-Agent、Referer 及 upstream 地址。为了不修改 NewAPI 仍能关联它写入 `logs.request_id` 的请求，v2 专用日志会在节点本地短期包含 Nginx/NewAPI Request ID；该文件必须仅宿主机管理员可读，采集器将其 HMAC 后才入持久 outbox/发往 Monitor，原值绝不出节点。Request ID 不是认证凭据，但仍按敏感运维数据管理。

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

access_log /var/log/nexusapi-monitor/nexusapi_access.jsonl nexus_monitor buffer=64k flush=1s;
```

启用请求证据时使用 schema 2；不修改 NewAPI。rc4 已由 RequestId 中间件在响应中返回
`X-Oneapi-Request-Id`，且 NewAPI 消费/错误日志使用同一上下文 ID。

```nginx
log_format nexus_monitor_v2 escape=json '{'
  '"log_schema":2,'
  '"msec":"$msec",'
  '"request_method":"$request_method",'
  '"uri":"$uri",'
  '"status":"$status",'
  '"request_time":"$request_time",'
  '"upstream_status":"$upstream_status",'
  '"upstream_response_time":"$upstream_response_time",'
  '"upstream_connect_time":"$upstream_connect_time",'
  '"upstream_header_time":"$upstream_header_time",'
  '"bytes_sent":"$bytes_sent",'
  '"request_id":"$request_id",'
  '"nginx_request_id":"$request_id",'
  '"oneapi_request_id":"$sent_http_x_oneapi_request_id",'
  '"request_completion":"$request_completion"'
'}';
```

`request_id` 字段保留是为了原分钟汇总的存在率兼容；证据 lane 使用两个明确命名字段。先用 `pilot` 核对一批真实请求的响应头、Nginx HMAC 证据和 `logs.request_id`；覆盖率与值一致性达标前不得配成 `verified`，也不得在客户排障中宣称精确关联。

## 请求证据灰度开关

Monitor 和节点必须使用相同、独立于登录与 ingest token 的 HMAC 密钥及 key id。默认全部 `off`：

```text
MONITOR_NGINX_EVIDENCE_MODE=pilot
MONITOR_NGINX_EVIDENCE_STORE_PATH=/evidence/nginx-evidence.db
MONITOR_NGINX_EVIDENCE_RETENTION_HOURS=168
MONITOR_NGINX_EVIDENCE_MAX_MIB=512
MONITOR_NGINX_EVIDENCE_HMAC_KEY=<at-least-32-random-bytes>
MONITOR_NGINX_EVIDENCE_HMAC_KEY_ID=2026-08-a
# 轮换期可暂时保留上一把，待旧 outbox 与留存窗口清空后删除：
MONITOR_NGINX_EVIDENCE_PREVIOUS_HMAC_KEY=<previous-key>
MONITOR_NGINX_EVIDENCE_PREVIOUS_HMAC_KEY_ID=2026-07-a

NGINXCOLLECTOR_EVIDENCE_MODE=pilot
NGINXCOLLECTOR_EVIDENCE_SINK_URL=https://monitor.example/internal/nginx-evidence/v1
NGINXCOLLECTOR_EVIDENCE_HMAC_KEY=<same-key>
NGINXCOLLECTOR_EVIDENCE_HMAC_KEY_ID=2026-08-a
NGINXCOLLECTOR_EVIDENCE_OUTBOX_MAX_MIB=256
```

`nginx-evidence.db` 不得与 Monitor 主库或 usage facts 库合并，也不进入主库长期备份集。生产环境必须挂载到独立、有配额的卷；`MONITOR_NGINX_EVIDENCE_MAX_MIB` 会设置 SQLite `max_page_count`，过期删除产生的 freelist 页可直接复用，不会因为主文件曾经长大而永久拒绝写入。该限制仍不能替代卷配额。默认保留 7 天并分批清理。管理端只能通过 `POST /nginx/evidence/lookup` 用精确 NewAPI Request ID 查询；输入只用于内存 HMAC，不入库。`pilot` 响应会明确标记 `linkage_verified=false`，供客户排障后续整合，但当前不自动改它的页面结论。

证据 outbox 使用写入游标中的单调序号排序，不依赖文件 mtime；空事件的数据区间也会写入 checkpoint。若证据目录暂时不可写，丢失代次与丢失事件数跟随分钟游标持久化，恢复后的下一批只允许跨越一次已声明缺口。永久 4xx 拒绝的原批次会移入有界 `rejected` 目录，临时 429/5xx 则保留在活跃队列重试。

当前请求级闭环只使用专用 access log。标准 Nginx error log 可能包含客户端 IP、完整请求行、query、upstream 地址等敏感信息，也没有可配置的稳定 Request ID 字段，因此不得把原文直接上传或与请求做时间邻近的伪精确关联。

标准 error log 使用同一采集器进程内的**独立 lane**：独立日志路径、独立持久游标、独立接收接口和独立重试循环。节点侧只保留有限类别（upstream 超时/连接失败/提前关闭/TLS、客户端断开、worker 容量、解析、限流、请求体、其他错误）、严重级别、分钟和数量；原文、IP、URI、请求行与 upstream 地址均不上传。该数据只能作为节点级运维证据，不能冒充请求级证据。error lane 故障不会阻塞 access 分钟汇总或请求证据。

入口分钟汇总另带 0～1s、1～5s、5～15s、15～30s、30～60s、60s 以上六个互斥耗时桶。Monitor 据此展示近似 P95/P99；它们是桶上界估算，不伪装成精确分位值。旧版样本没有直方图时继续可读，并明确反映为覆盖率不足。

error lane 默认关闭，节点和 Monitor 都必须显式开启：

```text
MONITOR_NGINX_ERROR_ENABLED=true

NGINXCOLLECTOR_ERROR_ENABLED=true
NGINXCOLLECTOR_ERROR_LOG_PATH=/logs/error.log
NGINXCOLLECTOR_ERROR_CURSOR_PATH=/data/error-cursor.json
NGINXCOLLECTOR_ERROR_SINK_URL=https://monitor.example/internal/nginx-errors
# 必须与 Nginx error_log 的实际时区一致
NGINXCOLLECTOR_ERROR_TIMEZONE=UTC
```

采集器挂载的 `/logs/error.log` 必须是现有标准 error log 的只读文件，不要求修改其格式。若该文件不在已有共享日志目录，仍需在节点摘流窗口内补只读挂载；不得为采集赋予 Docker socket 或宿主机广泛读取权限。

先执行 `nginx -t`，再平滑 reload。不要改动现有业务路由、超时、upstream 或原日志。
现有写往 stdout 的访问日志可保留作为原运维日志；采集器只读取上面这份
专用脱敏文件。

## 节点目录与轮转基线

建议两个 Nginx 节点都使用独立宿主机目录：

```text
/opt/nexusapi/logs/nginx-monitor/
  nexusapi_access.jsonl
  nexusapi_access.jsonl.1
  ...
```

Nginx 容器以读写方式挂载到 `/var/log/nexusapi-monitor`，采集器把同一宿主机
目录只读挂载到 `/logs`。不使用 `copytruncate`；宿主机在 Nginx 收到 `USR1`
后重新打开日志文件，保证 inode 切换可被采集器跟踪。

日志目录必须是 `root:nexus-monitor 0751`（或经过同等验证的 ACL）：采集器通过
`nexus-monitor` 组读取，Nginx worker 只获得目录穿越权限。`0750` 会导致 master
打开新文件后 worker 无法按路径重新打开，形成旧 `.1` 仍增长而采集水位假正常的缺口。
当前日志由 logrotate 以 `root:nexus-monitor 0640` 创建；成功处理 `USR1` 后 Nginx
会按其标准行为把 owner 调整为 worker UID，group 和 `0640` 必须保持不变。

参考 logrotate 基线（每节点）：

```text
/opt/nexusapi/logs/nginx-monitor/nexusapi_access.jsonl /opt/nexusapi/logs/nginx-monitor/error.log {
    daily
    maxsize 50M
    rotate 8
    missingok
    notifempty
    nocompress
    create 0640 root nexus-monitor
    sharedscripts
    postrotate
        /usr/local/sbin/nexusapi-nginxreopen \
          -container nexusapi-nginx \
          -collector-container nexusapi-nginxcollector \
          -log-dir /opt/nexusapi/logs/nginx-monitor \
          -logs nexusapi_access.jsonl,error.log \
          -log-gid <nexus-monitor 数字 GID> \
          -probe-url http://127.0.0.1:<本机 Nginx 端口>/api/status \
          -probe-expected-status 403
    endscript
}
```

`nexusapi-nginxreopen` 必须由本仓库 `cmd/nginxreopen` 构建并以 root 安装，
不得换回直接 `docker kill ... || true`。它只向既有 Nginx master 发 USR1，
不 reload/restart；但任何权限、collector 可读性、worker FD、容器身份或本地探针校验失败
都必须让 logrotate 失败。成功后它会原子写入 root 所有的
`.nginx-writer-release-v2.json`，source v2 只依据这份 inode 证明退役已读完且已消失的旧日志。
当前 NexusAPI 源站锁会让不带内部密钥的 loopback `/api/status` 返回 `403`，因此示例显式
要求 `403`；这个探针用于证明 worker 已在新 inode 写入，不携带或暴露源站密钥。公网/LB
健康状态仍应在摘流和回挂闸门中独立验证。
生产 Nginx 容器必须由 root Nginx master 直接作为 PID1；不得再套 shell、tini
或其他 supervisor，否则安全校验会按设计失败关闭。
该工具仅支持 Linux `/proc` 和 Docker host PID 语义，不支持 userns-remap。
如 postrotate 失败，不会中断用户请求，但必须立即告警、保留旧 inode 并暂停后续轮转；
严禁用 `|| true` 掩盖失败，以免留存删除尚未完整采集的日志。

8 份未压缩日志只是起始基线，不是必然覆盖 7 天；正式数值必须按峰值 bytes/hour
乘最长恢复窗口实测计算，并配置磁盘 70%/80%、inode 和轮转失败告警。实际保留窗口改动时，
logrotate 保留份数、`NGINXCOLLECTOR_RETENTION_DAYS` 和
`MONITOR_NGINX_RETENTION_DAYS` 必须一起调整。这份日志不含 IP、Key、query、请求体或响应体；
schema 2 含短期 Request ID 原值，因此必须只对宿主机管理员和专用
`nexus-monitor` 组可见。把该组的数字 GID 填入 `NGINXCOLLECTOR_LOG_GID`，
Compose 只把这个补充组授予非 root 采集器；不得回退到 `0644`。

## 启用顺序

1. 先在本地用模拟日志验证采集器、幂等重试、轮转和 Monitor 页面。
2. 节点创建专用日志并确认不含敏感字段；确认采集器处理速度持续高于日志增长速度。
3. 给 Nginx 与采集器挂载同一个专用日志目录：Nginx 只向该目录写
   `nexusapi_access.jsonl`，采集器只读挂载；不要把 Docker socket、主站数据目录或
   其他日志目录交给采集器。
4. 部署 `docker-compose.nginxcollector.yml`，游标用独立小卷持久化。
5. Monitor 配置相同的 `MONITOR_INGEST_TOKEN`，显式设置节点白名单。
6. 最后设置 `MONITOR_NGINX_ENABLED=true` 并重建 Monitor。

任一步失败都可把 `MONITOR_NGINX_ENABLED=false`；模型监控、用户用量、渠道管理和 NewAPI 请求链路不依赖该采集器。

`NGINXCOLLECTOR_SINK_URL` 默认必须使用 HTTPS，采集器不跟随任何
HTTP 重定向，避免 Bearer token 被带到非预期主机。只有经过核对的容器
内网/私网直连才可显式设置 `NGINXCOLLECTOR_ALLOW_INSECURE_HTTP=true`；不得
以此开关允许公网明文传输。

## NexusAPI 两节点滚动接入（生产变更闸门）

### 上线安排

该能力只能在批准的受控窗口内上线。先保持 `MONITOR_NGINX_ENABLED=false`
发布 Monitor 和采集器，再对 Nginx 节点逐台摘流、排空、重建与回挂。严禁同时重建
两台 Nginx，也不得借采集上线改动 NewAPI 镜像、业务路由、超时或 upstream 配置。

节点先后顺序以届时中转站版本的正式 release runbook 为准；无论先处理 Master 还是 Slave，
都必须一次只处理一个节点。禁止同时摘除、排空或重建两个节点。

当前 NexusAPI 的 `nexusapi-nginx` 只挂载配置模板，尚无与采集器共享的日志卷。
因此第一次接入需要在每个节点的升级窗口内各重建一次 Nginx 容器，不是单纯重启采集器。
变更前必须满足：

1. 当前 Monitor 长时补数任务已结束；不得在未确认本地游标和恢复行为时中断补数。
2. Nginx 采集上线前审查中的阻断项已修复并重新验收，包括日志缺失假绿、文件身份/
   批次碰撞、专用日志脱敏、生产网络接入和多 upstream 口径。
3. 新 Monitor、nginxcollector 和本次中转站候选镜像的 CI、签名/摘要、幂等、轮转及
   回滚测试全部通过；生产只能使用固定 tag 与 digest，不使用 `latest`。
4. 两节点当前 compose、Nginx 模板和运行镜像摘要已分别备份，且已有经过验证的逐节点
   回滚命令；不得用 `docker compose down`。
5. 脱敏配置在与生产同版的临时 Nginx 容器中通过 `nginx -t`，并人工确认专用日志不含
   IP、认证 Header、Key、query、请求/响应体或动态路径；Request ID 仅允许存在节点本地的 schema 2 短期日志。
6. 宿主机日志目录、所有者/权限、logrotate 配置和磁盘预算已验证；轮转必须使用
   rename + `USR1`，不得使用 `copytruncate`。
7. Master 到 Monitor 的私网固定解析、Slave 到 Monitor 的同 Docker 网络直连均已通过
   空心跳测试；机器接口不得经公网明文传输 token。

### 合并升级的实施顺序

1. 补数任务结束后先发布新 Monitor 代码，但保持 `MONITOR_NGINX_ENABLED=false`。
   这一步只重建 Monitor；会造成 Monitor/Usage 短暂不可访问，但不重启 NewAPI、Nginx、
   Redis、数据库或其他业务容器。
2. 按届时中转站 release runbook 摘除第一个候选节点的 ALB 流量，等待 AWS 目标状态为
   `unused`，再使用现有 `node-drain-check` 留证：所有普通、流式和图片连接均已自然结束。
   **摘流和排空完成前禁止重建任何业务容器。**
3. 在该已摘流节点上执行中转站版本升级；同时创建专用宿主机日志目录、安装 logrotate
   配置，并给 `nexusapi-nginx` 增加唯一的新日志挂载和脱敏 `access_log`。只使用
   `--no-deps --force-recreate nginx` 重建 Nginx，不重建无关容器。
4. 在同一节点启动 nginxcollector，确认其为非 root、日志目录只读、游标卷独立、
   64 MiB/0.25 CPU/64 PIDs 限额生效且没有 Docker socket。Monitor 入口仍关闭时，
   采集器收到 503 后必须原地重试且不推进游标。
5. 通过 SSH/本地隧道直测该节点，不用公网域名代替候选节点验证。必须检查：
   `nginx -t`、容器健康、源站锁、`/api/status`、核心非流式/流式/Responses/Messages 请求、
   专用日志脱敏、日志轮转、采集器资源和游标状态。
6. 候选节点全部通过后才重新注册到 ALB；观察约定窗口内的 4xx/5xx、连接中断、响应耗时、
   容器重启和磁盘增长。任何回归立即再次摘除该节点，不处理第二个节点。
7. 第一个节点稳定后，对另一个节点完整重复步骤 2～6。不得因为配置相同而跳过摘流、
   排空、节点直测或回挂观察。
8. 两节点均稳定后，给 Monitor 配置相同的 ingest token、
   `MONITOR_NGINX_ALLOWED_NODES=master,slave`，最后设置 `MONITOR_NGINX_ENABLED=true`
   并只重建 Monitor。
9. 最终验收两节点每分钟心跳、日志可读、待读字节持续回落至 0、游标不连续为 0、
   无效/超窗日志计数符合预期，并对比同一时间窗的专用日志行数与入口聚合请求数。

真正接触用户流量的是每个节点的 **ALB 摘除与重新注册**。Nginx/NewAPI 的重建只能发生在
该节点已经 `unused` 且连接完全排空之后；Monitor 和 nginxcollector 均不得作为用户请求链路
依赖项。

### 停止条件与回滚

出现任一情况立即停止，不进入下一节点：ALB 无法确认摘流、连接无法排空、仅存的在线节点
不健康、候选节点直测失败、源站锁失败、专用日志出现禁止字段、日志目录/轮转异常、磁盘异常
增长、采集器积压持续扩大，或公网错误率/耗时高于变更前基线。

候选节点尚未回挂时，保持其摘流，先停止该节点 nginxcollector，再按中转站 release runbook
回滚 NewAPI，并用备份的 compose、Nginx 模板和固定旧镜像摘要恢复 Nginx。完成节点直测后
才允许回挂。若 Monitor 入口已经启用，可先恢复 `MONITOR_NGINX_ENABLED=false`；这不会影响
NewAPI、数据库、模型监控、用户用量或渠道管理。

### Source v2 逐 lane 切换与恢复

Source v2 是日志源连续性协议，默认不启用。切换单位是“节点 + access/error lane”，
不得一次切换整个节点或两个节点。必须按以下顺序执行：

1. Monitor 先只设置 `MONITOR_NGINX_SOURCE_V2_ENABLED=true`，保持
   `MONITOR_NGINX_SOURCE_V2_CUTOVER_ENABLED=false`；完成隔离 schema 迁移和备份校验。
2. 采集器只设置 `NGINXCOLLECTOR_SOURCE_V2_PREPARE=true`，保持
   `NGINXCOLLECTOR_SOURCE_V2_LANES=` 为空。确认 access/error 的 v1 边界都得到精确 ACK，
   且游标卷、writer-release proof、积压和不连续计数正常。
3. Monitor 只把候选 lane 放入 `MONITOR_NGINX_SOURCE_V2_ALLOWED_LANES`，临时开启
   `MONITOR_NGINX_SOURCE_V2_CUTOVER_ENABLED=true`；采集器只把同一 lane 写入
   `NGINXCOLLECTOR_SOURCE_V2_LANES`。
4. manifest 持久化成功后立即关闭 `MONITOR_NGINX_SOURCE_V2_CUTOVER_ENABLED`，防止新 lane
   误切换。已切换 lane 会继续运行，这个闸门不得用作回滚开关。
5. 至少观察心跳、confirmed offset、backlog、gap/lost、discarded lines 和源端日志行数。
   全部符合基线后，才允许复制同一流程切换下一 lane。

manifest 一旦持久化，该 lane 不能通过删环境变量或回退二进制恢复 v1。
切换后发生故障时，先停止该 lane 采集器并保留日志和游标卷，优先前向修复。
只有在采集器已停止、日志不再增长的受控恢复窗口，才能把 Monitor 主库、事实库和
access/error 游标卷恢复到同一个已校验的时点；严禁只恢复其中一份。

遗留 v1 游标错误切到空 current、而 `.1` 仍增长时，不得从 `.1` 文件头重放。
先使用 `cmd/nginxcursoraudit`，把 Monitor 保留的同节点、同 lane 的 64 位数据批次 ID
按行输入，并指定最后成功数据批次。工具使用生产 v1 的 node、inode、起止偏移和逐行内容
摘要算法，从文件开头逐批证明边界，只输出最后确认 offset，不写日志或游标。任一中间批次
缺失、文件身份异常或目标批次无法到达都会失败关闭。只有在 writer-release 已证明 `.1`
不再被任何 Nginx 进程持有、四条 lane 的审计结果已留档且游标卷完成备份后，才可按该
offset 恢复；随后先开启 prepare 获取服务端边界 ACK，再逐 lane 切到 source v2。

若专用日志位于部署用户拥有的祖先目录（生产当前为 `/opt/nexusapi`、UID 1000），
`nginxreopen` 必须显式传入 `-trusted-parent-uid 1000`。该例外只允许匹配 UID 的祖先目录，
不允许终端日志目录脱离 root 所有权，也不放宽任何 group/world writable 目录；新节点安装前
必须重新核对 UID，不能照抄旧节点数值。

## 日志轮转与连续性

采集器以 inode 和字节偏移持久化游标。日志 rename 轮转后，只要旧 inode 仍以未压缩
普通文件保留在同一目录，采集器会按最后写入时间从旧到新依次追读，追完所有轮转文件
后再切到当前日志；进程重启、Monitor 暂时不可用或同一批次重试都不会直接跳到新文件。
批次幂等键同时包含该批日志内容摘要，避免 inode 复用且偏移相同时把新批次
误判为历史重试。

游标文件采用版本校验、临时文件写入、`fsync` 和原子 `rename`。文件存在但
内容损坏时会失败关闭，不会静默从头重读。游标卷是正确性数据，不得在重建、
升级或排障时删除；如果确认丢失，必须先停止采集器并按事故恢复流程核对可能的
重复/缺口，不得直接空卷重启。

专用日志必须保持 `nocompress`，并至少保留足以覆盖最长故障恢复时间的未压缩轮转文件。
采集器不会读取 `.gz/.xz/.bz2/.zst/.zip`；如果以后为了节省磁盘改为压缩，必须同时缩短
可恢复窗口或先增加压缩文件读取能力。采集器不会为了追日志获得 Docker socket 或宿主机
高权限。

若游标 inode 已被删除/压缩、copytruncate 后文件长度退到游标之前，或轮转旧文件以
不完整 JSON 结尾，采集器会记录一次“游标不连续”，尽量从仍保留的最旧文件继续。
Monitor 的“采集器状态”会客观显示累计次数、最近发生时间和待读字节数；该指标只说明
采集连续性，不能自动推断具体丢失了多少请求或由谁造成。

健康状态不会只看“心跳是否新鲜”。满足以下任一条件时，即使采集器仍在按分钟发送心跳，
来源也会标记为异常，`/stability/health` 同步返回 `degraded`：

- 已知待读积压达到 16 MiB；
- 仍有积压且最新业务事件落后超过 3 分钟；
- 最近 15 分钟发生游标不连续；
- 最近 15 分钟跳过了无效或不可接受的日志行；
- 心跳超过 3 分钟未更新（超过 15 分钟为严重异常）。

接口同时返回积压、落后来源数、大积压来源数和近期数据损失来源数，便于区分“进程活着”
与“数据仍在完整、及时地前进”。累计历史异常保留用于审计，但超过近期窗口且已无积压时，
不会让来源永久保持告警。

采集器还会跳过无法解析、时间在允许窗口之外或数值越界的日志行，并持续上报累计跳过
行数和最近时间。`NGINXCOLLECTOR_RETENTION_DAYS` 必须与 Monitor 的
`MONITOR_NGINX_RETENTION_DAYS` 保持一致；默认均为 7 天。超窗旧日志会被推进游标但
不会发送，避免一条过期数据让整个采集队列永久卡住。

该旁路能力与 NewAPI 请求链路解耦。关闭采集器或设置
`MONITOR_NGINX_ENABLED=false` 不会影响主站、模型监控、用户用量或渠道管理。
