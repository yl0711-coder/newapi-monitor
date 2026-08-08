# Nginx 入口层旁路采集

该能力默认关闭。它用于补充“请求是否到达入口、HTTP 状态、入口与 upstream 耗时”这层客观事实，不替代 NewAPI 使用日志，也不自动判断责任方。

## 安全边界

采集器只读取一份专用 JSON access log，在节点本地先脱敏并按分钟聚合。Monitor 只接收：节点、归一化路径、HTTP 方法、HTTP/upstream 状态、请求数、耗时汇总、响应字节数和“是否携带 Request ID”的计数。

当 Nginx 因重试或内部跳转在 `$upstream_status` /
`$upstream_response_time` 中记录逗号或冒号分隔的多个值时，
单值报表取最后一次 upstream 响应；客户最终看到的状态仍以
`$status` 为准。这两个口径分开保留，不把中间重试误当成多个用户请求。

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

access_log /var/log/nexusapi-monitor/nexusapi_access.jsonl nexus_monitor buffer=64k flush=1s;
```

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
目录只读挂载到 `/logs`。不使用 `copytruncate`；宿为 Nginx 收到 `USR1`
后重新打开日志文件，保证 inode 切换可被采集器跟踪。

参考 logrotate 基线（每节点）：

```text
/opt/nexusapi/logs/nginx-monitor/nexusapi_access.jsonl {
    daily
    maxsize 50M
    rotate 8
    missingok
    notifempty
    nocompress
    create 0644 root root
    sharedscripts
    postrotate
        /usr/bin/docker kill -s USR1 nexusapi-nginx >/dev/null 2>&1 || true
    endscript
}
```

8 份未压缩日志用来覆盖默认 7 天留存和较长故障恢复；实际保留天数改动时，
logrotate 保留份数、`NGINXCOLLECTOR_RETENTION_DAYS` 和
`MONITOR_NGINX_RETENTION_DAYS` 必须一起调整。这份日志不含 IP、Key、query、请求体或
Request ID 原值，但仍应只对宿主机管理员可见。

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

该能力暂不单独上线，保持 `MONITOR_NGINX_ENABLED=false`，生产 Nginx、ALB 和节点均不做变更。
下一次中转站版本滚动升级本来就需要逐节点摘流、排空和重建容器，届时在**同一个已摘流窗口**
增加专用日志挂载和 nginxcollector，避免仅为日志挂载额外重建一次 Nginx。

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
   IP、Header、Key、query、请求/响应体、原始 Request ID 或动态路径。
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
