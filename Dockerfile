# new-api 上游监控 —— 独立项目、独立容器(无外部依赖)。
# 构建上下文是 monitor 模块根目录:
#   按 CONTRIBUTING.md 先生成 SOURCE_MANIFEST_SHA256，再传给 docker build。
# 页面(page/alert/login.html)已 go:embed 进二进制,无需单独拷模板。
# 本地采样库与报警配置落在挂载卷 /data 上,不进镜像。

# 默认交付仍从官方 Alpine 空运行层开始。纯本地断网验收可显式
# 传入一份已安装 ca-certificates/tzdata 的本机已缓存运行镜像；
# OFFLINE_RUNTIME 会先校验这两份运行资产，不会把缺包的镜像伪装成可交付产物。
ARG RUNTIME_IMAGE=alpine:3.23
# BUILDER_IMAGE 与 RUNTIME_IMAGE 同理：镜像代理不可达时（dockerproxy.com 超时、
# Docker Hub 直连失败），可传入本机已缓存的任意 golang 镜像完成构建。
# 产物是 CGO_ENABLED=0 的静态二进制，构建镜像的基础发行版不影响运行结果，
# 因此 golang:1.26.11（Debian 基）编出的二进制同样能在 alpine 运行层跑。
# 构建工具链必须为 1.26.11，与产物元数据和镜像标签一致。
ARG BUILDER_IMAGE=golang:1.26.11-alpine3.23

# ---- 构建阶段 ----
FROM ${BUILDER_IMAGE} AS builder
ENV GOTOOLCHAIN=local
WORKDIR /build
COPY go.mod go.sum ./
# 默认仍使用官方 Go 代理；纯本地验收可把 GOPROXY 指向只读的
# module cache HTTP 容器，并在 Docker internal network 中完成不可发外的构建。
ARG GOPROXY=https://goproxy.cn,direct
ARG GOSUMDB=sum.golang.google.cn
ARG VCS_REF=unknown
ARG IMAGE_VERSION=unknown
ARG SOURCE_MANIFEST_SHA256
RUN GOPROXY="$GOPROXY" GOSUMDB="$GOSUMDB" go mod download
COPY . .
# glebarez/modernc 纯 Go sqlite,无需 CGO,静态编译;main 在模块根。
# 产物输出到 /app —— 不能用 /build/monitor:源码里有 monitor/ 目录,COPY . . 后 /build/monitor 已是目录,
# go build -o 到已存在目录会把二进制塞进去,导致产物变成目录、容器 ENTRYPOINT 报 "is a directory"。
RUN test "$(go env GOVERSION)" = "go1.26.11" && CGO_ENABLED=0 GOOS=linux go build -trimpath \
      -ldflags="-s -w -X github.com/yl0711-coder/newapi-monitor/monitor.BuildGitSHA=${VCS_REF} -X github.com/yl0711-coder/newapi-monitor/monitor.BuildImageVersion=${IMAGE_VERSION}" \
      -o /app .
RUN test -n "$SOURCE_MANIFEST_SHA256" \
    && CGO_ENABLED=0 GOOS=linux go run ./cmd/buildmanifest -out /buildinfo -binary /app \
      -revision "$VCS_REF" -version "$IMAGE_VERSION" -expect "$SOURCE_MANIFEST_SHA256"

# ---- 运行阶段(最小镜像)----
FROM ${RUNTIME_IMAGE} AS runtime
USER root
ARG OFFLINE_RUNTIME=false
# 官方 tag 的根文件系统可能早于同一 Alpine 小版本仓库中的安全补丁；构建时先把
# 已安装基础包升级到 3.23 当前补丁，再安装运行所需的证书和时区数据。
RUN if [ "$OFFLINE_RUNTIME" = "true" ]; then \
      test -s /etc/ssl/certs/ca-certificates.crt \
      && test -e /usr/share/zoneinfo/Asia/Shanghai; \
    else \
      apk upgrade --no-cache \
      && apk add --no-cache ca-certificates tzdata; \
    fi
# An application image cannot be a runtime base: deleting its old binary later
# would still retain that program layer in the final image.
RUN test ! -e /app/monitor \
    && if ! id app >/dev/null 2>&1; then adduser -D -u 1000 app; fi \
    && mkdir -p /app/buildinfo /data /backup /evidence \
    && chown app:app /data /backup /evidence \
    && chmod 0555 /app /app/buildinfo
ARG VCS_REF=unknown
ARG IMAGE_VERSION=unknown
ARG SOURCE_MANIFEST_SHA256
LABEL org.opencontainers.image.revision="$VCS_REF"
LABEL org.opencontainers.image.version="$IMAGE_VERSION"
LABEL org.opencontainers.image.source-manifest-sha256="$SOURCE_MANIFEST_SHA256"
LABEL org.opencontainers.image.go-version="go1.26.11"
WORKDIR /app
COPY --from=builder --chmod=0555 /app /app/monitor
COPY --from=builder --chmod=0444 /buildinfo/source-manifest.json /buildinfo/build-info.json /app/buildinfo/
# 运行用户只需要写 /data、独立备份卷 /backup 和短期证据卷 /evidence。二进制与 /app 保持 root 所有且只读，
# 避免应用进程被利用后直接改写自身可执行文件并跨重启留驻。
USER app
ENV MONITOR_ADDR=:8090 \
    MONITOR_STORE_PATH=/data/monitor.db \
    TZ=Asia/Shanghai
EXPOSE 8090
VOLUME ["/data", "/backup", "/evidence"]
ENTRYPOINT ["/app/monitor"]
