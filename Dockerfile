# new-api 上游监控 —— 独立项目、独立容器(无外部依赖)。
# 构建上下文是 monitor 模块根目录:
#   docker build -t newapi-monitor .
# 页面(page/alert/login.html)已 go:embed 进二进制,无需单独拷模板。
# 本地采样库与报警配置落在挂载卷 /data 上,不进镜像。

# ---- 构建阶段 ----
FROM golang:1.26-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# glebarez/modernc 纯 Go sqlite,无需 CGO,静态编译;main 在模块根
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /build/monitor .

# ---- 运行阶段(最小镜像)----
FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /build/monitor /app/monitor
RUN adduser -D -u 1000 app && mkdir -p /data && chown -R app /app /data
USER app
ENV MONITOR_ADDR=:8090 \
    MONITOR_STORE_PATH=/data/monitor.db \
    TZ=Asia/Shanghai
EXPOSE 8090
VOLUME ["/data"]
ENTRYPOINT ["/app/monitor"]
