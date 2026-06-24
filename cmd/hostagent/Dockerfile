# hostagent —— 每个节点上采集 OS 指标(内存/Swap/磁盘/load/容器)推送给 monitor。
# 构建上下文是 monitor 模块根目录:
#   docker build -f cmd/hostagent/Dockerfile -t newapi-hostagent .
# 无第三方依赖、纯 stdlib、静态编译。
#
# 运行需挂载(只读):宿主 /proc -> /host/proc、宿主 / -> /host/root、/var/run/docker.sock。
# 见 docker-compose.hostagent.yml。

# ---- 构建阶段 ----
FROM golang:1.26-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /app/hostagent ./cmd/hostagent

# ---- 运行阶段(最小镜像)----
FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /app/hostagent /usr/local/bin/hostagent
ENV TZ=Asia/Shanghai \
    HOSTAGENT_PROC=/host/proc \
    HOSTAGENT_ROOTFS=/host/root
ENTRYPOINT ["/usr/local/bin/hostagent"]
