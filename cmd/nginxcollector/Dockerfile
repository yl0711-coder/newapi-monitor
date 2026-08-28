FROM golang:1.26.6-alpine3.23 AS build
WORKDIR /src
COPY cmd/nginxcollector/*.go ./
# 采集器只依赖 Go 标准库；关闭 module 解析，避免为了它下载 Monitor 的 AWS/Gin/GORM 依赖。
RUN GO111MODULE=off CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/nginxcollector .

FROM alpine:3.23
RUN apk upgrade --no-cache \
    && apk add --no-cache ca-certificates \
    && addgroup -S collector \
    && adduser -S -G collector collector \
    && mkdir -p /data /evidence \
    && chown collector:collector /data /evidence
COPY --from=build /out/nginxcollector /usr/local/bin/nginxcollector
USER collector
ENTRYPOINT ["/usr/local/bin/nginxcollector"]
