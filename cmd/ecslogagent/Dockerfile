# Isolated agent artifact. Never replaces the production Monitor image.
FROM golang:1.26.6-alpine3.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY internal/ecslogagent/*.go ./internal/ecslogagent/
COPY internal/ecsarchive/*.go ./internal/ecsarchive/
COPY cmd/ecslogagent/*.go ./cmd/ecslogagent/
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ecslogagent ./cmd/ecslogagent

FROM alpine:3.23 AS runtime
RUN apk upgrade --no-cache && apk add --no-cache ca-certificates \
    && addgroup -S collector && adduser -S -G collector collector \
    && mkdir -p /data/ecs && chown collector:collector /data/ecs && chmod 700 /data/ecs
COPY --from=build /out/ecslogagent /usr/local/bin/ecslogagent
USER collector
ENTRYPOINT ["/usr/local/bin/ecslogagent"]
