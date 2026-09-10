FROM golang:1.26.6-alpine3.23 AS build
WORKDIR /src
COPY cmd/ecssynthetic/*.go ./
RUN GO111MODULE=off CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /synthetic .
FROM alpine:3.23 AS runtime
RUN apk upgrade --no-cache && addgroup -S collector && adduser -S -G collector collector
COPY --from=build /synthetic /usr/local/bin/synthetic
USER collector
ENTRYPOINT ["/usr/local/bin/synthetic"]
