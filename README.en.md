English | [简体中文](README.md)

# newapi-monitor

> **Upstream monitor for new-api** — a zero-intrusion, read-only sampling sidecar for stability monitoring and email alerts.

[![CI](https://github.com/yl0711-coder/newapi-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/yl0711-coder/newapi-monitor/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/yl0711-coder/newapi-monitor)](https://goreportcard.com/report/github.com/yl0711-coder/newapi-monitor)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A standalone "upstream stability" dashboard for the [new-api](https://github.com/Calcium-Ion/new-api) gateway. It uses a **read-only account** to run one small aggregate query per minute against new-api's log database, stores the result in a local SQLite, and shows success rate, anomalies and latency (TTFB/TTFT) broken down by **group / channel / model**, with email alerts on anomalies. **It never modifies new-api and never writes to its database.**

## Features
- **Zero intrusion**: read-only sampling, one small aggregate query per cycle, no load on your production DB.
- **Three-state stability**: success / anomaly (`client_gone` and other client aborts) / failure (upstream errors), aggregated by group × channel × model.
- **Latency**: P50/P95 total latency, TTFB/TTFT first-token distribution, output speed (tok/s).
- **Auth via new-api**: reuses new-api user identity (calls its `/api/user/login`), role-gated, no separate account system.
- **Email alerts**: error rate / error burst / anomaly cluster / sampler-down rules with configurable thresholds.
- **Lightweight & self-contained**: pure Go + embedded SQLite (`CGO_ENABLED=0`), single container, no external dependencies.

## How it works
```
new-api log DB (MySQL) ──one read-only aggregate query / 60s──► newapi-monitor ──► local SQLite ──► dashboard / email alerts
```
The sampler is the **only** component that touches new-api's DB; the dashboard reads from the local SQLite, fully isolated from production.

## Quick start (Docker)
```bash
docker run -d --name newapi-monitor \
  -p 8090:8090 \
  -e NEWAPI_LOG_DSN='ro_user:pass@tcp(db-host:3306)/newapi?charset=utf8mb4&timeout=5s&readTimeout=10s' \
  -e MONITOR_NEWAPI_BASE_URL='https://your-newapi.example.com' \
  -e MONITOR_SESSION_SECRET="$(openssl rand -hex 32)" \
  -v newapi_monitor_data:/data \
  ghcr.io/yl0711-coder/newapi-monitor:latest
```

Open `http://<host>:8090` and log in with a new-api admin account. See [`docker-compose.example.yml`](docker-compose.example.yml) for a full compose file. In production, put a reverse proxy (nginx / Caddy) in front for HTTPS.

## Configuration (environment variables)
| Variable | Description | Default |
|---|---|---|
| `NEWAPI_LOG_DSN` | **Read-only** DSN to new-api's DB (MySQL) | required |
| `MONITOR_NEWAPI_BASE_URL` | new-api base URL, used for login auth | required |
| `MONITOR_SESSION_SECRET` | Session signing key (`openssl rand -hex 32`) | random if empty |
| `MONITOR_ADDR` | Listen address | `:8090` |
| `MONITOR_STORE_PATH` | Local sampling DB path | `/data/monitor.db` |
| `MONITOR_SAMPLE_SECONDS` | Sampling interval (seconds) | `60` |
| `MONITOR_RETENTION_DAYS` | Local retention (days) | `7` |
| `MONITOR_BACKFILL_HOURS` | Hours of history to backfill on start | `24` |
| `MONITOR_HOUR_RETENTION_DAYS` | Hourly-rollup retention (long-term trend + WoW/DoD) | `90` |
| `MONITOR_HEARTBEAT_URL` | Dead-man heartbeat URL (e.g. healthchecks.io); empty = off | empty |
| `MONITOR_SITE_NAME` | Fallback site name for the public board; name/favicon are synced from new-api `system_name`/`logo` at deploy, this is only used when the main site is unreachable | empty |
| `MONITOR_INGEST_TOKEN` | Auth token for the "Rejected requests" ingest endpoint `POST /internal/rejections`, used by per-node [newapi-reject-collector](https://github.com/yl0711-coder/newapi-reject-collector) to push pre-routing rejections; **empty = endpoint disabled** | empty |

## Rejected requests (pre-routing · logs blind spot)

new-api's "No available channel" and other **pre-routing rejections** are not written to the `logs` table, so any logs-based monitor is blind to them. The companion sidecar collector [newapi-reject-collector](https://github.com/yl0711-coder/newapi-reject-collector) tails new-api logs on each node, extracts these rejections, and `POST`s them to `/internal/rejections` (authenticated by `MONITOR_INGEST_TOKEN`); the monitor stores them in `rejection_samples` and shows a "Rejected requests" panel by model × group.

The panel is gated by a **super-admin toggle** (Alert settings, **off by default**): it only shows when enabled, with a note that the collector must be installed on each node; when enabled but no data has arrived yet, it shows an empty state. The ingest endpoint returns 503 when `MONITOR_INGEST_TOKEN` is unset. Toggle off / no token / no data — none of these affects other monitor features.

## Public status board (public, no login)
Besides the internal monitor, the same process serves a **customer-facing public status page** (sanitized, no login), ideal for a dedicated subdomain (e.g. `status.example.com`):

- `GET /status` — light card-style status page (embedded, self-contained).
- `GET /public/status` — sanitized JSON polled by the page.

Dimensions are **group (line) × model**: channels are transparent to users. Visible groups come from new-api's `/api/pricing` (`usable_group`, i.e. the groups selectable when creating a token); display names match the main site. Status is synthesized from **topology health (whether a group×model has any usable channel)** + **last-7-day traffic**: a configured group×model with no usable channel shows "outage".

> **Disabled channels are excluded from stability** (board + internal monitor): stability aggregates (overview / group / model / trend) only count traffic from channels that are **currently enabled and after their enable time**. Failures from manually-disabled / auto-disabled channels no longer drag a model down; a re-enabled channel (including a fresh deploy) is counted from its enable time (`channel_snaps.enabled_since`). The internal "by channel" table still lists disabled channels for diagnosis.

**Hard isolation**: the board is the standalone `monitor/public` package, reads only the local sampling DB, and never references internal structs; the public surface **never emits** channel names/IDs/IPs, cost/quota, tokens/users, request volume/QPS, or error details.

Reverse-proxy example (Caddy, by subdomain):
```
status.example.com {
    reverse_proxy monitor:8090
    rewrite / /status
}
```

## Permissions
Login reuses new-api identity (only calls its `/api/user/login`):
- `role >= 10` (admin): can log in and view;
- `role = 100` (super admin): can edit alert settings.

## Read-only account
Create a dedicated read-only account for new-api's DB, granting only `SELECT` on `logs` and `channels`, for `NEWAPI_LOG_DSN`:
```sql
CREATE USER 'ro_user'@'%' IDENTIFIED BY '<strong-password>';
GRANT SELECT ON newapi.logs     TO 'ro_user'@'%';
GRANT SELECT ON newapi.channels TO 'ro_user'@'%';
```

## Security
- The image contains **no secrets**; DSN, session key and SMTP credentials are injected via environment variables.
- SMTP credentials are never echoed back to the frontend.

## Build
```bash
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o newapi-monitor .   # binary
docker build -t newapi-monitor .                                        # image
```
On push to `main` or a `v*` tag, GitHub Actions runs `go vet` + `go test`, then builds and publishes the image to GHCR (see [`.github/workflows/ci.yml`](.github/workflows/ci.yml)).

## Third-party
- [Apache ECharts](https://echarts.apache.org/) (Apache-2.0) — dashboard charts, vendored & self-served (no CDN).
- [go-mail](https://github.com/wneessen/go-mail) (MIT) — alert email delivery.
- [gin](https://github.com/gin-gonic/gin) / [GORM](https://gorm.io) / [glebarez/sqlite](https://github.com/glebarez/sqlite) / [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) / [godotenv](https://github.com/joho/godotenv).

## License
[MIT](LICENSE)
