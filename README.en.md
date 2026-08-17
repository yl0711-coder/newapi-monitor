English | [简体中文](README.md)

# newapi-monitor

> **Upstream monitor for new-api** — a read-only, bounded sampling and local-facts sidecar for stability monitoring and email alerts.

[![CI](https://github.com/yl0711-coder/newapi-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/yl0711-coder/newapi-monitor/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/yl0711-coder/newapi-monitor)](https://goreportcard.com/report/github.com/yl0711-coder/newapi-monitor)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A standalone "upstream stability" dashboard for the [new-api](https://github.com/Calcium-Ion/new-api) gateway. It uses a **read-only account** for bounded, serialized, backoff-protected sampling and fact synchronization, stores results in local SQLite, and shows success rate, anomalies and latency (TTFB/TTFT) by **group / channel / model**, with email alerts. **It never writes to the new-api database.**

## Features
- **Read-only source boundary**: bounded sampling/fact queries, a shared background-query gate, and exponential backoff protect the production database.
- **Three-state stability**: success / anomaly (`client_gone` and other client aborts) / failure (upstream errors), aggregated by group × channel × model.
- **Latency**: P50/P95 total latency, TTFB/TTFT first-token distribution, output speed (tok/s).
- **Auth via new-api**: reuses new-api user identity (calls its `/api/user/login`), role-gated, no separate account system.
- **Email alerts**: error rate / error burst / anomaly cluster / sampler-down rules with configurable thresholds.
- **Lightweight & self-contained**: pure Go + embedded SQLite (`CGO_ENABLED=0`), single container, no external dependencies.

## How it works
```
new-api log DB (MySQL) ──bounded read-only sampling/sync──► newapi-monitor ──► local SQLite ──► dashboard / email alerts
```
Stability and aggregate-usage dashboards read local SQLite. Background samplers/fact builders are the normal source readers; raw log detail and CSV export remain explicitly bounded direct reads from new-api's database.

## Quick start (Docker)
```bash
docker run -d --name newapi-monitor \
  -p 8090:8090 \
  -e NEWAPI_LOG_DSN='ro_user:pass@tcp(db-host:3306)/newapi?charset=utf8mb4&timeout=5s&readTimeout=10s' \
  -e MONITOR_NEWAPI_BASE_URL='https://your-newapi.example.com' \
  -e MONITOR_SESSION_SECRET="$(openssl rand -hex 32)" \
  -v newapi_monitor_data:/data \
  ghcr.io/yl0711-coder/newapi-monitor:REPLACE_WITH_ACCEPTED_TAG_OR_DIGEST
```

Open `http://<host>:8090` and log in with a new-api admin account. See [`docker-compose.example.yml`](docker-compose.example.yml) for a full compose file. In production, put a reverse proxy (nginx / Caddy) in front for HTTPS.

## Configuration (environment variables)
| Variable | Description | Default |
|---|---|---|
| `NEWAPI_LOG_DSN` | **Read-only** DSN to new-api's DB (MySQL) | required |
| `MONITOR_NEWAPI_BASE_URL` | new-api base URL, used for login auth | required |
| `MONITOR_SESSION_SECRET` | Session signing key (`openssl rand -hex 32`) | random if empty |
| `MONITOR_ADDR` | Listen address | `:8090` |
| `MONITOR_PORTAL_ADDR` | Dedicated client usage-portal listener; empty disables it | empty |
| `MONITOR_USAGE_REDIS_ADDR` | Private Redis address for usage aggregates; empty uses the bounded local fallback only | empty |
| `MONITOR_USAGE_REDIS_USERNAME` | Redis ACL user; production should restrict it to `nxmon:*` | empty |
| `MONITOR_USAGE_REDIS_PASSWORD` | Redis password, injected through the environment only | empty |
| `MONITOR_USAGE_REDIS_DB` | Redis DB number; ACL and key prefix remain the security boundary | `0` |
| `MONITOR_USAGE_REDIS_PREFIX` | Usage-cache key prefix | `nxmon:usage:v1` |
| `MONITOR_TRUSTED_PROXIES` | Comma-separated trusted proxy IPs/CIDRs allowed to supply the client IP; empty trusts no forwarding headers | empty |
| `MONITOR_STORE_PATH` | Local sampling DB path | `/data/monitor.db` |
| `MONITOR_USAGE_FACTS_STORE_PATH` | Separate usage-facts SQLite path, isolating high-volume facts/WAL from control data | `<MONITOR_STORE_PATH directory>/usage-facts.db` |
| `MONITOR_STORE_BACKUP_ENABLED` | Runtime paired main+facts backup; migration snapshots remain mandatory independently | `true` |
| `MONITOR_STORE_BACKUP_DIR` | Runtime and pre-migration snapshot directory | `<MONITOR_STORE_PATH directory>/backups` |
| `MONITOR_STORE_BACKUP_RETENTION` | Number of verified main+facts `backup-set` manifests retained | `7` |
| `MONITOR_STORE_MIGRATION_BACKUP_RETENTION` | Number of paired pre-migration snapshot sets retained | `3` |
| `MONITOR_USAGE_FACTS_ENABLED` | Build member-scoped local usage facts in the background | `false` |
| `MONITOR_USAGE_FACTS_READ_ENABLED` | Serve aggregates only from a completely published local snapshot; unavailable data fails closed and never falls back to source `logs` | `false` |
| `MONITOR_USAGE_FACTS_FULL_HISTORY_ENABLED` | Build full history from each member's signed registration/first-log boundary | `false` |
| `MONITOR_USAGE_FACTS_HISTORY_SOURCE_MODE` | Source-completeness declaration; full history starts only with DBA-attested `complete` | `unverified` |
| `MONITOR_USAGE_FACTS_HISTORY_SOURCE_EPOCH` | Stable identifier for the source retention/routing contract; changes force a complete re-sign | empty |
| `MONITOR_USAGE_FACTS_HISTORY_SOURCE_DUTY_PERCENT` | Maximum cold-history source duty cycle; recent Tail always has priority | `20` |
| `MONITOR_USAGE_FACTS_CLASSIFICATION_MIGRATION_ENABLED` | Explicit classification-maintenance switch; while enabled, usage reads must be disabled | `false` |
| `MONITOR_SAMPLE_SECONDS` | Sampling interval (seconds) | `60` |
| `MONITOR_RETENTION_DAYS` | Local retention (days) | `7` |
| `MONITOR_BACKFILL_HOURS` | Source-epoch startup catch-up; durable-watermark based and hard-capped at one hour | `1` |
| `MONITOR_HOUR_RETENTION_DAYS` | Hourly-rollup retention (long-term trend + WoW/DoD) | `90` |
| `MONITOR_STABILITY_CLASSIFICATION_MIGRATION_ENABLED` | Explicit low-priority Stability classification migration; do not run alongside Usage expansion | `false` |
| `MONITOR_HEARTBEAT_URL` | Dead-man heartbeat URL (e.g. healthchecks.io); empty = off | empty |
| `MONITOR_SITE_NAME` | Fallback site name for the public board; name/favicon are synced from new-api `system_name`/`logo` at deploy, this is only used when the main site is unreachable | empty |
| `MONITOR_INGEST_TOKEN` | Auth token for the "Rejected requests" ingest endpoint `POST /internal/rejections`, used by per-node [newapi-reject-collector](https://github.com/yl0711-coder/newapi-reject-collector) to push pre-routing rejections; **empty = endpoint disabled** | empty |

The production compose example deliberately requires pre-created external data and backup volumes plus a DBA-signed source epoch. `SOURCE_MODE=complete` is a declaration, not proof: before enabling full history, verify with read-only SQL and the archive/retention policy that hot `logs` has no gap from every active member's registration time. The paired local `backup-set` protects application consistency, but it is **not** off-site disaster recovery; production sign-off still requires encrypted immutable replication to another failure domain and a restore-to-new-volume drill. See [`docs/monitor-operations.md`](docs/monitor-operations.md).

## Rejected requests (pre-routing · logs blind spot)

new-api's "No available channel" and other **pre-routing rejections** are not written to the `logs` table, so any logs-based monitor is blind to them. The companion sidecar collector [newapi-reject-collector](https://github.com/yl0711-coder/newapi-reject-collector) tails new-api logs on each node, extracts these rejections, and `POST`s them to `/internal/rejections` (authenticated by `MONITOR_INGEST_TOKEN`); the monitor stores them in `rejection_samples` and shows a "Rejected requests" panel by model × group.

The panel is gated by a **super-admin toggle** (Alert settings, **off by default**): it only shows when enabled, with a note that the collector must be installed on each node; when enabled but no data has arrived yet, it shows an empty state. The ingest endpoint returns 503 when `MONITOR_INGEST_TOKEN` is unset. Toggle off / no token / no data — none of these affects other monitor features.

## Public status board (public, no login)
Besides the internal monitor, the same process serves a **customer-facing public status page** (sanitized, no login), ideal for a dedicated subdomain (e.g. `status.example.com`):

- `GET /status` — light card-style status page (embedded, self-contained).
- `GET /public/status` — sanitized JSON polled by the page.

Dimensions are **group (line) × model**: channels are transparent to users. Visible groups come from new-api's `/api/pricing` (`usable_group`, i.e. the groups selectable when creating a token); display names match the main site. Status is synthesized from **recent uptime + whether any usable upstream exists**: Operational (≥99%) · Degraded (50–99%, still serving) · Outage (<50% or no usable upstream). A line is at most Degraded as long as any model is Operational; Outage only when no model is Operational (it does not take the worst model, so one degraded model won't mark the whole line as down).

> **Disabled channels are excluded from stability** (board + internal monitor): stability aggregates (overview / group / model / trend) only count traffic from channels that are **currently enabled and after their enable time**. Failures from manually-disabled / auto-disabled channels no longer drag a model down; a re-enabled channel (including a fresh deploy) is counted from its enable time (`channel_snaps.enabled_since`). The internal "by channel" table still lists disabled channels for diagnosis.

> **Only user-selectable models are shown / counted**: the board only displays — and the internal monitor only aggregates — (group, model) pairs that are both in a visible group (`/api/pricing`) and configured on an enabled channel. Non-selectable ones (all-disabled / only in non-selectable groups / merely mis-routed to a channel that doesn't list them) are excluded everywhere — board, monitor and alerts (alerting on something nobody can select is pointless). The internal "by channel" table is not filtered, so mis-routes stay visible for diagnosis.

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
Create a dedicated read-only account for new-api's DB, granting only `SELECT` on `logs`, `channels`, `users`, `tokens`, and `options`, for `NEWAPI_LOG_DSN`:
```sql
CREATE USER 'ro_user'@'%' IDENTIFIED BY '<strong-password>';
GRANT SELECT ON newapi.logs     TO 'ro_user'@'%';
GRANT SELECT ON newapi.channels TO 'ro_user'@'%';
GRANT SELECT ON newapi.users    TO 'ro_user'@'%';
GRANT SELECT ON newapi.tokens   TO 'ro_user'@'%';
GRANT SELECT ON newapi.options  TO 'ro_user'@'%';
```

## Client usage portal

The client portal has its own listener, cookies, and routes. The production compose example enables it at
`127.0.0.1:8091` through `MONITOR_PORTAL_ADDR=:8091`; this is **not** an IP allowlist for a developer machine or customers.
Expose the admin site and the client portal through separate HTTPS reverse-proxy hosts:

```caddy
monitor.example.com {
    reverse_proxy 127.0.0.1:8090
}
usage.example.com {
    reverse_proxy 127.0.0.1:8091
}
```

Do not publish ports `8090` or `8091` directly. A super admin provisions each group account from Usage management.
If Caddy/Nginx runs on another host or in another container network, add its actual IP/CIDR to `MONITOR_TRUSTED_PROXIES`; otherwise login limiting uses the proxy address.

Optional Redis stores only reproducible matrix, daily/group/model, and per-token log aggregates. Ranges containing today use a 60-second TTL; closed historical ranges use a 10-minute TTL. Reselecting dates in the admin view forces a fresh aggregation. Usernames, emails, current balances, current token metadata, sessions, raw logs, and CSV data are never written to Redis. Redis failures fall back to a bounded local cache (128 entries, 16 MiB, at most 60 seconds), whose expiry never exceeds the remote record's remaining TTL. A failed remote operation opens a 30-second backoff window so steady-state fallback does not query the source more frequently than the previous 60-second local cache. Redis is an optimization, not a correctness dependency; production must use a private endpoint and a prefix-scoped ACL user.

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
