# Monitor recovery candidate review

## Scope

- Separate business figures from collection diagnostics; preserve unknown monetary values and accounting integrity checks.
- Keep model dashboard historical facts independent of current channel routing; distinguish observed, finalized and offline snapshot views.
- Prevent stale responses or failed queries from leaving values under a different selected range.
- Preserve gaps in capacity curves and publish complete averages only with coverage evidence.
- Reconcile Lightsail inventory only after successful complete discovery; keep historical metrics.
- Separate expected Nginx producers from ingestion authorization.
- Retry rollback-safe local SQLite writes with bounded attempts and time; never repeat source queries as part of local retry.
- Reparse legacy nullable parser versions without losing raw fund evidence or inventing currency conversions.

## Local gates

- Go 1.26.6 full regression passed.
- Changed-area race tests passed.
- 26 offline frontend state tests passed, including late bodies/errors, unknown amounts, diagnostic navigation and snapshot mode.
- Matching-toolchain golangci-lint passed. Reachable vulnerability scan found no reachable vulnerabilities; dependency modules still include uncalled advisories.
- Real browser checks on an isolated local snapshot passed; injected local HTTP failure clears stale money, rows and latency charts.
- Latest remote main is an ancestor of the candidate base.

Private snapshots, operating figures, raw logs, credentials and local real-data acceptance notes are not release artifacts and must not be committed.

## Release gates and limitations

Local success does not replace CI on the exact committed revision. Use the fixed registry digest of a successful CI build; do not deploy a mutable tag or a locally built image.

Before switching Monitor, verify the current deployment configuration, source lease, immutable paired backup, volume permissions and storage headroom. Never run two production-source workers together. Do not alter relay services, relay database configuration or start bulk recovery as part of this release.

Existing paused historical jobs, upstream evidence gaps and migrated producer/probe configuration remain separate operational work. A code release does not certify historical financial completeness or complete source coverage.
