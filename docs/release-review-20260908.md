# Monitor reporting and synchronization release

## Changes

- Preserve numeric business summaries; display quiet consumption freshness notes below upstream amounts and detailed diagnostics on the sync page.
- Show independently scoped natural-day bills when an upstream cannot provide the selected hourly boundary; never combine incompatible periods in exact interval totals.
- Preserve Spring daily conversion evidence under its existing 1:1 contract, including compatible legacy pending stages. Keep optional record-based hourly collection disabled by default.
- Add bounded, authenticated, read-only upstream diagnostics with typed provider signatures, SSRF protections, queue/network deadlines and safe credential handling.
- Remove the production Redis client while retaining bounded local caches and explicit stale-result policy.
- Strengthen rollback-safe SQLite contention retries, preserve unfinished backfill jobs during retention cleanup and protect record writes with local disk admission checks.
- Harden report coverage, delayed response handling, missing monetary values and error precedence.
- Verify exact CI race-shard coverage and database assertions; add deterministic retry-policy and renderer regressions.

## Acceptance and release gates

Local full Go regression, focused race tests, static checks, renderer regressions,
isolated Docker/browser acceptance and independent snapshot reconciliation passed.
These do not certify current production completeness, peak load or upstream API
availability. Private snapshots, operating figures, credentials and detailed local
acceptance notes are intentionally not release artifacts.

Publish only the immutable digest built by CI for the exact committed revision.
Before replacing Monitor, verify its actual Compose inputs, source lease, volume
identity, paired backup, disk headroom and recoverable prior image. Stop only the
old Monitor; never run duplicate production-source workers or change relay/RDS
configuration as part of this release. Preserve existing collection budgets and
keep Spring record mode disabled pending separate controlled validation.

After replacement, verify readiness, continued source progress, fixed-window
usage/financial consistency, reporting views and absence of unexpected restarts.
Historical backfill and third-party account faults remain separately observable;
successful deployment does not imply all historical gaps have disappeared.
