# v1.18.15 integration acceptance

## Scope

- Base: main `f080ed9bd302a288d1726752bc188a3b80cbdaeb` (deployed v1.18.13 code).
- Merge: v1.18.14 `ab9326235fa782d50d3e4ef7ee7f244e0c6a25e7`.
- Preserve both histories. Do not rewrite either release tag, update main, or deploy as part of tagging.
- Exclude unrelated dirty development files, local SQLite snapshots, credentials and acceptance artifacts.

## Conflict resolutions

1. `model_statistics.go`: retain main's request cancellation checks and the incoming channel-name lookup. Bind that lookup to the request context, and propagate cancellation after it.
2. `page.html`: retain finance asset revision 57 and incoming model-statistics asset revision 5.
3. Cancellation regression: cover both user-directory and channel-name reads while the SQLite pool is occupied. Seed the fixture inside the new finalized report window rather than the unfinalized current minute.

No dependency, finance formula, upstream configuration, production traffic or collector deployment changes were added for the integration.

## Local verification (2026-10-08)

- Go 1.26.6 full test suite: 2,541 top-level tests passed, 41 conditional tests skipped, no failures (20 packages).
- Integration-focused race tests: 108 passed; model statistics, FRT migration/coverage, customer health and source lifecycle.
- Frontend: 222 passed, 6 conditional skips, no failures.
- `go vet`, golangci-lint, dependency policy and Linux/amd64 build passed.
- CI race-shard inventory: all 2,311 monitor tests assigned exactly once to the retained 12 shards.
- Gitleaks source scan: no leaks. Linux/amd64 govulncheck: no reachable or imported-package findings; GO-2026-5932 remains a module-only advisory. Dependencies are unchanged.

## SQLite/container verification

Used a copy of an existing closed local snapshot, with Docker networking disabled, no host ports and no production credentials. Limits: 768 MiB and 1.25 CPUs.

- v1.18.13 baseline -> candidate -> restart -> v1.18.13 binary rollback all started and served reports.
- Six report/status endpoints checked sequentially and concurrently at each stage (48 successful requests).
- User usage, channel management and finance business projections matched the baseline. Only the naturally advancing usage-age field was excluded from comparison.
- All 112 existing main-store tables retained their original columns and values across upgrade, restart and rollback. The incoming FRT columns and coverage table are additive.
- The usage-facts database was byte-identical; source snapshots were unchanged.
- The migration generated a verified pre-migration dual-store backup.
- No panic, SQLite busy/locked, OOM or unexpected restart was observed. Temporary test containers were removed.

## Boundaries

This is integration acceptance, not proof that historical production evidence is complete. Conditional live/credential/private-fixture tests were not counted as passed. Source replay, real upstream recovery and production-load effects were not exercised offline. Full tag CI (including all race shards and image vulnerability scans) must pass before a separately authorized deployment. A production rollback must follow the existing image/configuration and verified backup procedure; the offline old-binary check does not replace a production backup.
