# v1.18.75-rc.1 — usage-log diagnostics and SQL query quality

## Candidate scope

Independent branch: `fix/usage-log-stat-latency-20261126`.
Base commit: `c309d2e9da8fc2aeb9fb088b792d8105fd633dd8`.

- Preserve ordinary usage-log filters, ordering, billing details and monetary
  semantics while improving list/count/aggregate query dispatch.
- Add explicit rejection display and parameterized error/rejection text search.
  A model refusal does not change the stored consumption type or billing amount.
- Search diagnostic candidates in bounded statements and assemble a complete
  logical page. Confirm another matching record before enabling the next page.
  Incomplete searches retain acknowledged progress and are labelled incomplete.
- Keep normal consumption statistics automatic. Explicit non-consumption types
  or complete first-page results without consumption do not trigger amount/RPM/TPM
  requests. Recent-minute RPM/TPM and selected-window amount scopes are unchanged.
- Improve related local and replica SQL, narrow projections and bounded batch
  access. Retain historical identity, full-result and authorization semantics.
- Keep audit/benchmark tooling outside the production Docker build context.
  Raw local evidence and customer-derived diagnostics are not published here.

## Local acceptance

Acceptance uses deterministic synthetic data unless specifically stated. No
production database, service or setting was changed during this final round.

- Frontend suite: 701 passed, 9 conditional skips, no failures.
- Usage-log race tests: 52 top-level passes. Related Monitor/monthly-source/task
  race tests: 23 further top-level passes, including the scale recheck; 75 distinct
  top-level race tests passed overall. Environment-dependent skips are not passes.
- Complete-page coverage includes 60 cap/batch/page-size combinations, sparse
  matches, empty gaps, time/ID disorder, ties, exact last pages and lookahead.
- Fault coverage includes cancellation, stale responses, source failures,
  checkpoints, invalid cursors, query admission, query termination and statistics
  failures without fabricated zeros.
- Seven local embedded-JS/live-Go-handler integration scenarios passed.
- Real Chrome desktop checks passed for error/rejection filters, original billing
  details, complete small results and ordinary first/next/last-page behavior.
  This is not mobile or cross-browser acceptance.
- Network-isolated MySQL 8.4.9: 200,000 synthetic rows; full-page result IDs,
  channel enrichment and next-page state passed for no match, oldest-only match,
  rejection and error-text cases. Whole-page timings included the socket-CLI test
  adapter, so they are not production latency or a universal speedup claim.
- `go vet ./...`, function-size ratchet, JavaScript syntax, diff checks and the
  Linux-target dependency policy passed locally. Local Go was 1.27.4 darwin/arm64,
  not the pinned production Go/Linux toolchain.

Initial failed checks were retained locally and resolved/rechecked, not counted
as passes. The historical-group scale test keeps 100,000 rows and all semantic
checks in both modes: seven timing rounds natively and one under race; race runs
do not overwrite native performance evidence.

## Release boundaries

This tag identifies a CI candidate, not permission to deploy or merge main.
Before production release, require:

1. Full CI/security gates on this exact commit and an immutable Linux image.
2. Verify the target MySQL version, read-only connection and existing
   `idx_created_at_id_rc26(created_at,id)` index used by diagnostic batching.
   This release does not create an index on the source database. Missing required
   indexes fail explicitly rather than silently selecting a full-window sort.
3. Formal candidate startup/migration/rollback acceptance and authenticated
   read-only baselines under the project's release process.
4. Explicit deployment approval, controlled observation and rollback readiness.

Only merge main after the version is verified stable in production. Preserve the
previous immutable image and required recovery evidence; this document does not
authorize deleting backups, changing credentials or running production queries.
