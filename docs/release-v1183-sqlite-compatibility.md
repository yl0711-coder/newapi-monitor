# v1.18.3 integration: compatibility with the v1.17.15 SQLite store

## Failure and scope

The integrated candidate includes production v1.17.15. Delivery classification
changes from 6 to 7, but finance and channel accounting previously filtered
hourly facts by that delivery version. On the same historical database, the
production baseline returned a report while the candidate failed with an
internal-account subtraction mismatch and returned zero channel requests.

The patch changes only that upgrade boundary. It does not change NewAPI,
accounting formulas, upstream collection, source traffic, or the SQLite schema.
No production changes were made during acceptance.

## Compatibility contract

- Monetary accounting accepts the explicitly reviewed delivery versions 6 and 7.
  Source membership, request totals, token totals, consumption and refunds are
  unchanged between these versions. Other versions remain excluded.
- Coverage uses the same compatibility set and still rejects failed receipts,
  missing hours, and zero-hour receipts contradicted by positive minute facts.
- Internal-account and excluded-group deductions retain their existing checks.
- The hourly writer already replaces facts and their receipt in one transaction;
  the version is not part of the primary key. Reclassification and retries do
  not add a second monetary copy of an hour.
- Delivery classification remains version 7. Channel rates from all-v6 facts
  are labelled historical; a mixed-policy interval does not publish one combined
  rate. Accounting amounts remain available while delivery history is migrated.
- Report and monthly cache keys are advanced so previously computed partial
  projections cannot survive the compatibility change.

## Local acceptance, 2026-09-26

- Targeted accounting, finance and channel regression suite: passed.
- Targeted compatibility/cache/shutdown race suite: passed.
- Frontend suite: 107 passed, 1 skipped, 0 failed.
- Go vet and golangci-lint: passed.
- Real historical snapshot: 49,182 version-6 hourly rows. The immutable read
  comparison matched v1.17.15 for the overall statement, five monthly statements,
  142 daily statements, coverage checked by the existing parity test, and channel
  consumption. Test artifacts and SQLite copies are outside the repository.
- Single local candidate container, upgraded using its existing data volumes:
  finance changed from repeated failed builds to HTTP 200; channel request count
  recovered from 0 to the baseline 126,939 for the selected interval. Overall,
  monthly and daily statement equality was also checked through the HTTP API.

The historical fixture predates some schema additions. The baseline container
performed its normal migrations before closed copies were used for immutable
comparison. Both versions report a pre-existing usage-fact proof gap in this
fixture; this acceptance does not claim to validate fresh production collection.
All Docker acceptance used an internal network with source collection disabled.

## Release boundary

Keep the original v1.18.3 tag immutable. The integration and this compatibility
fix require a new candidate commit, its CI, and eventually a new release tag.
Production remains a single Monitor container using its existing data store.
The combined schema migration retains the pre-migration paired backup. As with
the underlying v1.18.3 upgrade, rollback must account for new delivery-version
facts and use the verified pre-upgrade store where required; merely relabelling
old facts or downgrading the image is not a data rollback.
