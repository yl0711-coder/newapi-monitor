# SQL audit evidence directory

This tracked placeholder makes the output directory available to offline CI
tests. Generated plans, captures, timing reports and logs are intentionally
ignored by Git: local diagnostics can contain private source metadata or filter
values. Do not upload retained production-derived evidence to a public repository.

Test code and deterministic synthetic fixtures are versioned separately. Tests
which require an external source, an explicitly owned MySQL container, browser
control or an explicit report path are opt-in; a skipped test is not acceptance
evidence for that environment.

The release-level, customer-data-free summary is in
[release-v1.18.75-rc.1.md](../release-v1.18.75-rc.1.md).
