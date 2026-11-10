#!/usr/bin/env bash
# Runs one CI race job: builds the package's race test binary once, then runs
# the job's planned test processes (dev/race-plan.mjs) at the same time.
#
# usage: dev/run-race-job.sh <monitor|monthly> <job> <output-dir>
#
# Each process writes Go test JSON events to <output-dir>/<name>-<job>-<n>.jsonl
# (the same events `go test -json` emits). If a process fails, the failed tests
# may be re-run once alone (see below); otherwise the job exits non-zero.
set -euo pipefail

name=${1:?package name (monitor|monthly)}
job=${2:?job index}
out=${3:?output directory}
module=github.com/yl0711-coder/newapi-monitor
case "$name" in
  monitor) dir=monitor ;;
  monthly) dir=internal/monthlydraft ;;
  *) echo "unknown race package: $name" >&2; exit 2 ;;
esac

root=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$out"
out=$(cd "$out" && pwd)
bin="$out/$name.race.test"
cd "$root"
go test -race -c -o "$bin" "./$dir"
# Inventory from the race-built binary itself, run where `go test` runs it.
(cd "$dir" && "$bin" -test.list .) > "$out/$name-list.txt"
node dev/race-plan.mjs "$name" "$job" < "$out/$name-list.txt" > "$out/$name-$job-patterns.txt"

if test "$name" = monitor; then
  # Real collector process tests may be planned into any monitor process.
  go build -o "$out/nginxcollector" ./cmd/nginxcollector
  export MONITOR_TEST_NGINX_COLLECTOR_BIN="$out/nginxcollector"
fi

cd "$root/$dir"
pids=()
i=0
while IFS= read -r pattern; do
  test -n "$pattern" || continue
  report="$out/$name-$job-$i.jsonl"
  # Like `go test -json`: stderr (race reports, panics) joins the event stream.
  go tool test2json -t -p "$module/$dir" \
    sh -c 'exec "$0" -test.v=test2json -test.count=1 -test.timeout=25m -test.run "$1" 2>&1' "$bin" "$pattern" > "$report" &
  pids+=("$!")
  i=$((i + 1))
done < "$out/$name-$job-patterns.txt"
test "$i" -gt 0

status=0
reports=()
for index in "${!pids[@]}"; do
  report="$out/$name-$job-$index.jsonl"
  if wait "${pids[$index]}"; then
    echo "$name race job $job process $index: pass"
    reports+=("$report:0")
  else
    status=1
    echo "$name race job $job process $index: FAIL" >&2
    reports+=("$report:1")
  fi
done
test "$status" -eq 0 && exit 0

# One isolated re-run of the failed tests, after every process of this job has
# finished (no other test competes for the CPU). It absorbs failures caused by
# machine speed, never real ones: data races, panics, timeouts, unidentified
# failures and more than a few failed tests are not re-run, and a test that
# fails again fails the job. Passing re-runs are reported as flaky tests that
# must be fixed (docs/CI-规范.md §7).
verdict=$(cd "$root" && node dev/race-failures.mjs "${reports[@]}")
node -e '
  const v = JSON.parse(process.argv[1]);
  for (const name of v.tests) console.error("  failed: " + name);
  if (!v.tests.length) console.error("  (no test-level failure recorded; see artifact)");
  if (!v.rerunAllowed && v.reasons.length) console.error("  not re-run: " + v.reasons.join("; "));
' "$verdict"
pattern=$(node -e 'const v = JSON.parse(process.argv[1]); process.stdout.write(v.rerunAllowed ? v.pattern : "")' "$verdict")
test -n "$pattern" || exit 1

rerun="$out/$name-$job-rerun.jsonl"
echo "$name race job $job: re-running the failed tests once, alone" >&2
if go tool test2json -t -p "$module/$dir" \
    sh -c 'exec "$0" -test.v=test2json -test.count=1 -test.timeout=25m -test.run "$1" 2>&1' "$bin" "$pattern" > "$rerun"; then
  node -e '
    const fs = require("fs");
    const v = JSON.parse(process.argv[1]);
    const lines = ["## Flaky race tests: " + process.argv[2], "",
      "These tests failed in the parallel run and passed when re-run alone. They are timing-sensitive and must be fixed (docs/CI-规范.md §7):", "",
      ...v.tests.map(n => "- `" + n + "`"), ""];
    fs.writeFileSync(process.argv[3], lines.join("\n"));
    for (const n of v.tests) console.log("::warning title=Flaky race test::" + n + " failed in " + process.argv[2] + " and passed when re-run alone; fix it (docs/CI-规范.md §7)");
  ' "$verdict" "$name-$job" "$out/$name-$job-flaky.md"
  echo "$name race job $job: re-run passed; flaky tests reported"
  exit 0
fi
echo "$name race job $job: re-run FAILED" >&2
exit 1
