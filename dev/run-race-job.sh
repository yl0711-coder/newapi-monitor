#!/usr/bin/env bash
# Runs one CI race job (see docs/CI-规范.md): builds the package's race test
# binary once, then runs the job's planned test processes (dev/race-plan.mjs).
#
# usage: dev/run-race-job.sh <monitor|monthly> <job> <output-dir>
#
# Each process writes Go test JSON events (the same events `go test -json`
# emits) to <output-dir>/<name>-<job>-<n>.jsonl.
#
# Modes (from dev/race-plan.mjs):
#   race                       all planned processes run at the same time under
#                              the race detector; failed tests may be re-run
#                              once alone (see rerun_failed), otherwise the job
#                              fails.
#   native-gate-race-advisory  serial jobs only: the tests run natively first
#                              (all assertions must pass, same re-run rule),
#                              then under race, where only data races, panics,
#                              hangs and unidentified failures fail the job;
#                              other race-mode failures are reported.
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
mode=$(node dev/race-plan.mjs "$name" "$job" --mode < "$out/$name-list.txt")

if test "$name" = monitor; then
  # Real collector process tests may be planned into any monitor process.
  go build -o "$out/nginxcollector" ./cmd/nginxcollector
  export MONITOR_TEST_NGINX_COLLECTOR_BIN="$out/nginxcollector"
fi
native=""
if test "$mode" = native-gate-race-advisory; then
  native="$out/$name.native.test"
  go test -c -o "$native" "./$dir"
fi

cd "$root/$dir"

# run_tests <binary> <pattern> <report>: like `go test -json`, stderr (race
# reports, panics) joins the event stream.
run_tests() {
  go tool test2json -t -p "$module/$dir" \
    sh -c 'exec "$0" -test.v=test2json -test.count=1 -test.timeout=25m -test.run "$1" 2>&1' "$1" "$2" > "$3"
}

# classify <report:status>...: prints the JSON verdict of dev/race-failures.mjs
# and names the failed tests in the job log.
classify() {
  local verdict
  verdict=$(cd "$root" && node dev/race-failures.mjs "$@")
  node -e '
    const v = JSON.parse(process.argv[1]);
    for (const name of v.tests) console.error("  failed: " + name);
    if (!v.tests.length) console.error("  (no test-level failure recorded; see artifact)");
  ' "$verdict"
  printf '%s' "$verdict"
}

# rerun_failed <binary> <label> <verdict>: one isolated re-run of the failed
# tests, after every process of this job has finished. It absorbs failures
# caused by machine speed, never real ones: data races, panics, timeouts,
# unidentified failures and more than a few failed tests are not re-run, and a
# test that fails again fails the job. A passing re-run is reported as flaky
# (docs/CI-规范.md §7). Returns 0 when the re-run passed.
rerun_failed() {
  local binary=$1 label=$2 verdict=$3 pattern
  node -e '
    const v = JSON.parse(process.argv[1]);
    if (!v.rerunAllowed && v.reasons.length) console.error("  not re-run: " + v.reasons.join("; "));
  ' "$verdict"
  pattern=$(node -e 'const v = JSON.parse(process.argv[1]); process.stdout.write(v.rerunAllowed ? v.pattern : "")' "$verdict")
  test -n "$pattern" || return 1
  echo "$label: re-running the failed tests once, alone" >&2
  if ! run_tests "$binary" "$pattern" "$out/$label-rerun.jsonl"; then
    echo "$label: re-run FAILED" >&2
    return 1
  fi
  node -e '
    const fs = require("fs");
    const v = JSON.parse(process.argv[1]);
    const lines = ["## Flaky race tests: " + process.argv[2], "",
      "These tests failed and passed when re-run alone. They are timing-sensitive and must be fixed (docs/CI-规范.md §7):", "",
      ...v.tests.map(n => "- `" + n + "`"), ""];
    fs.appendFileSync(process.argv[3], lines.join("\n"));
    for (const n of v.tests) console.log("::warning title=Flaky race test::" + n + " failed in " + process.argv[2] + " and passed when re-run alone; fix it (docs/CI-规范.md §7)");
  ' "$verdict" "$label" "$out/$name-$job-flaky.md"
  echo "$label: re-run passed; flaky tests reported"
}

if test "$mode" = native-gate-race-advisory; then
  pattern=$(head -n 1 "$out/$name-$job-patterns.txt")
  test -n "$pattern"
  # 1. Native run: every assertion (including performance thresholds) gates.
  report="$out/$name-$job-native.jsonl"
  if run_tests "$native" "$pattern" "$report"; then
    echo "$name race job $job native run: pass"
  else
    echo "$name race job $job native run: FAIL" >&2
    verdict=$(classify "$report:1")
    rerun_failed "$native" "$name-$job-native" "$verdict" || exit 1
  fi
  # 2. Race run: data races, panics, hangs and unidentified failures gate;
  #    other race-mode failures are reported, not fatal.
  report="$out/$name-$job-0.jsonl"
  if run_tests "$bin" "$pattern" "$report"; then
    echo "$name race job $job race run: pass"
    exit 0
  fi
  echo "$name race job $job race run: failures (advisory)" >&2
  verdict=$(classify "$report:1")
  blocking=$(node -e 'process.stdout.write(JSON.parse(process.argv[1]).blocking.join("; "))' "$verdict")
  if test -n "$blocking"; then
    echo "$name race job $job race run: FAIL ($blocking)" >&2
    exit 1
  fi
  node -e '
    const fs = require("fs");
    const v = JSON.parse(process.argv[1]);
    const lines = ["## Race-mode advisory failures: " + process.argv[2], "",
      "These tests passed natively and failed only under the race detector (no data race, panic or hang). " +
      "They assert wall-clock performance the race slowdown cannot meet; they do not fail CI until they stop depending on machine speed (docs/CI-规范.md §3):", "",
      ...v.tests.map(n => "- `" + n + "`"), ""];
    fs.writeFileSync(process.argv[3], lines.join("\n"));
    for (const n of v.tests) console.log("::notice title=Race-mode advisory failure::" + n + " passed natively, failed only under race in " + process.argv[2]);
  ' "$verdict" "$name-$job" "$out/$name-$job-race-advisory.md"
  echo "$name race job $job: native run passed, race run had advisory failures only"
  exit 0
fi

pids=()
i=0
while IFS= read -r pattern; do
  test -n "$pattern" || continue
  run_tests "$bin" "$pattern" "$out/$name-$job-$i.jsonl" &
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
verdict=$(classify "${reports[@]}")
rerun_failed "$bin" "$name-$job" "$verdict" || exit 1
exit 0
