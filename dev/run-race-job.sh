#!/usr/bin/env bash
# Runs one CI race job: builds the package's race test binary once, then runs
# the job's planned test processes (dev/race-plan.mjs) at the same time.
#
# usage: dev/run-race-job.sh <monitor|monthly> <job> <output-dir>
#
# Each process writes Go test JSON events to <output-dir>/<name>-<job>-<n>.jsonl
# (the same events `go test -json` emits). Exits non-zero if any process fails.
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
for index in "${!pids[@]}"; do
  if wait "${pids[$index]}"; then
    echo "$name race job $job process $index: pass"
  else
    status=1
    echo "$name race job $job process $index: FAIL" >&2
    # Name the failing tests in the job log; full events stay in the artifact.
    node -e '
      const lines = require("fs").readFileSync(process.argv[1], "utf8").split("\n");
      const failed = new Set();
      for (const line of lines) { try { const e = JSON.parse(line); if (e.Action === "fail" && e.Test) failed.add(e.Test); } catch {} }
      console.error(failed.size ? [...failed].map(n => "  failed: " + n).join("\n") : "  (no test-level failure recorded; see artifact)");
    ' "$out/$name-$job-$index.jsonl" >&2
  fi
done
exit "$status"
