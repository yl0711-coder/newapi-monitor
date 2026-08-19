#!/bin/sh
set -eu

target="${1:-}"
report="${2:-}"
duration="${3:-3600}"

case "$target" in
  nxmon-facts-monitor-*|nxmon-facts-mysql-*|nxmon-facts-redis-*) ;;
  *) echo "refusing unexpected container: $target" >&2; exit 2 ;;
esac
case "$report" in
  /private/tmp/newapi-monitor-facts-acceptance-*/*) ;;
  *) echo "refusing unexpected report path: $report" >&2; exit 2 ;;
esac
case "$duration" in
  *[!0-9]*|'') echo "duration must be integer seconds" >&2; exit 2 ;;
esac

: >"$report"
started="$(date +%s)"
while :; do
  now="$(date +%s)"
  elapsed="$((now - started))"
  stats="$(docker stats --no-stream --format '{{json .}}' "$target" 2>/dev/null || true)"
  state="$(docker inspect --format '{{json .State}}' "$target" 2>/dev/null || true)"
  printf '{"timestamp":%s,"elapsed":%s,"stats":%s,"state":%s}\n' \
    "$now" "$elapsed" "${stats:-null}" "${state:-null}" >>"$report"
  if [ "$elapsed" -ge "$duration" ]; then
    break
  fi
  sleep 5
done
