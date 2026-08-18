#!/bin/sh
# Run the facts load test from the Monitor container's own network namespace.
# This keeps the synthetic MySQL/Redis/Monitor network internal even on Docker
# engines that do not expose published ports from an internal network to macOS.
set -eu

env_file="${1:-}"
duration="${LOCAL_FACTS_LOADTEST_DURATION:-10m}"
report="${LOCAL_FACTS_LOADTEST_REPORT:-}"
portal_email="${LOCAL_FACTS_PORTAL_EMAIL:-}"
portal_password="${LOCAL_FACTS_PORTAL_PASSWORD:-}"
session_secret="${LOCAL_FACTS_SESSION_SECRET:-local-acceptance-session-only}"

if [ -z "$env_file" ] || [ ! -f "$env_file" ]; then
  echo "usage: $0 <isolated-compose-env-file>" >&2
  exit 2
fi
case "$report" in
  /private/tmp/newapi-monitor-facts-acceptance-*/*.json) ;;
  *) echo "LOCAL_FACTS_LOADTEST_REPORT must be /private/tmp/newapi-monitor-facts-acceptance-*/<name>.json" >&2; exit 2 ;;
esac
case "$portal_email" in
  *@local.test) ;;
  *) echo "LOCAL_FACTS_PORTAL_EMAIL must be a synthetic @local.test account" >&2; exit 2 ;;
esac
case "$portal_password" in
  ???????*) ;;
  *) echo "LOCAL_FACTS_PORTAL_PASSWORD must contain at least 8 characters" >&2; exit 2 ;;
esac
case "$session_secret" in
  local-acceptance-*|local-facts-*) ;;
  *) echo "LOCAL_FACTS_SESSION_SECRET must be the isolated local test secret" >&2; exit 2 ;;
esac

compose() {
  docker compose --env-file "$env_file" \
    -f docker-compose.local-acceptance.yml \
    -f docker-compose.local-facts-acceptance.yml "$@"
}

container_id="$(compose ps -q monitor)"
if [ -z "$container_id" ]; then
  echo "isolated monitor is not running" >&2
  exit 1
fi
container_name="$(docker inspect --format '{{.Name}}' "$container_id" | sed 's#^/##')"
case "$container_name" in
  nxmon-facts-monitor-*) ;;
  *) echo "refusing unexpected monitor container: $container_name" >&2; exit 2 ;;
esac
if [ "$(docker inspect --format '{{.State.Running}}' "$container_id")" != true ]; then
  echo "isolated monitor is not running" >&2
  exit 1
fi

report_dir="$(dirname "$report")"
mkdir -p "$report_dir"
nonce="$(date -u +%Y%m%d%H%M%S)-$$"
host_binary="/private/tmp/newapi-monitor-facts-loadtest-$nonce"
container_binary="/tmp/local-facts-loadtest-$nonce"
container_report="/tmp/local-facts-loadtest-$nonce.json"
cleanup() {
  status=$?
  rm -f "$host_binary"
  # docker cp creates the binary as root; /tmp is sticky in the read-only
  # candidate, so cleanup must use Docker's explicit local root exec rather
  # than silently leaving a root-owned binary behind.
  docker exec --user 0 "$container_id" rm -f "$container_binary" "$container_report" >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT HUP INT TERM

# The candidate is linux/amd64 even when the local development host is ARM.
GOCACHE="${GOCACHE:-/private/tmp/newapi-monitor-local-facts-gocache}" \
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
  go build -o "$host_binary" ./dev/local-facts-loadtest
docker cp "$host_binary" "$container_id:$container_binary"
docker exec "$container_id" "$container_binary" \
  -portal-base http://127.0.0.1:8091 \
  -admin-base http://127.0.0.1:8090 \
  -email "$portal_email" \
  -password "$portal_password" \
  -session-secret "$session_secret" \
  -duration "$duration" \
  -report "$container_report"
docker cp "$container_id:$container_report" "$report"
echo "local synthetic load-test report: $report"
