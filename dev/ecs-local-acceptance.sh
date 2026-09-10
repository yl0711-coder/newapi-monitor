#!/usr/bin/env bash
# Real local collector processes + temporary stores only. No AWS/deploy calls.
# Security scanners and real AWS scaling are separate, mandatory release gates.
set -euo pipefail

monitor_repo="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
reject_repo="${1:-$(dirname -- "$monitor_repo")/newapi-reject-collector}"
if [[ ! -f "$reject_repo/go.mod" || ! -f "$reject_repo/durable_runner.go" ]]; then
  echo 'A local newapi-reject-collector checkout is required; do not silently skip the real-process acceptance.' >&2
  exit 2
fi
reject_repo="$(cd -- "$reject_repo" && pwd)"
acceptance_dir="$(mktemp -d /private/tmp/ecs-local-acceptance.XXXXXX)"
chmod 700 "$acceptance_dir"
echo "Local acceptance evidence: $acceptance_dir"
trap 'echo "Acceptance failed at line $LINENO; evidence retained at $acceptance_dir" >&2' ERR

# CI pins the reviewed toolchain separately; local acceptance defaults to an
# already-installed toolchain and never silently downloads one. Set
# GOTOOLCHAIN=go1.26.6 explicitly when that reviewed toolchain is installed.
export GOTOOLCHAIN="${GOTOOLCHAIN:-local}"
# Process fixtures provide synthetic credentials and their own local settings.
# Do not give this acceptance script accidental production environment inputs.
unset NEWAPI_LOG_DSN DATABASE_URL SQL_DSN MONITOR_SOURCE_DSN MONITOR_INGEST_TOKEN
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE
unset AWS_CONTAINER_CREDENTIALS_RELATIVE_URI AWS_CONTAINER_CREDENTIALS_FULL_URI

cd -- "$reject_repo"
go build -o "$acceptance_dir/reject-collector" .
go test -race -count=1 ./... > "$acceptance_dir/reject-race.log" 2>&1
cd -- "$monitor_repo"
go build -o "$acceptance_dir/nginxcollector" ./cmd/nginxcollector
export MONITOR_TEST_REJECT_COLLECTOR_BIN="$acceptance_dir/reject-collector"
export MONITOR_TEST_NGINX_COLLECTOR_BIN="$acceptance_dir/nginxcollector"
go test -race -count=1 ./internal/ecsarchive ./internal/ecslogagent ./cmd/ecslogagent ./cmd/nginxcollector ./cmd/ecsacceptance ./cmd/ecssynthetic > "$acceptance_dir/agent-collector-race.log" 2>&1
go test -race -count=1 ./monitor -run '^TestECSLog' > "$acceptance_dir/ecs-monitor-race.log" 2>&1
python3 -m unittest discover -s deploy/ecs-log-bridge -v > "$acceptance_dir/bridge.log" 2>&1
python3 -m unittest discover -s dev -p 'test_ecs_cutover_preflight.py' -v > "$acceptance_dir/cutover-preflight.log" 2>&1
python3 -m unittest discover -s dev -p 'test_ecs_image_acceptance.py' -v > "$acceptance_dir/image-runner-safety.log" 2>&1
python3 -m unittest discover -s dev -p 'test_ecs_task*.py' -v > "$acceptance_dir/task-contract.log" 2>&1
python3 -m unittest discover -s dev -p 'test_ecs_acceptance*.py' -v > "$acceptance_dir/template-iam.log" 2>&1
python3 -m unittest discover -s dev -p 'test_ecs_split*.py' -v > "$acceptance_dir/split-preparation.log" 2>&1
node --test dev/tests/*.test.mjs > "$acceptance_dir/frontend.log" 2>&1
go test -list . ./monitor | node dev/check-race-shards.mjs > "$acceptance_dir/test-shards.log"
git diff --check
git -C "$reject_repo" diff --check
echo 'Local process acceptance PASSED. This does not approve production, real IAM, external recovery, or image security.'
