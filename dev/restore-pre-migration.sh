#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
  echo "usage: $0 SNAPSHOT_DIR EMPTY_TARGET_DIR" >&2
  exit 2
fi

snapshot_dir=$1
target_dir=$2
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(dirname -- "$script_dir")

# MONITOR_RESTORE_BIN is useful inside an immutable candidate image. Without
# it, run the same command from the local source tree; neither path contacts
# NewAPI, MySQL, Redis, or any external service.
if [ -n "${MONITOR_RESTORE_BIN:-}" ]; then
  exec "$MONITOR_RESTORE_BIN" restore-pre-migration \
    --snapshot "$snapshot_dir" \
    --target-dir "$target_dir" \
    --confirm RESTORE_PRE_MIGRATION_SNAPSHOT
fi

cd "$repo_dir"
exec go run . restore-pre-migration \
  --snapshot "$snapshot_dir" \
  --target-dir "$target_dir" \
  --confirm RESTORE_PRE_MIGRATION_SNAPSHOT
