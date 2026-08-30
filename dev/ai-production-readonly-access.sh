#!/bin/zsh
# AI 专用的生产只读访问入口。
#
# 这个脚本不接受任意 SQL，不打印 DSN/密码，不自行实现 SSH 逻辑。
# 隧道安全边界由 run-local-production-readonly.sh 统一校验。
set -euo pipefail

script_dir="${0:A:h}"
repo_dir="${script_dir:h}"
tunnel_runner="${script_dir}/run-local-production-readonly.sh"
acceptance_env_file="${MONITOR_PROD_READONLY_ENV_FILE:-${HOME}/.config/newapi-monitor/local-acceptance.env}"
action="${1:-help}"

fail() {
  print -u2 -- "ai-production-readonly: $*"
  exit 1
}

usage() {
  print -- 'Usage:'
  print -- '  dev/ai-production-readonly-access.sh start'
  print -- '  dev/ai-production-readonly-access.sh stop'
  print -- '  dev/ai-production-readonly-access.sh probe'
  print -- '  dev/ai-production-readonly-access.sh channel <CHANNEL_IDS>'
  print -- '  dev/ai-production-readonly-access.sh request <REQUEST_IDS>'
  print -- '  dev/ai-production-readonly-access.sh usage <USER_IDS> <UTC_FROM_UNIX> <UTC_TO_UNIX>'
  print -- '  dev/ai-production-readonly-access.sh raw-user <USER_ID> <UTC_HOUR_UNIX>'
  print -- '  dev/ai-production-readonly-access.sh raw-email <EMAIL> <UTC_HOUR_UNIX>'
  print
  print -- 'start leaves the loopback-only tunnel running; stop closes it.'
  print -- 'All query/probe commands open, verify, use, and then close the tunnel automatically.'
  print -- 'Arbitrary SQL is intentionally unsupported.'
}

require_arg_count() {
  local expected="$1"
  shift
  [[ "$#" -eq "$expected" ]] || fail "invalid arguments; run with help"
}

load_readonly_dsn() {
  [[ -f "$acceptance_env_file" ]] || fail "required local acceptance env is missing"
  local dsn_value
  dsn_value="$(awk 'index($0, "NEWAPI_LOG_DSN=") == 1 {sub(/^NEWAPI_LOG_DSN=/, ""); sub(/\r$/, ""); print; exit}' "$acceptance_env_file")"
  [[ -n "$dsn_value" ]] || fail "NEWAPI_LOG_DSN is missing from the local acceptance env"
  # 完整校验还会由 tunnel_runner 执行；这里只再做一层失效关闭。
  [[ "${dsn_value%%:*}" == "nexus_ro" ]] || fail "database user must be nexus_ro"
  print -r -- "$dsn_value"
}

cleanup_required=false
cleanup() {
  local original_status="${1:-0}"
  if [[ "$cleanup_required" == true ]]; then
    "$tunnel_runner" tunnel-stop >/dev/null 2>&1 || true
  fi
  return "$original_status"
}

run_bounded_query() {
  local -a inspect_args
  inspect_args=("$@")
  cleanup_required=true
  trap 'cleanup $?' EXIT
  trap 'exit 130' INT TERM
  "$tunnel_runner" preflight
  local readonly_dsn
  readonly_dsn="$(load_readonly_dsn)"
  (
    cd "$repo_dir"
    NEWAPI_LOG_DSN="$readonly_dsn" \
      GOCACHE=/private/tmp/newapi-monitor-ai-readonly-gocache \
      go run ./tools/readonly-inspect "${inspect_args[@]}"
  )
  cleanup_required=false
  trap - EXIT INT TERM
  "$tunnel_runner" tunnel-stop
}

case "$action" in
  start)
    require_arg_count 0 "${@:2}"
    exec "$tunnel_runner" preflight
    ;;
  stop)
    require_arg_count 0 "${@:2}"
    exec "$tunnel_runner" tunnel-stop
    ;;
  probe)
    require_arg_count 0 "${@:2}"
    cleanup_required=true
    trap 'cleanup $?' EXIT
    trap 'exit 130' INT TERM
    "$tunnel_runner" preflight
    print -- 'AI readonly probe: passed; closing tunnel'
    ;;
  channel)
    require_arg_count 1 "${@:2}"
    run_bounded_query -channels "$2"
    ;;
  request)
    require_arg_count 1 "${@:2}"
    run_bounded_query -requests "$2"
    ;;
  usage)
    require_arg_count 3 "${@:2}"
    run_bounded_query -usage-users "$2" -usage-from "$3" -usage-to "$4"
    ;;
  raw-user)
    require_arg_count 2 "${@:2}"
    run_bounded_query -raw-page-user "$2" -raw-page-hour "$3"
    ;;
  raw-email)
    require_arg_count 2 "${@:2}"
    run_bounded_query -raw-page-email "$2" -raw-page-hour "$3"
    ;;
  help|-h|--help)
    usage
    ;;
  *)
    usage >&2
    fail "unknown action: $action"
    ;;
esac
