#!/bin/zsh
# 在本机 8100/8101 运行当前候选代码，数据源只允许是经 SSH 隧道访问的
# 生产 monitor_ro。脚本绝不打印 DSN/密码，也不修改线上容器或数据库。
set -euo pipefail

script_dir="${0:A:h}"
repo_dir="${script_dir:h}"
action="${1:-status}"

acceptance_env_file="${MONITOR_PROD_READONLY_ENV_FILE:-${HOME}/.config/newapi-monitor/local-acceptance.env}"
release_env_file="${NEXUS_RELEASE_ENV_FILE:-${repo_dir}/../NexusAPI/deploy/release-rc19/release.env}"
control_socket="${MONITOR_PROD_READONLY_CONTROL_SOCKET:-/private/tmp/newapi-monitor-prod-readonly-13316.sock}"
local_db_port="${MONITOR_PROD_READONLY_DB_PORT:-13316}"
image_name="${MONITOR_PROD_READONLY_IMAGE:-}"
container_name=""
volume_name=""
backup_volume_name=""
admin_port=""
portal_port=""
start_cleanup_required=false

fail() {
  print -u2 -- "local-production-readonly: $*"
  exit 1
}

require_file() {
  [[ -f "$1" ]] || fail "required file is missing: $1"
}

env_value() {
  local key="$1" file="$2"
  awk -v wanted="$key" 'index($0, wanted "=") == 1 {sub("^" wanted "=", ""); sub(/\r$/, ""); print; exit}' "$file"
}

validate_local_dsn() {
  require_file "$acceptance_env_file"
  local dsn_value user_value address_value database_value rest_value
  dsn_value="$(env_value NEWAPI_LOG_DSN "$acceptance_env_file")"
  [[ -n "$dsn_value" ]] || fail "NEWAPI_LOG_DSN is missing from $acceptance_env_file"
  user_value="${dsn_value%%:*}"
  [[ "$user_value" == "monitor_ro" ]] || fail "database user must be monitor_ro"
  rest_value="${dsn_value#*@tcp\(}"
  [[ "$rest_value" != "$dsn_value" ]] || fail "NEWAPI_LOG_DSN must use tcp(...)"
  address_value="${rest_value%%\)*}"
  [[ "$address_value" == "host.docker.internal:${local_db_port}" ]] || \
    fail "DSN must target host.docker.internal:${local_db_port}"
  database_value="${rest_value#*\)/}"
  database_value="${database_value%%\?*}"
  [[ "$database_value" == "nexusapi" ]] || fail "database must be nexusapi"
}

load_release_access() {
  require_file "$release_env_file"
  set -a
  source "$release_env_file"
  set +a
  [[ -n "${SSH_IDENTITY_FILE:-}" && -f "$SSH_IDENTITY_FILE" ]] || fail "SSH identity file is unavailable"
  [[ -n "${MONITOR_SSH_TARGET:-}" ]] || fail "MONITOR_SSH_TARGET is missing"
}

tunnel_is_healthy() {
  [[ -S "$control_socket" ]] || return 1
  ssh -S "$control_socket" -O check "$MONITOR_SSH_TARGET" >/dev/null 2>&1 || return 1
  nc -z 127.0.0.1 "$local_db_port" >/dev/null 2>&1
}

start_tunnel() {
  validate_local_dsn
  load_release_access
  if tunnel_is_healthy; then
    print -- "readonly tunnel: healthy on 127.0.0.1:${local_db_port}"
    return
  fi
  if [[ -S "$control_socket" ]]; then
    ssh -S "$control_socket" -O exit "$MONITOR_SSH_TARGET" >/dev/null 2>&1 || true
    rm -f "$control_socket"
  fi
  if lsof -nP -iTCP:"$local_db_port" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "127.0.0.1:${local_db_port} is already occupied by an unmanaged process"
  fi

  # 只在内存中解析线上容器的只读 DSN，并只保留 host:port；不输出密码或完整 DSN。
  local remote_target
  remote_target="$(
    ssh -i "$SSH_IDENTITY_FILE" -o BatchMode=yes -o ConnectTimeout=15 "$MONITOR_SSH_TARGET" \
      "sudo docker inspect nexusapi-monitor --format '{{range .Config.Env}}{{println .}}{{end}}'" |
      awk -F= '
        $1 == "NEWAPI_LOG_DSN" {
          value=$0
          sub(/^NEWAPI_LOG_DSN=/, "", value)
          user=value
          sub(/:.*/, "", user)
          if (user != "monitor_ro") exit 41
          sub(/^.*@tcp\(/, "", value)
          sub(/\).*/, "", value)
          print value
          found=1
          exit
        }
        END {if (!found) exit 42}
      '
  )" || fail "failed to resolve the production monitor_ro target"
  [[ "$remote_target" == *:* ]] || fail "invalid production database target"

  ssh -i "$SSH_IDENTITY_FILE" -M -S "$control_socket" -fN \
    -o BatchMode=yes \
    -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=30 \
    -o ServerAliveCountMax=3 \
    -L "127.0.0.1:${local_db_port}:${remote_target}" \
    "$MONITOR_SSH_TARGET"
  tunnel_is_healthy || fail "SSH tunnel did not become healthy"
  print -- "readonly tunnel: started on 127.0.0.1:${local_db_port}"
}

stop_tunnel() {
  if [[ ! -S "$control_socket" ]]; then
    print -- "readonly tunnel: stopped"
    return
  fi
  load_release_access
  ssh -S "$control_socket" -O exit "$MONITOR_SSH_TARGET" >/dev/null 2>&1 || true
  rm -f "$control_socket"
  print -- "readonly tunnel: stopped"
}

probe_database() {
  validate_local_dsn
  local dsn_value
  dsn_value="$(env_value NEWAPI_LOG_DSN "$acceptance_env_file")"
  (
    cd "$repo_dir"
    export NEWAPI_LOG_DSN="$dsn_value"
    GOCACHE=/private/tmp/newapi-monitor-prod-readonly-gocache \
      go run ./tools/readonly-inspect -channels 2147483647 >/dev/null
  )
  print -- "database preflight: monitor_ro SELECT succeeded"
}

compose() {
  # Config/status/stop must still resolve the exact identities selected by the
  # env-file when no candidate image was supplied. Up/build separately reject
  # this inert placeholder before creating a container.
  local compose_image="${image_name:-invalid.local/newapi-monitor@sha256:0000000000000000000000000000000000000000000000000000000000000000}"
  MONITOR_ACCEPTANCE_IMAGE="$compose_image" \
    docker compose \
      --env-file "$acceptance_env_file" \
      -f "$repo_dir/docker-compose.local-acceptance.yml" \
      -f "$repo_dir/docker-compose.local-production-readonly.yml" \
      "$@"
}

resolve_compose_identity() {
  local resolved_json
  command -v jq >/dev/null 2>&1 || fail "jq is required to resolve Compose identities"
  resolved_json="$(compose config --format json)"
  container_name="$(printf '%s' "$resolved_json" | jq -er '.services.monitor.container_name')" || \
    fail "cannot resolve the Monitor container from Compose"
  volume_name="$(printf '%s' "$resolved_json" | jq -er '.volumes.monitor_local_data.name')" || \
    fail "cannot resolve the Monitor data volume from Compose"
  backup_volume_name="$(printf '%s' "$resolved_json" | jq -er '.volumes.monitor_local_backup.name')" || \
    fail "cannot resolve the Monitor backup volume from Compose"
  admin_port="$(printf '%s' "$resolved_json" | jq -er '.services.monitor.ports[] | select(.target == 8090) | .published')" || \
    fail "cannot resolve the Monitor admin port from Compose"
  portal_port="$(printf '%s' "$resolved_json" | jq -er '.services.monitor.ports[] | select(.target == 8091) | .published')" || \
    fail "cannot resolve the Monitor portal port from Compose"
}

build_image() {
  validate_local_dsn
  [[ -n "$image_name" ]] || fail "set MONITOR_PROD_READONLY_IMAGE to an explicit local development tag"
  compose build monitor
  print -- "development image built: $image_name (not a release acceptance artifact until referenced by digest)"
}

validate_candidate_image() {
  [[ -n "$image_name" ]] || fail "MONITOR_PROD_READONLY_IMAGE must be an immutable candidate reference"
  [[ "$image_name" == *@sha256:* ]] || \
    fail "candidate image must be pinned as repository@sha256:digest; mutable tags are rejected"
  docker image inspect "$image_name" >/dev/null 2>&1 || fail "candidate image is missing: $image_name"
}

wait_for_monitor_endpoints() {
  local attempt
  for attempt in {1..30}; do
    if curl --noproxy '*' -fsS --max-time 3 "http://127.0.0.1:${admin_port}/live" >/dev/null 2>&1 && \
       curl --noproxy '*' -fsS --max-time 3 "http://127.0.0.1:${admin_port}/ready" >/dev/null 2>&1; then
      return
    fi
    sleep 2
  done
  fail "candidate did not expose successful /live and /ready within 60 seconds"
}

start_monitor() {
  validate_candidate_image
  local requested_data_volume requested_backup_volume
  requested_data_volume="${MONITOR_ACCEPTANCE_VOLUME:-$(env_value MONITOR_ACCEPTANCE_VOLUME "$acceptance_env_file")}"
  requested_backup_volume="${MONITOR_ACCEPTANCE_BACKUP_VOLUME:-$(env_value MONITOR_ACCEPTANCE_BACKUP_VOLUME "$acceptance_env_file")}"
  [[ -n "$requested_data_volume" ]] || fail "MONITOR_ACCEPTANCE_VOLUME must explicitly select the already-verified data volume"
  [[ -n "$requested_backup_volume" ]] || fail "MONITOR_ACCEPTANCE_BACKUP_VOLUME must explicitly select its independent backup volume"
  resolve_compose_identity
  [[ "$volume_name" != "$backup_volume_name" ]] || fail "data and backup volumes must be different"

  # Any failed `up` attempt must fail closed. Without this trap an image-ID,
  # mount, or endpoint assertion could leave a source-enabled container and
  # its SSH tunnel running after the command itself returned non-zero.
  start_cleanup_required=true
  cleanup_failed_start() {
    local original_status="${1:-1}"
    if [[ "$start_cleanup_required" == true ]]; then
      compose stop -t 40 monitor >/dev/null 2>&1 || true
      stop_tunnel >/dev/null 2>&1 || true
    fi
    return "$original_status"
  }
  trap 'cleanup_failed_start $?' EXIT
  trap 'exit 130' INT TERM

  start_tunnel
  probe_database
  docker volume inspect "$volume_name" >/dev/null 2>&1 || \
    fail "required external volume is missing: $volume_name"
  docker volume inspect "$backup_volume_name" >/dev/null 2>&1 || \
    fail "required external backup volume is missing: $backup_volume_name"
  compose up -d redis
  compose up -d --no-deps --force-recreate monitor
  local expected_image_id actual_image_id monitor_id
  monitor_id="$(compose ps -q monitor)"
  [[ -n "$monitor_id" ]] || fail "Compose did not start the Monitor service"
  expected_image_id="$(docker image inspect -f '{{.Id}}' "$image_name")"
  actual_image_id="$(docker inspect -f '{{.Image}}' "$monitor_id")"
  [[ "$expected_image_id" == "$actual_image_id" ]] || \
    fail "running container image does not match the pinned candidate digest"
  wait_for_monitor_endpoints
  start_cleanup_required=false
  trap - EXIT INT TERM
  print -- "admin:  http://127.0.0.1:${admin_port}"
  print -- "portal: http://127.0.0.1:${portal_port}"
}

show_status() {
  validate_local_dsn
  load_release_access
  resolve_compose_identity
  if tunnel_is_healthy; then
    print -- "readonly tunnel: healthy"
  else
    print -- "readonly tunnel: unavailable"
  fi
  local configured_image actual_image_id monitor_id
  monitor_id="$(compose ps -q monitor)"
  [[ -n "$monitor_id" ]] || fail "Monitor service is not running"
  docker ps --filter "id=$monitor_id" --format '{{.Names}}\t{{.Status}}\t{{.Image}}\t{{.Ports}}'
  configured_image="$(docker inspect -f '{{.Config.Image}}' "$monitor_id")"
  actual_image_id="$(docker inspect -f '{{.Image}}' "$monitor_id")"
  print -- "configured image: $configured_image"
  print -- "actual image id: $actual_image_id"
  print -- "live:"
  curl --noproxy '*' -fsS --max-time 5 "http://127.0.0.1:${admin_port}/live" || fail "/live is unavailable"
  print
  print -- "ready:"
  curl --noproxy '*' -fsS --max-time 5 "http://127.0.0.1:${admin_port}/ready" || fail "/ready is unavailable"
  print
}

stop_monitor_without_rendering_source_config() {
  # A stop must still work after the signed SOURCE_EPOCH was intentionally
  # removed or the acceptance env became incomplete.  Select only a container
  # created for this repository's exact two-file Compose stack; never fall back
  # to a mutable default name that could target another project.
  local expected_base expected_overlay ids id config_files matched=""
  expected_base="$repo_dir/docker-compose.local-acceptance.yml"
  expected_overlay="$repo_dir/docker-compose.local-production-readonly.yml"
  ids="$(docker ps -aq \
    --filter 'label=com.docker.compose.service=monitor' \
    --filter "label=com.docker.compose.project.working_dir=$repo_dir")"
  for id in ${(f)ids}; do
    [[ -n "$id" ]] || continue
    config_files="$(docker inspect -f '{{index .Config.Labels "com.docker.compose.project.config_files"}}' "$id")"
    [[ ",$config_files," == *",$expected_base,"* ]] || continue
    [[ ",$config_files," == *",$expected_overlay,"* ]] || continue
    [[ -z "$matched" ]] || fail "multiple local production-readonly Monitor containers matched; stop manually after inspection"
    matched="$id"
  done
  if [[ -n "$matched" ]]; then
    if [[ "$(docker inspect -f '{{.State.Running}}' "$matched")" == true ]]; then
      docker stop -t 40 "$matched" >/dev/null
    fi
    [[ "$(docker inspect -f '{{.State.Running}}' "$matched")" == false ]] || \
      fail "local production-readonly Monitor did not stop"
  fi
}

case "$action" in
  tunnel-start) start_tunnel ;;
  tunnel-stop) stop_tunnel ;;
  preflight) start_tunnel; probe_database ;;
  build) build_image ;;
  up) start_monitor ;;
  status) show_status ;;
  stop)
    stop_monitor_without_rendering_source_config
    stop_tunnel
    ;;
  *)
    fail "usage: $0 {tunnel-start|tunnel-stop|preflight|build|up|status|stop}"
    ;;
esac
