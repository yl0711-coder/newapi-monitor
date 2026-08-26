#!/bin/zsh
#
# 新人本机开发入口：完全离线启动 Monitor，不读取生产数据库、主站或任何上游。
#
# 用法：
#   ./dev/run-local-dev.sh up
#   ./dev/run-local-dev.sh status
#   ./dev/run-local-dev.sh logs
#   ./dev/run-local-dev.sh stop

set -euo pipefail

SCRIPT_DIR="${0:A:h}"
REPO_DIR="${SCRIPT_DIR:h}"
IMAGE_TAG="${MONITOR_LOCAL_DEV_IMAGE:-newapi-monitor:local-dev}"
DATA_VOLUME="${MONITOR_ACCEPTANCE_VOLUME:-newapi-monitor-local-data}"
BACKUP_VOLUME="${MONITOR_ACCEPTANCE_BACKUP_VOLUME:-newapi-monitor-local-backup}"
ADMIN_PORT="${MONITOR_ACCEPTANCE_ADMIN_PORT:-8100}"
PORTAL_PORT="${MONITOR_ACCEPTANCE_PORTAL_PORT:-8101}"

fail() {
  print -u2 -- "错误：$*"
  exit 1
}

require_docker() {
  command -v docker >/dev/null 2>&1 || fail "未找到 Docker。请先安装并启动 Docker Desktop。"
  docker compose version >/dev/null 2>&1 || fail "未找到 Docker Compose v2。请检查 Docker Desktop 是否已启动。"
}

compose() {
  MONITOR_ACCEPTANCE_IMAGE="$IMAGE_TAG" \
    MONITOR_ACCEPTANCE_VOLUME="$DATA_VOLUME" \
    MONITOR_ACCEPTANCE_BACKUP_VOLUME="$BACKUP_VOLUME" \
    docker compose \
      -f "$REPO_DIR/docker-compose.local-acceptance.yml" \
      -f "$REPO_DIR/docker-compose.local-snapshot.yml" \
      "$@"
}

wait_for_ready() {
  local attempts=30
  local attempt=1

  while (( attempt <= attempts )); do
    if curl -fsS "http://127.0.0.1:${ADMIN_PORT}/live" >/dev/null 2>&1 && \
      curl -fsS "http://127.0.0.1:${ADMIN_PORT}/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
    (( attempt++ ))
  done

  return 1
}

show_urls() {
  print -- ""
  print -- "本机 Monitor 已启动（完全离线模式）。"
  print -- "健康检查： http://127.0.0.1:${ADMIN_PORT}/live"
  print -- "就绪检查： http://127.0.0.1:${ADMIN_PORT}/ready"
  print -- "管理端地址： http://127.0.0.1:${ADMIN_PORT}"
  print -- "门户地址：   http://127.0.0.1:${PORTAL_PORT}"
  print -- ""
  print -- "说明：离线模式不连接 NewAPI，因此页面不能登录、也没有真实业务数据；"
  print -- "它用于确认镜像、SQLite 数据卷、Redis 与健康检查可正常工作。"
  print -- "需要验证数据同步时，请阅读 docs/local-docker-testing.md 的“隔离合成数据验收”。"
}

main() {
  local action="${1:-up}"
  (( $# <= 1 )) || fail "只接受一个操作：up、status、logs 或 stop。"
  require_docker

  case "$action" in
    up)
      # compose 文件将这两个卷标记为 external；这里显式、幂等地创建，避免新人首次启动报错。
      docker volume create "$DATA_VOLUME" >/dev/null
      docker volume create "$BACKUP_VOLUME" >/dev/null
      print -- "正在构建并启动完全离线的本机环境…"
      compose up -d --build redis monitor
      if wait_for_ready; then
        show_urls
      else
        print -u2 -- "容器已启动，但 60 秒内未通过就绪检查。可执行："
        print -u2 -- "  ./dev/run-local-dev.sh logs"
        exit 1
      fi
      ;;
    status)
      compose ps
      print -- ""
      print -- "健康检查："
      curl -fsS "http://127.0.0.1:${ADMIN_PORT}/live" || true
      print -- ""
      print -- "就绪检查："
      curl -fsS "http://127.0.0.1:${ADMIN_PORT}/ready" || true
      print -- ""
      ;;
    logs)
      compose logs --tail=100 -f monitor
      ;;
    stop)
      # 仅停止容器；刻意不使用 down -v，保留开发数据库和备份卷。
      compose stop
      print -- "本机容器已停止；数据卷与备份卷已保留。"
      ;;
    *)
      fail "未知操作：$action（可用：up、status、logs、stop）。"
      ;;
  esac
}

main "$@"
