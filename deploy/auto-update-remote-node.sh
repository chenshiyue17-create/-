#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${1:-${OKX_REMOTE_APP_DIR:-/opt/okx-local-app}}"
BRANCH="${OKX_REMOTE_BRANCH:-main}"
REMOTE_NAME="${OKX_REMOTE_GIT_REMOTE:-origin}"
SERVICE_NAME="${OKX_REMOTE_SERVICE_NAME:-okx-remote-node}"
ENV_FILE="${OKX_REMOTE_ENV_FILE:-/etc/okx-remote-node.env}"
LOCK_FILE="${OKX_REMOTE_LOCK_FILE:-/tmp/okx-remote-auto-update.lock}"
VERIFY_TIMEOUT_SECONDS="${OKX_REMOTE_VERIFY_TIMEOUT_SECONDS:-15}"
REQUIREMENTS_MARKER="${APP_DIR}/.remote-requirements.sha256"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

HOST="${OKX_LOCAL_APP_HOST:-127.0.0.1}"
PORT="${OKX_LOCAL_APP_PORT:-18765}"
DATA_DIR="${OKX_LOCAL_APP_DATA_DIR:-${APP_DIR}/data-remote}"
TOKEN="${OKX_DESK_GATEWAY_TOKEN:-}"

exec 9>"$LOCK_FILE"
if command -v flock >/dev/null 2>&1; then
  if ! flock -n 9; then
    echo "已有远端自动更新任务在运行，跳过本轮。"
    exit 0
  fi
fi

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "缺少命令：$cmd"
    exit 1
  fi
}

hash_stream() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 | awk '{print $1}'
    return
  fi
  echo "缺少 sha256sum/shasum，无法计算依赖指纹。"
  exit 1
}

pick_python() {
  local candidate version
  for candidate in \
    "${APP_DIR}/.venv/bin/python3" \
    python3.13 python3.12 python3.11 python3.10 python3
  do
    if ! command -v "$candidate" >/dev/null 2>&1; then
      continue
    fi
    version="$("$candidate" - <<'PY'
import sys
print(f"{sys.version_info.major}.{sys.version_info.minor}")
PY
)"
    case "$version" in
      3.1[0-9]|3.[2-9][0-9])
        echo "$candidate"
        return 0
        ;;
    esac
  done
  return 1
}

restart_with_systemd() {
  if ! command -v systemctl >/dev/null 2>&1; then
    return 1
  fi
  if ! systemctl list-unit-files "${SERVICE_NAME}.service" >/dev/null 2>&1; then
    return 1
  fi
  systemctl restart "$SERVICE_NAME"
  systemctl --no-pager --full status "$SERVICE_NAME" || true
}

restart_with_nohup() {
  mkdir -p "$DATA_DIR"
  fuser -k "${PORT}/tcp" >/dev/null 2>&1 || true
  pkill -f "${APP_DIR}/server.py" >/dev/null 2>&1 || true
  nohup env \
    OKX_LOCAL_APP_HOST="$HOST" \
    OKX_LOCAL_APP_PORT="$PORT" \
    OKX_LOCAL_APP_DATA_DIR="$DATA_DIR" \
    OKX_DESK_GATEWAY_TOKEN="$TOKEN" \
    bash "${APP_DIR}/scripts/start-remote-node.sh" >/tmp/okx-remote-node.log 2>&1 &
}

wait_for_health() {
  local deadline now
  if [[ -z "$TOKEN" ]]; then
    echo "缺少 OKX_DESK_GATEWAY_TOKEN，无法校验远端节点健康状态。"
    exit 1
  fi
  deadline=$((SECONDS + VERIFY_TIMEOUT_SECONDS))
  while true; do
    if curl -fsS \
      -H "X-OKX-Desk-Gateway-Token: $TOKEN" \
      "http://127.0.0.1:${PORT}/api/ping" >/dev/null 2>&1; then
      return 0
    fi
    now=$SECONDS
    if (( now >= deadline )); then
      echo "远端节点启动后健康校验超时。"
      tail -n 80 /tmp/okx-remote-node.log || true
      return 1
    fi
    sleep 1
  done
}

ensure_runtime() {
  local pybin requirements_hash next_hash

  pybin="$(pick_python)" || {
    echo "未找到 Python 3.10+，无法更新远端节点。"
    exit 1
  }

  if [[ ! -x "${APP_DIR}/.venv/bin/python3" ]]; then
    "$pybin" -m venv "${APP_DIR}/.venv"
  fi

  requirements_hash="$(
    { cat "${APP_DIR}/requirements-remote.txt"; [[ -f "${APP_DIR}/requirements.txt" ]] && cat "${APP_DIR}/requirements.txt"; } \
      | hash_stream
  )"
  next_hash="$(cat "$REQUIREMENTS_MARKER" 2>/dev/null || true)"

  if [[ "$requirements_hash" != "$next_hash" ]]; then
    "${APP_DIR}/.venv/bin/python3" -m pip install --upgrade pip >/dev/null
    "${APP_DIR}/.venv/bin/python3" -m pip install -r "${APP_DIR}/requirements-remote.txt"
    printf '%s' "$requirements_hash" >"$REQUIREMENTS_MARKER"
  fi
}

require_cmd git
require_cmd curl

if [[ ! -d "$APP_DIR/.git" ]]; then
  echo "未找到 $APP_DIR/.git，远端节点目录还没准备好。"
  exit 1
fi

cd "$APP_DIR"

CURRENT_HEAD="$(git rev-parse HEAD)"
git fetch "$REMOTE_NAME" "$BRANCH"
TARGET_HEAD="$(git rev-parse FETCH_HEAD)"

if [[ "$CURRENT_HEAD" == "$TARGET_HEAD" ]]; then
  echo "okx-remote-node 已是最新版本：$CURRENT_HEAD"
  wait_for_health
  exit 0
fi

git stash push -u -m "okx-auto-update-$(date +%Y%m%d-%H%M%S)" >/dev/null 2>&1 || true
git checkout -B "$BRANCH" "$REMOTE_NAME/$BRANCH"
git reset --hard "$REMOTE_NAME/$BRANCH"

ensure_runtime

if ! restart_with_systemd; then
  restart_with_nohup
fi

wait_for_health
echo "okx-remote-node 已更新到 ${TARGET_HEAD}"
