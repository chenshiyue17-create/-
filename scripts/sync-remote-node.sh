#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOCAL_SERVER="${OKX_LOCAL_CONTROL_URL:-http://127.0.0.1:8765}"
BRANCH="${OKX_REMOTE_BRANCH:-main}"
REMOTE_APP_DIR="${OKX_REMOTE_APP_DIR:-/opt/okx-local-app}"
REMOTE_PORT="${OKX_REMOTE_PORT:-18765}"
REMOTE_DATA_DIR="${OKX_REMOTE_DATA_DIR:-$REMOTE_APP_DIR/data-remote}"
FIRST_ARG="${1:-}"
DRY_RUN=0
if [[ "$FIRST_ARG" == "--help" || "$FIRST_ARG" == "-h" ]]; then
  usage
  exit 0
fi
if [[ "$FIRST_ARG" == "--dry-run" ]]; then
  DRY_RUN=1
  shift || true
  FIRST_ARG="${1:-}"
fi
REMOTE_SSH_TARGET="${FIRST_ARG:-${OKX_REMOTE_SSH_TARGET:-}}"
REMOTE_SSH_USER="${OKX_REMOTE_SSH_USER:-ecs-assist-user}"

usage() {
  cat <<'EOF'
用法：
  scripts/sync-remote-node.sh [user@host]

行为：
  1. 自动从本地 App 读取远端节点 URL、当前环境和鉴权令牌
  2. 通过 SSH 连到远端节点
  3. 自动拉取 GitHub 主线、重启远端节点
  4. 自动校验远端健康状态

可选环境变量：
  OKX_REMOTE_SSH_TARGET   远端 SSH 目标，例如 ecs-assist-user@47.87.68.74
  OKX_REMOTE_SSH_USER     当远端 URL 里只有 host 时，默认拼接的 SSH 用户，默认 ecs-assist-user
  OKX_REMOTE_APP_DIR      远端项目目录，默认 /opt/okx-local-app
  OKX_REMOTE_PORT         远端节点端口，默认 18765
  OKX_REMOTE_DATA_DIR     远端数据目录，默认 /opt/okx-local-app/data-remote
  OKX_REMOTE_BRANCH       远端更新分支，默认 main
  OKX_LOCAL_CONTROL_URL   本地控制面地址，默认 http://127.0.0.1:8765
  OKX_DESK_GATEWAY_TOKEN  如果本地无法自动解出远端节点令牌，可手动传入
  OKX_REMOTE_GATEWAY_URL  如果本地无法自动读到远端 URL，可手动传入
EOF
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "缺少命令：$cmd"
    exit 1
  fi
}

require_cmd python3
require_cmd ssh
require_cmd git
require_cmd curl

resolve_local_remote_config() {
  ROOT_DIR="$ROOT_DIR" LOCAL_SERVER="$LOCAL_SERVER" python3 - <<'PY'
import importlib.util
import json
import os
import sys
import urllib.request
from pathlib import Path

root = Path(os.environ["ROOT_DIR"])
local_server = os.environ["LOCAL_SERVER"].rstrip("/")

server_path = root / "server.py"
spec = importlib.util.spec_from_file_location("okx_local_server", server_path)
if spec is None or spec.loader is None:
    raise SystemExit("无法加载本地 server.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

candidates = []
env_data_dir = os.environ.get("OKX_LOCAL_APP_DATA_DIR")
if env_data_dir:
    candidates.append(Path(env_data_dir))

home = Path.home()
candidates.extend(
    [
        home / "Library" / "Application Support" / "OKXLocalApp" / "data",
        root / "data",
    ]
)

seen = set()
config = {}
for data_dir in candidates:
    if not data_dir:
        continue
    data_dir = data_dir.expanduser().resolve()
    if str(data_dir) in seen:
        continue
    seen.add(str(data_dir))
    try:
        os.environ["OKX_LOCAL_APP_DATA_DIR"] = str(data_dir)
        module.DATA_DIR = data_dir
        module.CONFIG_PATH = data_dir / "local-config.json"
        module.FALLBACK_SECRET_FILE = data_dir / ".local-file-key"
        module.CONFIG = module.ConfigStore(module.CONFIG_PATH)
        current = module.CONFIG.current()
        if current.get("remoteGatewayUrl") or current.get("remoteGatewayToken"):
            config = current
            break
    except Exception:
        continue

if not config:
    try:
        with urllib.request.urlopen(f"{local_server}/api/local-config", timeout=3) as resp:
            payload = json.load(resp)
            config = (payload or {}).get("config") or {}
    except Exception:
        config = {}

result = {
    "remoteGatewayUrl": str(config.get("remoteGatewayUrl") or "").strip(),
    "remoteGatewayToken": str(config.get("remoteGatewayToken") or os.environ.get("OKX_DESK_GATEWAY_TOKEN") or "").strip(),
    "envPreset": str(config.get("envPreset") or "").strip(),
}
print(json.dumps(result, ensure_ascii=False))
PY
}

CONFIG_JSON="$(resolve_local_remote_config)"
REMOTE_GATEWAY_URL="$(CONFIG_JSON="$CONFIG_JSON" python3 - <<'PY'
import json, os
print((json.loads(os.environ["CONFIG_JSON"]) or {}).get("remoteGatewayUrl",""))
PY
)"
REMOTE_GATEWAY_TOKEN="$(CONFIG_JSON="$CONFIG_JSON" python3 - <<'PY'
import json, os
print((json.loads(os.environ["CONFIG_JSON"]) or {}).get("remoteGatewayToken",""))
PY
)"
EXPECT_ENV="$(CONFIG_JSON="$CONFIG_JSON" python3 - <<'PY'
import json, os
env = str((json.loads(os.environ["CONFIG_JSON"]) or {}).get("envPreset","")).lower()
print("demo" if "demo" in env else "live")
PY
)"

if [[ -z "$REMOTE_GATEWAY_URL" ]]; then
  echo "无法自动读取远端节点 URL，请设置 OKX_REMOTE_GATEWAY_URL 或先在本地 App 保存远端节点。"
  usage
  exit 1
fi

REMOTE_GATEWAY_URL="${OKX_REMOTE_GATEWAY_URL:-$REMOTE_GATEWAY_URL}"

if [[ -z "$REMOTE_GATEWAY_TOKEN" ]]; then
  echo "无法自动读取远端节点令牌，请先导出 OKX_DESK_GATEWAY_TOKEN。"
  exit 1
fi

REMOTE_HOST="$(REMOTE_GATEWAY_URL="$REMOTE_GATEWAY_URL" python3 - <<'PY'
import os
from urllib.parse import urlparse
url = urlparse(os.environ["REMOTE_GATEWAY_URL"])
print(url.hostname or "")
PY
)"

if [[ -z "$REMOTE_SSH_TARGET" ]]; then
  if [[ -z "$REMOTE_HOST" ]]; then
    echo "无法从远端节点 URL 解析出主机。"
    exit 1
  fi
  REMOTE_SSH_TARGET="${REMOTE_SSH_USER}@${REMOTE_HOST}"
fi

REPO_URL="$(git -C "$ROOT_DIR" remote get-url origin)"

echo "准备同步远端节点"
echo "SSH 目标: $REMOTE_SSH_TARGET"
echo "远端地址: $REMOTE_GATEWAY_URL"
echo "期望环境: $EXPECT_ENV"
echo

if [[ "$DRY_RUN" == "1" ]]; then
  echo "dry-run 模式：只输出自动识别结果，不执行远端同步。"
  exit 0
fi

ssh "$REMOTE_SSH_TARGET" bash -s -- \
  "$REMOTE_APP_DIR" \
  "$REMOTE_PORT" \
  "$REMOTE_DATA_DIR" \
  "$REMOTE_GATEWAY_TOKEN" \
  "$REPO_URL" \
  "$BRANCH" <<'REMOTE'
set -euo pipefail

APP_DIR="$1"
PORT="$2"
DATA_DIR="$3"
TOKEN="$4"
REPO_URL="$5"
BRANCH="$6"

mkdir -p "$(dirname "$APP_DIR")"

if [[ ! -d "$APP_DIR/.git" ]]; then
  git clone "$REPO_URL" "$APP_DIR"
fi

cd "$APP_DIR"

git stash push -u -m "okx-auto-sync-$(date +%Y%m%d-%H%M%S)" >/dev/null 2>&1 || true
git fetch origin "$BRANCH"
git checkout -B "$BRANCH" "origin/$BRANCH"

fuser -k "${PORT}/tcp" >/dev/null 2>&1 || true
pkill -f "$APP_DIR/server.py" >/dev/null 2>&1 || true

nohup env \
  OKX_LOCAL_APP_HOST=0.0.0.0 \
  OKX_LOCAL_APP_PORT="$PORT" \
  OKX_LOCAL_APP_DATA_DIR="$DATA_DIR" \
  OKX_DESK_GATEWAY_TOKEN="$TOKEN" \
  bash "$APP_DIR/scripts/start-remote-node.sh" >/tmp/okx-remote-node.log 2>&1 &

sleep 3
curl -fsS \
  -H "X-OKX-Desk-Gateway-Token: $TOKEN" \
  "http://127.0.0.1:${PORT}/api/ping" >/dev/null
REMOTE

echo "远端节点已重启，开始健康校验..."
echo

OKX_DESK_GATEWAY_TOKEN="$REMOTE_GATEWAY_TOKEN" \
  "$ROOT_DIR/scripts/verify-remote-safety.sh" "$REMOTE_GATEWAY_URL" "$EXPECT_ENV"
