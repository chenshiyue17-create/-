#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
HOST="${OKX_LOCAL_APP_HOST:-127.0.0.1}"
PORT="${OKX_LOCAL_APP_PORT:-8765}"
DATA_DIR="${OKX_LOCAL_APP_DATA_DIR:-$ROOT_DIR/data-remote}"
PYTHON_BIN="${ROOT_DIR}/.venv/bin/python3"

if [[ -z "${OKX_DESK_GATEWAY_TOKEN:-}" ]]; then
  echo "OKX_DESK_GATEWAY_TOKEN 未设置，拒绝以远端执行节点模式启动。"
  echo "示例：OKX_DESK_GATEWAY_TOKEN=your-token OKX_LOCAL_APP_PORT=8765 $0"
  exit 1
fi

mkdir -p "$DATA_DIR"

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

echo "Starting OKX remote execution node on $HOST:$PORT"
echo "Data dir: $DATA_DIR"
echo "Python: $PYTHON_BIN"

exec "$PYTHON_BIN" "$ROOT_DIR/server.py"
