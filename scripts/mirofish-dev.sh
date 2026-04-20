#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MIROFISH_DIR="$ROOT_DIR/vendor/MiroFish"

pick_python() {
  for candidate in python3.12 python3.11 python3; do
    if command -v "$candidate" >/dev/null 2>&1; then
      command -v "$candidate"
      return 0
    fi
  done
  return 1
}

pick_node() {
  local candidate
  for candidate in /usr/local/bin/node /opt/homebrew/bin/node node; do
    if [ -n "$candidate" ] && command -v "$candidate" >/dev/null 2>&1; then
      command -v "$candidate"
      return 0
    fi
  done
  return 1
}

pick_npm_cli() {
  local candidate
  for candidate in \
    /usr/local/lib/node_modules/npm/bin/npm-cli.js \
    /opt/homebrew/lib/node_modules/npm/bin/npm-cli.js \
    "$HOME/.npm-global/lib/node_modules/npm/bin/npm-cli.js"
  do
    if [ -f "$candidate" ]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  return 1
}

pick_codex() {
  local candidate
  for candidate in \
    /Applications/Codex.app/Contents/Resources/codex \
    "$HOME/.npm-global/bin/codex" \
    codex
  do
    if [ -n "$candidate" ] && command -v "$candidate" >/dev/null 2>&1; then
      command -v "$candidate"
      return 0
    fi
  done
  return 1
}

if [ ! -d "$MIROFISH_DIR" ]; then
  echo "MiroFish 目录不存在: $MIROFISH_DIR" >&2
  exit 1
fi

cd "$MIROFISH_DIR"

if [ ! -f ".env" ]; then
  echo "缺少 .env，请先运行 $ROOT_DIR/scripts/mirofish-setup.sh" >&2
  exit 1
fi

PYTHON_BIN="$(pick_python || true)"
if [ -z "$PYTHON_BIN" ]; then
  echo "未找到可用的 Python 3.11+/3.12 运行时" >&2
  exit 1
fi

NODE_BIN="$(pick_node || true)"
NPM_CLI="$(pick_npm_cli || true)"
if [ -z "$NODE_BIN" ] || [ -z "$NPM_CLI" ]; then
  echo "未找到可用的 Node.js / npm-cli.js" >&2
  exit 1
fi

export UV_PYTHON="$PYTHON_BIN"
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
export CODEX_PATH="${CODEX_PATH:-$(pick_codex || true)}"
export MIROFISH_LLM_BACKEND="${MIROFISH_LLM_BACKEND:-codex}"
export MIROFISH_GRAPH_BACKEND="${MIROFISH_GRAPH_BACKEND:-local}"
export MIROFISH_CODEX_COMMAND="${MIROFISH_CODEX_COMMAND:-${CODEX_PATH:-codex}}"
export MIROFISH_CODEX_TIMEOUT_SECONDS="${MIROFISH_CODEX_TIMEOUT_SECONDS:-240}"
unset PYTHONHOME PYTHONPATH PYTHONEXECUTABLE PYTHONSTARTUP VIRTUAL_ENV __PYVENV_LAUNCHER__

echo "启动 MiroFish..."
echo "运行模式: LLM=${MIROFISH_LLM_BACKEND} / Graph=${MIROFISH_GRAPH_BACKEND}"
echo "Python: $UV_PYTHON"
echo "Node: $NODE_BIN"
echo "前端: http://127.0.0.1:3000"
echo "后端: http://127.0.0.1:5001"
echo "当前工作台内嵌入口: http://127.0.0.1:8765/mirofish/"
echo

cleanup() {
  local code=$?
  trap - EXIT INT TERM
  if [ -n "${BACKEND_PID:-}" ]; then
    kill "$BACKEND_PID" >/dev/null 2>&1 || true
  fi
  if [ -n "${FRONTEND_PID:-}" ]; then
    kill "$FRONTEND_PID" >/dev/null 2>&1 || true
  fi
  wait >/dev/null 2>&1 || true
  exit "$code"
}

trap cleanup EXIT INT TERM

(
  cd backend
  uv run python run.py
) &
BACKEND_PID=$!

(
  cd frontend
  "$NODE_BIN" "$NPM_CLI" run dev -- --host
) &
FRONTEND_PID=$!

wait -n "$BACKEND_PID" "$FRONTEND_PID"
