#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MIROFISH_DIR="$ROOT_DIR/vendor/MiroFish"

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

CODEX_PATH="$(pick_codex || true)"

pick_python() {
  local candidate
  for candidate in "${MIROFISH_PYTHON:-}" python3.12 python3.11 python3; do
    if [ -n "$candidate" ] && command -v "$candidate" >/dev/null 2>&1; then
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

if [ ! -d "$MIROFISH_DIR" ]; then
  echo "MiroFish 目录不存在: $MIROFISH_DIR" >&2
  exit 1
fi

cd "$MIROFISH_DIR"

PYTHON_BIN="$(pick_python || true)"
if [ -z "$PYTHON_BIN" ]; then
  echo "未找到可用 Python（需要 python3.11+）" >&2
  exit 1
fi

NODE_BIN="$(pick_node || true)"
NPM_CLI="$(pick_npm_cli || true)"
if [ -z "$NODE_BIN" ] || [ -z "$NPM_CLI" ]; then
  echo "未找到可用的 Node.js / npm-cli.js" >&2
  exit 1
fi

export CODEX_PATH
export UV_PYTHON="$PYTHON_BIN"
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
unset PYTHONHOME PYTHONPATH PYTHONEXECUTABLE PYTHONSTARTUP VIRTUAL_ENV __PYVENV_LAUNCHER__

if [ ! -f ".env" ] && [ -f ".env.example" ]; then
  cp ".env.example" ".env"
  echo "已创建 .env，默认使用 Codex CLI + 本地图谱，无需额外 API key。"
fi

python3 - <<'PY'
from pathlib import Path
import os

env_path = Path(".env")
if not env_path.exists():
    raise SystemExit(0)

raw = env_path.read_text(encoding="utf-8", errors="ignore").splitlines()
values = {}
for line in raw:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        continue
    key, value = stripped.split("=", 1)
    values[key.strip()] = value.strip()

defaults = {
    "MIROFISH_LLM_BACKEND": "codex",
    "MIROFISH_GRAPH_BACKEND": "local",
    "MIROFISH_CODEX_COMMAND": os.environ.get("CODEX_PATH") or "codex",
    "MIROFISH_CODEX_MODEL": values.get("MIROFISH_CODEX_MODEL", ""),
    "MIROFISH_CODEX_TIMEOUT_SECONDS": values.get("MIROFISH_CODEX_TIMEOUT_SECONDS", "240"),
}

for key, value in defaults.items():
    current = str(values.get(key, "")).strip()
    if not current or current.lower().startswith("your_"):
        values[key] = value

for key in ("LLM_API_KEY", "ZEP_API_KEY", "LLM_BOOST_API_KEY", "LLM_BOOST_BASE_URL", "LLM_BOOST_MODEL_NAME"):
    values.setdefault(key, "")

ordered_keys = [
    "MIROFISH_LLM_BACKEND",
    "MIROFISH_GRAPH_BACKEND",
    "MIROFISH_CODEX_COMMAND",
    "MIROFISH_CODEX_MODEL",
    "MIROFISH_CODEX_TIMEOUT_SECONDS",
    "LLM_API_KEY",
    "LLM_BASE_URL",
    "LLM_MODEL_NAME",
    "ZEP_API_KEY",
    "LLM_BOOST_API_KEY",
    "LLM_BOOST_BASE_URL",
    "LLM_BOOST_MODEL_NAME",
]

lines = []
seen = set()
for key in ordered_keys:
    if key in values:
        lines.append(f"{key}={values[key]}")
        seen.add(key)
for key, value in values.items():
    if key not in seen:
        lines.append(f"{key}={value}")
env_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
PY

echo "开始安装 MiroFish 依赖..."
echo "使用 Python: $UV_PYTHON"
echo "使用 Node: $NODE_BIN"
echo "安装根目录依赖..."
"$NODE_BIN" "$NPM_CLI" install
echo "安装前端依赖..."
( cd frontend && "$NODE_BIN" "$NPM_CLI" install )
echo "同步后端依赖..."
( cd backend && uv sync )

echo
echo "MiroFish 依赖安装完成。"
echo "目录: $MIROFISH_DIR"
echo "下一步:"
echo "  1) 如需自定义模型，可编辑 $MIROFISH_DIR/.env"
echo "  2) 运行 $ROOT_DIR/scripts/mirofish-dev.sh"
