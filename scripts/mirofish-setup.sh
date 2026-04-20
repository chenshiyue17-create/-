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

pick_uv() {
  local candidate
  for candidate in "${MIROFISH_UV:-}" "$HOME/.local/bin/uv" /usr/local/bin/uv /opt/homebrew/bin/uv uv; do
    if [ -n "$candidate" ] && command -v "$candidate" >/dev/null 2>&1; then
      command -v "$candidate"
      return 0
    fi
  done
  return 1
}

pick_node() {
  local candidate
  for candidate in /usr/local/bin/node /opt/homebrew/bin/node /usr/bin/node node; do
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
    /usr/lib/node_modules/npm/bin/npm-cli.js \
    /usr/local/lib/node_modules/npm/bin/npm-cli.js \
    /opt/homebrew/lib/node_modules/npm/bin/npm-cli.js \
    "$HOME/.npm-global/lib/node_modules/npm/bin/npm-cli.js"
  do
    if [ -f "$candidate" ]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  if command -v npm >/dev/null 2>&1; then
    local npm_root
    npm_root="$(npm root -g 2>/dev/null || true)"
    if [ -n "$npm_root" ] && [ -f "$npm_root/npm/bin/npm-cli.js" ]; then
      printf '%s\n' "$npm_root/npm/bin/npm-cli.js"
      return 0
    fi
  fi
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

ensure_uv() {
  local uv_bin
  uv_bin="$(pick_uv || true)"
  if [ -n "$uv_bin" ]; then
    printf '%s\n' "$uv_bin"
    return 0
  fi
  echo "未检测到 uv，正在尝试自动安装到 ~/.local/bin ..."
  if ! "$PYTHON_BIN" -m pip install --user uv; then
    printf '\n'
    return 0
  fi
  uv_bin="$(pick_uv || true)"
  printf '%s\n' "$uv_bin"
}

setup_backend_with_venv() {
  local requirements_file="${1:-requirements.txt}"
  echo "未使用 uv，回退到 Python venv 安装后端依赖..."
  (
    cd backend
    "$PYTHON_BIN" -m venv .venv
    . .venv/bin/activate
    python -m pip install --upgrade pip setuptools wheel
    python -m pip install -r "$requirements_file"
  )
}

determine_backend_requirements() {
  local llm_backend graph_backend
  llm_backend="$(grep '^MIROFISH_LLM_BACKEND=' .env 2>/dev/null | tail -n 1 | cut -d= -f2- | tr -d '\r' || true)"
  graph_backend="$(grep '^MIROFISH_GRAPH_BACKEND=' .env 2>/dev/null | tail -n 1 | cut -d= -f2- | tr -d '\r' || true)"
  llm_backend="${llm_backend:-codex}"
  graph_backend="${graph_backend:-local}"
  if [ "$llm_backend" = "codex" ] && [ "$graph_backend" = "local" ]; then
    printf '%s\n' "requirements-lite.txt"
  else
    printf '%s\n' "requirements.txt"
  fi
}

NODE_BIN="$(pick_node || true)"
NPM_CLI="$(pick_npm_cli || true)"
if [ -z "$NODE_BIN" ] || [ -z "$NPM_CLI" ]; then
  echo "未找到可用的 Node.js / npm-cli.js" >&2
  exit 1
fi

UV_BIN="${UV_BIN:-}"
export CODEX_PATH
export UV_PYTHON="$PYTHON_BIN"
if [ -n "$UV_BIN" ]; then
  export PATH="$(dirname "$UV_BIN"):$PATH"
fi
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

BACKEND_REQUIREMENTS="$(determine_backend_requirements)"
UV_BIN=""
if [ "$BACKEND_REQUIREMENTS" != "requirements-lite.txt" ]; then
  UV_BIN="$(ensure_uv || true)"
fi

echo "开始安装 MiroFish 依赖..."
echo "使用 Python: $UV_PYTHON"
if [ "$BACKEND_REQUIREMENTS" = "requirements-lite.txt" ]; then
  echo "使用 uv: 当前链路不需要（lite 后端依赖）"
elif [ -n "$UV_BIN" ]; then
  echo "使用 uv: $UV_BIN"
else
  echo "使用 uv: 未找到，改走 venv 回退链路"
fi
echo "使用 Node: $NODE_BIN"
echo "后端依赖清单: backend/$BACKEND_REQUIREMENTS"
if [ -f "frontend/dist/index.html" ]; then
  echo "检测到已打包前端资源，跳过前端依赖安装。"
else
  echo "安装前端依赖..."
  ( cd frontend && "$NODE_BIN" "$NPM_CLI" install --no-fund --no-audit )
fi
echo "同步后端依赖..."
if [ "$BACKEND_REQUIREMENTS" = "requirements-lite.txt" ]; then
  setup_backend_with_venv "$BACKEND_REQUIREMENTS"
elif [ -n "$UV_BIN" ]; then
  if ! ( cd backend && "$UV_BIN" sync ); then
    setup_backend_with_venv "$BACKEND_REQUIREMENTS"
  fi
else
  setup_backend_with_venv "$BACKEND_REQUIREMENTS"
fi

echo
echo "MiroFish 依赖安装完成。"
echo "目录: $MIROFISH_DIR"
echo "下一步:"
echo "  1) 如需自定义模型，可编辑 $MIROFISH_DIR/.env"
echo "  2) 运行 $ROOT_DIR/scripts/mirofish-dev.sh"
