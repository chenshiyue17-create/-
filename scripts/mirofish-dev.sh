#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MIROFISH_DIR="$ROOT_DIR/vendor/MiroFish"

if [ ! -d "$MIROFISH_DIR" ]; then
  echo "MiroFish 目录不存在: $MIROFISH_DIR" >&2
  exit 1
fi

cd "$MIROFISH_DIR"

if [ ! -f ".env" ]; then
  echo "缺少 .env，请先运行 $ROOT_DIR/scripts/mirofish-setup.sh" >&2
  exit 1
fi

echo "启动 MiroFish..."
echo "前端: http://127.0.0.1:3000"
echo "后端: http://127.0.0.1:5001"
echo

npm run dev
