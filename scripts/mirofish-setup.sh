#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MIROFISH_DIR="$ROOT_DIR/vendor/MiroFish"

if [ ! -d "$MIROFISH_DIR" ]; then
  echo "MiroFish 目录不存在: $MIROFISH_DIR" >&2
  exit 1
fi

cd "$MIROFISH_DIR"

if [ ! -f ".env" ] && [ -f ".env.example" ]; then
  cp ".env.example" ".env"
  echo "已创建 .env，请补充 LLM/Zep 等必要配置后再启动。"
fi

echo "开始安装 MiroFish 依赖..."
npm run setup:all

echo
echo "MiroFish 依赖安装完成。"
echo "目录: $MIROFISH_DIR"
echo "下一步:"
echo "  1) 编辑 $MIROFISH_DIR/.env"
echo "  2) 运行 $ROOT_DIR/scripts/mirofish-dev.sh"
