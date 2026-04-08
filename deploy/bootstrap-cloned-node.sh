#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
SERVICE_NAME="${2:-okx-remote-node}"
PORT="${3:-18765}"

if [[ ! -f "$APP_DIR/server.py" ]]; then
  echo "未找到 $APP_DIR/server.py，当前目录不像是 okx-local-app 项目。"
  exit 1
fi

chmod +x \
  "$APP_DIR/deploy/check-server-collisions.sh" \
  "$APP_DIR/deploy/install-remote-node.sh" \
  "$APP_DIR/scripts/start-remote-node.sh"

echo "[bootstrap] checking collisions on port $PORT"
"$APP_DIR/deploy/check-server-collisions.sh" "$PORT" "$SERVICE_NAME"

echo
echo "[bootstrap] installing systemd service $SERVICE_NAME"
sudo "$APP_DIR/deploy/install-remote-node.sh" "$APP_DIR" "$SERVICE_NAME"
