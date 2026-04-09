#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${1:-/opt/okx-local-app}"
SERVICE_NAME="${2:-okx-remote-auto-update}"
NODE_SERVICE_NAME="${3:-okx-remote-node}"
TIMER_NAME="${SERVICE_NAME}.timer"

echo "[1/4] installing auto-update script"
sudo chmod +x "$APP_DIR/deploy/auto-update-remote-node.sh"

echo "[2/4] installing systemd service"
sudo sed \
  -e "s|^WorkingDirectory=.*|WorkingDirectory=$APP_DIR|" \
  -e "s|^Environment=OKX_REMOTE_APP_DIR=.*|Environment=OKX_REMOTE_APP_DIR=$APP_DIR|" \
  -e "s|^Environment=OKX_REMOTE_SERVICE_NAME=.*|Environment=OKX_REMOTE_SERVICE_NAME=$NODE_SERVICE_NAME|" \
  -e "s|^ExecStart=.*|ExecStart=$APP_DIR/deploy/auto-update-remote-node.sh|" \
  "$APP_DIR/deploy/systemd/okx-remote-auto-update.service" | sudo tee "/etc/systemd/system/${SERVICE_NAME}.service" >/dev/null

echo "[3/4] installing systemd timer"
sudo cp "$APP_DIR/deploy/systemd/okx-remote-auto-update.timer" "/etc/systemd/system/${TIMER_NAME}"

echo "[4/4] reloading systemd and enabling timer"
sudo systemctl daemon-reload
sudo systemctl enable --now "$TIMER_NAME"
sudo systemctl --no-pager --full status "$TIMER_NAME" || true

echo
echo "Done. 远端节点现在会按定时器自动拉取 GitHub 最新代码并重启 ${NODE_SERVICE_NAME}。"

