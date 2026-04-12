#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${1:-/opt/okx-local-app}"
SERVICE_NAME="${2:-okx-remote-auto-update}"
NODE_SERVICE_NAME="${3:-okx-remote-node}"
INTERVAL_MINUTES="${OKX_REMOTE_AUTO_UPDATE_INTERVAL_MINUTES:-2}"
TIMER_NAME="${SERVICE_NAME}.timer"

echo "[1/5] installing auto-update script"
sudo chmod +x "$APP_DIR/deploy/auto-update-remote-node.sh"

echo "[2/5] installing systemd service"
sudo sed \
  -e "s|^WorkingDirectory=.*|WorkingDirectory=$APP_DIR|" \
  -e "s|^Environment=OKX_REMOTE_APP_DIR=.*|Environment=OKX_REMOTE_APP_DIR=$APP_DIR|" \
  -e "s|^Environment=OKX_REMOTE_SERVICE_NAME=.*|Environment=OKX_REMOTE_SERVICE_NAME=$NODE_SERVICE_NAME|" \
  -e "s|^Environment=OKX_REMOTE_ENV_FILE=.*|Environment=OKX_REMOTE_ENV_FILE=/etc/okx-remote-node.env|" \
  -e "s|^ExecStart=.*|ExecStart=$APP_DIR/deploy/auto-update-remote-node.sh|" \
  "$APP_DIR/deploy/systemd/okx-remote-auto-update.service" | sudo tee "/etc/systemd/system/${SERVICE_NAME}.service" >/dev/null

echo "[3/5] installing systemd timer"
sudo sed \
  -e "s|^OnBootSec=.*|OnBootSec=${INTERVAL_MINUTES}min|" \
  -e "s|^OnUnitActiveSec=.*|OnUnitActiveSec=${INTERVAL_MINUTES}min|" \
  -e "s|^Unit=.*|Unit=${SERVICE_NAME}.service|" \
  "$APP_DIR/deploy/systemd/okx-remote-auto-update.timer" | sudo tee "/etc/systemd/system/${TIMER_NAME}" >/dev/null

echo "[4/5] reloading systemd and enabling timer"
sudo systemctl daemon-reload
sudo systemctl enable --now "$TIMER_NAME"
sudo systemctl --no-pager --full status "$TIMER_NAME" || true

echo "[5/5] dry-run auto update"
sudo systemctl start "${SERVICE_NAME}.service" || true
sudo systemctl --no-pager --full status "${SERVICE_NAME}.service" || true

echo
echo "Done. 远端节点现在会每 ${INTERVAL_MINUTES} 分钟自动拉取 GitHub 最新代码并重启 ${NODE_SERVICE_NAME}。"
