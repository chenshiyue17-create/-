#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${1:-/opt/okx-local-app}"
SERVICE_NAME="${2:-okx-remote-node}"
RUN_USER="${SUDO_USER:-$USER}"
RUN_GROUP="$(id -gn "$RUN_USER")"

echo "[1/6] preparing app dir: $APP_DIR"
sudo mkdir -p "$APP_DIR"
sudo rsync -a --delete ./ "$APP_DIR/"
sudo chown -R "$RUN_USER":"$RUN_GROUP" "$APP_DIR"

echo "[2/6] preparing python venv"
if [[ ! -d "$APP_DIR/.venv" ]]; then
  sudo -u "$RUN_USER" python3 -m venv "$APP_DIR/.venv"
fi
sudo -u "$RUN_USER" "$APP_DIR/.venv/bin/python3" -m pip install --upgrade pip >/dev/null
sudo -u "$RUN_USER" "$APP_DIR/.venv/bin/python3" -m pip install -r "$APP_DIR/requirements-remote.txt"

echo "[3/6] installing systemd env"
if [[ ! -f /etc/okx-remote-node.env ]]; then
  sudo cp "$APP_DIR/deploy/systemd/okx-remote-node.env.example" /etc/okx-remote-node.env
fi

echo "[4/6] installing systemd service"
sudo sed \
  -e "s|^WorkingDirectory=.*|WorkingDirectory=$APP_DIR|" \
  -e "s|^ExecStart=.*|ExecStart=$APP_DIR/scripts/start-remote-node.sh|" \
  -e "s|^User=.*|User=$RUN_USER|" \
  -e "s|^Group=.*|Group=$RUN_GROUP|" \
  "$APP_DIR/deploy/systemd/okx-remote-node.service" | sudo tee "/etc/systemd/system/${SERVICE_NAME}.service" >/dev/null

echo "[5/6] reloading systemd"
sudo systemctl daemon-reload
sudo systemctl enable --now "$SERVICE_NAME"

echo "[6/6] status"
sudo systemctl --no-pager --full status "$SERVICE_NAME" || true

echo
echo "Done. Edit /etc/okx-remote-node.env to set OKX_DESK_GATEWAY_TOKEN and data path if needed."
echo "Then use:"
echo "  sudo systemctl restart $SERVICE_NAME"
