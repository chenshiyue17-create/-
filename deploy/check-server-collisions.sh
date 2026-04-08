#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-18765}"
SERVICE_NAME="${2:-okx-remote-node}"

echo "== Port check =="
if command -v ss >/dev/null 2>&1; then
  ss -ltnp | (grep -E ":${PORT}\\b" || true)
elif command -v lsof >/dev/null 2>&1; then
  lsof -nP -iTCP:"$PORT" -sTCP:LISTEN || true
else
  echo "Neither ss nor lsof is available."
fi

echo
echo "== systemd service name check =="
systemctl list-unit-files | (grep -E "^${SERVICE_NAME}\\.service\\b" || true)

echo
echo "== Reverse proxy check =="
systemctl list-unit-files | grep -E '^(nginx|caddy)\\.service' || true

echo
echo "== Python check =="
command -v python3 || true
python3 --version || true

echo
echo "== Disk/memory =="
df -h / || true
free -h 2>/dev/null || vm_stat 2>/dev/null || true
