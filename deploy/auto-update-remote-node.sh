#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${1:-${OKX_REMOTE_APP_DIR:-/opt/okx-local-app}}"
BRANCH="${OKX_REMOTE_BRANCH:-main}"
REMOTE_NAME="${OKX_REMOTE_GIT_REMOTE:-origin}"
SERVICE_NAME="${OKX_REMOTE_SERVICE_NAME:-okx-remote-node}"

if [[ ! -d "$APP_DIR/.git" ]]; then
  echo "未找到 $APP_DIR/.git，远端节点目录还没准备好。"
  exit 1
fi

cd "$APP_DIR"

CURRENT_HEAD="$(git rev-parse HEAD)"
git fetch "$REMOTE_NAME" "$BRANCH"
TARGET_HEAD="$(git rev-parse FETCH_HEAD)"

if [[ "$CURRENT_HEAD" == "$TARGET_HEAD" ]]; then
  echo "okx-remote-node 已是最新版本：$CURRENT_HEAD"
  exit 0
fi

git stash push -u -m "okx-auto-update-$(date +%Y%m%d-%H%M%S)" >/dev/null 2>&1 || true
git checkout -B "$BRANCH" "$REMOTE_NAME/$BRANCH"
git reset --hard "$REMOTE_NAME/$BRANCH"

systemctl restart "$SERVICE_NAME"
sleep 2
systemctl --no-pager --full status "$SERVICE_NAME" || true

