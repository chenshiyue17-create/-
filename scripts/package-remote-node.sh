#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/dist"
STAMP="$(date +%Y%m%d-%H%M%S)"
NAME="okx-remote-node-${STAMP}.tar.gz"

mkdir -p "$OUT_DIR"

tar \
  --exclude '.git' \
  --exclude 'dist' \
  --exclude '__pycache__' \
  --exclude '.DS_Store' \
  -czf "${OUT_DIR}/${NAME}" \
  -C "$ROOT_DIR" \
  server.py \
  static \
  scripts \
  deploy \
  requirements-remote.txt \
  REMOTE_EXECUTION.md

echo "${OUT_DIR}/${NAME}"
