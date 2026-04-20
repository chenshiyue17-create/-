#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/dist"
STAMP="$(date +%Y%m%d-%H%M%S)"
NAME="okx-remote-node-${STAMP}.tar.gz"

mkdir -p "$OUT_DIR"

ROOT_PY_FILES=()
while IFS= read -r -d '' file; do
  ROOT_PY_FILES+=("$(basename "$file")")
done < <(find "$ROOT_DIR" -maxdepth 1 -type f -name '*.py' -print0)

tar \
  --exclude '.git' \
  --exclude 'dist' \
  --exclude '__pycache__' \
  --exclude '.DS_Store' \
  -czf "${OUT_DIR}/${NAME}" \
  -C "$ROOT_DIR" \
  "${ROOT_PY_FILES[@]}" \
  static \
  scripts \
  deploy \
  requirements-remote.txt \
  REMOTE_EXECUTION.md

echo "${OUT_DIR}/${NAME}"
