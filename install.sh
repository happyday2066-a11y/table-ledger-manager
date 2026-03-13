#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: ./install.sh <openclaw-skills-root> [python-bin]"
  exit 1
fi

TARGET_ROOT="$1"
PYTHON_BIN="${2:-${PYTHON_BIN:-python3}}"
CREATE_VENV="${CREATE_VENV:-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL_NAME="table-ledger-manager"
DESTINATION="${TARGET_ROOT%/}/${SKILL_NAME}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
TARGET_PARENT="$(cd "${TARGET_ROOT%/}/.." && pwd)"
BACKUP_ROOT="${TARGET_PARENT}/.backup/skills"

mkdir -p "$TARGET_ROOT"
mkdir -p "$BACKUP_ROOT"
STAGING_DIR="$(mktemp -d "${TARGET_ROOT%/}/.${SKILL_NAME}.staging.${TIMESTAMP}.XXXXXX")"
BACKUP_PATH=""

cleanup() {
  rm -rf "$STAGING_DIR"
}
trap cleanup EXIT

echo "[install] building clean skill tree into staging: $STAGING_DIR"
"$PYTHON_BIN" "$SCRIPT_DIR/scripts/build_package.py" --format dir --output "$STAGING_DIR/$SKILL_NAME" >/dev/null
STAGED_SKILL="$STAGING_DIR/$SKILL_NAME"

if [[ "$CREATE_VENV" == "1" ]]; then
  echo "[install] creating virtualenv and installing dependencies"
  "$PYTHON_BIN" -m venv "$STAGED_SKILL/.venv"
  "$STAGED_SKILL/.venv/bin/pip" install -r "$STAGED_SKILL/requirements.txt" >/dev/null
  RUN_PYTHON="$STAGED_SKILL/.venv/bin/python"
else
  RUN_PYTHON="$PYTHON_BIN"
fi

echo "[install] running health checks in staging"
"$RUN_PYTHON" "$STAGED_SKILL/scripts/init_db.py" >/dev/null
"$RUN_PYTHON" "$STAGED_SKILL/scripts/query_records.py" --ledger default --count >/dev/null

if [[ -d "$DESTINATION" ]]; then
  BACKUP_PATH="${BACKUP_ROOT}/${SKILL_NAME}.${TIMESTAMP}"
  echo "[install] backing up existing version -> $BACKUP_PATH"
  mv "$DESTINATION" "$BACKUP_PATH"
fi

if compgen -G "${TARGET_ROOT%/}/${SKILL_NAME}.bak.*" >/dev/null; then
  LEGACY_DIR="${BACKUP_ROOT}/legacy-from-skills-${TIMESTAMP}"
  mkdir -p "$LEGACY_DIR"
  for path in "${TARGET_ROOT%/}/${SKILL_NAME}".bak.*; do
    [[ -e "$path" ]] || continue
    mv "$path" "$LEGACY_DIR/"
  done
  echo "[install] moved legacy backup dirs out of skills root -> $LEGACY_DIR"
fi

echo "[install] activating staged version -> $DESTINATION"
mv "$STAGED_SKILL" "$DESTINATION"

if [[ -n "$BACKUP_PATH" ]]; then
  echo "[install] previous version backup: $BACKUP_PATH"
fi
echo "[install] installed ${SKILL_NAME} to ${DESTINATION}"
