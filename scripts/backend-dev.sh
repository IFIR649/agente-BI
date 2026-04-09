#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/Resolve-RepoPython.sh"

cd "${REPO_ROOT}"
echo "backend-dev.sh es solo para desarrollo local. Usa --reload y no debe emplearse como servidor normal." >&2
echo "Para servidor usa bash scripts/backend-prod.sh --host 0.0.0.0 --port 8000" >&2
exec "${PYTHON_PATH}" -m uvicorn backend.app.main:app --reload "$@"
