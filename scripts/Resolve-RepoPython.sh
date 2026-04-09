#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

declare -a _python_candidates=()

if [[ -n "${VIRTUAL_ENV:-}" && -x "${VIRTUAL_ENV}/bin/python" ]]; then
  _python_candidates+=("${VIRTUAL_ENV}/bin/python")
fi

_python_candidates+=(
  "${REPO_ROOT}/env/bin/python"
  "${REPO_ROOT}/.venv/bin/python"
  "${REPO_ROOT}/venv/bin/python"
)

PYTHON_PATH=""
for candidate in "${_python_candidates[@]}"; do
  if [[ -x "${candidate}" ]]; then
    PYTHON_PATH="${candidate}"
    break
  fi
done

if [[ -z "${PYTHON_PATH}" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_PATH="$(command -v python3)"
  else
    echo "No se encontro Python para el proyecto." >&2
    echo "Crea ./env con Python 3.14 e instala las dependencias antes de ejecutar este script." >&2
    return 1 2>/dev/null || exit 1
  fi
fi

export REPO_ROOT
export PYTHON_PATH
