#!/usr/bin/env bash
set -euo pipefail

# run from repo root so pytest finds pytest.ini/pyproject.toml config
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCATIONS=(dagster-global-gf dagster-kielsa-gf dagster-sap-gf)
FAILED=0

for loc in "${LOCATIONS[@]}"; do
    echo "============================================"
    echo "Running tests for $loc"
    VENV="$loc/.venv/bin/python"
    if [[ -x "$VENV" ]]; then
        echo "Using $VENV"
        (cd "$ROOT_DIR" && "$VENV" -m pytest "$loc") || FAILED=$?
    else
        echo "No venv python at $VENV — falling back to host pytest (may fail)"
        (cd "$ROOT_DIR" && pytest "$loc") || FAILED=$?
    fi
    echo "Finished $loc (exit $FAILED)"
done

if [[ $FAILED -ne 0 ]]; then
    echo "One or more locations failed (last exit $FAILED)"
    exit $FAILED
fi

echo "All location tests passed"
