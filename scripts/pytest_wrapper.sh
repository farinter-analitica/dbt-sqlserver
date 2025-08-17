#!/usr/bin/env bash
# Wrapper that dispatches pytest calls to each location's .venv pytest so VS Code
# can use per-location virtualenvs while keeping a single-root workspace view.

set -euo pipefail

ROOT="$(pwd)"
LOCATIONS=(
  "dagster-shared-gf"
  "dagster-global-gf"
  "dagster-kielsa-gf"
  "dagster-sap-gf"
)

# If a specific path is provided as an arg, detect which location it belongs to and
# run only that location's pytest. Otherwise run pytest in each location.

ARGS=("$@")

# helper to run pytest in a location
run_in_loc() {
  local loc="$1"
  shift
  local pybin="$ROOT/$loc/.venv/bin/pytest"
  if [[ -x "$pybin" ]]; then
    echo "\n>>> Running pytest in $loc: $pybin $*"
    "$pybin" "$@"
    return $?
  else
    echo "(skip) no pytest in $loc/.venv"
    return 0
  fi
}

# If args include a path that starts with a known location, route to that one
for a in "${ARGS[@]}"; do
  for loc in "${LOCATIONS[@]}"; do
    if [[ "$a" == $loc/* ]] || [[ "$a" == ./$loc/* ]]; then
      # run only for that location
      run_in_loc "$loc" "${ARGS[@]}"
      exit $?
    fi
  done
done

# Default: run discovery/pytest on each location in sequence, aggregate exit code
EXIT_CODE=0
for loc in "${LOCATIONS[@]}"; do
  set +e
  run_in_loc "$loc" "${ARGS[@]}"
  code=$?
  set -e
  if [[ $code -ne 0 ]]; then
    EXIT_CODE=$code
  fi
done

exit $EXIT_CODE
