#!/bin/bash

WORKSPACE_ROOT="$(dirname "$(dirname "$0")")"
# Set PYTHONWARNINGS for this specific execution
if [ -f "$WORKSPACE_ROOT/.env" ]; then
    source "$WORKSPACE_ROOT/.env"
else
    echo "Warning: .env file not found in workspace root ($WORKSPACE_ROOT). Skipping environment variable loading."
fi
export PYTHONWARNINGS="ignore"
# Execute the actual sqlfluff command, passing all arguments
# Make sure this path points to your actual sqlfluff executable in your venv
sqlfluff "$@"
