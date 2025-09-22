#!/bin/bash

cd $DAGSTER_HOME
# Set PYTHONWARNINGS for this specific execution
if [ -f ".env" ]; then
    source ".env"
else
    echo "Warning: .env file not found in workspace root ($WORKSPACE_ROOT). Skipping environment variable loading."
fi
export PYTHONWARNINGS="ignore"
# Execute the actual sqlfluff command, passing all arguments
# Make sure this path points to your actual sqlfluff executable in your venv
exec uv run --no-sync sqlfluff "$@"
