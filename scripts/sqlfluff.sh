#!/bin/bash

cd $DAGSTER_HOME
# Set PYTHONWARNINGS for this specific execution
export PYTHONWARNINGS="ignore"
# Execute the actual sqlfluff command, passing all arguments
# Make sure this path points to your actual sqlfluff executable in your venv
exec uv run --env-file .env --no-sync sqlfluff "$@"
