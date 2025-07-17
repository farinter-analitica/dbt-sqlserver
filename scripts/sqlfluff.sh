#!/bin/bash

# Set PYTHONWARNINGS for this specific execution
source .env
export PYTHONWARNINGS="ignore"
# Execute the actual sqlfluff command, passing all arguments
# Make sure this path points to your actual sqlfluff executable in your venv
sqlfluff "$@"
