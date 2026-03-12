#!/bin/bash
set -euo pipefail

if [ ! -f test.env ]; then
	cp test.env.sample test.env
fi
export UV_VENV_CLEAR=1
uv venv .venv
uv pip install --python .venv/bin/python -r dev_requirements.txt
.venv/bin/pre-commit install
