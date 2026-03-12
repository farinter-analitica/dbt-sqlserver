.DEFAULT_GOAL:=help
THREADS ?= auto
VENV ?= .venv
PYTHON ?= $(VENV)/bin/python
PRE_COMMIT ?= $(VENV)/bin/pre-commit
PYTEST ?= $(VENV)/bin/pytest

.PHONY: dev
dev: ## Installs adapter in develop mode along with development dependencies
	@\
	uv venv $(VENV) && uv pip install --python $(PYTHON) -r dev_requirements.txt && $(PRE_COMMIT) install

.PHONY: mypy
mypy: ## Runs mypy against staged changes for static type checking.
	@\
	$(PRE_COMMIT) run --hook-stage manual mypy-check | grep -v "INFO"

.PHONY: flake8
flake8: ## Runs flake8 against staged changes to enforce style guide.
	@\
	$(PRE_COMMIT) run --hook-stage manual flake8-check | grep -v "INFO"

.PHONY: black
black: ## Runs black  against staged changes to enforce style guide.
	@\
	$(PRE_COMMIT) run --hook-stage manual black-check -v | grep -v "INFO"

.PHONY: lint
lint: ## Runs flake8 and mypy code checks against staged changes.
	@\
	$(PRE_COMMIT) run flake8-check --hook-stage manual | grep -v "INFO"; \
	$(PRE_COMMIT) run mypy-check --hook-stage manual | grep -v "INFO"

.PHONY: all
all: ## Runs all checks against staged changes.
	@\
	$(PRE_COMMIT) run -a

.PHONY: linecheck
linecheck: ## Checks for all Python lines 100 characters or more
	@\
	find dbt -type f -name "*.py" -exec grep -I -r -n '.\{100\}' {} \;

.PHONY: unit
unit: ## Runs unit tests.
	@\
	$(PYTEST) -n auto -ra -v tests/unit

.PHONY: functional
functional: ## Runs functional tests.
	@\
	$(PYTEST) -n $(THREADS) -ra -v tests/functional

.PHONY: test
test: ## Runs unit tests and code checks against staged changes.
	@\
	$(PYTEST) -n auto -ra -v tests/unit; \
	$(PRE_COMMIT) run black-check --hook-stage manual | grep -v "INFO"; \
	$(PRE_COMMIT) run flake8-check --hook-stage manual | grep -v "INFO"; \
	$(PRE_COMMIT) run mypy-check --hook-stage manual | grep -v "INFO"

.PHONY: server
server: ## Spins up a local MS SQL Server instance for development. Docker-compose is required.
	@\
	docker compose up -d

.PHONY: clean
	@echo "cleaning repo"
	@git clean -f -X

.PHONY: help
help: ## Show this help message.
	@echo 'usage: make [target]'
	@echo
	@echo 'targets:'
	@grep -E '^[7+a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
