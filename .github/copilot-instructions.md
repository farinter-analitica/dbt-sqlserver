# Copilot instructions — dbt-sqlserver

Purpose
- Quickly orient AI coding agents to the structure, conventions, and workflows required to make safe, useful changes to the dbt-sqlserver adapter.
- Focuses on concrete, discoverable facts: major entry points, test/run commands, files to edit for common changes, and CI/development quirks.

Branch and patching purpose
- This repository comes from a fork of upstream dbt-sqlserver
- This file lives in the `patched` branch and documents the current state of the codebase
- All new feature and bugfix branches should be created off `master` and merged into `patched` for internal production ready code.

Big picture (keep in mind)
- This repository implements a dbt adapter for Microsoft SQL Server / Azure SQL. Core adapter code is under `dbt/adapters/sqlserver/` and is built on top of `dbt.adapters.fabric` (`FabricAdapter`).
- Key responsibilities:
  - Connection & auth: `sqlserver_connections.py` (pyodbc + AAD support; extends FabricConnectionManager)
  - Credentials: `sqlserver_credentials.py` (dataclass, inherits FabricCredentials)
  - Adapter surface: `sqlserver_adapter.py` (exports `ConnectionManager`, `Column`, `Relation`, constraint rendering, date function, and incremental strategies)
  - Relation policies and limits: `relation_configs/policies.py` and `sqlserver_relation.py` (identifier length enforcement — max 127 chars)
  - SQL helper macros & materializations: `dbt/include/sqlserver/macros/…` and `dbt/include/sqlserver/materializations/…` (use these as canonical SQL patterns)

Concrete code patterns & examples
- Adapter subclass pattern: `SQLServerAdapter(FabricAdapter)` sets class attributes to concrete implementations. To add new surface behavior, add/override methods in `sqlserver_adapter.py` and supply new classes for `ConnectionManager`, `Column`, or `Relation`.
- Connection/auth extension: `sqlserver_connections.py` merges Fabric's `AZURE_AUTH_FUNCTIONS` and adds `"serviceprincipal"` and `"msi"` keys. To add new auth types, provide a function that returns an `azure.core.credentials.AccessToken` and register it in `AZURE_AUTH_FUNCTIONS`.
- Constraints: `render_model_constraint` produces SQL like `add constraint {name} unique nonclustered(col1,col2)` — note the adapter intentionally uses nonclustered constraints.
- Limits and pagination: `SQLServerRelation.render_limited()` uses SQL Server `TOP` and special aliasing for `limit == 0`.
- Versioning: adapter version is declared in `dbt/adapters/sqlserver/__version__.py` — `sqlserver_connections` uses it to set `APP` in the connection string.

Tests & CI (how to run locally and what CI does)
- Install dev/test deps: `pip install -r dev_requirements.txt` (CI runs this step).
- Unit tests: `make unit` or `pytest -n auto -ra -v tests/unit`.
- Functional tests: `make functional` or:
  - Example: `pytest -ra -v tests/functional --profile "ci_sql_server"`
  - Functional tests select dbt profile via `--profile` (see `tests/conftest.py` for profiles: `ci_sql_server`, `ci_azure_*`, `user`, `user_azure`)
  - CI sets environment variables such as `DBT_TEST_USER_1`, `SQLSERVER_TEST_DRIVER` (see `.github/workflows/integration-tests-sqlserver.yml`).
- Local SQL Server for functional testing: `make server` (uses `docker-compose.yml` and `devops/server.Dockerfile`) or `docker compose up -d`.
- CI images and behavior: workflows build and use GHCR images named like `ghcr.io/${{ github.repository }}:CI-<py>-msodbc18` and `server-<version>`; updating CI container logic requires editing `devops/*` and `.github/workflows/*`.

Development environment & onboarding
- Recommended Python version for development: Python 3.10 (the project examples use 3.10.7). A common bootstrapping flow uses `pyenv`:
  - `pyenv install 3.10.7`
  - `pyenv virtualenv 3.10.7 dbt-sqlserver`
  - `pyenv activate dbt-sqlserver`
- Install dev dependencies and enable pre-commit with `make dev`; run `make help` to list Makefile targets (e.g. `make unit`, `make functional`, `make server`).
- A devcontainer is provided (since v1.7.2) for a consistent development environment — look for `.devcontainer` in the repo when onboarding.

Test environment file
- Functional tests expect a `test.env` file in the repo root. Create it from the sample: `cp test.env.sample test.env`.
- Functional tests require three test users for grants-related tests. Ensure these environment variables are set in `test.env` or your environment:
  - `DBT_TEST_USER_1`
  - `DBT_TEST_USER_2`
  - `DBT_TEST_USER_3`

Releasing a new version
- To cut a release: bump the version in `dbt/adapters/sqlserver/__version__.py`, create a git tag named `v<version>`, and push it to GitHub. The `release-version` workflow will publish the package to PyPI.

Project conventions and gotchas
- Many classes use Python dataclasses (credentials, relation policies) — follow that style for new small value types.
- String length limit: identifiers are limited to 127 chars (`MAX_CHARACTERS_IN_IDENTIFIER` in `relation_configs/policies.py`). Tests and runtime enforce this in `sqlserver_relation.py`.
- SQL is generated with explicit SQL Server semantics (e.g., uses `TOP`, nonclustered constraints, `CAST(... as datetimeoffset)` for time filters). Prefer existing macros in `dbt/include/sqlserver/macros` when creating SQL.
- Authentication modes: `authentication` in credentials can be `sql`, `serviceprincipal`, `msi` or Fabric-provided options (see `sqlserver_connections.py` and tests). When adding auth modes, add tests under `tests/functional` and `tests/unit/adapters/mssql`.
- Logging/debug: connection-level logs use logger name `sqlserver` (see `sqlserver_connections.py`). Use these logs when debugging connection/auth issues.

Where to change common features
- To change connection behavior or add a new driver tweak: edit `dbt/adapters/sqlserver/sqlserver_connections.py` and corresponding unit tests in `tests/unit/adapters/mssql`.
- To add adapter-level behavior: edit `dbt/adapters/sqlserver/sqlserver_adapter.py` and add tests under `tests/functional/adapter/dbt`.
- To expose or change macros/materializations: update files under `dbt/include/sqlserver/macro*` and `materializations/*`.

Quick checklist for a typical change
- Add/modify implementation in `dbt/adapters/sqlserver/*`.
- Add unit tests in `tests/unit` and functional tests in `tests/functional` (pick proper profile).
- Run `make dev` to install test deps and enable pre-commit, run `make unit` and `make functional` locally where possible.
- If the change impacts connection images or CI, update `devops/Dockerfile*`, `docker-compose.yml`, and `.github/workflows/*` accordingly.

If anything here is unclear or you need more detail on specific areas (auth flows, CI images, macros), find files or behavior you want expanded and iterate.