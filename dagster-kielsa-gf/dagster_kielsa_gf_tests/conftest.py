"""Pytest configuration for dagster_kielsa_gf tests.

Instead of hardcoding fake secrets, we load the repo's sample files to
populate environment variables (acting as dummy values) so modules that
access `dlt.secrets[...]` at import time do not fail.

Files leveraged (only if present):
  .dlt/secrets.toml.sample  -> provides dlt secret structure
  .env.sample               -> provides general env vars (skips blank and 'NOT-SET')

The intent is to mirror real configuration keys without leaking real
credentials. In CI you should still inject secure values separately.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Iterable, Mapping

import sys
import site

import tomllib  # Python 3.11+


def _flatten(prefix: Iterable[str], node) -> Iterable[tuple[str, str]]:
    """Yield (ENV_KEY, value) pairs flattening a nested dict from toml.

    Converts path components to UPPERCASE joined by double underscores to
    match dlt's environment variable resolution strategy.
    """

    if isinstance(node, Mapping):
        for k, v in node.items():
            yield from _flatten([*prefix, str(k)], v)
    else:
        env_key = "__".join(part.upper() for part in prefix)
        yield env_key, str(node)


def _load_sample_secrets(root: Path) -> None:
    secrets_file = root / ".dlt" / "secrets.toml.sample"
    if not secrets_file.is_file():  # pragma: no cover - defensive
        return
    data = tomllib.loads(secrets_file.read_text())
    for k, v in _flatten([], data):
        # Only set if not already provided; avoid overriding real secrets.
        os.environ.setdefault(k, v)


_ENV_LINE_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$")


def _parse_env_value(raw: str) -> str:
    raw = raw.strip().strip("'").strip('"')
    return raw


def _load_sample_dotenv(root: Path) -> None:
    dotenv_file = root / ".env.sample"
    if not dotenv_file.is_file():  # pragma: no cover
        return
    for line in dotenv_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = _ENV_LINE_RE.match(line)
        if not m:
            continue
        key, value = m.groups()
        value = _parse_env_value(value)
        if not value or value == "NOT-SET":
            continue
        os.environ.setdefault(key, value)


# --- local .venv injection --------------------------------------------------
def _inject_local_venv() -> None:
    """Add this location's .venv site-packages to sys.path (if present).

    This does NOT change the Python interpreter, only the import path. It
    helps VS Code/pytest find packages installed inside the location's .venv
    while running tests from a global workspace interpreter.
    """
    tests_root = Path(__file__).resolve().parents[1]  # dagster-kielsa-gf
    venv = tests_root / ".venv"
    pyver = f"python{sys.version_info.major}.{sys.version_info.minor}"
    candidates = [
        venv / "lib" / pyver / "site-packages",
        venv / "lib64" / pyver / "site-packages",
    ]
    for cand in candidates:
        if cand.is_dir() and str(cand) not in sys.path:
            site.addsitedir(str(cand))
            if str(cand) in sys.path:
                sys.path.remove(str(cand))
            sys.path.insert(0, str(cand))
            break


def pytest_configure() -> None:  # pragma: no cover - side effects only
    _inject_local_venv()
    root = Path(__file__).resolve().parents[2]
    _load_sample_secrets(root)
    _load_sample_dotenv(root)
