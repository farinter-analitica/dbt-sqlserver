"""Pytest configuration for dagster_shared_gf tests.

Responsabilidad: cargar valores dummy desde archivos de ejemplo para que las
imports de componentes compartidos que lean secretos/env no fallen en tests.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Mapping


import tomllib  # Python 3.11+


def _flatten(prefix, node):
    if isinstance(node, Mapping):
        for k, v in node.items():
            yield from _flatten([*prefix, str(k)], v)
    else:
        yield "__".join(p.upper() for p in prefix), str(node)


def _load_sample_secrets(root: Path) -> None:
    sample = root / ".dlt" / "secrets.toml.sample"
    if not sample.is_file():  # pragma: no cover
        return
    data = tomllib.loads(sample.read_text())
    for k, v in _flatten([], data):
        os.environ.setdefault(k, v)


_ENV_LINE_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$")


def _load_env_sample(root: Path) -> None:
    sample = root / ".env.sample"
    if not sample.is_file():  # pragma: no cover
        return
    for line in sample.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = _ENV_LINE_RE.match(line)
        if not m:
            continue
        key, val = m.groups()
        val = val.strip().strip("'").strip('"')
        if not val or val == "NOT-SET":
            continue
        os.environ.setdefault(key, val)


# --- local .venv injection --------------------------------------------------
# def _inject_local_venv() -> None:
#     tests_root = Path(__file__).resolve().parents[1]  # dagster-shared-gf
#     venv = tests_root / ".venv"
#     pyver = f"python{sys.version_info.major}.{sys.version_info.minor}"
#     candidates = [
#         venv / "bin",
#         venv / "lib" / pyver / "site-packages",
#         venv / "lib64" / pyver / "site-packages",
#     ]
#     for cand in candidates:
#         if cand.is_dir() and str(cand) not in sys.path:
#             site.addsitedir(str(cand))
#             if str(cand) in sys.path:
#                 sys.path.remove(str(cand))
#             sys.path.insert(0, str(cand))
#             break


def pytest_configure() -> None:  # pragma: no cover
    # _inject_local_venv()
    root = Path(__file__).resolve().parents[2]
    _load_sample_secrets(root)
    _load_env_sample(root)
