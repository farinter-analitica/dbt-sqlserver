"""Pytest configuration for dagster_global_gf tests.

Carga variables dummy desde archivos de ejemplo para evitar fallos de import
cuando módulos consultan secretos/vars al inicio. Patrón homogéneo con otras
code locations.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Iterable, Mapping


import tomllib  # Python 3.11+


def _flatten(prefix: Iterable[str], node) -> Iterable[tuple[str, str]]:
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


def _parse_env_value(raw: str) -> str:
    raw = raw.strip().strip("'").strip('"')
    return raw


def _load_sample_dotenv(root: Path) -> None:
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
        key, value = m.groups()
        value = _parse_env_value(value)
        if not value or value == "NOT-SET":
            continue
        os.environ.setdefault(key, value)


# --- local .venv injection --------------------------------------------------
# def _inject_local_venv() -> None:
#     tests_root = Path(__file__).resolve().parents[1]  # dagster-global-gf
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
    _load_sample_dotenv(root)
