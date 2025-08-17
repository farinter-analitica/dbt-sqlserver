#!/usr/bin/env python3
"""Check which parts of dagster_shared_gf are used by each code location
and whether the external packages those parts import are covered by
`dagster-shared-gf/pyproject.toml` (core dependencies or optional extras).

Outputs a compact JSON mapping suitable for CI or quick inspection.
"""

import ast
import json
import re
import sys
from pathlib import Path

try:
    import tomllib  # Python 3.11+
except Exception:
    tomllib = None


STD_LIB_COMMON = {
    # small conservative set of stdlib module names we expect to see
    "os",
    "sys",
    "re",
    "json",
    "typing",
    "pathlib",
    "datetime",
    "itertools",
    "functools",
    "logging",
    "collections",
    "io",
    "inspect",
    "time",
    "types",
    "hashlib",
    "subprocess",
    "threading",
    "concurrent",
    "math",
    "statistics",
}


def load_pyproject(path: Path):
    data = tomllib.loads(path.read_text()) if tomllib else {}
    proj = data.get("project", {})
    deps = proj.get("dependencies", []) or []
    opt = proj.get("optional-dependencies", {}) or {}
    return deps, opt


def top_level_name(modname: str) -> str:
    return modname.split(".")[0] if modname else ""


def extract_pkg_name(dep_str: str) -> str:
    # crude extraction: from start until first non-name/number/./-/_ character
    m = re.match(r"([A-Za-z0-9_\-\.]+)", dep_str)
    if not m:
        return dep_str
    name = m.group(1)
    # strip extras e.g. psycopg[binary,pool] -> psycopg
    name = re.split(r"[\[<>=!]", name)[0]
    return name


def find_shared_imports_in_file(path: Path):
    try:
        tree = ast.parse(path.read_text())
    except Exception:
        return set()
    used = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("dagster_shared_gf"):
                    used.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if mod.startswith("dagster_shared_gf"):
                used.add(mod)
    return used


def gather_shared_modules_for_location(location_dir: Path):
    mods = set()
    for p in location_dir.rglob("*.py"):
        mods |= find_shared_imports_in_file(p)
    return sorted(mods)


def resolve_shared_module_path(shared_root: Path, module: str) -> Path | None:
    # module like dagster_shared_gf.resources.smb_resources
    parts = module.split(".")
    if parts[0] != "dagster_shared_gf":
        return None
    rel = parts[1:]
    # Try file path
    if not rel:
        candidate = shared_root / "__init__.py"
        return candidate if candidate.exists() else None
    p = shared_root.joinpath(*rel)
    if p.with_suffix(".py").exists():
        return p.with_suffix(".py")
    if p.is_dir() and (p / "__init__.py").exists():
        return p / "__init__.py"
    # fallback: try progressively shorter
    for i in range(len(rel), 0, -1):
        sub = shared_root.joinpath(*rel[:i])
        if sub.with_suffix(".py").exists():
            return sub.with_suffix(".py")
        if sub.is_dir() and (sub / "__init__.py").exists():
            return sub / "__init__.py"
    return None


def external_imports_from_shared_file(path: Path):
    try:
        tree = ast.parse(path.read_text())
    except Exception:
        return set()
    exts = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = top_level_name(alias.name)
                if (
                    name
                    and name not in STD_LIB_COMMON
                    and not name.startswith("dagster_shared_gf")
                ):
                    exts.add(name)
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if mod and not mod.startswith("dagster_shared_gf"):
                name = top_level_name(mod)
                if name and name not in STD_LIB_COMMON:
                    exts.add(name)
    return exts


def analyse(shared_dir: Path, locations: list[Path]):
    pyproject = shared_dir / "pyproject.toml"
    if not pyproject.exists():
        print(f"ERROR: can't find {pyproject}", file=sys.stderr)
        sys.exit(2)
    deps_list, opt_deps = load_pyproject(pyproject)
    core_pkgs = {extract_pkg_name(d) for d in deps_list}
    extras_map = {}
    for extra, pkgs in opt_deps.items():
        for p in pkgs:
            extras_map.setdefault(extract_pkg_name(p), []).append(extra)

    out = {}
    for loc in locations:
        loc_path = Path(loc)
        if not loc_path.exists():
            out[str(loc)] = {"error": "location not found"}
            continue
        mods = gather_shared_modules_for_location(loc_path)
        required = {}
        for m in mods:
            spath = resolve_shared_module_path(shared_dir / "dagster_shared_gf", m)
            if not spath:
                # module might be a package-level reference; skip
                continue
            exts = external_imports_from_shared_file(spath)
            for e in exts:
                if e not in required:
                    required[e] = {"used_by": set(), "status": None}
                required[e]["used_by"].add(m)

        # finalize statuses
        for pkg, info in required.items():
            status = None
            if pkg in core_pkgs:
                status = "core"
            elif pkg in extras_map:
                status = "extra"
            else:
                status = "missing"
            required[pkg]["status"] = status
            required[pkg]["extras"] = sorted(extras_map.get(pkg, []))
            required[pkg]["used_by"] = sorted(required[pkg]["used_by"])

        out[str(loc)] = {
            "shared_modules_imported": mods,
            "external_packages_required": required,
            "extras_suggested": sorted(
                {
                    ex
                    for pkg, info in required.items()
                    for ex in info["extras"]
                    if info["status"] == "extra"
                }
            ),
            "missing_packages": sorted(
                [pkg for pkg, info in required.items() if info["status"] == "missing"]
            ),
        }

    return out


def main(argv):
    import argparse

    p = argparse.ArgumentParser(
        description="Analyse dagster_shared_gf usage by code locations"
    )
    p.add_argument(
        "--shared", default="dagster-shared-gf", help="path to dagster-shared-gf root"
    )
    p.add_argument(
        "locations",
        nargs="*",
        default=["dagster-global-gf", "dagster-kielsa-gf", "dagster-sap-gf"],
        help="code locations to analyse (folders)",
    )
    args = p.parse_args(argv)
    shared = Path(args.shared)
    res = analyse(shared, [Path(x) for x in args.locations])
    print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main(sys.argv[1:])
