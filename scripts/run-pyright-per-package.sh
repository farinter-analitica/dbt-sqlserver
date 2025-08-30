#!/usr/bin/env bash
set -euo pipefail

# Run pyright using each package's .venv python. No installs performed.
# Usage:
#   ./scripts/run-precommit-per-package.sh [path1 path2 ...]
# If no paths provided, the script will search immediate subdirectories for pyproject.toml

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Behavior:
# - If the script receives file paths as arguments, group them by package (searching
#   upward for pyproject.toml) and invoke pyright for each package with only those files.
# - If no file args are provided, fall back to running pyright for each package (full project).

FILES=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --help|-h)
      echo "Usage: $0 [file1.py file2.py ...]"
      exit 0
      ;;
    *)
      FILES+=("$1")
      shift
      ;;
  esac
done

EXIT_CODE=0

find_package_root() {
  # Given an absolute path to a file, find nearest ancestor containing pyproject.toml
  local file="$1"
  local dir
  dir="$(cd "$(dirname "$file")" && pwd)"
  while [ "$dir" != "$ROOT_DIR" ] && [ "$dir" != "/" ]; do
    if [ -f "$dir/pyproject.toml" ]; then
      printf "%s" "$dir"
      return 0
    fi
    dir="$(dirname "$dir")"
  done
  # check root itself
  if [ -f "$ROOT_DIR/pyproject.toml" ]; then
    printf "%s" "$ROOT_DIR"
    return 0
  fi
  return 1
}

run_pyright_on_files() {
  local pkg_dir="$1"
  shift
  local files=("$@")
  venv_pyright_cmd="$pkg_dir/.venv/bin/pyright"
  venv_python="$pkg_dir/.venv/bin/python"

  if [ -x "$venv_pyright_cmd" ]; then
  echo "\n=== Running pyright (binary) for package: $pkg_dir on ${#files[@]} file(s) ==="
  # Try to include the package and the venv site-packages in PYTHONPATH so pyright
  # can resolve imports that live inside the package venv.
  site_pkgs="$($venv_python -c 'import sys,site
sp=[]
if hasattr(site,"getsitepackages"):
  try:
    sp=site.getsitepackages()
  except Exception:
    sp=[]
if not sp:
  sp=[p for p in sys.path if "site-packages" in p]
print(":".join(sp))' 2>/dev/null || true)"

  if ! (cd "$pkg_dir" && env PYTHONPATH="$pkg_dir${site_pkgs:+:$site_pkgs}${PYTHONPATH:+:$PYTHONPATH}" "$venv_pyright_cmd" --project "$pkg_dir" "${files[@]}"); then
    echo "pyright failed for $pkg_dir"
    EXIT_CODE=1
  else
    echo "pyright OK for $pkg_dir"
  fi
    return
  fi

  if [ ! -x "$venv_python" ]; then
    echo "[SKIP] no .venv python at $pkg_dir/.venv/bin/python"
    return
  fi

  # Check pyright module presence
  if ! "$venv_python" -c "import importlib, sys
try:
    importlib.import_module('pyright')
    sys.exit(0)
except Exception:
    sys.exit(1)"; then
    echo "[SKIP] pyright not installed in $pkg_dir/.venv; skipping"
    return
  fi

  echo "\n=== Running pyright (module) for package: $pkg_dir on ${#files[@]} file(s) ==="

  site_pkgs="$($venv_python -c 'import sys,site
sp=[]
if hasattr(site,"getsitepackages"):
    try:
        sp=site.getsitepackages()
    except Exception:
        sp=[]
if not sp:
    sp=[p for p in sys.path if "site-packages" in p]
print(":".join(sp))' 2>/dev/null || true)"

  if ! (cd "$pkg_dir" && env PYTHONPATH="$pkg_dir${site_pkgs:+:$site_pkgs}${PYTHONPATH:+:$PYTHONPATH}" "$venv_python" -m pyright --project "$pkg_dir" "${files[@]}"); then
    echo "pyright failed for $pkg_dir"
    EXIT_CODE=1
  else
    echo "pyright OK for $pkg_dir"
  fi
}

if [ ${#FILES[@]} -gt 0 ]; then
  # Resolve files to absolute paths and group by package root
  declare -A groups
  for f in "${FILES[@]}"; do
    # ignore non-python files
    case "$f" in
      *.py)
        abs="$(cd "$ROOT_DIR" && printf "%s" "$(realpath -- "$f")" 2>/dev/null || printf "%s" "$(realpath "$f")")"
        if [ ! -e "$abs" ]; then
          # path may be relative to git root or absolute; try as-is
          abs="$ROOT_DIR/$f"
        fi
        pkg_root="$(find_package_root "$abs" 2>/dev/null || true)"
        if [ -z "$pkg_root" ]; then
          echo "[SKIP] no package root found for $f"
          continue
        fi
        # Append with a real newline separator so multiple file paths don't get concatenated.
        # If there is no existing entry, set it directly to avoid a leading empty line.
        if [ -z "${groups[$pkg_root]:-}" ]; then
          groups["$pkg_root"]="$abs"
        else
          groups["$pkg_root"]+=$'\n'"$abs"
        fi
        ;;
      *)
        # skip non-py files
        ;;
    esac
  done

  for pkg in "${!groups[@]}"; do
    # Read the grouped paths (newline-separated) into an array safely
    mapfile -t files_arr <<< "${groups[$pkg]}"
    # Filter out any empty entries (possible trailing newline)
    filtered_files=()
    for f in "${files_arr[@]}"; do
      if [ -n "$f" ]; then
        filtered_files+=("$f")
      fi
    done
    if [ ${#filtered_files[@]} -gt 0 ]; then
      run_pyright_on_files "$pkg" "${filtered_files[@]}"
    fi
  done
else
  # No files passed: run full per-package check (existing behavior)
  while IFS= read -r -d $'\0' proj; do
    pkg_dir="$(dirname "$proj")"
    # skip repo root
    if [ "$(cd "$pkg_dir" && pwd)" = "$ROOT_DIR" ]; then
      continue
    fi
    run_pyright_on_files "$(cd "$pkg_dir" && pwd)"
  done < <(find "$ROOT_DIR" -maxdepth 2 -type f -name pyproject.toml -print0 | sort -z)
fi

exit $EXIT_CODE
