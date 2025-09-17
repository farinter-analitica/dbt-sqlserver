#!/usr/bin/env python3
"""Compact test runner (parallel by default) for the Dagster monorepo.

Features kept:
  * Load locations from workspace.yaml + shared package
  * Parallel / sequential execution (--sequential)
  * Pass-through of -k / --tb / extra pytest args / -j workers / --verbose / --dry-run
  * Robust parsing of pytest textual output (passed/failed/skipped + collection errors)
  * Summary table + non-zero exit on failure/error
"""

import argparse
import concurrent.futures
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional
import yaml
from dotenv import load_dotenv
from tabulate import tabulate

load_dotenv()
ROOT = Path(__file__).parent.parent.resolve()
WORKSPACE = ROOT / "workspace.yaml"


@dataclass
class Result:
    loc: str
    status: str
    tests: int
    passed: int
    failed: int
    skipped: int
    dur: float
    err: Optional[str] = None
    raw: Optional[str] = None
    issues: List[str] = field(default_factory=list)
    targets: List[str] = field(default_factory=list)


def load_locations() -> List[tuple[str, str]]:  # (working_dir, executable)
    if not WORKSPACE.exists():
        return []
    try:
        data = yaml.safe_load(WORKSPACE.read_text()) or {}
    except Exception as e:  # pragma: no cover
        print(f"Error loading workspace.yaml: {e}")
        return []
    locs: List[tuple[str, str]] = []
    load_from = data.get("load_from", []) if isinstance(data, dict) else []
    for entry in load_from if isinstance(load_from, list) else []:
        if isinstance(entry, dict):
            pm = entry.get("python_module")
            if isinstance(pm, dict):
                wd = pm.get("working_directory")
                exe = pm.get("executable_path")
                if wd and exe:
                    locs.append((wd, exe))
    # always include shared
    locs.append(("dagster-shared-gf", "dagster-shared-gf/.venv/bin/python"))
    # de‑duplicate preserving order
    seen: set[str] = set()
    dedup: List[tuple[str, str]] = []
    for wd, exe in locs:
        if wd not in seen:
            dedup.append((wd, exe))
            seen.add(wd)
    return dedup


def build_cmd(wd: str, exe: str | None, extra: List[str], verbose: bool) -> List[str]:
    base = (
        [exe, "-m", "pytest"]
        if exe
        else ["uv", "run", "--no-sync", "-qq", "-m", "pytest"]
    )
    # Add a short summary for failed/errors so we can show concise info without full verbosity.
    # Respect user-provided -r if present in extra.
    add_report = not any(arg.startswith("-r") for arg in extra)
    default_report = ["-r", "fE"] if add_report else []
    return (
        base
        + [str(ROOT / wd), "--tb=short"]
        + (["-v"] if verbose else [])
        + default_report
        + extra
    )


def parse_output(
    returncode: int, stdout: str, stderr: str, dur: float, loc: str, verbose: bool
) -> Result:
    out = stdout + stderr
    passed = failed = skipped = 0
    error_count = 0
    lines = out.splitlines()
    # Collect concise failure/error lines so we can display a compact summary later
    issues: List[str] = []
    targets: List[str] = []
    for ln in lines:
        s = ln.strip()
        if "ERROR collecting" in s:
            error_count += 1
            if s not in issues:
                issues.append(s)
            # Example: "ERROR collecting path/to/test.py - ImportError: ..."
            parts = s.split()
            if len(parts) >= 3:
                target = parts[2]
                if target and target not in targets:
                    targets.append(target)
        if s.startswith("=") and any(
            w in s for w in ("passed", "failed", "skipped", "error")
        ):
            parts = s.replace("=", " ").replace(",", " ").split()
            for i, p in enumerate(parts):
                if p.isdigit() and i + 1 < len(parts):
                    tag = parts[i + 1].lower()
                    n = int(p)
                    if tag.startswith("passed"):
                        passed = n
                    elif tag.startswith("failed"):
                        failed = n
                    elif tag.startswith("skipped") or tag.startswith("deselected"):
                        skipped = n
                    elif tag.startswith("error"):
                        error_count = max(error_count, n)
        elif s.startswith(("PASSED", "FAILED", "SKIPPED", "ERROR")):
            # fallback incremental counting if needed
            if s.startswith("PASSED"):
                passed += 1
            elif s.startswith("FAILED"):
                failed += 1
                # Keep a brief record like: FAILED path::test - AssertionError: ...
                issues.append(s)
                toks = s.split()
                if len(toks) >= 2:
                    target = toks[1]
                    if target and target not in targets:
                        targets.append(target)
            elif s.startswith("ERROR"):
                error_count += 1
                # Keep the short summary line: ERROR path - Exception: ...
                issues.append(s)
                toks = s.split()
                if len(toks) >= 2:
                    target = toks[1]
                    if target and target not in targets:
                        targets.append(target)
            else:
                skipped += 1
    total = passed + failed + skipped
    if returncode == 0:
        status = "success"
    elif returncode == 1:
        status = "failure"
    elif returncode in (2,):
        status = "error"
    elif returncode == 5:
        status = "skipped" if total == 0 else "success"
    else:
        status = "error"
    if error_count:
        status = "error"
    err_msg = None
    if status == "error" and not error_count and returncode not in (0,):
        err_msg = f"Pytest exit {returncode}"
    if error_count:
        err_msg = f"{error_count} collection error(s)"
    # If we have a failure/error but didn't capture any concise issues, include a short tail of output
    if (status in ("failure", "error")) and not issues:
        tail = [line.strip() for line in lines[-30:] if line.strip()]
        issues = tail[-10:] if tail else []

    return Result(
        loc=loc,
        status=status,
        tests=total,
        passed=passed,
        failed=failed,
        skipped=skipped,
        dur=dur,
        err=err_msg,
        raw=out if verbose else None,
        issues=issues,
        targets=targets,
    )


def run_location(args, loc: tuple[str, str], extra: List[str]) -> Result:
    wd, exe_rel = loc
    exe_path = ROOT / exe_rel if exe_rel else None
    if exe_path and not exe_path.exists():
        return Result(wd, "error", 0, 0, 0, 0, 0.0, f"Executable not found: {exe_rel}")
    cmd = build_cmd(wd, str(exe_path) if exe_path else None, extra, args.verbose)
    start = time.time()
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
            cwd=str(ROOT),
            env=dict(os.environ, PYTHONUNBUFFERED="1"),
        )
        return parse_output(
            proc.returncode,
            proc.stdout,
            proc.stderr,
            time.time() - start,
            wd,
            args.verbose,
        )
    except subprocess.TimeoutExpired:
        return Result(wd, "error", 0, 0, 0, 0, 300.0, "Timeout (300s)")
    except Exception as e:  # pragma: no cover
        return Result(wd, "error", 0, 0, 0, 0, 0.0, str(e))


def execute(args, locations: List[tuple[str, str]], extra: List[str]) -> List[Result]:
    if not locations:
        return []
    if not args.sequential and len(locations) > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
            return list(pool.map(lambda loc: run_location(args, loc, extra), locations))
    return [run_location(args, loc, extra) for loc in locations]


def print_summary(results: List[Result]):
    if not results:
        print("No test results")
        return
    rows = []
    tot_tests = sum(r.tests for r in results)
    tot_pass = sum(r.passed for r in results)
    tot_fail = sum(r.failed for r in results)
    tot_skip = sum(r.skipped for r in results)
    tot_dur = sum(r.dur for r in results)
    for r in results:
        status_symbol = {
            "success": "✓",
            "failure": "✗",
            "error": "⚠",
            "skipped": "-",
        }.get(r.status, r.status)
        rows.append(
            [
                r.loc,
                f"{status_symbol} {r.status.upper()}",
                r.tests,
                r.passed,
                r.failed,
                r.skipped,
                f"{r.dur:.1f}s",
                r.err or "",
            ]
        )
    print("\n" + "=" * 100 + "\nTEST EXECUTION SUMMARY\n" + "=" * 100)
    print(
        tabulate(
            rows,
            headers=[
                "Location",
                "Status",
                "Tests",
                "Passed",
                "Failed",
                "Skipped",
                "Duration",
                "Error",
            ],
            tablefmt="grid",
            maxcolwidths=[20, 10, 8, 8, 8, 8, 10, 30],
        )
    )
    print("\n" + "-" * 100 + "\nOVERALL RESULTS\n" + "-" * 100)
    print(
        f"Total Locations: {len(results)}\nTotal Tests: {tot_tests}\nPassed: {tot_pass}\nFailed: {tot_fail}\nSkipped: {tot_skip}\nTotal Duration: {tot_dur:.1f}s\nAverage Duration: {tot_dur / len(results):.1f}s per location"
    )
    # Print concise failure/error summaries so it's clear where and what broke
    bad = [r for r in results if r.status in ("failure", "error")]
    if bad:
        print("\n" + "!" * 100 + "\nISSUES BY LOCATION\n" + "!" * 100)
        for r in bad:
            print(f"\n[{r.loc}] -> {r.status.upper()} ({r.err or 'see below'})")
            to_show = r.issues or []
            if not to_show and r.raw:
                to_show = [ln.strip() for ln in r.raw.splitlines() if ln.strip()][-10:]
            for line in to_show:
                print(f"  - {line}")
            # Provide a copy-pastable rerun command using uv for this location
            unique_targets = []
            seen = set()
            for t in r.targets:
                if t not in seen:
                    unique_targets.append(t)
                    seen.add(t)
            if unique_targets:
                # Make targets relative to the package dir, since we'll pass --directory <pkg>
                rel_targets: List[str] = []
                pkg_prefix = r.loc.rstrip("/") + "/"
                for t in unique_targets:
                    # Split nodeid into path and optional ::node part
                    if "::" in t:
                        path_part, node_part = t.split("::", 1)
                    else:
                        path_part, node_part = t, None
                    # Strip package prefix if present
                    if path_part.startswith(pkg_prefix):
                        rel_path = path_part[len(pkg_prefix) :]
                    else:
                        # Try absolute path relative_to <ROOT>/<pkg>
                        try:
                            p = Path(path_part)
                            rel_path = str(p.relative_to(ROOT / r.loc))
                        except Exception:
                            rel_path = path_part
                    rel_targets.append(
                        f"{rel_path}::{node_part}" if node_part else rel_path
                    )
                targets_str = " ".join(rel_targets)
                print(
                    f"  Re-run failed tests: uv run --directory {r.loc} pytest -q --maxfail=1 {targets_str}"
                )
    if any(r.status in ("failure", "error") for r in results):
        print("\n❌ Some locations had failures/errors")
        sys.exit(1)
    print("\n✅ All tests passed successfully!")


def main():
    p = argparse.ArgumentParser(
        description="Compact monorepo test runner (parallel by default)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:\n  %(prog)s -k automated              # Run tests matching pattern\n  %(prog)s --sequential              # Force sequential\n  %(prog)s --dry-run                 # Show what would run\n""",
    )
    p.add_argument("-k", "--keyword")
    p.add_argument(
        "--sequential", action="store_true", help="Run sequentially (default parallel)"
    )
    p.add_argument("-j", "--jobs", type=int, default=4, help="Parallel workers")
    p.add_argument("--tb", choices=["short", "line", "no"], default="short")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("pytest_args", nargs="*")
    args = p.parse_args()

    extra: List[str] = []
    if args.keyword:
        extra += ["-k", args.keyword]
    if args.tb != "short":
        extra += ["--tb", args.tb]
    extra += args.pytest_args

    locations = load_locations()
    if args.dry_run:
        print("DRY RUN locations:")
        for wd, exe in locations:
            print(f"  - {wd} (exe: {exe})")
        print("Extra pytest args:", " ".join(extra) or "<none>")
        return

    start = time.time()
    results = execute(args, locations, extra)
    print(f"Total execution time: {time.time() - start:.1f}s")
    print_summary(results)


if __name__ == "__main__":
    main()
