import subprocess
import sys
from pathlib import Path
import warnings
from tabulate import tabulate
import yaml
from dotenv import load_dotenv as load_env_vars

warnings.filterwarnings("ignore", message=".*shadows an attribute in parent.*")

load_env_vars()

ROOT = Path(__file__).parent.parent.resolve()
WORKSPACE_YAML = ROOT / "workspace.yaml"


def load_locations_from_workspace(yaml_path: Path):
    data = yaml.safe_load(yaml_path.read_text())
    locations = []
    for entry in data.get("load_from", []):
        pm = entry.get("python_module") or {}
        wd = pm.get("working_directory")
        exe = pm.get("executable_path")
        if wd:
            locations.append({"working_directory": wd, "executable_path": exe})
    locations.append(
        {
            "working_directory": "dagster-shared-gf",
            "executable_path": "dagster-shared-gf/.venv/bin/python",
        }
    )
    return locations


# Allow passing extra args to pytest. If --help is provided, show pytest help and exit.
extra_args = sys.argv[1:]
if "--help" in extra_args:
    cmd = ["uv", "run", "--no-sync", "-qq", "-m", "pytest", "--help"]
    output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    print(output.decode("utf-8"))
    sys.exit(0)

locations = load_locations_from_workspace(WORKSPACE_YAML)

results = []

for loc in locations:
    test_dir = ROOT / loc["working_directory"]
    exe = loc.get("executable_path")
    print(f"Running tests in {test_dir}")
    try:
        exe_path = ROOT / exe
        if exe and exe_path.exists() and exe_path.is_file():
            # Use the target python executable to run pytest.
            cmd = [
                str(exe_path),
                "-m",
                "pytest",
                str(test_dir),
                "-W",
                "ignore",
                "--tb=short",
            ] + extra_args
        else:
            raise FileNotFoundError(f"Executable not found: {exe_path}")

        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        output_lines = output.decode("utf-8").splitlines()
        test_results = [
            line
            for line in output_lines
            if line.startswith("==")
            and (" passed in " in line or " skipped in " in line)
        ]
        if test_results:
            test_result = test_results[0]
            if " passed in " in test_result:
                num_passed = test_result.split(" passed in ")[0].split("==")[-1].strip()
                results.append([str(test_dir), "Success", num_passed, ""])
            elif " skipped in " in test_result:
                num_skipped = (
                    test_result.split(" skipped in ")[0].split("==")[-1].strip()
                )
                results.append([str(test_dir), "Skipped", num_skipped, ""])
        else:
            results.append([str(test_dir), "Failure", "", "No test results found"])
    except subprocess.CalledProcessError as e:
        failure_details = (
            e.output.decode("utf-8", errors="replace") if e.output else str(e)
        )
        print(f"Error running pytest in {test_dir}: {failure_details}")
        results.append([str(test_dir), "Failure", "", failure_details])

print("\nSummary of Results:")
print(
    tabulate(
        results,
        headers=["Test Directory", "Result", "Number of Tests", "Details"],
        tablefmt="grid",
    )
)
