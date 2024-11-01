import subprocess
from dagster_shared_gf.load_env_run import load_env_vars
import os
from pathlib import Path
import warnings
import dagster
from tabulate import tabulate

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

load_env_vars()

paths: list[Path] =[os.path.join(Path(__file__).parent.resolve(), path) for path in ["dagster_kielsa_gf", "dagster_sap_gf", "dagster_shared_gf"]]

results = []

for test_dir in paths:
    print(test_dir)
    try:
        output = subprocess.check_output(["pytest", f"{test_dir}", "-W", "ignore"], stderr=subprocess.STDOUT)
        output_lines = output.decode("utf-8").splitlines()
        test_results = [line for line in output_lines if line.startswith("==") and " passed in " in line]
        if test_results:
            test_result = test_results[0]
            num_passed = test_result.split(" passed in ")[0].split("==")[-1].strip()
            results.append([test_dir, "Success", num_passed])
        else:
            results.append([test_dir, "Failure", ""])
    except subprocess.CalledProcessError as e:
        print(f"Error running pytest in {test_dir}: {e.output.decode('utf-8')}")
        results.append([test_dir, "Failure", e.output.decode("utf-8")])

print("\nSummary of Results:")
print(tabulate(results, headers=["Test Directory", "Result", "Number of Tests Passed"], tablefmt="grid"))