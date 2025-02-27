import subprocess
from dagster_shared_gf.load_env_run import load_env_vars
import os
from pathlib import Path
import warnings
import dagster
from tabulate import tabulate

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)
warnings.filterwarnings("ignore", message=".*shadows an attribute in parent.*")

load_env_vars()

paths = [Path(__file__).parent.resolve() / path for path in ["dagster_kielsa_gf", "dagster_sap_gf", "dagster_shared_gf"]]

results = []

for test_dir in paths:
    print(f"Running tests in {test_dir}")
    try:
        output = subprocess.check_output(["pytest", str(test_dir), "-W", "ignore", "--tb=short"], stderr=subprocess.STDOUT)
        output_lines = output.decode("utf-8").splitlines()
        test_results = [line for line in output_lines if line.startswith("==") and (" passed in " in line or " skipped in " in line)]
        if test_results:
            test_result = test_results[0]
            if " passed in " in test_result:
                num_passed = test_result.split(" passed in ")[0].split("==")[-1].strip()
                results.append([str(test_dir), "Success", num_passed, ""])
            elif " skipped in " in test_result:
                num_skipped = test_result.split(" skipped in ")[0].split("==")[-1].strip()
                results.append([str(test_dir), "Skipped", num_skipped, ""])
        else:
            results.append([str(test_dir), "Failure", "", "No test results found"])
    except subprocess.CalledProcessError as e:
        print(f"Error running pytest in {test_dir}: {e.output.decode('utf-8')}")
        failure_details = e.output.decode("utf-8")
        results.append([str(test_dir), "Failure", "", failure_details])

print("\nSummary of Results:")
print(tabulate(results, headers=["Test Directory", "Result", "Number of Tests", "Details"], tablefmt="grid"))