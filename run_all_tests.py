
import subprocess
test_dirs = [f"dagster_kielsa_loc\dagster_kielsa_tests"
             , f"dagster_sap_loc\dagster_sap_tests"
             , f"dagster_shared_gf_loc\dagster_shared_gf_tests"]

#run
for test_dir in test_dirs:
    subprocess.run(["pytest", f"{test_dir}"])
