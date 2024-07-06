
import subprocess
from dagster_shared_gf.load_env_run import load_env_vars
load_env_vars([])

test_dirs = [f"""./dagster_kielsa_gf/dagster_kielsa_gf_tests"""
             , f"""./dagster_sap_gf/dagster_sap_gf_tests"""
             , f"""./dagster_shared_gf/dagster_shared_gf_tests"""]

#run
for test_dir in test_dirs:
    subprocess.run(["pytest", f"{test_dir}","--disable-warnings", "--disable-pytest-warnings"])
