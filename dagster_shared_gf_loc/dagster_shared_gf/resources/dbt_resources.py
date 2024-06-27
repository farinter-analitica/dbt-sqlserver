import os
from pathlib import Path
import json
from typing import Mapping, Any
#from ...dbt_kielsa
from dagster import ExperimentalWarning
from dagster_dbt import DbtCliResource
import warnings
warnings.filterwarnings("ignore", category=ExperimentalWarning)

base_os_path = os.path.dirname(__file__)
dbt_project_dir = Path(base_os_path).joinpath("..", "..", "..",  "dbt_dwh_farinter").resolve()
dbt_target = "dev"
if os.environ.get("CURRENT_ENV", "dev")=="PRD":
    dbt_target = "prd"
#print(os.fspath(dbt_project_dir))
#dbt_project_dir="/opt/main_dagster_dev/dbt_dwh_farinter"
dbt_resource = DbtCliResource(project_dir=os.fspath(dbt_project_dir), profiles_dir=os.fspath(dbt_project_dir), target=dbt_target #, dbt_executable="/opt/main_dagster_dev/.venv_main_dagster/bin/dbt"
                              )

# If DAGSTER_DBT_PARSE_PROJECT_ON_LOAD is set, a manifest will be created at runtime.
# Otherwise, we expect a manifest to be present in the project's target directory.
if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD")==1:
    dbt_manifest_path = (
        dbt_resource.cli(
            ["--quiet", "parse"],
            target_path=Path("target"),
        )
        .wait()
        .target_path.joinpath("manifest.json")
    )
else:
    dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")


def load_manifest(manifest_path) -> Mapping[str, Any]:
    with open(manifest_path, 'r') as file:
        return json.load(file)

# Load the manifest from the path
dbt_manifest = load_manifest(dbt_manifest_path)
