import os
from pathlib import Path
import json
from typing import Mapping, Any
#from ...dbt_kielsa
from dagster import ExperimentalWarning, AssetKey
from dagster_dbt import DbtCliResource, DagsterDbtTranslator
from dagster_shared_gf import shared_variables as shared_vars
from dagster_shared_gf.shared_functions import get_for_current_env
import warnings
from dagster._core.definitions.utils import is_valid_definition_tag_key

warnings.filterwarnings("ignore", category=ExperimentalWarning)
env_str:str = shared_vars.env_str

base_os_path = os.path.dirname(__file__)
dbt_project_dir = Path(base_os_path).joinpath("..", "..", "..",  "dbt_dwh_farinter").resolve()
dbt_target = get_for_current_env({ "dev": "dev","prd": "prd" }) #resuelve el target dependiendo de la variable de ambiente
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

class MyDbtSourceTranslator(DagsterDbtTranslator):
    def get_tags(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, str]:
        """A function that takes a dictionary representing properties of a dbt resource, and
        returns the Dagster tags for that resource.

        Copy from dagster_dbt.DagsterDbtTranslator.get_tags modified
        """
        if dbt_resource_props["resource_type"] == "source":
            tags = dbt_resource_props.get("config", {}).get("tags", [])
            return {tag: "" for tag in tags if is_valid_definition_tag_key(tag)}

        return super().get_tags(dbt_resource_props)
    
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        """
        A function that takes a dictionary representing properties of a dbt resource, and
        returns the Dagster asset key for that resource.

        Note that a dbt resource is unrelated to Dagster's resource concept, and simply represents
        a model, seed, snapshot or source in a given dbt project. You can learn more about dbt
        resources and the properties available in this dictionary here:
        https://docs.getdbt.com/reference/artifacts/manifest-json#resource-details

        """
        if dbt_resource_props["resource_type"] in ["model", "source", "snapshot"]:
            configured_database = (
                dbt_resource_props.get("source_name")
                if dbt_resource_props["resource_type"] == "source"
                else dbt_resource_props.get("database")
            )
            configured_schema = dbt_resource_props.get("schema")
            configured_name = dbt_resource_props["name"]
            if configured_schema is not None and configured_database is not None:
                components = [configured_database , configured_schema , configured_name]
                return AssetKey(components)
            
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
        """
        This method can be overridden to provide a custom group name for a dbt resource.

        Args:
            dbt_resource_props (Mapping[str, Any]): A dictionary representing the dbt resource.

        Returns:
            Optional[str]: A Dagster group name.

        Examples:
            .. code-block:: python

                from typing import Any, Mapping

                from dagster_dbt import DagsterDbtTranslator


                class CustomDagsterDbtTranslator(DagsterDbtTranslator):
                    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
                        return "custom_group_prefix" + dbt_resource_props.get("config", {}).get("group")
        """
        group = super().get_group_name(dbt_resource_props)
        if group is not None and group != "default":
            return group
        return "dbt_default_group"
