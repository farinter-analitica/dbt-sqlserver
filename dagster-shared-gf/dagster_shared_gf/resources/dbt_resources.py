import json
import os
import warnings
from pathlib import Path
from typing import Any, Mapping, Optional

# from ...dbt_kielsa
from dagster import AssetKey, AutomationCondition
from dagster._utils.tags import normalize_tags
from dagster_dbt import DagsterDbtTranslator, DbtCliResource

from dagster_shared_gf import shared_variables as shared_vars
from dagster_shared_gf.automation import (
    get_mapped_automation_condition,
)
from dagster_shared_gf.shared_functions import get_for_current_env
from pydantic import Field
import shutil
import sys

warnings.filterwarnings(
    "ignore", message=".*Pydantic V1 style `@validator` validators are deprecated..*"
)
env_str: str = shared_vars.env_str

base_path = os.environ.get("DAGSTER_HOME")

if not base_path:
    base_os_path = os.path.dirname(__file__)
    base_path = Path(base_os_path).joinpath("..", "..", "..").resolve()

dbt_project_dir = Path(base_path).joinpath("dbt_dwh_farinter").resolve()
dbt_target = get_for_current_env(
    {"dev": "dev", "prd": "prd"}
)  # resuelve el target dependiendo de la variable de ambiente


# print(os.fspath(dbt_project_dir))
# dbt_project_dir="/opt/main_dagster_dev/dbt_dwh_farinter"
class MyDbtCliResource(DbtCliResource):
    full_refresh: Optional[bool] = Field(
        default=False, description="Refresh full dbt models"
    )


# Find a dbt executable: prefer explicit env vars, then system `dbt`,
# and finally fall back to the current Python interpreter so pydantic
# validation during test collection doesn't fail when `dbt` isn't installed.
dbt_executable = os.getenv("DBT_EXECUTABLE") or shutil.which("dbt") or sys.executable

# Ensure we pass a string path (pydantic validation expects a valid executable path).
try:
    dbt_executable = os.fspath(dbt_executable)
except Exception:
    dbt_executable = str(dbt_executable)

dbt_resource = MyDbtCliResource(
    project_dir=os.fspath(dbt_project_dir),
    profiles_dir=os.fspath(dbt_project_dir),
    target=dbt_target,
    state_path=None,
    dbt_executable=dbt_executable,
)

dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")
# If DAGSTER_DBT_PARSE_PROJECT_ON_LOAD is set, a manifest will be created at runtime.
# Otherwise, we expect a manifest to be present in the project's target directory.
if (
    os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD") == 1
    or not dbt_manifest_path.exists()
):
    dbt_manifest_path = (
        dbt_resource.cli(
            ["--quiet", "parse"],
            target_path=Path("target"),
        )
        .wait()
        .target_path.joinpath("manifest.json")
    )


def load_manifest(manifest_path) -> Mapping[str, Any]:
    with open(manifest_path, "r") as file:
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
            return {
                **normalize_tags({tag: "" for tag in tags}),
                f"dbt_source_name/{dbt_resource_props['source_name']}": "",
            }
        tags = dbt_resource_props.get("tags", [])
        return (
            normalize_tags({tag: "" for tag in tags})
            if tags
            else super().get_tags(dbt_resource_props)
        )

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
            configured_database = dbt_resource_props.get("database")
            configured_schema = dbt_resource_props.get("schema")
            configured_name = dbt_resource_props["name"]
            if configured_schema is not None and configured_database is not None:
                components = [configured_database, configured_schema, configured_name]
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

    def get_automation_condition(
        self, dbt_resource_props
    ) -> Optional[AutomationCondition]:
        tags = self.get_tags(dbt_resource_props)
        all_automations = super().get_automation_condition(dbt_resource_props)
        mapped_automations = get_mapped_automation_condition(tags)
        if mapped_automations is not None:
            all_automations = (
                mapped_automations
                if all_automations is None
                else all_automations | mapped_automations
            )
        return all_automations
