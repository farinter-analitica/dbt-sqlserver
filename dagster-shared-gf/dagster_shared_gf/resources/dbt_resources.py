from functools import cache
import json
import warnings
from pathlib import Path
from typing import Any, Mapping, Optional
import threading
import time
import os

# portalocker provides a simple cross-process file lock on Unix/Windows.
try:
    import portalocker
except (
    Exception
) as e:  # pragma: no cover - environment may not have portalocker installed
    raise ImportError(
        "portalocker is required for inter-process locking in dbt_resources. Install it."
    ) from e

# from ...dbt_kielsa
from dagster import AssetKey, AutomationCondition
from dagster._utils.tags import normalize_tags
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject

from dagster_shared_gf import shared_variables as shared_vars
from dagster_shared_gf.automation import (
    get_mapped_automation_condition,
)
from dagster_shared_gf.shared_functions import get_for_current_env
from dagster_shared_gf.config import get_dagster_config
from pydantic import Field

warnings.filterwarnings(
    "ignore", message=".*Pydantic V1 style `@validator` validators are deprecated..*"
)
env_str: str = shared_vars.env_str

cfg = get_dagster_config()
base_path = cfg.dagster_home
if not base_path:
    raise ValueError("dagster_home is not set in dagster config")

dbt_project_dir = Path(base_path).joinpath("dbt_dwh_farinter").resolve()
dbt_target = get_for_current_env({"dev": "dev", "prd": "prd"})
# resuelve el target dependiendo de la variable de ambiente
dbt_project = DbtProject(dbt_project_dir)


# print(os.fspath(dbt_project_dir))
# dbt_project_dir="/opt/main_dagster_dev/dbt_dwh_farinter"
class MyDbtCliResource(DbtCliResource):
    full_refresh: Optional[bool] = Field(
        default=False, description="Refresh full dbt models"
    )


@cache
def load_manifest(manifest_path) -> Mapping[str, Any]:
    """Load and parse a dbt manifest JSON file from disk.

    Accepts a Path or string. Cached to avoid repeated disk reads during the
    process lifetime.
    """
    p = Path(manifest_path)
    with p.open("r") as file:
        return json.load(file)


@cache
def get_dbt_resource() -> MyDbtCliResource:
    """Lazily construct and return the dbt CLI resource."""
    return MyDbtCliResource(
        project_dir=dbt_project.project_dir.as_posix(),
        profiles_dir=dbt_project.profiles_dir.as_posix(),
        target=dbt_target,
        state_path=None,
    )


# Intra-process lock to prevent multiple threads from attempting parse at once,
# and a file used with portalocker for inter-process coordination.
_manifest_init_lock = threading.Lock()
_manifest_lock_file = dbt_project.target_path.joinpath(".manifest.lock")


@cache
def get_dbt_manifest() -> Mapping[str, Any]:
    """Lazily resolve (and if needed, parse) the dbt manifest and return it as a dict.

    This avoids running `dbt parse` or reading the manifest at import time.
    """
    dbt_manifest_path = dbt_project.manifest_path

    # Whether parse-on-load is requested
    parse_on_load = get_dagster_config().dagster_dbt_parse_project_on_load == "1"

    # Cooldown interval (seconds) to avoid frequent parses when parse_on_load is enabled.
    min_interval = int(os.getenv("DAGSTER_DBT_PARSE_MIN_INTERVAL_SECONDS", "120"))

    # Fast path: manifest exists and no forced parse-on-load. Avoid locking.
    if not parse_on_load and dbt_manifest_path.exists():
        return load_manifest(dbt_manifest_path)

    # If parse_on_load is enabled but manifest exists and is recent, skip parse.
    if parse_on_load and dbt_manifest_path.exists():
        try:
            mtime = dbt_manifest_path.stat().st_mtime
            if (time.time() - mtime) < min_interval:
                return load_manifest(dbt_manifest_path)
        except Exception:
            # If stat fails, proceed to locking and parse attempt
            pass

    # Ensure only one thread in this process attempts to create/parse the manifest.
    with _manifest_init_lock:
        # Ensure target_path exists for the lock file
        dbt_project.target_path.mkdir(parents=True, exist_ok=True)

        # Use portalocker to coordinate between processes on the same host/filesystem.
        lock_path = str(_manifest_lock_file)
        # open/create the lock file and acquire an exclusive lock (blocking, with timeout)
        with portalocker.Lock(lock_path, mode="w", timeout=120):
            # Recompute manifest path after acquiring locks (double-check)
            dbt_manifest_path = dbt_project.manifest_path

            # Re-evaluate parse_on_load and recency inside the lock to avoid races
            parse_on_load = (
                get_dagster_config().dagster_dbt_parse_project_on_load == "1"
            )
            if dbt_manifest_path.exists():
                try:
                    mtime = dbt_manifest_path.stat().st_mtime
                    if not parse_on_load or (time.time() - mtime) < min_interval:
                        # Nothing to do: manifest is fresh enough or parse not requested
                        pass
                    else:
                        result = (
                            get_dbt_resource()
                            .cli(
                                ["--quiet", "parse"],
                                target_path=dbt_project.target_path,
                            )
                            .wait()
                        )
                        dbt_manifest_path = result.target_path.joinpath("manifest.json")
                except Exception:
                    # If stat fails, attempt parse
                    result = (
                        get_dbt_resource()
                        .cli(
                            ["--quiet", "parse"],
                            target_path=dbt_project.target_path,
                        )
                        .wait()
                    )
                    dbt_manifest_path = result.target_path.joinpath("manifest.json")
            else:
                # Manifest doesn't exist, must parse
                result = (
                    get_dbt_resource()
                    .cli(
                        ["--quiet", "parse"],
                        target_path=dbt_project.target_path,
                    )
                    .wait()
                )
                dbt_manifest_path = result.target_path.joinpath("manifest.json")

    return load_manifest(dbt_manifest_path)


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
