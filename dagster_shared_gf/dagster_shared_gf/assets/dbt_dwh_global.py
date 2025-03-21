import json
from collections import deque
from typing import Sequence

from dagster import (
    AssetChecksDefinition,
    AssetExecutionContext,
    Config,
    load_asset_checks_from_current_module,
)
from dagster_dbt import DbtCliResource, dbt_assets
from pydantic import Field

from dagster_shared_gf.resources.dbt_resources import (
    MyDbtSourceTranslator,
    dbt_manifest,
)
from dagster_shared_gf.shared_variables import tags_repo


class MyDbtConfig(Config):
    full_refresh: bool = Field(default=False, description="Refresh full dbt models")


MAIN_SELECT_STR = "tag:dagster_global_gf/dbt"


# Common function to handle dbt run arguments
def get_dbt_run_args(context, config, dbt_resource, date_from=None, date_to=None):
    dbt_run_args: deque[str] = deque(("build",))

    # Handle full refresh
    if config.full_refresh:
        dbt_run_args.append("--full-refresh")
    elif dbt_resource.model_dump(include={"full_refresh"}).get("full_refresh") is True:
        dbt_run_args.append("--full-refresh")

    # Add date variables if provided
    if date_from and date_to:
        dbt_run_args += [
            "--vars",
            json.dumps({"P_FECHADESDE_INC": date_from, "P_FECHAHASTA_EXC": date_to}),
        ]

    return dbt_run_args


# Function to create an asset function for a specific filter
def create_group_asset_function(select: str, exclude: str, group_name: str):
    # Define the function that will be decorated
    def group_asset_function(
        context: AssetExecutionContext,
        dbt_resource: DbtCliResource,
        config: MyDbtConfig,
    ):
        context.log.info(f"Running dbt for group: {group_name}")
        dbt_run_args = get_dbt_run_args(context, config, dbt_resource)
        yield from (
            dbt_resource.cli(dbt_run_args, context=context).stream().fetch_row_counts()
        )

    # Set a unique name for the function
    group_asset_function.__name__ = f"{group_name}_assets"

    # Apply the dbt_assets decorator
    decorated_function = dbt_assets(
        manifest=dbt_manifest,
        select=select,
        exclude=exclude,
        dagster_dbt_translator=MyDbtSourceTranslator(),
    )(group_asset_function)

    return decorated_function


# Create dbt assets for each group in the manifest
dbt_group_assets = []
group_names = {
    group_def["name"]
    for group_def in dbt_manifest.get("groups", {}).values()
    if any(MAIN_SELECT_STR in tag for tag in group_def.get("tags", []))
}

# Create assets for each group, coma es AND
for group_name in group_names:
    dbt_group_assets.append(
        create_group_asset_function(
            select=f"{MAIN_SELECT_STR},group:{group_name}",
            exclude=f"tag:{tags_repo.PartitionedAuto.key}",
            group_name=group_name,
        )
    )

# Add remaining models that don't belong to any group, espacio es OR
dbt_group_assets.append(
    create_group_asset_function(
        select=f"{MAIN_SELECT_STR}",
        exclude=f"tag:{tags_repo.PartitionedAuto.key} group:{' group:'.join(group_names)}",
        group_name="global_default_group",
    )
)


all_assets = tuple(dbt_group_assets)

all_asset_checks: Sequence[AssetChecksDefinition] = (
    load_asset_checks_from_current_module()
)
