import json
import shutil
from collections import deque
from datetime import datetime, timedelta
from typing import Sequence

from dagster import (
    AssetChecksDefinition,
    AssetExecutionContext,
    BackfillPolicy,
    Config,
    load_asset_checks_from_current_module,
    load_assets_from_current_module,
)
from dagster_dbt import DbtCliResource, dbt_assets
from pydantic import Field

from dagster_shared_gf.partitions.time_based import (
    diario_desde_360_dias_atras_hasta_hoy,
)
from dagster_shared_gf.resources.dbt_resources import (
    MyDbtSourceTranslator,
    dbt_manifest,
)
from dagster_shared_gf.shared_variables import tags_repo


class MyDbtConfig(Config):
    full_refresh: bool = Field(default=False, description="Refresh full dbt models")


MAIN_SELECT_STR = "tag:dagster_kielsa_gf/dbt"


# Common function to handle dbt run arguments
def get_dbt_run_args(
    context, config, dbt_resource, date_from=None, date_to=None
) -> deque[str]:
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
        dbt_invocation = dbt_resource.cli(dbt_run_args, context=context)
        yield from dbt_invocation.stream().fetch_row_counts()

        # Clean up the target directory after the run
        shutil.rmtree(dbt_invocation.target_path, ignore_errors=True)

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


# La coma en el select es una intersección.
# Crear los grupos de assets dbt, un hilo de ejecucion (la funcion dbt_assets) para cada uno.
dbt_group_assets = []
group_names = {
    group_def["name"] for group_def in dbt_manifest.get("groups", {}).values()
}
for group_name in group_names:
    dbt_group_assets.append(
        create_group_asset_function(
            select=f"{MAIN_SELECT_STR},group:{group_name}",
            exclude=f"tag:{tags_repo.AutomationOnlyParticionado.key}",
            group_name=group_name,
        )
    )

# Añadir remanentes, el espacio en vez de coma significa OR, estamos excluyendo los ya cargados que si tienen grupo.
dbt_group_assets.append(
    create_group_asset_function(
        select=f"{MAIN_SELECT_STR}",
        exclude=f"tag:{tags_repo.AutomationOnlyParticionado.key} group:{' group:'.join(group_names)}",
        group_name="dbt_default_group",
    )
)


@dbt_assets(
    manifest=dbt_manifest,
    select=f"{MAIN_SELECT_STR},tag:{tags_repo.AutomationOnlyParticionado.key}",
    dagster_dbt_translator=MyDbtSourceTranslator(),
    partitions_def=diario_desde_360_dias_atras_hasta_hoy,
    backfill_policy=BackfillPolicy.single_run(),
)
def CRM_Kielsa_RecetasContactarHist(
    context: AssetExecutionContext, dbt_resource: DbtCliResource, config: MyDbtConfig
):
    v_date_from: str = (
        (datetime.now().date() - timedelta(days=360)).replace(day=1).strftime("%Y%m%d")
    )
    v_date_to: str = (datetime.now().date() + timedelta(days=1)).strftime("%Y%m%d")

    if context.has_partition_key_range:
        v_date_from = datetime.fromisoformat(
            context.partition_key_range.start
        ).strftime("%Y%m%d")
        v_date_to = (
            datetime.fromisoformat(context.partition_key_range.end) + timedelta(days=1)
        ).strftime("%Y%m%d")

    dbt_run_args = get_dbt_run_args(
        context, config, dbt_resource, v_date_from, v_date_to
    )

    dbt_invocation = dbt_resource.cli(dbt_run_args, context=context)
    yield from dbt_invocation.stream().fetch_row_counts()

    # Clean up the target directory after the run
    shutil.rmtree(dbt_invocation.target_path, ignore_errors=True)


if __name__ == "__main__":
    all_assets = tuple(load_assets_from_current_module())

    all_asset_checks: Sequence[AssetChecksDefinition] = (
        load_asset_checks_from_current_module()
    )
    import dagster as dg

    print(
        [
            asset.asset_deps
            for asset in all_assets
            if isinstance(asset, dg.AssetsDefinition)
        ]
    )
