from dagster import (
    AssetExecutionContext,
    Config,
)
from dagster_dbt import DbtCliResource, dbt_assets
from pydantic import Field

from dagster_shared_gf.resources.dbt_resources import (
    MyDbtSourceTranslator,
    dbt_manifest,
)


class MyDbtConfig(Config):
    full_refresh: bool = Field(default=False, description="Refresh full dbt models")


@dbt_assets(
    manifest=dbt_manifest,
    select="tag:dagster_global_gf/dbt",
    dagster_dbt_translator=MyDbtSourceTranslator(),
)
def dbt_dwh_kielsa_mart_datos_maestros_assets(
    context: AssetExecutionContext, dbt_resource: DbtCliResource, config: MyDbtConfig
):
    dbt_run_args = ["build"]
    if config.full_refresh:
        dbt_run_args += ["--full-refresh"]
    yield from (
        dbt_resource.cli(dbt_run_args, context=context).stream().fetch_row_counts()
    )
