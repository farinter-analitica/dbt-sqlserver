from dagster import AssetExecutionContext, Config
from pydantic import Field
from dagster_dbt import DbtCliResource, dbt_assets

from dagster_shared_gf.resources.dbt_resources import dbt_manifest


@dbt_assets(manifest=dbt_manifest, select="tag:dagster_sap_gf", exclude="group:sap_etl_dwh")
def dbt_dwh_sap_mart_datos_maestros_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource):
    yield from dbt_resource.cli(["build"], context=context).stream().fetch_row_counts()


class MyDbtConfig(Config):
    full_refresh: bool = Field(default=False, description="Refresh full dbt models")

@dbt_assets(manifest=dbt_manifest, select="group:sap_etl_dwh")
def dbt_sap_etl_dwh_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource, config: MyDbtConfig):
    dbt_run_args = ["build"]
    if config.full_refresh:
        dbt_run_args += ["--full-refresh"]
        #context.log.info(context.selected_asset_keys)
    yield from dbt_resource.cli(dbt_run_args, context=context).stream().fetch_row_counts()

