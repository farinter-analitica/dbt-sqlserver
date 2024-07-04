from dagster import AssetExecutionContext, Config
from pydantic import Field
from dagster_dbt import DbtCliResource, dbt_assets

from dagster_shared_gf.resources.dbt_resources import dbt_manifest

class MyDbtConfig(Config):
    full_refresh: bool = Field(default=False, description="Refresh full dbt models")

@dbt_assets(manifest=dbt_manifest, select="group:sap_etl_dwh")
def dbt_sap_etl_dwh_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource, config: MyDbtConfig):
    dbt_run_args = ["build"]
    if config.full_refresh:
        dbt_run_args += ["--full-refresh"]
        #context.log.info(context.selected_asset_keys)
    yield from dbt_resource.cli(dbt_run_args, context=context).stream().fetch_row_counts()

