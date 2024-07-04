from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from dagster_shared_gf.resources.dbt_resources import dbt_manifest

@dbt_assets(manifest=dbt_manifest, select="group:sap_etl_dwh")
def dbt_sap_etl_dwh_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource ):
    yield from dbt_resource.cli(["build"], context=context).stream().fetch_row_counts()

