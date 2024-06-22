from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from dagster_shared_gf.resources.dbt_resources import dbt_manifest_path


@dbt_assets(manifest=dbt_manifest_path, select="group:dbt_dwh_sap_mart_datos_maestros")
def dbt_dwh_sap_mart_datos_maestros_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource):
    yield from dbt_resource.cli(["build"], context=context).stream()
