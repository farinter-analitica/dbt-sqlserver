from dagster import (AssetExecutionContext, OpExecutionContext, asset, op, execute_job, DagsterInstance  )
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_shared_gf.resources.dbt_resources import dbt_manifest

@dbt_assets(manifest=dbt_manifest, select="tag:dagster_kielsa_gf")
def dbt_dwh_kielsa_mart_datos_maestros_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource):
    yield from dbt_resource.cli(["build"], context=context).stream()

