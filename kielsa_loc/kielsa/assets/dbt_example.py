from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from kielsa.constants import dbt_manifest_path


@dbt_assets(manifest=dbt_manifest_path)
def kielsa_dbt_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource):
    yield from dbt_resource.cli(["build"], context=context).stream()
    