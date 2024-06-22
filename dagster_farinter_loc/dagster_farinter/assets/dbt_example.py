from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from dagster_farinter.constants import dbt_manifest_path


@dbt_assets(manifest=dbt_manifest_path, select="example")
def firts_example_dbt_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource):
    yield from dbt_resource.cli(["build"], context=context).stream()

@dbt_assets(manifest=dbt_manifest_path, select="dagster_example")
def dep_example_dbt_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource):
    yield from dbt_resource.cli(["build"], context=context).stream()
