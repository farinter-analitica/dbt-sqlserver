from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from dagster_shared_gf.resources.dbt_resources import dbt_manifest_path


@dbt_assets(manifest=dbt_manifest_path, select="example")
def firts_example_dbt_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource):
    yield from dbt_resource.cli(["build"], context=context).stream().fetch_row_counts()

@dbt_assets(manifest=dbt_manifest_path, select="dagster_example")
def dep_example_dbt_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource):
    yield from dbt_resource.cli(["build"], context=context).stream().fetch_row_counts()
