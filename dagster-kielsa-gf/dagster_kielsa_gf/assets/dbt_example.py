import shutil
from typing import Sequence

from dagster import (
    AssetChecksDefinition,
    AssetExecutionContext,
    load_asset_checks_from_current_module,
    load_assets_from_current_module,
)
from dagster_dbt import DbtCliResource, dbt_assets

from dagster_shared_gf.resources.dbt_resources import dbt_manifest_path


@dbt_assets(manifest=dbt_manifest_path, select="example")
def firts_example_dbt_assets(
    context: AssetExecutionContext, dbt_resource: DbtCliResource
):
    dbt_invocation = dbt_resource.cli(["build"], context=context)
    yield from dbt_invocation.stream().fetch_row_counts()

    # Clean up the target directory after the run
    shutil.rmtree(dbt_invocation.target_path, ignore_errors=True)


@dbt_assets(manifest=dbt_manifest_path, select="dagster_example")
def dep_example_dbt_assets(
    context: AssetExecutionContext, dbt_resource: DbtCliResource
):
    dbt_invocation = dbt_resource.cli(["build"], context=context)
    yield from dbt_invocation.stream().fetch_row_counts()

    # Clean up the target directory after the run
    shutil.rmtree(dbt_invocation.target_path, ignore_errors=True)


all_assets = tuple(load_assets_from_current_module())

all_asset_checks: Sequence[AssetChecksDefinition] = tuple(
    load_asset_checks_from_current_module()
)
