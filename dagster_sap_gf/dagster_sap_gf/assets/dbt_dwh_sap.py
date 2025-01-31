from dagster import (AssetExecutionContext, Config, AssetsDefinition, build_last_update_freshness_checks, load_asset_checks_from_current_module, AssetChecksDefinition, load_assets_from_current_module, AssetKey)
from pydantic import Field
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator
from datetime import timedelta
from dagster_shared_gf.resources.dbt_resources import dbt_manifest, MyDbtSourceTranslator
from dagster_shared_gf.shared_functions import filter_assets_by_tags, get_all_instances_of_class
from dagster_shared_gf.shared_variables import tags_repo
from typing import Sequence, List, Mapping, Dict, Any

class MyDbtConfig(Config):
    full_refresh: bool = Field(default=False, description="Refresh full dbt models")

@dbt_assets(manifest=dbt_manifest, select="tag:dagster_sap_gf/dbt", exclude="group:sap_etl_dwh", dagster_dbt_translator=MyDbtSourceTranslator())
def dbt_dwh_sap_mart_datos_maestros_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource, config: MyDbtConfig):
    dbt_run_args = ["build"]
    if config.full_refresh:
        dbt_run_args += ["--full-refresh"]
        #context.log.info(context.selected_asset_keys)
    yield from dbt_resource.cli(dbt_run_args, context=context).stream().fetch_row_counts()


@dbt_assets(manifest=dbt_manifest, select="group:sap_etl_dwh", dagster_dbt_translator=MyDbtSourceTranslator())
def dbt_sap_etl_dwh_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource, config: MyDbtConfig):
    dbt_run_args = ["build"]
    if config.full_refresh:
        dbt_run_args += ["--full-refresh"]
        #context.log.info(context.selected_asset_keys)
    yield from dbt_resource.cli(dbt_run_args, context=context).stream().fetch_row_counts()


all_assets = tuple(load_assets_from_current_module())

all_asset_checks: Sequence[AssetChecksDefinition] = tuple(load_asset_checks_from_current_module())
