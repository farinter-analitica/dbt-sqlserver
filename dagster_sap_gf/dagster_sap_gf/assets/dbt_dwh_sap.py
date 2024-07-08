from dagster import AssetExecutionContext, Config, AssetsDefinition, build_last_update_freshness_checks, load_asset_checks_from_current_module, AssetChecksDefinition
from pydantic import Field
from dagster_dbt import DbtCliResource, dbt_assets
from datetime import timedelta
from dagster_shared_gf.resources.dbt_resources import dbt_manifest
from dagster_shared_gf.shared_functions import filter_assets_by_tags, get_all_instances_of_class
from dagster_shared_gf.shared_variables import TagsRepositoryGF
from typing import Sequence
tags_repo = TagsRepositoryGF
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


all_assets = get_all_instances_of_class([AssetsDefinition]) 

all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(all_assets, tags=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"),
    lower_bound_delta=timedelta(hours=26),
    deadline_cron="0 9 * * 1-6",
)
#print(filter_assets_by_tags(all_assets, tags=hourly_tag, filter_type="any_tag_matches"), "\n")
all_assets_hourly_freshness_checks = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(all_assets, tags=tags_repo.Hourly.tag, filter_type="any_tag_matches"),
    lower_bound_delta=timedelta(hours=13),
    deadline_cron="0 10-16 * * 1-6",
)

all_asset_checks: Sequence[AssetChecksDefinition] = load_asset_checks_from_current_module()
all_asset_freshness_checks = all_assets_non_hourly_freshness_checks + all_assets_hourly_freshness_checks
