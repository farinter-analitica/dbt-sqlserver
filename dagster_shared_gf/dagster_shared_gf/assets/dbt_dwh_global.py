from datetime import timedelta
from typing import Sequence

from dagster import (
    AssetChecksDefinition,
    AssetExecutionContext,
    Config,
    build_last_update_freshness_checks,
    load_asset_checks_from_current_module,
    load_assets_from_current_module,
)
from dagster_dbt import DbtCliResource, dbt_assets
from pydantic import Field

from dagster_shared_gf.resources.dbt_resources import (
    MyDbtSourceTranslator,
    dbt_manifest,
)
from dagster_shared_gf.shared_functions import filter_assets_by_tags
from dagster_shared_gf.shared_variables import TagsRepositoryGF

tags_repo = TagsRepositoryGF


class MyDbtConfig(Config):
    full_refresh: bool = Field(default=False, description="Refresh full dbt models")

@dbt_assets(manifest=dbt_manifest, select="tag:dagster_global_gf/dbt", dagster_dbt_translator=MyDbtSourceTranslator())
def dbt_dwh_kielsa_mart_datos_maestros_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource, config: MyDbtConfig):
    dbt_run_args = ["build"]
    if config.full_refresh:
        dbt_run_args += ["--full-refresh"]
    yield from dbt_resource.cli(dbt_run_args, context=context).stream().fetch_row_counts()


all_assets = load_assets_from_current_module()

all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"),
    lower_bound_delta=timedelta(hours=26),
    deadline_cron="0 9 * * 1-6",
)
#print(filter_assets_by_tags(all_assets, tags=hourly_tag, filter_type="any_tag_matches"), "\n")
all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="any_tag_matches"),
    lower_bound_delta=timedelta(hours=13),
    deadline_cron="0 10-16 * * 1-6",
)

all_asset_checks: Sequence[AssetChecksDefinition] = load_asset_checks_from_current_module()
all_asset_freshness_checks = all_assets_non_hourly_freshness_checks + all_assets_hourly_freshness_checks
