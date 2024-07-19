from dagster import AssetExecutionContext, load_assets_from_current_module, load_asset_checks_from_current_module, build_last_update_freshness_checks, AssetChecksDefinition,AssetKey
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator
from typing import Sequence, List, Mapping, Dict, Any
from datetime import timedelta
from dagster_shared_gf.shared_variables import TagsRepositoryGF
from dagster_shared_gf.shared_functions import filter_assets_by_tags
from dagster_shared_gf.resources.dbt_resources import dbt_manifest, MyDbtSourceTranslator

tags_repo = TagsRepositoryGF

@dbt_assets(manifest=dbt_manifest, select="tag:dagster_kielsa_gf/dbt", dagster_dbt_translator=MyDbtSourceTranslator())
def dbt_dwh_kielsa_mart_datos_maestros_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource):
    yield from dbt_resource.cli(["build"], context=context).stream()


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
