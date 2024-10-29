from dagster import (
    AssetExecutionContext,
    load_assets_from_current_module,
    load_asset_checks_from_current_module,
    build_last_update_freshness_checks,
    AssetChecksDefinition,
    Config,
    DailyPartitionsDefinition,
    BackfillPolicy,
)
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator
from typing import Sequence, List, Mapping, Dict, Any
from datetime import timedelta
from datetime import datetime, tzinfo
from pydantic import Field
from dagster_shared_gf.shared_variables import TagsRepositoryGF, default_timezone_teg
from dagster_shared_gf.shared_functions import filter_assets_by_tags
from dagster_shared_gf.resources.dbt_resources import (
    dbt_manifest,
    MyDbtSourceTranslator,
)

import json
import pytz

tags_repo = TagsRepositoryGF


class MyDbtConfig(Config):
    full_refresh: bool = Field(default=False, description="Refresh full dbt models")


@dbt_assets(
    manifest=dbt_manifest,
    select="tag:dagster_kielsa_gf/dbt",
    exclude="tag:particionado/si",
    dagster_dbt_translator=MyDbtSourceTranslator(),
)
def dbt_dwh_kielsa_mart_datos_maestros_assets(
    context: AssetExecutionContext, dbt_resource: DbtCliResource, config: MyDbtConfig
):
    dbt_run_args = ["build"]
    if config.full_refresh:
        dbt_run_args += ["--full-refresh"]
    yield from (
        dbt_resource.cli(dbt_run_args, context=context).stream().fetch_row_counts()
    )


p_start_date = datetime.combine((datetime.now() - timedelta(days=360)).date(), datetime.min.time()).astimezone(pytz.timezone(default_timezone_teg))
p_end_date = datetime.combine((datetime.now() + timedelta(days=30)).date(), datetime.min.time()).astimezone(pytz.timezone(default_timezone_teg))

particionado_contactar_hist = DailyPartitionsDefinition(
        start_date=p_start_date,
        end_date=p_end_date,
        timezone=default_timezone_teg,
        end_offset=1,
    )
@dbt_assets(
    manifest=dbt_manifest,
    select="tag:dagster_kielsa_gf/dbt,tag:particionado/si",
    dagster_dbt_translator=MyDbtSourceTranslator(),
    partitions_def=particionado_contactar_hist,
    backfill_policy=BackfillPolicy.single_run(),
)
def CRM_Kielsa_RecetasContactarHist(
    context: AssetExecutionContext, dbt_resource: DbtCliResource, config: MyDbtConfig
):
    dbt_run_args: List[str] = ["build"]
    v_date_from: str = (datetime.now().date() - timedelta(days=360)).replace(day=1).strftime("%Y%m%d")
    v_date_to: str = (datetime.now().date() + timedelta(days=1)).strftime("%Y%m%d")
    if config.full_refresh or not context.has_partition_key_range:
        dbt_run_args += ["--full-refresh"]
    if context.has_partition_key_range:
        v_date_from = datetime.fromisoformat(
            context.partition_key_range.start
        ).strftime("%Y%m%d")
        v_date_to = (
            datetime.fromisoformat(context.partition_key_range.end) + timedelta(days=1)
        ).strftime("%Y%m%d")

    dbt_run_args += [
        "--vars",
        json.dumps({"P_FECHADESDE_INC": v_date_from, "P_FECHAHASTA_EXC": v_date_to}),
    ]
    yield from (
        dbt_resource.cli(dbt_run_args, context=context).stream().fetch_row_counts()
    )


all_assets = load_assets_from_current_module()

all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(
        all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"
    ),
    lower_bound_delta=timedelta(hours=26),
    deadline_cron="0 9 * * 1-6",
)
# print(filter_assets_by_tags(all_assets, tags=hourly_tag, filter_type="any_tag_matches"), "\n")
all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = (
    build_last_update_freshness_checks(
        assets=filter_assets_by_tags(
            all_assets,
            tags_to_match=tags_repo.Hourly.tag,
            filter_type="any_tag_matches",
        ),
        lower_bound_delta=timedelta(hours=13),
        deadline_cron="0 10-16 * * 1-6",
    )
)

all_asset_checks: Sequence[AssetChecksDefinition] = (
    load_asset_checks_from_current_module()
)
all_asset_freshness_checks = (
    all_assets_non_hourly_freshness_checks + all_assets_hourly_freshness_checks
)
