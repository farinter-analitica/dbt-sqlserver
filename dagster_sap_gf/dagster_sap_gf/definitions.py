from collections import deque
from datetime import timedelta
from typing import Any, Sequence

from dagster import (
    AssetChecksDefinition,
    AssetsDefinition,
    Definitions,
    AutomationConditionSensorDefinition as ACS,
    AssetSelection,
    build_last_update_freshness_checks,
    build_sensor_for_freshness_checks,
)
from dagster_shared_gf.shared_variables import tags_repo
from dagster_sap_gf.jobs import all_jobs
from dagster_sap_gf.schedules import all_schedules
from dagster_sap_gf.sensors import (
    all_sensors,
    only_prd_running_default_sensor_status,
    running_default_sensor_status,
    hourly_freshness_lbound_per_environ,
    hourly_freshness_seconds_per_environ,
)
from dagster_shared_gf import all_shared_resources
from dagster_sap_gf.dlt_defs.definitions import (
    all_assets as dlt_all_assets,
    all_resources as dlt_all_resources,
)

from dagster_sap_gf.assets import (
    dbt_dwh_sap,
    dbt_sources,
    sap_etl_dwh,
    sap_etl_dwh_sp,
)
from dagster_shared_gf.shared_functions import filter_assets_by_tags

all_assets: Sequence[AssetsDefinition | Any] = (
    *dbt_dwh_sap.all_assets,
    *sap_etl_dwh.all_assets,
    *sap_etl_dwh_sp.all_assets,
    *dlt_all_assets,
)

all_asset_checks = (
    *sap_etl_dwh.all_asset_checks,
    *dbt_dwh_sap.all_asset_checks,
    *sap_etl_dwh_sp.all_asset_checks,
)
# Update the set with keys from each asset
all_asset_keys = set()
all_asset_defs = deque()
for asset in all_assets:
    if isinstance(asset, AssetsDefinition):
        all_asset_keys.update(asset.keys)
        all_asset_defs.append(asset)

dbt_sources_assets = (
    source_asset
    for source_asset in dbt_sources.source_assets
    if source_asset.key not in all_asset_keys
)

all_asset_defs_hourly_tag = filter_assets_by_tags(
    all_asset_defs,
    tags_to_match=tags_repo.Hourly.tag,
    filter_type="any_tag_matches",
)

all_asset_defs_non_hourly_tag = tuple(
    asset
    for asset in filter_assets_by_tags(
        all_asset_defs,
        tags_to_match=tags_repo.Hourly.tag,
        filter_type="exclude_if_any_tag",
    )
    if asset not in all_asset_defs_hourly_tag
)

all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = (
    build_last_update_freshness_checks(
        assets=all_asset_defs_hourly_tag,
        lower_bound_delta=hourly_freshness_lbound_per_environ,
        deadline_cron="0 10-16 * * 1-6",
    )
)

all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
    assets=all_asset_defs_non_hourly_tag,
    lower_bound_delta=timedelta(hours=26),
    deadline_cron="0 9 * * 1-6",
)

#
all_asset_freshness_checks = (
    *all_assets_hourly_freshness_checks,
    *all_assets_non_hourly_freshness_checks,
)
all_assets_non_hourly_freshness_checks_sensor = build_sensor_for_freshness_checks(
    freshness_checks=all_assets_non_hourly_freshness_checks,
    default_status=running_default_sensor_status,
    minimum_interval_seconds=60 * 60 * 6,  # 6 hour
    name="all_assets_non_hourly_freshness_checks_sensor",
)
all_assets_hourly_freshness_checks_sensor = build_sensor_for_freshness_checks(
    freshness_checks=all_assets_hourly_freshness_checks,
    default_status=running_default_sensor_status,
    minimum_interval_seconds=hourly_freshness_seconds_per_environ,  # 1 hour
    name="all_assets_hourly_freshness_checks_sensor",
)


dagster_sap_gf_resources = {**all_shared_resources, **dlt_all_resources}

defs = Definitions(
    assets=(*all_assets, *dbt_sources_assets),
    asset_checks=(*all_asset_checks, *all_asset_freshness_checks),
    resources=dagster_sap_gf_resources,
    jobs=all_jobs,
    sensors=(
        *all_sensors,
        ACS(
            "automation_condition_sensor",
            target=AssetSelection.all(),
            use_user_code_server=True,
        ),
        all_assets_non_hourly_freshness_checks_sensor,
        all_assets_hourly_freshness_checks_sensor,
    ),
    schedules=all_schedules,
)
