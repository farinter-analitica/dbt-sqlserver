from dagster import (
    Definitions,
    build_sensor_for_freshness_checks,
)

from dagster_global_gf import assets as assets_repo
from dagster_global_gf.assets import dbt_sources
from dagster_global_gf.jobs import all_jobs
from dagster_global_gf.schedules import all_schedules
from dagster_shared_gf import shared_failed_sensors
from dagster_shared_gf.shared_constants import (
    running_default_sensor_status,
    hourly_freshness_seconds_per_environ,
)
from dagster_shared_gf.shared_helpers import (
    get_unique_source_assets,
    create_freshness_checks_for_assets,
)
from dagster_shared_gf.shared_defs import all_shared_resources, ACSSensorFactory

all_assets = assets_repo.all_assets
dbt_sources_assets: list = get_unique_source_assets(
    assets_repo.all_assets, dbt_sources.source_assets
)

all_freshness_checks = create_freshness_checks_for_assets(all_assets)

all_shared_assets_freshness_checks_sensor = build_sensor_for_freshness_checks(
    freshness_checks=all_freshness_checks,
    default_status=running_default_sensor_status,
    minimum_interval_seconds=hourly_freshness_seconds_per_environ,
    name="all_shared_assets_freshness_checks_sensor",
)

defs = Definitions(
    assets=(*assets_repo.all_assets, *dbt_sources_assets),
    jobs=(*all_jobs,),
    schedules=(*all_schedules,),
    asset_checks=(*assets_repo.all_asset_checks, *all_freshness_checks),
    sensors=(
        *ACSSensorFactory().get_sensors(),
        all_shared_assets_freshness_checks_sensor,
        shared_failed_sensors.failed_asset_notification_sensor,
    ),
    resources=all_shared_resources,
)
