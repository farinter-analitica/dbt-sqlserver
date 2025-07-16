from typing import Any, Sequence

from dagster import (
    AssetsDefinition,
    Definitions,
    build_sensor_for_freshness_checks,
)

from dagster_sap_gf.assets import (
    dbt_sources,
    sap_etl_dwh,
    sap_etl_dwh_sp,
    control_demanda,
    analysis_services,
    all_assets as sap_all_assets,
    all_asset_checks as sap_all_asset_checks,
)
from dagster_sap_gf.dlt_defs.definitions import (
    all_assets as dlt_all_assets,
)
from dagster_sap_gf.dlt_defs.definitions import (
    all_resources as dlt_all_resources,
)
from dagster_sap_gf.jobs import all_jobs
from dagster_sap_gf.schedules import all_schedules
from dagster_sap_gf.sensors import (
    all_sensors,
)
from dagster_shared_gf.shared_defs import (
    all_shared_resources,
    ACSSensorFactory,
)
from dagster_shared_gf.shared_constants import (
    hourly_freshness_seconds_per_environ,
    running_default_sensor_status,
)
from dagster_shared_gf.shared_helpers import (
    create_freshness_checks_for_assets,
    get_unique_source_assets,
)

all_assets: Sequence[AssetsDefinition | Any] = (
    *sap_etl_dwh.all_assets,
    *sap_etl_dwh_sp.all_assets,
    *dlt_all_assets,
    *control_demanda.all_assets,
    *analysis_services.all_assets,
    *sap_all_assets,
)

all_asset_checks = (
    *sap_etl_dwh.all_asset_checks,
    *sap_etl_dwh_sp.all_asset_checks,
    *control_demanda.all_asset_checks,
    *analysis_services.all_asset_checks,
    *sap_all_asset_checks,
)

dbt_sources_assets: list = get_unique_source_assets(
    all_assets, dbt_sources.source_assets
)


all_asset_freshness_checks = create_freshness_checks_for_assets(all_assets)

all_sap_assets_freshness_checks_sensor = build_sensor_for_freshness_checks(
    freshness_checks=all_asset_freshness_checks,
    default_status=running_default_sensor_status,
    minimum_interval_seconds=hourly_freshness_seconds_per_environ,  # 1 hour
    name="all_sap_assets_freshness_checks_sensor",
)

dagster_sap_gf_resources = {**all_shared_resources, **dlt_all_resources}

defs = Definitions(
    assets=(*all_assets, *dbt_sources_assets),
    asset_checks=(*all_asset_checks, *all_asset_freshness_checks),
    resources=dagster_sap_gf_resources,
    jobs=all_jobs,
    sensors=(
        *all_sensors,
        *ACSSensorFactory().get_sensors(),
        all_sap_assets_freshness_checks_sensor,
    ),
    schedules=all_schedules,
)
