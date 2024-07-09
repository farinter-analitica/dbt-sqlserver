import os
from typing import Sequence, Mapping, Any
from dagster_shared_gf.shared_functions import filter_assets_by_tags
from dagster import (Definitions
                     , load_assets_from_modules
                     , build_sensor_for_freshness_checks
                     , AssetsDefinition)
from datetime import timedelta
from .assets import (dbt_dwh_sap
                     , dbt_sources
                     , sap_etl_dwh
                     )
from . import assets

all_assets: Sequence[AssetsDefinition | Any] =  dbt_dwh_sap.all_assets + sap_etl_dwh.all_assets

all_asset_checks = sap_etl_dwh.all_asset_checks + dbt_dwh_sap.all_asset_checks
all_asset_keys = set()
for asset in all_assets:
    # Update the set with keys from each asset
    all_asset_keys.update(asset.keys)

dbt_sources_assets = [source_asset for source_asset in dbt_sources.source_assets if source_asset.key not in all_asset_keys]

from dagster_shared_gf import all_shared_resources
from dagster_sap_gf.jobs import all_jobs
from dagster_sap_gf.schedules import all_schedules
from dagster_sap_gf.sensors import all_sensors

dagster_sap_gf_resources = all_shared_resources

defs = Definitions(
    assets=(all_assets + dbt_sources_assets),
    asset_checks=all_asset_checks,
    resources= dagster_sap_gf_resources,
    jobs=all_jobs,
    sensors=all_sensors,
    schedules=all_schedules
)
