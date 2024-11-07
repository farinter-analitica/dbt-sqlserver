from typing import Any, Sequence

from dagster import AssetsDefinition, Definitions, AutomationConditionSensorDefinition as ACS, AssetSelection

from dagster_sap_gf.jobs import all_jobs
from dagster_sap_gf.schedules import all_schedules
from dagster_sap_gf.sensors import all_sensors
from dagster_shared_gf import all_shared_resources
from dagster_sap_gf.dlt.definitions import (
    all_assets as dlt_all_assets,
    all_resources as dlt_all_resources,
)

from dagster_sap_gf.assets import (
    dbt_dwh_sap,
    dbt_sources,
    sap_etl_dwh,
    sap_etl_dwh_sp,
    smb_etl_dwh,
)

all_assets: Sequence[AssetsDefinition | Any] = (
    *dbt_dwh_sap.all_assets,
    *sap_etl_dwh.all_assets,
    *smb_etl_dwh.all_assets,
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

for asset in all_assets:
    if type(asset) is AssetsDefinition:
        all_asset_keys.update(asset.keys)

dbt_sources_assets = (
    source_asset
    for source_asset in dbt_sources.source_assets
    if source_asset.key not in all_asset_keys
)

dagster_sap_gf_resources = {**all_shared_resources, **dlt_all_resources}

defs = Definitions(
    assets=(*all_assets, *dbt_sources_assets),
    asset_checks=all_asset_checks,
    resources=dagster_sap_gf_resources,
    jobs=all_jobs,
    sensors=(*all_sensors,ACS("automation_condition_sensor", target=AssetSelection.all(), use_user_code_server=True)),
    schedules=all_schedules,
)
