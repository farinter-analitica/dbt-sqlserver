import os
from dagster import Definitions, load_assets_from_modules
from .assets import (dbt_dwh_sap
                     , dbt_sources
                     , sap_etl_dwh
                     )

dbt_dwh_sap_assets = load_assets_from_modules([dbt_dwh_sap] #, group_name="dbt_examples" #group name already on the dbt models
                                       )
sap_etl_dwh_assets = load_assets_from_modules([sap_etl_dwh],group_name="sap_etl_dwh")

all_assets =  dbt_dwh_sap_assets + sap_etl_dwh_assets#+
all_asset_keys = set()
for asset in all_assets:
    all_asset_keys.update(asset.keys)
dbt_sources_assets = [source_asset for source_asset in dbt_sources.source_assets if source_asset.key not in all_asset_keys]

from dagster_shared_gf import all_shared_resources
from dagster_sap.jobs import all_jobs
from dagster_sap.schedules import all_schedules

dagster_sap_resources = all_shared_resources

defs = Definitions(
    assets=all_assets + dbt_sources_assets,
    resources= dagster_sap_resources,
    jobs=all_jobs,
    schedules=all_schedules
)

