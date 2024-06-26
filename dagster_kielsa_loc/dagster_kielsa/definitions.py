import os

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from dagster_kielsa.assets import (examples
                                   , kielsa_general
                                   , dbt_example
                                   , dbt_dwh_kielsa_mart_datos_maestros
                                   , dbt_sources
                                   , ldcom_etl_dwh)

examples = load_assets_from_modules([examples], group_name="examples")
kielsa_general = load_assets_from_modules([kielsa_general], group_name="kielsa_general")
dbt_example = load_assets_from_modules([dbt_example] #, group_name="dbt_examples" #group name already on the dbt models
                                       )
dbt_dwh_kielsa_mart_assets = load_assets_from_modules([dbt_dwh_kielsa_mart_datos_maestros])
ldcom_etl_dwh_assets = load_assets_from_modules([ldcom_etl_dwh], group_name="ldcom_etl_dwh")

all_assets = examples + kielsa_general + dbt_example  + dbt_dwh_kielsa_mart_assets + ldcom_etl_dwh_assets

# Extract the asset keys from the AssetsDefinition instances
all_asset_keys = set()
for asset in all_assets:
    all_asset_keys.update(asset.keys)

dbt_sources_assets = [source_asset for source_asset in dbt_sources.source_assets if source_asset.key not in all_asset_keys]

from dagster_shared_gf import all_shared_resources
from dagster_kielsa.jobs import all_jobs
from dagster_kielsa.schedules import all_schedules

dagster_kielsa_resources = all_shared_resources


defs = Definitions(
    assets=all_assets + dbt_sources_assets,
    resources= dagster_kielsa_resources,
    jobs=all_jobs,
    schedules=all_schedules
)

# @repository
# def dagster_kielsa_repo():
#     return defs