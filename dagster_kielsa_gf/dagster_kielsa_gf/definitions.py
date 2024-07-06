import os

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from dagster_kielsa_gf.assets import (examples
                                   , kielsa_general
                                   , dbt_example
                                   , dbt_dwh_kielsa
                                   , dbt_sources
                                   , ldcom_etl_dwh
                                   , knime_asset_factory)

examples = load_assets_from_modules([examples], group_name="examples")
kielsa_general = load_assets_from_modules([kielsa_general], group_name="kielsa_general")
dbt_example = load_assets_from_modules([dbt_example] #, group_name="dbt_examples" #group name already on the dbt models
                                       )
dbt_dwh_kielsa_assets =  load_assets_from_modules([dbt_dwh_kielsa])
ldcom_etl_dwh_assets = load_assets_from_modules([ldcom_etl_dwh], group_name="ldcom_etl_dwh")
knime_assets = knime_asset_factory.knime_assets_definitions

all_assets = examples + kielsa_general + dbt_example  + dbt_dwh_kielsa_assets + ldcom_etl_dwh_assets + knime_assets

# Extract the asset keys from the AssetsDefinition instances
all_asset_keys = set()
for asset in all_assets:
    all_asset_keys.update(asset.keys)

dbt_sources_assets:list = [source_asset for source_asset in dbt_sources.source_assets if source_asset.key not in all_asset_keys]

from dagster_kielsa_gf.assets import dbt_dwh_kielsa
from dagster_shared_gf import all_shared_resources
from dagster_kielsa_gf.jobs import all_jobs
from dagster_kielsa_gf.schedules import all_schedules

all_resources = all_shared_resources


defs = Definitions(
    assets=all_assets + dbt_sources_assets,
    resources= all_resources,
    jobs=all_jobs,
    schedules=all_schedules
)

# @repository
# def dagster_kielsa_gf_repo():
#     return defs