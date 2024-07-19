import os

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from dagster_kielsa_gf.assets import (examples
                                   , dbt_example
                                   , dbt_dwh_kielsa
                                   , dbt_sources
                                   , ldcom_etl_dwh
                                   , knime_asset_factory
                                   ,recetas_libros_etl_dwh)

all_assets = examples.all_assets + dbt_example.all_assets  + dbt_dwh_kielsa.all_assets \
    + ldcom_etl_dwh.all_assets + knime_asset_factory.all_assets + recetas_libros_etl_dwh.all_assets
all_asset_checks = dbt_example.all_asset_checks + dbt_dwh_kielsa.all_asset_checks \
    + ldcom_etl_dwh.all_asset_checks + knime_asset_factory.all_asset_checks + recetas_libros_etl_dwh.all_asset_checks

# Extract the asset keys from the AssetsDefinition instances
all_asset_keys = set()
for asset in all_assets:
    all_asset_keys.update(asset.keys)

dbt_sources_assets:list = [source_asset for source_asset in dbt_sources.source_assets if source_asset.key not in all_asset_keys]

from dagster_kielsa_gf.assets import dbt_dwh_kielsa
from dagster_shared_gf import all_shared_resources
from dagster_kielsa_gf.jobs import all_jobs
from dagster_kielsa_gf.schedules import all_schedules
from dagster_kielsa_gf.sensors import all_sensors

all_resources = all_shared_resources


import dagster_kielsa_gf.dlt.definitions as dlt_defs
import dagster_kielsa_gf.gobernor.jobs_gobernor as gobernor_defs
defs = Definitions.merge(
    #dlt_defs.defs, #antes todos los subrepos
    Definitions(
        assets=all_assets + dbt_sources_assets + dlt_defs.all_assets,
        asset_checks=all_asset_checks,
        resources=all_resources | dlt_defs.all_resources,
        jobs=all_jobs,
        schedules=all_schedules,
        sensors=all_sensors,
    ),
    gobernor_defs.defs,  # De ultimo ya que puede gobernar los demas subrepos
)

# @repository
# def dagster_kielsa_gf_repo():
#     return defs
