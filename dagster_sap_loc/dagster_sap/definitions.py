import os
from dagster import Definitions, load_assets_from_modules
from .assets import dbt_dwh_sap_mart_datos_maestros, dbt_dwh_sap_mart_finanzas, dbt_sources

dbt_dwh_sap_mart_assets = load_assets_from_modules([dbt_dwh_sap_mart_datos_maestros,dbt_dwh_sap_mart_finanzas] #, group_name="dbt_examples" #group name already on the dbt models
                                       )
dbt_sources_assets = dbt_sources.source_assets

all_assets =  dbt_dwh_sap_mart_assets + dbt_sources_assets #+

from dagster_shared_gf import all_shared_resources
from dagster_sap.jobs import all_jobs
from dagster_sap.schedules import all_schedules

dagster_sap_resources = all_shared_resources

defs = Definitions(
    assets=all_assets,
    resources= dagster_sap_resources,
    jobs=all_jobs,
    schedules=all_schedules
)

