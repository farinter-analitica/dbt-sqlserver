import os

from dagster import Definitions, load_assets_from_modules

from .assets import dbt_dwh_sap_mart_datos_maestros

dbt_dwh_sap_mart_datos_maestros = load_assets_from_modules([dbt_dwh_sap_mart_datos_maestros] #, group_name="dbt_examples" #group name already on the dbt models
                                       )

all_assets =  dbt_dwh_sap_mart_datos_maestros #+

from dagster_shared_gf import all_shared_resources
import dagster_farinter.jobs as jobs
import dagster_farinter.schedules as schedules

dagster_farinter_resources = all_shared_resources
all_jobs = [jobs.dbt_dwh_farinter_mart_datos_maestros_job]
all_schedules = [schedules.dbt_dwh_farinter_mart_datos_maestros_schedule]

defs = Definitions(
    assets=all_assets,
    resources= dagster_farinter_resources,
    jobs=all_jobs,
    schedules=all_schedules
)
