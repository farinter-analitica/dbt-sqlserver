

from dagster import ScheduleDefinition, DefaultScheduleStatus
from dagster_sap.jobs import *
from dagster_shared_gf.shared_functions import (get_all_instances_of_class)
from dagster_shared_gf import shared_variables as shared_vars
#cron: minute hour day month day_of_week, example daily at midnight: 0 0 * * *
#cron example daily at midnight mon-fri with numbers: 0 0 * * 1-5
env_str:str=shared_vars.env_str

# Define the schedule
# dbt_dwh_sap_mart_datos_maestros_schedule = ScheduleDefinition(
#     name="dbt_dwh_sap_mart_datos_maestros_schedule",
#     cron_schedule="14 1 * * *",  # 10:01 AM every day
#     execution_timezone="America/Tegucigalpa",
#     job=dbt_dwh_sap_mart_datos_maestros_job,
#     default_status=DefaultScheduleStatus.RUNNING

# )

# Define the schedule, name defaults to the name of the job + _schedule
dbt_dwh_sap_marts_job_schedule = ScheduleDefinition(
    #name="dbt_dwh_sap_mart_schedule",
    cron_schedule = {"dev":"15 2 * * *","prd":"30 1 * * *"}.get(env_str),  # 10:01 AM every day
    execution_timezone="America/Tegucigalpa",
    job=dbt_dwh_sap_marts_job,
    default_status=DefaultScheduleStatus.STOPPED
)
dbt_dwh_sap_etl_dwh_all_downstream_job_schedule = ScheduleDefinition(
    #name="dbt_dwh_sap_mart_schedule",
    cron_schedule = {"dev":"15 3 * * *","prd":"30 2 * * *"}.get(env_str),  # 10:01 AM every day
    execution_timezone="America/Tegucigalpa",
    job=dbt_dwh_sap_etl_dwh_all_downstream_job,
    default_status=DefaultScheduleStatus.RUNNING
)

dbt_dwh_sap_marts_all_orphan_job_schedule = ScheduleDefinition(
    #name="dbt_dwh_sap_mart_schedule",
    cron_schedule = {"dev":"15 2 * * *","prd":"30 1 * * *"}.get(env_str),  # 10:01 AM every day
    execution_timezone="America/Tegucigalpa",
    job=dbt_dwh_sap_marts_all_orphan_job,
    default_status=DefaultScheduleStatus.RUNNING
)

all_schedules = get_all_instances_of_class([ScheduleDefinition])

__all__ = list(map(lambda x: x.name, all_schedules) )