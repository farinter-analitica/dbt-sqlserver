

from dagster import ScheduleDefinition, DefaultScheduleStatus
from dagster_kielsa.jobs import *
from dagster_shared_gf.shared_functions import (get_all_instances_of_class)
from dagster_shared_gf import shared_variables as shared_vars
#cron: minute hour day month day_of_week, example daily at midnight: 0 0 * * *
#cron example daily at midnight mon-fri with numbers: 0 0 * * 1-5
env_str:str=shared_vars.env_str

# Define the schedule, name defaults to the name of the job + _schedule
dbt_dwh_kielsa_marts_job_schedule = ScheduleDefinition(
    cron_schedule = {"dev":"0 3 * * *","prd":"0 2 * * *"}.get(env_str),
    execution_timezone="America/Tegucigalpa",
    job=dbt_dwh_kielsa_marts_job,
    default_status=DefaultScheduleStatus.STOPPED #Se ejecutaran los orphan

)

# Define the schedule, name defaults to the name of the job + _schedule
dbt_dwh_kielsa_marts_orphan_assets_job_schedule = ScheduleDefinition(
    cron_schedule = {"dev":"0 3 * * *","prd":"0 2 * * *"}.get(env_str),
    execution_timezone="America/Tegucigalpa",
    job=dbt_dwh_kielsa_marts_orphan_assets_job,
    default_status=DefaultScheduleStatus.RUNNING

)

ldcom_etl_dwh_job_schedule = ScheduleDefinition(
    cron_schedule = {"dev":"0 2 * * *","prd":"0 1 * * *"}.get(env_str),  # 01:00 AM every day
    execution_timezone="America/Tegucigalpa",
    job=ldcom_etl_dwh_job,
    default_status=DefaultScheduleStatus.STOPPED #Se ejecutaran con los downstream jobs
)

ldcom_etl_dwh_all_downstream_job = ScheduleDefinition(
    cron_schedule = {"dev":"30 1 * * *","prd":"30 0 * * *"}.get(env_str),  # 01:00 AM every day
    execution_timezone="America/Tegucigalpa",
    job=ldcom_etl_dwh_all_downstream_job,
    default_status=DefaultScheduleStatus.RUNNING
)


all_schedules = get_all_instances_of_class([ScheduleDefinition])

__all__ = list(map(lambda x: x.name, all_schedules) )