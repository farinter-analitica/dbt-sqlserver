

from dagster import ScheduleDefinition, DefaultScheduleStatus
from dagster_kielsa.jobs import *
from dagster_shared_gf.shared_functions import get_all_instances_of_class 
#cron: minute hour day month day_of_week, example daily at midnight: 0 0 * * *
#cron example daily at midnight mon-fri with numbers: 0 0 * * 1-5


# Define the schedule
dbt_dwh_kielsa_marts_schedule = ScheduleDefinition(
    name="dbt_dwh_kielsa_marts_schedule",
    cron_schedule="14 1 * * *",  # 01:14 AM every day
    execution_timezone="America/Tegucigalpa",
    job=dbt_dwh_kielsa_marts_job,
    # job=wait_if_job_running_to_execute_next_job,
    # run_config={"ops": {"wait_if_job_running_to_execute_next_op": 
    #                     {"config": {"wait_for_job": ldcom_etl_dwh_job.name
    #                                 , "job_to_execute": dbt_dwh_kielsa_marts_job.name}}}},
    #default_status=DefaultScheduleStatus.RUNNING

)

ldcom_etl_dwh_schedule = ScheduleDefinition(
    name="ldcom_etl_dwh_schedule",
    cron_schedule="0 1 * * *",  # 01:00 AM every day
    execution_timezone="America/Tegucigalpa",
    job=ldcom_etl_dwh_job,
    default_status=DefaultScheduleStatus.RUNNING
)



all_schedules = get_all_instances_of_class([ScheduleDefinition])

__all__ = list(map(lambda x: x.name, all_schedules) )