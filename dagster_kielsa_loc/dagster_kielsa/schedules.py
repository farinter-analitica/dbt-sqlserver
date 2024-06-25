

from dagster import ScheduleDefinition, DefaultScheduleStatus
from dagster_sap.jobs import dbt_dwh_sap_mart_datos_maestros_job, dbt_dwh_sap_marts_job
from dagster_shared_gf.shared_functions import get_all_instances_of_class 
#cron: minute hour day month day_of_week, example daily at midnight: 0 0 * * *
#cron example daily at midnight mon-fri with numbers: 0 0 * * 1-5


# Define the schedule
# dbt_dwh_sap_mart_datos_maestros_schedule = ScheduleDefinition(
#     name="dbt_dwh_sap_mart_datos_maestros_schedule",
#     cron_schedule="14 1 * * *",  # 10:01 AM every day
#     execution_timezone="America/Tegucigalpa",
#     job=dbt_dwh_sap_mart_datos_maestros_job,
#     default_status=DefaultScheduleStatus.RUNNING

# )



all_schedules = get_all_instances_of_class([ScheduleDefinition])

__all__ = all_schedules
