from dagster import ScheduleDefinition
from .jobs import dbt_dwh_farinter_mart_datos_maestros_job
#cron: minute hour day month day_of_week, example daily at midnight: 0 0 * * *
#cron example daily at midnight mon-fri with numbers: 0 0 * * 1-5


# Define the schedule
dbt_dwh_farinter_mart_datos_maestros_schedule = ScheduleDefinition(
    name="dbt_dwh_farinter_mart_datos_maestros_schedule",
    cron_schedule="14 1 * * *",  # 10:01 AM every day
    execution_timezone="America/Tegucigalpa",
    job=dbt_dwh_farinter_mart_datos_maestros_job
)