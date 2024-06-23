

from dagster import ScheduleDefinition, DefaultScheduleStatus
from dagster_sap.jobs import dbt_dwh_sap_mart_datos_maestros_job, dbt_dwh_sap_marts_job
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

dbt_dwh_sap_mart_schedule = ScheduleDefinition(
    name="dbt_dwh_sap_mart_schedule",
    cron_schedule="14 1 * * *",  # 10:01 AM every day
    execution_timezone="America/Tegucigalpa",
    job=dbt_dwh_sap_marts_job,
    default_status=DefaultScheduleStatus.RUNNING

)

import inspect,sys
# Function to filter functions by keyword
def get_all_schedules():
    caller_frame = inspect.currentframe().f_back
    caller_module = inspect.getmodule(caller_frame)

    # Include all relevant schedule types
    schedule_types = (ScheduleDefinition)

    # Collect all schedule instances from the caller's module
    schedules = {name: obj for name, obj in caller_module.__dict__.items() if isinstance(obj, schedule_types)} 
    # to list
    schedules_list = list(schedules.values())
    return schedules_list

# print([dbt_dwh_sap_mart_schedule])
# print(globals())
#print(get_instances_of_class(ScheduleDefinition))
# print(get_functions_by_keyword("_schedule"))

all_schedules = get_all_schedules()
# print("Funcion")
# print(all_schedules)
__all__ = all_schedules
# print("Correcto:")
# print([dbt_dwh_sap_mart_schedule])