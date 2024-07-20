from dagster import ScheduleDefinition, DefaultScheduleStatus, RunsFilter, DagsterRunStatus, ScheduleEvaluationContext
from dagster_sap_gf.jobs import *
from dagster_shared_gf.shared_functions import (get_all_instances_of_class, get_for_current_env)
from dagster_shared_gf import shared_variables as shared_vars
# cron: minute hour day month day_of_week, example daily at midnight: 0 0 * * *
# cron example daily at midnight mon-fri with numbers: 0 0 * * 1-5
# cron template: hour minute day month day_of_week
env_str:str=shared_vars.env_str

default_timezone: str = "America/Tegucigalpa"
# running_default_schedule_status: DefaultScheduleStatus = (lambda x= {"local":DefaultScheduleStatus.STOPPED,"dev":DefaultScheduleStatus.RUNNING,"prd":DefaultScheduleStatus.RUNNING}: x.get(env_str,x.get("dev")))
running_default_schedule_status: DefaultScheduleStatus = get_for_current_env({"local":DefaultScheduleStatus.STOPPED
                                                                              ,"dev":DefaultScheduleStatus.RUNNING
                                                                              ,"prd":DefaultScheduleStatus.RUNNING})
stopped_default_schedule_status: DefaultScheduleStatus = get_for_current_env({"local":DefaultScheduleStatus.STOPPED
                                                                              ,"dev":DefaultScheduleStatus.STOPPED
                                                                              ,"prd":DefaultScheduleStatus.STOPPED})

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
    # name="dbt_dwh_sap_mart_schedule",
    cron_schedule=get_for_current_env(
        dict={"dev": "15 2 * * *", "prd": "30 1 * * *"}
    ),  # cron template: hour minute day month day_of_week
    execution_timezone=default_timezone,
    job=dbt_dwh_sap_marts_job,
    default_status=stopped_default_schedule_status,
)
sap_etl_dwh_all_downstream_job_schedule = ScheduleDefinition(
    cron_schedule=get_for_current_env(
        dict={"dev": "15 01 * * *", "prd": "05 00 * * *"}
    ),  # cron template: hour minute day month day_of_week
    execution_timezone=default_timezone,
    job=sap_etl_dwh_all_downstream_job,
    default_status=running_default_schedule_status,
)


def should_exec_sap_etl_dwh_hourly_all_downstream_job_run(
    context: ScheduleEvaluationContext,
    job_name: str = sap_etl_dwh_hourly_all_downstream_job.name,
) -> bool:
    filters = RunsFilter(
        job_name=job_name,
        statuses=[DagsterRunStatus.STARTED],
    )
    if context.instance.get_runs(filters=filters):
        return False
    return True


sap_etl_dwh_hourly_all_downstream_job_schedule = ScheduleDefinition(
    cron_schedule=get_for_current_env(
        dict={
            "dev": ["05 6-18 * * *", "15 23 * * *"],
            "prd": ["05 6-18 * * *", "15 23 * * *"],
        }
    ),  # cron template: hour minute day month day_of_week
    execution_timezone=default_timezone,
    job=sap_etl_dwh_hourly_all_downstream_job,
    default_status=get_for_current_env(
        {
            "local": DefaultScheduleStatus.STOPPED,
            "dev": DefaultScheduleStatus.STOPPED,
            "prd": DefaultScheduleStatus.RUNNING,
        }
    ),
    should_execute=should_exec_sap_etl_dwh_hourly_all_downstream_job_run,
)

dbt_dwh_sap_marts_all_orphan_job_schedule = ScheduleDefinition(
    cron_schedule=get_for_current_env(dict={"dev": "15 2 * * *", "prd": "30 1 * * *"}),
    execution_timezone=default_timezone,
    job=dbt_dwh_sap_marts_all_orphan_job,
    default_status=running_default_schedule_status,
)


all_schedules = get_all_instances_of_class([ScheduleDefinition])

__all__ = list(map(lambda x: x.name, all_schedules) )
