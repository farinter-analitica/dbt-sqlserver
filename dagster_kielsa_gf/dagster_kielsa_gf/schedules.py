from datetime import datetime, timedelta

import pytz
from dagster import (
    DagsterRunStatus,
    DefaultScheduleStatus,
    RunsFilter,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    build_schedule_context,
)

from dagster_kielsa_gf.job_control_replicas import *
from dagster_kielsa_gf.jobs import *
from dagster_shared_gf import shared_variables as shared_vars
from dagster_shared_gf.shared_functions import (
    get_all_instances_of_class,
    get_for_current_env,
)

# cron: minute hour day month day_of_week, example daily at midnight: 0 0 * * *
# cron example daily at midnight mon-fri with numbers: 0 0 * * 1-5
env_str:str=shared_vars.env_str

# Helper function to determine if the run should be skipped
def should_skip_run(context: ScheduleEvaluationContext, timezone_str):
    # Parse the timezone string into a pytz timezone object
    tz = pytz.timezone(timezone_str)
    # Convert the current time to the same timezone    
    current_time = datetime.now(tz)
    # Set your threshold here, e.g., 5 minutes
    threshold = timedelta(minutes=120)

    # Check if the time difference is greater than the threshold
    #print(context.scheduled_execution_time.weekday())
    #return
    scheduled_time: datetime = context.scheduled_execution_time.astimezone(tz)
    return (current_time - scheduled_time) > threshold or env_str != "dev"

default_timezone: str = "America/Tegucigalpa"
# running_default_schedule_status: DefaultScheduleStatus = (lambda x= {"local":DefaultScheduleStatus.STOPPED,"dev":DefaultScheduleStatus.RUNNING,"prd":DefaultScheduleStatus.RUNNING}: x.get(env_str,x.get("dev")))
running_default_schedule_status: DefaultScheduleStatus = get_for_current_env(
    {
        "local": DefaultScheduleStatus.STOPPED,
        "dev": DefaultScheduleStatus.RUNNING,
        "prd": DefaultScheduleStatus.RUNNING,
    }
)
stopped_default_schedule_status: DefaultScheduleStatus = get_for_current_env(
    {
        "local": DefaultScheduleStatus.STOPPED,
        "dev": DefaultScheduleStatus.STOPPED,
        "prd": DefaultScheduleStatus.STOPPED,
    }
)
only_prd_default_schedule_status: DefaultScheduleStatus = get_for_current_env(
    {
        "local": DefaultScheduleStatus.STOPPED,
        "dev": DefaultScheduleStatus.STOPPED,
        "prd": DefaultScheduleStatus.RUNNING,
    }
)
only_dev_default_schedule_status: DefaultScheduleStatus = get_for_current_env(
    {
        "local": DefaultScheduleStatus.STOPPED,
        "dev": DefaultScheduleStatus.RUNNING,
        "prd": DefaultScheduleStatus.STOPPED,
    }
)

# Define the schedule, name defaults to the name of the job + _schedule
dbt_dwh_kielsa_marts_job_schedule = ScheduleDefinition(
    cron_schedule = get_for_current_env({"dev":"0 3 * * *","prd":"0 2 * * *"}),
    execution_timezone=default_timezone,
    job=dbt_dwh_kielsa_marts_job,
    default_status=stopped_default_schedule_status, #Se ejecutaran los orphan
)

# Define the schedule, name defaults to the name of the job + _schedule
dbt_dwh_kielsa_marts_orphan_assets_job_schedule = ScheduleDefinition(
    cron_schedule = get_for_current_env({"dev":"0 3 * * *","prd":"0 2 * * *"}),
    execution_timezone=default_timezone,
    job=dbt_dwh_kielsa_marts_orphan_assets_job,
    default_status=running_default_schedule_status,
)

ldcom_etl_dwh_job_schedule = ScheduleDefinition(
    cron_schedule = get_for_current_env({"dev":"0 2 * * *","prd":"0 1 * * *"}),  
    execution_timezone=default_timezone,
    job=ldcom_etl_dwh_job,
    default_status=stopped_default_schedule_status, #Se ejecutaran con los downstream jobs
)

dlt_etl_dwh_job_schedule = ScheduleDefinition(
    cron_schedule = get_for_current_env({"dev":"0 4 * * *","prd":"30 1 * * *"}),  
    execution_timezone=default_timezone,
    job=dlt_dwh_kielsa_all_downstream_job,
    default_status=running_default_schedule_status, 
)

kielsa_etl_dwh_all_downstream_job_schedule = ScheduleDefinition(
    cron_schedule = get_for_current_env({"dev":"0 1 * * *","prd":"10 0 * * *"}),  
    description=f"Selection: {str(kielsa_etl_dwh_all_downstream_job.selection)}",
    execution_timezone=default_timezone,
    job=kielsa_etl_dwh_all_downstream_job,
    default_status=running_default_schedule_status,
)

knime_workflows_all_downstream_job_schedule = ScheduleDefinition(
    cron_schedule = get_for_current_env({"dev":"20 4 * * 1-6","prd":"15 2 * * 1-6"}),  
    execution_timezone=default_timezone,
    job=knime_workflows_all_downstream_job,
    default_status=running_default_schedule_status,

)

knime_workflows_start_of_month_job_schedule = ScheduleDefinition(
    cron_schedule = get_for_current_env({"dev":"0 5 1 * *","prd":"30 5 1 * *"}),  
    execution_timezone=default_timezone,
    job=knime_workflows_start_of_month_job,
    default_status=only_prd_default_schedule_status, #Solo en PRD es necesario, destino unico por el momento.
)

comprobar_sinc_replicas_job_schedule = ScheduleDefinition(
    cron_schedule = get_for_current_env({"dev":"30 7-18 * * *","prd":"35 7-18 * * *"}),  
    execution_timezone=default_timezone,
    job=comprobar_sinc_replicas_job,
    default_status=only_prd_default_schedule_status, 
)


def should_exec_kielsa_hourly_job_run(
    context: ScheduleEvaluationContext,
    job_name: str = kielsa_hourly_job.name,
) -> bool:
    filters = RunsFilter(
        job_name=job_name,
        statuses=[DagsterRunStatus.STARTED],
    )
    if context.instance.get_runs(filters=filters):
        return False
    return True


kielsa_hourly_job_schedule = ScheduleDefinition(
    cron_schedule=get_for_current_env(
        dict={
            "dev": ["01 6-19 * * *", "01 23 * * *"],
            "prd": ["01 6-19 * * *", "01 23 * * *"],
        }
    ),  # cron template: hour minute day month day_of_week
    execution_timezone=default_timezone,
    job=kielsa_hourly_job,
    default_status=only_prd_default_schedule_status,
    should_execute=should_exec_kielsa_hourly_job_run,
)

kielsa_start_of_month_job_schedule = ScheduleDefinition(
    cron_schedule=get_for_current_env(
        dict={
            "dev": ["01 3 1 * *"],
            "prd": ["01 1 1 * *"],
        }
    ),  # cron template: hour minute day month day_of_week
    execution_timezone=default_timezone,
    job=kielsa_start_of_month_job,
    default_status=running_default_schedule_status,
)

def should_exec_kielsa_hourly_additional_job_run(
    context: ScheduleEvaluationContext,
    job_name: str = kielsa_hourly_additional_job.name,
) -> bool:
    filters = RunsFilter(
        job_name=job_name,
        statuses=[DagsterRunStatus.STARTED],
    )
    if context.instance.get_runs(filters=filters):
        return False
    return True

kielsa_hourly_additional_job_schedule = ScheduleDefinition(
    cron_schedule=get_for_current_env(
        dict={
            "dev": ["31 6-19 * * *", "31 23 * * *"],
            "prd": ["31 6-19 * * *", "31 23 * * *"],
        }
    ),  # cron template: hour minute day month day_of_week
    execution_timezone=default_timezone,
    job=kielsa_hourly_additional_job,
    default_status=only_prd_default_schedule_status,
    should_execute=should_exec_kielsa_hourly_additional_job_run,
)

kielsa_olap_kielsa_general_temp_dev_job_schedule = ScheduleDefinition(
    cron_schedule=get_for_current_env(
        dict={
            "dev": ["05 6-19 * * *", "05 23 * * *"],
            "prd": ["05 6-19 * * *", "05 23 * * *"],
        }
    ),  # cron template: hour minute day month day_of_week
    execution_timezone=default_timezone,
    job=kielsa_olap_kielsa_general_temp_dev_job,
    default_status=only_dev_default_schedule_status,
    should_execute=should_exec_kielsa_hourly_job_run,
)


all_schedules = get_all_instances_of_class([ScheduleDefinition])


if __name__ == "__main__":
    ##tests
    context = build_schedule_context(scheduled_execution_time=datetime.now())
    assert should_skip_run(context,default_timezone) is False
    context = build_schedule_context(scheduled_execution_time=datetime.now()-timedelta(days=1))
    assert should_skip_run(context,default_timezone) is True
   
    
    print(all_schedules)
