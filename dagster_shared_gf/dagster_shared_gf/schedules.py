from dagster import (ScheduleDefinition, 
                     DefaultScheduleStatus, 
                     ScheduleEvaluationContext, 
                     build_schedule_context,
                     RunsFilter,
                     DagsterRunStatus)
from dagster_shared_gf.jobs import *
from dagster_shared_gf.shared_functions import (get_all_instances_of_class, get_for_current_env)
from dagster_shared_gf import shared_variables as shared_vars
from datetime import datetime, timedelta
import pytz
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
shared_daily_assets_job_schedule = ScheduleDefinition(
    cron_schedule = get_for_current_env({"dev":"1 0 * * *","prd":"1 0 * * *"}),
    execution_timezone=default_timezone,
    job=shared_daily_assets_job,
    default_status=running_default_schedule_status, 
)

def should_exec_shared_hourly_job_run(
    context: ScheduleEvaluationContext,
    job_name: str = shared_hourly_assets_job.name,
) -> bool:
    filters = RunsFilter(
        job_name=job_name,
        statuses=[DagsterRunStatus.STARTED],
    )
    if context.instance.get_runs(filters=filters):
        return False
    return True


shared_hourly_job_schedule = ScheduleDefinition(
    cron_schedule=get_for_current_env(
        dict={
            "dev": ["00 6-19 * * *", "00 23 * * *"],
            "prd": ["00 6-19 * * *", "00 23 * * *"],
        }
    ),  # cron template: hour minute day month day_of_week
    execution_timezone=default_timezone,
    job=shared_hourly_assets_job,
    default_status=running_default_schedule_status,
    should_execute=should_exec_shared_hourly_job_run,
)

all_schedules = get_all_instances_of_class([ScheduleDefinition])

__all__ = list(map(lambda x: x.name, all_schedules) )

if __name__ == "__main__":
    ##tests
    context = build_schedule_context(scheduled_execution_time=datetime.now())
    assert should_skip_run(context,default_timezone) is False
    context = build_schedule_context(scheduled_execution_time=datetime.now()-timedelta(days=1))
    assert should_skip_run(context,default_timezone) is True
   
    
    print(all_schedules)
