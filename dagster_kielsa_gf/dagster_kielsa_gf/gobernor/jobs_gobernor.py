#working with assets and jobs.py context as an overseer, do not import on definitions main.
from dagster_shared_gf.shared_functions import get_all_instances_of_class
from dagster_shared_gf.shared_ops import wait_if_job_running_to_execute_next_op
import dagster_kielsa_gf
from ..jobs import *
from dagster import (Definitions, load_assets_from_current_module, OpExecutionContext, job, op, ScheduleDefinition, JobDefinition, Field, DefaultScheduleStatus )


wait_if_job_running_to_execute_next_op = wait_if_job_running_to_execute_next_op(current_location_name=dagster_kielsa_gf.__name__)

# Define the job and add to definitions on main __init__.py
@job
def wait_if_job_running_to_execute_next_job():
   """Wait for the job to finish before executing the next job"""	
   wait_if_job_running_to_execute_next_op()   
  

# dbt_dwh_kielsa_marts_wait_schedule = ScheduleDefinition(
#     #name="dbt_dwh_kielsa_marts_wait_schedule",
#     cron_schedule="14 1 * * *",  # 01:14 AM every day
#     execution_timezone="America/Tegucigalpa",
#     job=wait_if_job_running_to_execute_next_job,
#     run_config={"ops": {"wait_if_job_running_to_execute_next_op": 
#                         {"config": {"wait_for_job": ldcom_etl_dwh_job.name
#                                     , "job_to_execute": dbt_dwh_kielsa_marts_job.name}}}},
#     default_status=DefaultScheduleStatus.RUNNING

# )

all_ops = load_assets_from_current_module()
all_jobs = get_all_instances_of_class([JobDefinition])
all_schedules = get_all_instances_of_class([ScheduleDefinition])

defs = Definitions(
    assets=all_ops,
    jobs=all_jobs,
    schedules=all_schedules
)
#print(ControlDefs.get_all_job_defs())

