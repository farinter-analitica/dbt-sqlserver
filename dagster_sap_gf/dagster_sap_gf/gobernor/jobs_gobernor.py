#working with assets and jobs.py context as an overseer, do not import on definitions main.
from dagster import (
   Definitions,
   JobDefinition,
   ScheduleDefinition,
   job,
   load_assets_from_current_module,
)

import dagster_sap_gf
from dagster_sap_gf.jobs import *
from dagster_shared_gf.shared_functions import get_all_instances_of_class
from dagster_shared_gf.shared_ops import wait_if_job_running_to_execute_next_op

wait_if_job_running_to_execute_next_op = wait_if_job_running_to_execute_next_op(current_location_name=dagster_sap_gf.__name__)

# Define the job and add to definitions on main __init__.py
@job
def wait_if_job_running_to_execute_next_job():
   """Wait for the job to finish before executing the next job"""	
   wait_if_job_running_to_execute_next_op()   
  

# freshness_checks_sensor = build_sensor_for_freshness_checks(
#     freshness_checks=dagster_sap_gf.all_asset_freshness_checks,
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

