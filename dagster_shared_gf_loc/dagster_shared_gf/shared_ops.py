import time
from dagster import op, OpExecutionContext, Field, Definitions, DagsterInstance
from dagster_shared_gf.shared_functions import get_job_status, start_job_by_name, verify_location_name, get_all_locations_name

def wait_if_job_running_to_execute_next_op(current_location_name:str) -> op:
    @op(description="Wait for a job to complete if running and afterwards execute another job.",
            config_schema={"wait_for_job":  str
                        , "job_to_execute":  str
                        , "current_location_name": Field(str, is_required=False, default_value=current_location_name)
                        , "job_status_to_wait_change": Field(str, is_required=False, default_value="STARTED")
                        , "max_seconds_to_wait": Field(int, is_required=False, default_value=3600)
                        , "check_interval_on_seconds": Field(int, is_required=False, default_value=30)})
    def wait_if_job_running_to_execute_next_op(context: OpExecutionContext) -> None:
        wait_for_job:str  = context.op_config["wait_for_job"]
        max_seconds_to_wait:int =  context.op_config["max_seconds_to_wait"]
        check_interval_on_seconds:int = context.op_config["check_interval_on_seconds"] 
        job_status_to_wait_change = context.op_config["job_status_to_wait_change"]
        job_to_execute:str = context.op_config["job_to_execute"]
        current_location_name = context.op_config["current_location_name"]
        if not verify_location_name(current_location_name):
            raise Exception(f"Invalid location name: {current_location_name}, list found on workspace.yaml: {get_all_locations_name()}")
        
        context.log.info(f"Waiting for job '{wait_for_job}' to complete if running and afterwards execute another job.")
        try:
            while True:
                wait_for_job_status = get_job_status(wait_for_job)
                context.log.info(f"Status of job '{wait_for_job}': {wait_for_job_status}")       
                if wait_for_job_status == job_status_to_wait_change:
                    context.log.info(f"The job '{wait_for_job}' is currently {job_status_to_wait_change}. Waiting for change...")
                    time.sleep(check_interval_on_seconds)  # Wait for one minute
                    max_seconds_to_wait -= check_interval_on_seconds
                else:
                    context.log.info(f"The job '{wait_for_job}' is not running.")
                    break  # Exit the loop
                if max_seconds_to_wait <= 0:
                    context.log.info(f"Timed out waiting for job '{wait_for_job}' to complete. Returning true to run the next job.")
                    break 
            context.log.info(f"Executing job '{job_to_execute}'")
            start_job_by_name(job_to_execute, location_name=current_location_name)
        except Exception as e:
            context.log.error(f"Failed to get status of job '{wait_for_job}': {e}")
        return
    return wait_if_job_running_to_execute_next_op