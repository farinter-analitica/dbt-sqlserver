import time
from dagster import op, OpExecutionContext, Field, Definitions
from dagster_shared_gf.shared_functions import get_job_status

def wait_if_job_running_to_execute_next_op(defs: Definitions):
    @op(description="Wait for a job to complete if running and afterwards execute another job.",
            config_schema={"wait_for_job":  str
                        , "job_to_execute":  str
                        , "wait_for_job_status_to_change": Field(str, is_required=False, default_value="STARTED")
                        , "max_seconds_to_wait": Field(int, is_required=False, default_value=3600)
                        , "check_interval_on_seconds": Field(int, is_required=False, default_value=30)})
    def wait_if_job_running_to_execute_next_op(context: OpExecutionContext) -> None:
        wait_for_job = context.op_config["wait_for_job"]
        max_seconds_to_wait =  context.op_config["max_seconds_to_wait"]
        check_interval_on_seconds = context.op_config["check_interval_on_seconds"] 
        wait_for_job_status_to_change = context.op_config["wait_for_job_status_to_change"]
        job_to_execute = context.op_config["job_to_execute"]
        try:
            while True:
                wiat_for_job_status = get_job_status(wait_for_job)
                context.log.info(f"Status of job '{wait_for_job}': {wiat_for_job_status}")       
                if wiat_for_job_status == wait_for_job_status_to_change:
                    context.log.info(f"The job '{wait_for_job}' is currently {wait_for_job_status_to_change}. Waiting for change...")
                    time.sleep(check_interval_on_seconds)  # Wait for one minute
                    max_seconds_to_wait -= check_interval_on_seconds
                else:
                    context.log.info(f"The job '{wait_for_job}' is not running.")
                    break  # Exit the loop
                if max_seconds_to_wait <= 0:
                    context.log.info(f"Timed out waiting for job '{wait_for_job}' to complete. Returning true to run the next job.")
                    break 
            context.log.info(f"Executing job '{job_to_execute}'")
            defs.get_job_def(job_to_execute).execute_in_process()
        except Exception as e:
            context.log.error(f"Failed to get status of job '{wait_for_job}': {e}")
        return
    return wait_if_job_running_to_execute_next_op