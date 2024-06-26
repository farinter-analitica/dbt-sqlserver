from dagster import (AssetExecutionContext, OpExecutionContext, asset, op, execute_job, DagsterInstance, Field, RepositoryDefinition )
import requests
import time
from dagster_kielsa import dagster_kielsa_repo


def get_job_status(job_name: str) -> str:
    # Define the GraphQL query to get the status of a specific job
    QUERY = """
    query JobStatus($jobName: String!) {
    pipelineRunsOrError(filter: {pipelineName: $jobName}) {
        ... on PipelineRuns {
        results {
            status
        }
        }
        ... on PythonError {
        message
        stack
        }
    }
    }
    """

   # Retrieve the GraphQL server port from the environment variable
    #graphql_port = os.getenv('DAGSTER_GRAPHQL_PORT', '3000')  # Default to 3000 if not set
    graphql_endpoint = f'http://localhost:3000/graphql'
    response = requests.post(
        graphql_endpoint,
        json={'query': QUERY, 'variables': {'jobName': job_name}}
    )
    # Check if the response is successful
    if response.status_code != 200:
        raise Exception(f"GraphQL query failed with status code {response.status_code}: {response.text}")
    response_data = response.json()
    # Check if the response contains errors
    if 'errors' in response_data:
        raise Exception(f"GraphQL query returned errors: {response_data['errors']}")
    # Check if the expected data is present
    if 'data' not in response_data or 'pipelineRunsOrError' not in response_data['data']:
        raise Exception(f"Unexpected response format: {response_data}")
    pipeline_runs_or_error = response_data['data']['pipelineRunsOrError']
    # Check if the response contains a PythonError
    if 'message' in pipeline_runs_or_error:
        raise Exception(f"GraphQL query returned a PythonError: {pipeline_runs_or_error['message']}\nStack: {pipeline_runs_or_error.get('stack', 'No stack trace available')}")
    # Check if the results are present
    if 'results' not in pipeline_runs_or_error or not pipeline_runs_or_error['results']:
        raise Exception(f"No results found for job '{job_name}'")
    # Extract the status from the response
    status = pipeline_runs_or_error['results'][0]['status']
    return status


@op(description="Wait for a job to complete if running and afterwards execute another job.",
        config_schema={"wait_for_job":  str, "job_to_execute": str})
def wait_if_job_running_to_execute_next_op(context: OpExecutionContext) -> None:
    wait_for_job = context.op_config["wait_for_job"]
    job_to_execute =  context.op_config["job_to_execute"]
    dagster_instance = DagsterInstance.get()
    ### TODO ###
    my_repo = None #dagster_kielsa_repo.get_repository()
    job_to_execute = None#my_repo.get_job( name=job_to_execute)
    try:
        while True:
            other_job_status = get_job_status(wait_for_job)
            context.log.info(f"Status of job '{wait_for_job}': {other_job_status}")       
            if other_job_status == "STARTED":
                context.log.info(f"The job '{wait_for_job}' is currently running. Waiting for completion...")
                time.sleep(60)  # Wait for one minute
            else:
                context.log.info(f"The job '{wait_for_job}' is not running.")
                break  # Exit the loop
        execute_job(job_to_execute, dagster_instance)
    except Exception as e:
        context.log.error(f"Failed to get status of job '{wait_for_job}': {e}")
