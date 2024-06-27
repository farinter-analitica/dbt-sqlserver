
import inspect, os, requests
from typing import Dict
from pathlib import Path
from typing import List, Type, Callable
from dagster import config_from_files
from dagster_graphql import DagsterGraphQLClient

# Function to filter functions by keyword
def get_all_instances_of_class(class_type_list):
    caller_frame = inspect.currentframe().f_back
    caller_module = inspect.getmodule(caller_frame)

    # convert to tuple if list received

    types_tuple = tuple(class_type_list)

    # Collect all schedule instances from the caller's module
    all_instances = {name: obj for name, obj in caller_module.__dict__.items() if isinstance(obj, types_tuple)} 
    # to list
    all_instances_list = list(all_instances.values())
    return all_instances_list

    # print([dbt_dwh_sap_mart_schedule])
    # print(globals())
    #print(get_instances_of_class(ScheduleDefinition))
    # print(get_functions_by_keyword("_schedule"))

# Function to get mock arguments for a function
def get_mock_args(func: Callable) -> dict:
    sig = inspect.signature(func)
    mock_args = {}
    for param in sig.parameters.values():
        if param.default != inspect.Parameter.empty:
            mock_args[param.name] = param.default
        elif param.annotation == int:
            mock_args[param.name] = 0
        elif param.annotation == float:
            mock_args[param.name] = 0.0
        elif param.annotation == bool:
            mock_args[param.name] = False
        elif param.annotation == list:
            mock_args[param.name] = []
        elif param.annotation == dict:
            mock_args[param.name] = {}
        else:
            mock_args[param.name] = 'mock'
    return mock_args

# Function to filter variables created by a specific function
def get_variables_created_by_function(creation_function: Callable) -> List:
    # Create an instance using the provided function to determine its type
    mock_args = get_mock_args(creation_function)
    example_instance = creation_function(**mock_args)
    instance_type = type(example_instance)

    caller_frame = inspect.currentframe().f_back
    caller_module = inspect.getmodule(caller_frame)

    # Collect all variables that are instances of the determined type
    variables = {name: obj for name, obj in caller_module.__dict__.items() if isinstance(obj, instance_type)}
    return list(variables.values())


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
    graphql_port = os.getenv('DAGSTER_GRAPHQL_PORT', '3000')
    graphql_endpoint = f'http://localhost:{graphql_port}/graphql'
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



def start_job_by_name(job_name: str, location_name: str) -> None:
    client = DagsterGraphQLClient("localhost", port_number=int(os.getenv('DAGSTER_GRAPHQL_PORT', 3000)))
    client.submit_job_execution(job_name=job_name, repository_location_name=location_name, run_config={}, tags={})

def get_all_locations_name() -> list:
    home_dir = Path(os.getenv('DAGSTER_HOME')).resolve()
    workspace_yaml_path = os.path.join(home_dir, 'workspace.yaml')
    locations_data = config_from_files([workspace_yaml_path]).get('load_from')
    #example_data = [{'python_module': {'module_name': 'dagster_kielsa', 'working_directory': 'dagster_kielsa_loc', 'location_name': 'dagster_kielsa'}}, {'python_module': {'module_name': 'dagster_sap', 'working_directory': 'dagster_sap_loc', 'location_name': 'dagster_sap'}}, {'another': {'location_name': 'xyz'}}]

    #get location_name of each location
    if not locations_data:
        raise Exception("No locations found in workspace.yaml")
    location_names = []
    for location in locations_data:
        location_names.append(list(location.values())[0]['location_name'])
      #  location_names.append(next(iter(location.values()))['location_name'])
      #  for value in location: 
    return location_names

def verify_location_name(location_name: str) -> bool:
    locations = get_all_locations_name()
    if location_name in locations:
        return True
    else:
        return False

if __name__ == "__main__":
    print(get_all_locations_name())