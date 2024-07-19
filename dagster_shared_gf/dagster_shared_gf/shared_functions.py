
import inspect, os, requests, re, itertools
from typing import Dict, Any, Mapping, Literal, TypeVar, get_args, get_origin, List, Type, Callable, Iterable, Sequence
import hashlib
from types import ModuleType
from pydantic import Field
from datetime import timedelta
from pathlib import Path
from dagster import config_from_files, AssetsDefinition
from dagster_graphql import DagsterGraphQLClient
from dotenv import load_dotenv
from trycast import isassignable, eval_type

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
    #example_data = [{'python_module': {'module_name': 'dagster_kielsa_gf', 'working_directory': 'dagster_kielsa_gf', 'location_name': 'dagster_kielsa_gf'}}, {'python_module': {'module_name': 'dagster_sap_gf', 'working_directory': 'dagster_sap_gf', 'location_name': 'dagster_sap_gf'}}, {'another': {'location_name': 'xyz'}}]

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
def get_current_env():
  dagster_instance_current_env = os.getenv("DAGSTER_INSTANCE_CURRENT_ENV")
  assert dagster_instance_current_env != None, "Expected DAGSTER_INSTANCE_CURRENT_ENV, got None"  # env var must be set
  return dagster_instance_current_env

class DagsterInstanceCurrentEnv():
    """Holds the current env from dagster instance and useful methods"""	
    #env:str = get_current_env()
    def __init__(self):
        self.env:str = get_current_env()
        self.env_long_name: str
        if self.env == "dev":
            self.env_long_name = "development"
        elif self.env == "prod":
            self.env_long_name = "production"
        else:
            self.env_long_name = f"{self.env} no long name asigned"
        self.graphql_port = int(os.getenv('DAGSTER_GRAPHQL_PORT',3000))
    def __str__(self) -> str:
        return self.env

      
dagster_instance_current_env = DagsterInstanceCurrentEnv()

if __name__ == "__main__":
    print(get_all_locations_name())
    print(dagster_instance_current_env, type(dagster_instance_current_env))

def get_for_current_env(dict: dict[str:any] = {"dev" : "any_return_for_dev", "prd" : "any_return_for_prd"}, env:str = dagster_instance_current_env.env) -> Any:
    """
    Returns the value for the current env, defaults to dev when not found
    
    Parameters
    ----------
    dict : dict
        The dictionary to return the value from
    env : str
        The env [local, dev, prd] to return the value from
    """	
    return dict.get(env, dict.get("dev"))
    
def search_for_word_in_text(text: str, word: str) -> re.Match:
    return re.search(rf'\b{word}\b', text, re.IGNORECASE)


# Function to filter assets by tags
from typing import List, Mapping, Any, Literal, Union

def filter_assets_by_tags(assets_definitions: List[Union[Any, AssetsDefinition]],
                          tags: Mapping[str, str],
                          filter_type: Literal["all_tags_match", "any_tag_matches", "exclude_if_all_tags", "exclude_if_any_tag"] = "all_tags_match"
                          ) -> List[AssetsDefinition]:
    """
    Filters a list of assets based on the specified tags and filter type.

    Args:
        assets_definitions (List[Union[Any, AssetsDefinition]]): The list of assets to filter.
        tags (Mapping[str, str]): The tags to filter the assets by.
        filter_type (Literal["all_tags_match", "any_tag_matches", "exclude_if_all_tags", "exclude_if_any_tag"], optional): The filter type to use. Defaults to "all_tags_match".

    Returns:
        List[Union[Any, AssetsDefinition]]: The filtered list of assets.
    """
    def match_all(asset_tags: Mapping[str, str], tags: Mapping[str, str]) -> bool:
        return all(item in asset_tags.items() for item in tags.items())
    
    def match_any(asset_tags: Mapping[str, str], tags: Mapping[str, str]) -> bool:
        return any(item in asset_tags.items() for item in tags.items())

    filtered_assets = []
    asset_def: AssetsDefinition | Any
    for asset_def in assets_definitions:
        if isinstance(asset_def, AssetsDefinition):
            asset_tags = {}
            for key in asset_def.keys:
                asset_tags.update(asset_def.tags_by_key[key])

            if filter_type == "all_tags_match" and match_all(asset_tags, tags):
                filtered_assets.append(asset_def)
            elif filter_type == "any_tag_matches" and match_any(asset_tags, tags):
                filtered_assets.append(asset_def)
            elif filter_type == "exclude_if_all_tags" and not match_all(asset_tags, tags):
                filtered_assets.append(asset_def)
            elif filter_type == "exclude_if_any_tag" and not match_any(asset_tags, tags):
                filtered_assets.append(asset_def)
    
    return filtered_assets

import importlib

def get_full_type_info(obj: Any) -> str:
    """
    Gets the full typing information of the given object, including origin and type arguments.

    Args:
        obj (Any): The object to inspect.

    Returns:
        str: A string representation of the object's full type information.
    """
    def get_type_str(typ: Any) -> str:
        origin = get_origin(typ)
        args = get_args(typ)
        if origin and args:
            args_str = ', '.join(get_type_str(arg) for arg in args)
            return f"{origin.__name__}[{args_str}]"
        elif origin:
            return origin.__name__
        return str(typ)
    
    def infer_type(obj):
        if isinstance(obj, list):
            if len(obj) > 0:
                element_type = infer_type(obj[0])
                return List[element_type]
            else:
                return List
        elif isinstance(obj, Sequence) and not isinstance(obj, str):
            if len(obj) > 0:
                element_type = infer_type(obj[0])
                return Sequence[element_type]
            else:
                return Sequence
        else:
            return type(obj)
    
    obj_type = infer_type(obj)
    return get_type_str(obj_type)

def check_instance(obj: Any, class_type: Type[Any]) -> bool:
    """
    Recursively checks if an object is an instance of the specified class type, including nested iterables.

    Args:
        obj (Any): The object to check.
        class_type (Type[Any]): The class type to check against.

    Returns:
        bool: True if obj is an instance of class_type or nested within an iterable of class_type, False otherwise.
    """
    if isinstance(obj, Iterable) and isinstance(class_type, Iterable):
        return all(check_instance(o, t) for o, t in zip(obj, class_type))
    elif isinstance(obj, class_type):
        return True
    return False

def get_all_instances_of_class(class_type_list: Iterable[Type[Any]], module: ModuleType = None) -> List[Any]:
    """
    Returns a list of all instances of the specified class types in the given module or the caller's module if no module is provided.

    Args:
        class_type_list (Iterable[Type[Any]]): The class types to search for.
        module (ModuleType, optional): The module to search in. If not provided, the caller's module is used. Defaults to None.

    Returns:
        List[Any]: A list of all instances of the specified class types.
    """
    if module is None:
        caller_frame = inspect.currentframe().f_back
        module_to_use = inspect.getmodule(caller_frame)
    else:
        module_to_use = module

    all_instances_list = []
    for class_type in class_type_list:
        variables = {name: obj for name, obj in module_to_use.__dict__.items() if isassignable(obj, class_type)}
        all_instances_list.extend(variables.values())
    return list(itertools.chain(all_instances_list))
# Function to get mock arguments for a function
def get_mock_args(func: Callable) -> dict:
    def get_mock_value(annotation: Any) -> Any:
        mock_values = {
            int: 0,
            float: 0.0,
            bool: False,
            str: 'mock',
            list: [],
            dict: {},
            set: set(),
            tuple: (),
            bytes: b'',
            bytearray: bytearray(),
            timedelta: timedelta(hours=0),
            type(None): None
        }
        return mock_values.get(annotation, 'mnd')
    sig = inspect.signature(func)
    mock_args = {}

    for param in sig.parameters.values():
        if param.default != inspect.Parameter.empty:
            mock_args[param.name] = param.default
        else:
            try:
                mock_args[param.name] = get_mock_value(param.annotation)
            except:
                mock_args[param.name] = 'mnd'
    
    return mock_args

# Function to filter variables created by a specific function
def get_all_parent_instances_created_by_function(creation_function: Callable, module:ModuleType = None) -> List:
    # Create an instance using the provided function to determine its type
    mock_args = get_mock_args(creation_function)
    example_instance = creation_function(**mock_args)
    instance_type = type(example_instance)
    if module is None:
        caller_frame = inspect.currentframe().f_back
        module_to_use = inspect.getmodule(caller_frame)
    else:
        module_to_use = module

    # Collect all variables that are instances of the determined type
    return get_all_instances_of_class([instance_type], module_to_use)

def import_variable_from_module(variable_name: str, module: ModuleType = None) -> Any:
    """
    Imports a specified variable from a given module.

    Args:
        variable_name (str): The name of the variable to import.
        module (ModuleType, optional): The module to import from. If not provided, the caller's module is used. Defaults to None.

    Returns:
        The value of the specified variable from the module.

    Raises:
        AttributeError: If the specified variable is not found in the module.
    """
    if module is None:
        caller_frame = inspect.currentframe().f_back
        module_to_use = inspect.getmodule(caller_frame)
    else:
        module_to_use = module

    if not hasattr(module_to_use, variable_name):
        return None
        #raise AttributeError(f"Module '{module_to_use.__name__}' has no attribute '{variable_name}'")
    return getattr(module_to_use, variable_name)

def get_unique_hash_sha2_256(strings: List[str] | str, length: int = 18) -> str:
    charset:str ="utf-8"
    if isinstance(strings, str):
        strings = [strings]
    full_string:str = "-".join(strings)
    hash_str:str  = hashlib.sha256(full_string.encode(charset), usedforsecurity=False).hexdigest()
    return hash_str[:length]