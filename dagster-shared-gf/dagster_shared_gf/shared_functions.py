import hashlib
import inspect
import itertools
import os
import re
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path, PurePath
from types import ModuleType
from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Type,
    Union,
    get_args,
    get_origin,
)

import requests
import dagster as dg
from dagster_graphql import DagsterGraphQLClient
from dlt.common import pendulum
from scipy import stats
from trycast import isassignable
from dotenv import load_dotenv

from dagster_shared_gf.utils.snake_case_normalizer import SnakeCase

normalize_identifier = SnakeCase().normalize_identifier
normalize_table_identifier = SnakeCase().normalize_table_identifier


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
    # graphql_port = os.getenv('DAGSTER_GRAPHQL_PORT', '3000')  # Default to 3000 if not set
    graphql_port = os.getenv("DAGSTER_GRAPHQL_PORT", "3000")
    graphql_endpoint = f"http://localhost:{graphql_port}/graphql"
    response = requests.post(
        graphql_endpoint, json={"query": QUERY, "variables": {"jobName": job_name}}
    )
    # Check if the response is successful
    if response.status_code != 200:
        raise Exception(
            f"GraphQL query failed with status code {response.status_code}: {response.text}"
        )
    response_data = response.json()
    # Check if the response contains errors
    if "errors" in response_data:
        raise Exception(f"GraphQL query returned errors: {response_data['errors']}")
    # Check if the expected data is present
    if (
        "data" not in response_data
        or "pipelineRunsOrError" not in response_data["data"]
    ):
        raise Exception(f"Unexpected response format: {response_data}")
    pipeline_runs_or_error = response_data["data"]["pipelineRunsOrError"]
    # Check if the response contains a PythonError
    if "message" in pipeline_runs_or_error:
        raise Exception(
            f"GraphQL query returned a PythonError: {pipeline_runs_or_error['message']}\nStack: {pipeline_runs_or_error.get('stack', 'No stack trace available')}"
        )
    # Check if the results are present
    if "results" not in pipeline_runs_or_error or not pipeline_runs_or_error["results"]:
        raise Exception(f"No results found for job '{job_name}'")
    # Extract the status from the response
    status = pipeline_runs_or_error["results"][0]["status"]
    return status


def start_job_by_name(
    job_name: str,
    location_name: str | None,
    client: DagsterGraphQLClient | None = None,
    repository_name: str | None = None,
    run_config: dict | None = None,
    tags: dict | None = None,
    op_selection: list[str] | None = None,
) -> str:
    """
    Starts a Dagster job by name using the DagsterGraphQLClient.

    Args:
        job_name (str): The job's name.
        location_name (Optional[str]): The repository location name.
        client (Optional[DagsterGraphQLClient]): The Dagster GraphQL client to use. If None, a new client is created.
        repository_name (Optional[str]): The repository name.
        run_config (Optional[dict]): Run configuration for the job.
        tags (Optional[dict]): Tags for the job run.
        op_selection (Optional[list[str]]): List of ops to execute.

    Returns:
        str: The run id of the submitted pipeline run.
    """
    if client is None:
        client = DagsterGraphQLClient(
            "localhost", port_number=int(os.getenv("DAGSTER_GRAPHQL_PORT", 3000))
        )
    return client.submit_job_execution(
        job_name=job_name,
        repository_location_name=location_name,
        repository_name=repository_name,
        run_config=run_config,
        tags=tags,
        op_selection=op_selection,
    )


def get_all_locations_name() -> list:
    dagster_home_env = os.getenv("DAGSTER_HOME")
    home_dir = None
    locations_data = None
    if dagster_home_env:
        home_dir = Path(dagster_home_env).resolve()
        workspace_yaml_path = os.path.join(home_dir, "workspace.yaml")
        locations_data = dg.config_from_files([workspace_yaml_path]).get("load_from")
    # example_data = [{'python_module': {'module_name': 'dagster_kielsa_gf', 'working_directory': 'dagster_kielsa_gf', 'location_name': 'dagster_kielsa_gf'}}, {'python_module': {'module_name': 'dagster_sap_gf', 'working_directory': 'dagster_sap_gf', 'location_name': 'dagster_sap_gf'}}, {'another': {'location_name': 'xyz'}}]

    # get location_name of each location
    if not locations_data:
        raise Exception("No locations found in workspace.yaml")
    location_names = []
    for location in locations_data:
        location_names.append(list(location.values())[0]["location_name"])
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
    if not dagster_instance_current_env:
        load_dotenv()
    dagster_instance_current_env = os.getenv("DAGSTER_INSTANCE_CURRENT_ENV")
    assert dagster_instance_current_env is not None, (
        "Expected DAGSTER_INSTANCE_CURRENT_ENV, got None"
    )  # env var must be set
    return dagster_instance_current_env


class DagsterInstanceCurrentEnv:
    """Holds the current env from dagster instance and useful methods"""

    # env:str = get_current_env()
    def __init__(self):
        self.env: str = get_current_env()
        self.env_long_name: str
        if self.env == "dev":
            self.env_long_name = "development"
        elif self.env == "prd":
            self.env_long_name = "production"
        elif self.env == "local":
            self.env_long_name = "local"
        else:
            self.env_long_name = f"{self.env} no long name asigned"
        self.graphql_port = int(os.getenv("DAGSTER_GRAPHQL_PORT", 3000))

    def __str__(self) -> str:
        return self.env


dagster_instance_current_env = DagsterInstanceCurrentEnv()


def get_for_current_env(
    dict: dict[str, Any] = {"dev": "any_return_for_dev", "prd": "any_return_for_prd"},
    env: str = dagster_instance_current_env.env,
) -> Any:
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


def search_for_word_in_text(text: str, word: str) -> re.Match | None:
    return re.search(rf"\b{word}\b", text, re.IGNORECASE)


# Function to filter assets by tags


def filter_assets_by_tags(
    assets_definitions: Sequence[Union[Any, dg.AssetsDefinition]],
    tags_to_match: Mapping[str, str],
    filter_type: Literal[
        "all_tags_match", "any_tag_matches", "exclude_if_all_tags", "exclude_if_any_tag"
    ] = "all_tags_match",
) -> Sequence[dg.AssetsDefinition]:
    """
    Filters a list of assets based on the specified tags and filter type, only returns dg.AssetsDefinition (not sources).

    Args:
        assets_definitions (Sequence[Union[Any, dg.AssetsDefinition]]): The list of assets to filter.
        tags (Mapping[str, str]): The tags to filter the assets by.
        filter_type (Literal["all_tags_match", "any_tag_matches", "exclude_if_all_tags", "exclude_if_any_tag"], optional): The filter type to use. Defaults to "all_tags_match".

    Returns:
        Sequence[dg.AssetsDefinition]: The filtered list of assets.
    """

    def match_all(asset_tags: Mapping[str, str], tags: Mapping[str, str]) -> bool:
        return all(item in asset_tags.items() for item in tags.items())

    def match_any(asset_tags: Mapping[str, str], tags: Mapping[str, str]) -> bool:
        return any(item in asset_tags.items() for item in tags.items())

    filtered_assets = []
    asset_def: dg.AssetsDefinition | Any
    for asset_def in assets_definitions:
        if isinstance(asset_def, dg.AssetsDefinition):
            current_asset_tags = {}
            for key in asset_def.keys:
                current_asset_tags.update(asset_def.tags_by_key[key])

            if filter_type == "all_tags_match" and match_all(
                current_asset_tags, tags_to_match
            ):
                filtered_assets.append(asset_def)
            elif filter_type == "any_tag_matches" and match_any(
                current_asset_tags, tags_to_match
            ):
                filtered_assets.append(asset_def)
            elif filter_type == "exclude_if_all_tags" and not match_all(
                current_asset_tags, tags_to_match
            ):
                filtered_assets.append(asset_def)
            elif filter_type == "exclude_if_any_tag" and not match_any(
                current_asset_tags, tags_to_match
            ):
                filtered_assets.append(asset_def)
        else:
            print(f"{asset_def} is not an dg.AssetsDefinition, skipping...")

    return filtered_assets


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
            args_str = ", ".join(get_type_str(arg) for arg in args)
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


def get_all_instances_of_class(
    class_type_list: Iterable[Type[Any]],
    module: Optional[ModuleType] = None,
    namespace: Optional[dict] = None,
) -> tuple[Any]:
    """
    Returns a list of all instances of the specified class types in the given module or the caller's module if no module is provided.

    Args:
        class_type_list (Iterable[Type[Any]]): The class types to search for.
        module (ModuleType, optional): The module to search in. If not provided, the caller's module is used. Defaults to None.

    Returns:
        List[Any]: A list of all instances of the specified class types.
    """
    all_instances_dq = deque()
    if namespace:
        for class_type in class_type_list:
            variables = {
                name: obj
                for name, obj in namespace.items()
                if isinstance(obj, class_type)
            }
            all_instances_dq.extend(variables.values())
    else:
        if module is None:
            caller = inspect.stack()[1]
            module_to_use = inspect.getmodule(caller[0])
            if module_to_use is None:  # ERROR
                raise ModuleNotFoundError(f"Could not find module for {caller[3]}")

        else:
            module_to_use = module

        if module_to_use:
            for class_type in class_type_list:
                variables = {
                    name: obj
                    for name, obj in module_to_use.__dict__.items()
                    if isassignable(obj, class_type)
                }
                all_instances_dq.extend(variables.values())
    return tuple(itertools.chain(all_instances_dq))


# Function to get mock arguments for a function
def get_mock_args(func: Callable) -> dict:
    def get_mock_value(annotation: Any) -> Any:
        mock_values = {
            int: 0,
            float: 0.0,
            bool: False,
            str: "mock",
            list: [],
            dict: {},
            set: set(),
            tuple: (),
            bytes: b"",
            bytearray: bytearray(),
            timedelta: timedelta(hours=0),
            type(None): None,
        }
        return mock_values.get(annotation, "mnd")

    sig = inspect.signature(func)
    mock_args = {}

    for param in sig.parameters.values():
        if param.default != inspect.Parameter.empty:
            mock_args[param.name] = param.default
        else:
            try:
                mock_args[param.name] = get_mock_value(param.annotation)
            except Exception:
                mock_args[param.name] = "mnd"

    return mock_args


# Function to filter variables created by a specific function
def get_all_parent_instances_created_by_function(
    creation_function: Callable, module: ModuleType | None = None
) -> tuple:
    # Create an instance using the provided function to determine its type
    mock_args = get_mock_args(creation_function)
    example_instance = creation_function(**mock_args)
    instance_type = type(example_instance)
    if module is None:
        caller = inspect.stack()[1]
        module_to_use = inspect.getmodule(caller[0])
        if module_to_use is None:
            raise ModuleNotFoundError(f"Could not find module for {caller[3]}")
    else:
        module_to_use = module

    # Collect all variables that are instances of the determined type
    return get_all_instances_of_class([instance_type], module_to_use)


def import_variable_from_module(
    variable_name: str, module: ModuleType | None = None
) -> Any:
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
        caller = inspect.stack()[1]
        module_to_use = inspect.getmodule(caller[0])
        if module_to_use is None:
            raise ModuleNotFoundError(f"Could not find module for {caller[3]}")
    else:
        module_to_use = module

    if not hasattr(module_to_use, variable_name):
        return None
        # raise AttributeError(f"Module '{module_to_use.__name__}' has no attribute '{variable_name}'")
    return getattr(module_to_use, variable_name)


def get_unique_hash_sha2_256(strings: List[str] | str, length: int = 18) -> str:
    charset: str = "utf-8"
    if isinstance(strings, str):
        strings = [strings]
    full_string: str = "-".join(strings)
    hash_str: str = hashlib.sha256(
        full_string.encode(charset), usedforsecurity=False
    ).hexdigest()
    return hash_str[:length]


def clean_filename(filename: str) -> str:
    """
    Cleans a filename by removing or replacing unsafe characters and reducing multiple underscores to one,
    without altering the file extension.
    """
    # Split the filename and its extension
    name, ext = PurePath(filename).stem, PurePath(filename).suffix

    # Clean using a normalizer
    clean_name = normalize_identifier(name)

    # Replace multiple underscores with a single underscore
    clean_name = re.sub(r"_+", "_", clean_name)

    # Ensure no trailing underscore before the extension
    if clean_name.endswith("_"):
        clean_name = clean_name[:-1]

    # Ensure no leading underscore
    if clean_name.startswith("_"):
        clean_name = clean_name[1:]

    # Ensure not empty
    if len(clean_name) == 0:
        clean_name = "file"

    # Reattach the file extension
    return clean_name + ext.lower()


def clean_filename_to_key(string: str) -> str:
    """
    Cleans a filename by removing or replacing unsafe characters and reducing multiple underscores to one.
    """

    # Clean using a normalizer
    clean_string = normalize_identifier(str(string))

    # Replace multiple underscores with a single underscore
    clean_string = re.sub(r"_+", "_", clean_string)

    # Ensure no trailing underscore
    if clean_string.endswith("_"):
        clean_string = clean_string[:-1]

    # Ensure no leading underscore
    if clean_string.startswith("_"):
        clean_string = clean_string[1:]

    # Ensure not empty
    if len(clean_string) == 0:
        clean_string = "file"

    return clean_string


def normalize_string(string: str) -> str:
    """
    Cleans a string by removing or replacing unsafe characters and reducing multiple underscores to one.
    """

    # Clean using a normalizer
    clean_string = normalize_identifier(str(string))

    # Replace multiple underscores with a single underscore
    clean_string = re.sub(r"_+", "_", clean_string)

    # Ensure no trailing underscore
    if clean_string.endswith("_"):
        clean_string = clean_string[:-1]

    # Ensure no leading underscore
    if clean_string.startswith("_"):
        clean_string = clean_string[1:]

    return clean_string


def clean_phone_number(phone: str, country_code: str = "HN") -> str:
    """
    Clean phone numbers based on country patterns

    Args:
        phone: Phone number to clean
        country_code: Two letter country code (HN, GT, NI, CR, SV, PA)

    Returns:
        Cleaned phone number or empty string if invalid
    """
    patterns = {
        "HN": r"^(?:\+?504[-\s]?[23789]\d{7}|[23789]\d{7})$",
        "GT": r"^(?:\+?502[-\s]?\d{8}|[234567]\d{7})$",
        "NI": r"^(?:\+?505[-\s]?\d{8}|[23578]\d{7})$",
        "CR": r"^(?:\+?506[-\s]?\d{8}|[245678]\d{7})$",
        "SV": r"^(?:\+?503[-\s]?\d{8}|[67]\d{7})$",
        "PA": r"^(?:\+?507[-\s]?\d{8}|[26]\d{7})$",
    }

    if not phone:
        return ""

    # Remove any non-digit characters
    cleaned = re.sub(r"\D", "", str(phone))

    # Check if matches country pattern
    pattern = patterns.get(country_code.upper())
    if pattern and re.match(pattern, cleaned):
        return cleaned

    return ""


def get_function_path(func):
    return f"{func.__module__}.{func.__qualname__}"


def get_now_datetime() -> datetime:
    return datetime.now()


def pendulum_dt_to_datetime(dt: pendulum.DateTime) -> datetime:
    tz_info = dt.tzinfo

    if tz_info is None:
        return datetime(
            dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond
        )

    return datetime(
        dt.year,
        dt.month,
        dt.day,
        dt.hour,
        dt.minute,
        dt.second,
        dt.microsecond,
        tz_info,
    )


def get_chi_square_threshold(confidence_level: float) -> float:
    """
    Calculate the chi-square critical value from a confidence level percentage.

    Args:
        confidence_level: Confidence level as percentage (e.g., 95 for 95%)

    Returns:
        Chi-square critical value
    """
    # Convert confidence percentage to probability
    probability = confidence_level / 100

    # Get chi-square critical value for 1 degree of freedom
    critical_value = float(stats.chi2.ppf(probability, df=1))

    return critical_value


def calculate_file_checksum(file_path: Union[str, Path]) -> str:
    """
    Calculate a fast and secure checksum of a file using BLAKE2b.

    Args:
        file_path: Path to the file (string or Path object)

    Returns:
        Hexadecimal digest of the file content
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # BLAKE2b is both fast and secure
    hash_method = hashlib.blake2b()
    with open(file_path, "rb") as file:
        # 64KB chunks for optimal performance
        for chunk in iter(lambda: file.read(65536), b""):
            hash_method.update(chunk)
    return hash_method.hexdigest()


# Function alias
calculate_file_hash = calculate_file_checksum


def get_current_location_name(
    context: dg.HookContext | dg.OpExecutionContext | dg.AssetExecutionContext,
) -> str | None:
    """
    Retrieves the location name from the remote job origin associated with the current Dagster run.
    This only works if the run has a remote job origin (a dagster client is running).

    Args:
        context (dg.HookContext | dg.OpExecutionContext | dg.AssetExecutionContext):
            The Dagster execution context from which to extract the run information.

    Returns:
        str: The name of the remote code location.

    Raises:
        ValueError: If the run cannot be retrieved or if the remote job origin or location name is unavailable.
    """
    run = context.instance.get_run_by_id(context.run_id)
    if run is None:
        raise ValueError("No se pudo obtener el run")
    remote_job_origin = run.remote_job_origin
    if remote_job_origin:
        return remote_job_origin.repository_origin.code_location_origin.location_name
    return None
