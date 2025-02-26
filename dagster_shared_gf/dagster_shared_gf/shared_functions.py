import hashlib
import inspect
import itertools
import os
import re
import textwrap
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

import polars as pl
import requests
from dagster import AssetsDefinition, config_from_files
from dagster_graphql import DagsterGraphQLClient
from dlt.common import pendulum
from scipy import stats
from trycast import isassignable

from dagster_shared_gf.utils.snake_case_normalizer import SnakeCase

normalize_str_to_snake_case = SnakeCase().normalize_identifier


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


def start_job_by_name(job_name: str, location_name: str) -> None:
    client = DagsterGraphQLClient(
        "localhost", port_number=int(os.getenv("DAGSTER_GRAPHQL_PORT", 3000))
    )
    client.submit_job_execution(
        job_name=job_name,
        repository_location_name=location_name,
        run_config={},
        tags={},
    )


def get_all_locations_name() -> list:
    dagster_home_env = os.getenv("DAGSTER_HOME")
    if dagster_home_env:
        home_dir = Path(dagster_home_env).resolve()
    workspace_yaml_path = os.path.join(home_dir, "workspace.yaml")
    locations_data = config_from_files([workspace_yaml_path]).get("load_from")
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

if __name__ == "__main__":
    print(get_all_locations_name())
    print(dagster_instance_current_env, type(dagster_instance_current_env))


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
    assets_definitions: Sequence[Union[Any, AssetsDefinition]],
    tags_to_match: Mapping[str, str],
    filter_type: Literal[
        "all_tags_match", "any_tag_matches", "exclude_if_all_tags", "exclude_if_any_tag"
    ] = "all_tags_match",
) -> Sequence[AssetsDefinition]:
    """
    Filters a list of assets based on the specified tags and filter type, only returns AssetsDefinition (not sources).

    Args:
        assets_definitions (Sequence[Union[Any, AssetsDefinition]]): The list of assets to filter.
        tags (Mapping[str, str]): The tags to filter the assets by.
        filter_type (Literal["all_tags_match", "any_tag_matches", "exclude_if_all_tags", "exclude_if_any_tag"], optional): The filter type to use. Defaults to "all_tags_match".

    Returns:
        Sequence[AssetsDefinition]: The filtered list of assets.
    """

    def match_all(asset_tags: Mapping[str, str], tags: Mapping[str, str]) -> bool:
        return all(item in asset_tags.items() for item in tags.items())

    def match_any(asset_tags: Mapping[str, str], tags: Mapping[str, str]) -> bool:
        return any(item in asset_tags.items() for item in tags.items())

    filtered_assets = []
    asset_def: AssetsDefinition | Any
    for asset_def in assets_definitions:
        if isinstance(asset_def, AssetsDefinition):
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
            print(f"{asset_def} is not an AssetsDefinition, skipping...")

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
    clean_name = normalize_str_to_snake_case(name)

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
    clean_string = normalize_str_to_snake_case(str(string))

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
    clean_string = normalize_str_to_snake_case(str(string))

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


class SQLScriptGenerator:
    _df: pl.DataFrame
    _df_schema: pl.Schema
    _db_name: str | None
    _db_schema: str
    _table_name: str
    _primary_keys: tuple[str, ...] = tuple()
    _temp_table_name: Optional[str]
    _schema_table_relation: str
    _schema_temp_table_relation: str
    _full_relation: str | None = None
    _df_schema: pl.Schema
    _formatted_primary_keys: tuple[str, ...]

    def __init__(
        self,
        df: pl.DataFrame,
        db_schema: str,
        table_name: str,
        sql_lang: Literal["sqlserver"] = "sqlserver",
        db_name: str | None = None,
        primary_keys: tuple[str, ...] = tuple(),
        temp_table_name: Optional[str] = None,
    ):
        if sql_lang not in ["sqlserver"]:
            raise ValueError(f"SQL language {sql_lang} not implemented.")

        self._df = df
        self._df_schema = df.collect_schema()
        self._db_name = db_name
        self._db_schema = db_schema
        self._table_name = table_name
        self._temp_table_name = temp_table_name
        self._primary_keys = primary_keys
        self._schema_table_relation = f"[{self.db_schema}].[{self.table_name}]"
        self._full_relation = (
            f"[{self.db_name}].[{self.db_schema}].[{self.table_name}]"
            if db_name
            else None
        )
        self._schema_temp_table_relation = (
            f"[{self.db_schema}].[{self.temp_table_name}]"
        )
        self._formatted_primary_keys = self._validate_and_format_pks(
            columns=self.primary_keys
        )

    @property
    def df(self) -> pl.DataFrame:
        if self._df is None:
            raise ValueError("DataFrame has not been set")
        return self._df

    @property
    def df_schema(self) -> pl.Schema:
        if self._df_schema is None:
            raise ValueError("DataFrame schema has not been set")
        return self._df_schema

    @property
    def db_name(self) -> str:
        if not self._db_name:
            raise ValueError("db_name name has not been set")
        return self._db_name

    @property
    def db_schema(self) -> str:
        if not self._db_schema:
            raise ValueError("Database schema has not been set")
        return self._db_schema

    @property
    def table_name(self) -> str:
        if not self._table_name:
            raise ValueError("Table name has not been set")
        return self._table_name

    @property
    def primary_keys(self) -> tuple[str, ...]:
        return self._primary_keys

    @property
    def temp_table_name(self) -> str:
        return self._temp_table_name or self._table_name

    @property
    def schema_table_relation(self) -> str:
        return self._schema_table_relation

    @property
    def full_relation(self) -> str:
        if not self._full_relation:
            raise ValueError(
                "full_relation name has not been set, provide db_name and db_schema to set it"
            )
        return self._full_relation

    @property
    def schema_temp_table_relation(self) -> str:
        return self._schema_temp_table_relation

    @property
    def formatted_primary_keys(self) -> tuple[str, ...]:
        return self._formatted_primary_keys

    def _validate_and_format_pks(
        self, columns: tuple[str, ...] = tuple()
    ) -> tuple[str, ...]:
        # Get the schema of the DataFrame
        schema = self.df_schema

        # Check correct primary keys
        verified_columns = deque()
        formatted_columns = deque()

        for pk in columns:
            if pk not in schema:
                raise ValueError(
                    f"Primary key {pk} not in schema, available keys: {str(schema.names())}"
                )
            # Check for duplicates
            if pk in verified_columns:
                raise ValueError(f"Duplicate primary key: {pk}")
            # Validate not nulls
            column_data = self.df.get_column(pk)
            if column_data.has_nulls():
                raise ValueError(f"Primary key {pk} cannot be null")

            verified_columns.append(pk)
            formatted_columns.append(f"[{pk}]")

        # Check for data duplicates
        column_data = self.df.select(verified_columns).to_struct("primary_key")
        duplicates = column_data.filter(column_data.is_duplicated())
        if duplicates.len() > 0:
            raise ValueError(
                f"Primary key {str(verified_columns)} cannot have duplicates, found {str(duplicates.limit(10))}"
            )

        return tuple(formatted_columns)

    def create_table_sql_script(self, temp: bool = False) -> str:
        """
        Generate a SQL script to create a table based on a Polars DataFrame schema.

        Returns:
        - str: The SQL script to create the table.
        """
        schema = self.df_schema
        primary_keys = self.primary_keys
        df = self.df
        schema_table_relation = (
            self.schema_table_relation if not temp else self.schema_temp_table_relation
        )

        # Initialize the SQL script
        sql_script = f"CREATE TABLE {schema_table_relation} (\n"

        TYPE_MAPPING = {
            pl.Int8: "TINYINT",
            pl.Int16: "SMALLINT",
            pl.Int32: "INT",
            pl.Int64: "BIGINT",
            pl.Float32: "FLOAT(24)",
            pl.Float64: "FLOAT(53)",
            pl.Boolean: "BIT",
            pl.Date: "DATE",
            pl.UInt8: "TINYINT",
            pl.UInt16: "SMALLINT",
            pl.UInt32: "INT",
            pl.UInt64: "BIGINT",
        }

        # Iterate over the columns in the schema
        for col_name, col_type in schema.items():
            # Map the Polars data type to a SQL Server data type
            # Get the basic type mapping first
            sql_type = TYPE_MAPPING.get(col_type)

            # Handle special cases
            if sql_type is None:
                if col_type == pl.Utf8:
                    string_lenght = df.get_column(col_name).str.len_chars().max()  # type: ignore
                    string_lenght: int = string_lenght if string_lenght else 0
                    if col_name in primary_keys and not string_lenght > 50:
                        sql_type = "NVARCHAR(50)"
                    elif string_lenght <= 100:
                        sql_type = "NVARCHAR(100)"
                    elif string_lenght <= 255:
                        # Use NVARCHAR for string columns with a maximum length of 255
                        sql_type = "NVARCHAR(255)"
                    elif col_name in primary_keys:
                        raise ValueError(
                            f"Primary key {col_name} shouldn't be longer than 255 characters"
                        )
                    else:
                        sql_type = "NVARCHAR(MAX)"
                elif col_type == pl.Datetime:
                    if col_type.time_zone is not None:  # type: ignore
                        sql_type = "DATETIMEOFFSET(0)"
                    else:
                        sql_type = "DATETIME2(0)"
                elif col_type == pl.Decimal:
                    # Use DECIMAL with precision and scale
                    precision = col_type.precision  # type: ignore
                    scale = col_type.scale  # type: ignore
                    sql_type = f"DECIMAL({precision}, {scale})"
                else:
                    raise ValueError(f"Unsupported data type: {col_type}")

            # Add the column definition to the SQL script
            sql_script += f"    [{col_name}] {sql_type}"

            # Check if the column is a primary key
            if col_name in primary_keys:
                sql_script += " NOT NULL"

            sql_script += ",\n"

        # Remove the trailing comma and newline
        sql_script = sql_script[:-2] + "\n);\n"

        return sql_script

    def drop_table_sql_script(self, temp: bool = False) -> str:
        table_name = self.table_name if not temp else self.temp_table_name
        schema_table_relation = (
            self.schema_table_relation if not temp else self.schema_temp_table_relation
        )
        return textwrap.dedent(f"""
            IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{self.db_schema}' 
                AND TABLE_NAME = '{table_name}') 
                DROP VIEW {schema_table_relation};
            """)

    def drop_view_sql_script(self, temp: bool = False) -> str:
        table_name = self.table_name if not temp else self.temp_table_name
        schema_table_relation = (
            self.schema_table_relation if not temp else self.schema_temp_table_relation
        )
        return textwrap.dedent(f"""
            IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS 
                WHERE TABLE_SCHEMA = '{self.db_schema}' 
                AND TABLE_NAME = '{table_name}') 
                DROP VIEW {schema_table_relation};
            """)

    def primary_key_table_sql_script(self, temp: bool = False) -> str:
        schema_table_relation = (
            self.schema_table_relation if not temp else self.schema_temp_table_relation
        )
        pk = self.primary_keys
        tn = self.table_name
        if len(pk) == 0:
            raise ValueError("No primary keys defined")
        # Add the nonclustered primary key with randon name
        dynamic_part = get_now_datetime().strftime("%Y%m%dT%H%M%S%f")
        sql_script = f"ALTER TABLE {schema_table_relation} ADD CONSTRAINT [pk_{tn}_{dynamic_part}] PRIMARY KEY NONCLUSTERED ({', '.join(pk)});\n"
        return sql_script

    def columnstore_table_sql_script(self, temp: bool = False) -> str:
        schema_table_relation = (
            self.schema_table_relation if not temp else self.schema_temp_table_relation
        )
        dynamic_part = get_now_datetime().strftime("%Y%m%dT%H%M%S%f")
        tn = self.table_name
        return f"CREATE CLUSTERED COLUMNSTORE INDEX [idx_{tn}_{dynamic_part}] ON {schema_table_relation};\n"

    def swap_table_with_temp(self) -> str:
        return textwrap.dedent(f"""
            -- Swap the tables
            BEGIN TRANSACTION;
            IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES t
                WHERE t.TABLE_NAME = '{self.table_name}' and t.TABLE_SCHEMA = '{self.db_schema}')
                EXEC sp_rename '{self.schema_table_relation}', '{self.table_name}_OLD';
            EXEC sp_rename '{self.schema_temp_table_relation}', '{self.table_name}';
            COMMIT TRANSACTION;

            -- Drop the old table
            DROP TABLE IF EXISTS [{self.db_schema}].[{self.table_name}_OLD];

            -- Drop the NEW temp table
            DROP TABLE IF EXISTS {self.schema_temp_table_relation};
        """)

    def bulk_insert_sql_script(
        self,
        file_path: str,
        temp: bool = False,
        codepage: str = "65001",  # UTF-8
        format: str = "CSV",
        first_row: int | None = None,
        tablock: bool = True,
        row_terminator: str = r"\r\n",
        field_terminator: str | None = None,
        field_quote: str | None = None,
        batch_size: int | None = None,
        rows_per_batch: int | None = None,
        max_errors: int = 0,
        order_columns: list[str] | None = None,
        format_file_path: str | None = None,
        error_file_path: str | None = None,
    ) -> str:
        schema_table_relation = (
            self.schema_table_relation if not temp else self.schema_temp_table_relation
        )
        """
        Generate a SQL script for BULK INSERT from a file.

        Args:
            file_path: Path to the source file
            codepage: Character encoding (default: 65001/UTF-8)
            format: File format (default: CSV)
            first_row: Starting row number (default: None)
            tablock: Whether to use table locking (default: True)
            row_terminator: Line ending character(s) (default: \r\n)
            field_terminator: Field separator character(s) (default: None)
            field_quote: Character used for quoting fields (default: None)
            batch_size: Size of each batch in bytes (default: None)
            rows_per_batch: Number of rows per batch (default: None)
            max_errors: Maximum allowed errors (default: 0 - no errors allowed)
            order_columns: List of columns for ordering (default: None)
            format_file_path: Path to format file (default: None)
            error_file_path: Path to error file (default: None)

        Returns:
            str: SQL BULK INSERT statement
        """
        options = []

        # Add basic options
        options.append(f"CODEPAGE = '{codepage}'")
        options.append(f"FORMAT = '{format}'")

        # Add conditional options
        if first_row is not None:
            options.append(f"FIRSTROW = {first_row}")

        if tablock:
            options.append("TABLOCK")

        if row_terminator is not None:
            options.append(f"ROWTERMINATOR = '{row_terminator}'")

        if field_terminator is not None:
            options.append(f"FIELDTERMINATOR = '{field_terminator}'")

        if field_quote is not None:
            options.append(f"FIELDQUOTE = '{field_quote}'")

        if batch_size is not None:
            options.append(f"BATCHSIZE = {batch_size}")

        if rows_per_batch is not None:
            options.append(f"ROWS_PER_BATCH = {rows_per_batch}")

        options.append(f"MAXERRORS = {max_errors}")

        if order_columns is not None and len(order_columns) > 0:
            options.append(f"ORDER ({', '.join(order_columns)})")

        if format_file_path is not None:
            options.append(f"FORMATFILE = '{format_file_path}'")

        if error_file_path is not None:
            options.append(f"ERRORFILE = '{error_file_path}'")

        # Format the options with proper indentation
        formatted_options = ",\n    ".join(options)

        # Construct the final SQL statement
        sql = textwrap.dedent(f"""BULK INSERT {schema_table_relation}
            FROM '{file_path}'
            WITH (
                {formatted_options}
            );""")

        return sql


if __name__ == "__main__":
    pass


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
