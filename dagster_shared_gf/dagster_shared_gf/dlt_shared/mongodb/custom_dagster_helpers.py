from collections.abc import Generator
from datetime import datetime
import decimal
from decimal import Decimal
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

import dlt
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    AutomationCondition,
    MaterializeResult,
    MetadataValue,
    asset,
)
from dagster_embedded_elt.dlt import DagsterDltTranslator
from dlt.common import pendulum
from dlt.common.normalizers.naming.snake_case import NamingConvention
from dlt.common.pipeline import LoadInfo
from dlt.common.schema.typing import TSchemaContractDict
from dlt.extract import DltResource, DltSource
from pydantic import Field, dataclasses

from dagster_shared_gf.dlt_shared.dlt_resources import (
    BaseDltPipeline,
    merge_dlt_dagster_metadata,
)
from dagster_shared_gf.shared_functions import get_for_current_env
from dagster_shared_gf.dlt_shared.mongodb.helpers import max_dt_with_lag_last_value_func

snake_case_normalizer = NamingConvention()
# logger = get_dagster_logger()


def default_date_fn():
    pendulum_dt = get_for_current_env(
        {
            "local": pendulum.now().subtract(days=15),
            "dev": pendulum.now().subtract(years=2),
            "prd": pendulum.now().subtract(years=5),
        }
    )
    return pendulum_dt


@dataclasses.dataclass(frozen=True, config={"arbitrary_types_allowed": True})
class DltIncrementalPartialConfig:
    cursor_path: str
    """The path to the incremental field in the document."""
    initial_value: pendulum.DateTime = Field(default_factory=default_date_fn)
    lag: float = 2

    def to_dict(self):
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True, config={"arbitrary_types_allowed": True})
class DltResourceCollectionConfig:
    collection_name: str
    primary_key: Optional[str | tuple] = None
    table_new_name: Optional[str] = None
    columns_hints: Optional[dict[str, Any]] = None
    columns_to_remove: Optional[tuple[str, ...]] = None
    columns_to_include: Optional[tuple[str, ...]] = None
    limit: Optional[int] = None
    cursor_path: Optional[str] = None
    initial_value: Optional[pendulum.DateTime] = None
    incrementals: Optional[tuple[DltIncrementalPartialConfig, ...]] = None
    lag_days: Optional[int] = None
    automation_condition: Optional[AutomationCondition] = None
    dep_pipeline_cursor: Optional[str] = None
    max_table_nesting: Optional[int] = None
    force_columns_type: Optional[dict[str, Any]] = None
    import_schema_path: Optional[str] = None
    export_schema_path: Optional[str] = None
    schema_contract: Optional[TSchemaContractDict] = None
    """
    You can control the following schema entities:

    tables - the contract is applied when a new table is created
    columns - the contract is applied when a new column is created on an existing table
    data_type - the contract is applied when data cannot be coerced into a data type associated with an existing column.
    You can use contract modes to tell dlt how to apply the contract for a particular entity:

    evolve: No constraints on schema changes.
    freeze: This will raise an exception if data is encountered that does not fit the existing schema, so no data will be loaded to the destination.
    discard_row: This will discard any extracted row if it does not adhere to the existing schema, and this row will not be loaded to the destination.
    discard_value: This will discard data in an extracted row that does not adhere to the existing schema, and the row will be loaded without this data.
    
    Example:
    {"tables": "freeze", "columns": "freeze", "data_type": "freeze"}
    https://dlthub.com/docs/general-usage/schema-contracts
    """

    def get_all_configs_dict(self) -> Mapping[str, str]:
        return {key: value for key, value in self.__dict__.items() if value is not None}


@dataclasses.dataclass
class DagsterDltTranslatorMongodb(DagsterDltTranslator):
    dataset_name: str
    collection: DltResourceCollectionConfig | None

    def get_asset_key(self, resource: DltResource) -> AssetKey:
        """
        Para evitar duplicados en pipelines multi columnas de updated_at y created_at
        """
        asset_key_list = (
            "DL_FARINTER",
            self.get_normalized_dataset_name(),
            resource.name,
        )
        return AssetKey(asset_key_list)

    def get_deps_asset_keys(self, resource: DltResource) -> Iterable[AssetKey]:
        """
        Origen

        """
        dependencies = (
            AssetKey(
                [
                    "mongodb",
                    self.get_normalized_dataset_name(),
                    resource.name,
                ]
            ),
        )

        if self.collection and self.collection.dep_pipeline_cursor:
            dependencies = (
                *dependencies,
                AssetKey(
                    [
                        "DL_FARINTER",
                        self.get_normalized_dataset_name(),
                        resource.name,
                        self.collection.dep_pipeline_cursor,
                    ]
                ),
            )

        return dependencies

    def get_metadata(self, resource: DltResource) -> Mapping[str, Any]:
        collection_dict = (
            self.collection.get_all_configs_dict() if self.collection else None
        )
        collection_meta = {}
        if collection_dict:
            collection_meta = {
                key: MetadataValue.text(str(value))
                for key, value in collection_dict.items()
            }
        return {
            **super().get_metadata(resource=resource),
            **collection_meta,
            **resource.explicit_args,  # type: ignore
            # , **{"columns_schema": resource.columns}
        }

    def get_automation_condition(
        self, resource: DltResource
    ) -> Optional[AutomationCondition]:
        if self.collection and self.collection.automation_condition:
            super_auto = super().get_automation_condition(resource)
            if super_auto:
                return self.collection.automation_condition | super_auto
            else:
                return self.collection.automation_condition

    def get_normalized_table_identifier(self, resource: DltResource) -> str:
        return snake_case_normalizer.normalize_table_identifier(resource.name)

    def get_normalized_column_identifier(self, column_identifier: str) -> str:
        return snake_case_normalizer.normalize_identifier(column_identifier)

    def get_normalized_dataset_name(self) -> str:
        return snake_case_normalizer.normalize_table_identifier(self.dataset_name)

    def get_normalized_cursor_path(self, resource: DltResource) -> str | None:
        return (
            snake_case_normalizer.normalize_identifier(
                resource.incremental.incremental.cursor_path
            )
            if resource.incremental and resource.incremental.incremental
            else None
        )

    def get_incremental_config(self) -> tuple[DltIncrementalPartialConfig, ...]:
        collection = self.collection
        if collection and collection.incrementals:
            return collection.incrementals
        else:
            return ()

    def get_import_schema_path(self, resource: DltResource) -> str | None:
        if self.collection:
            return self.collection.import_schema_path

    def get_export_schema_path(self, resource: DltResource) -> str | None:
        if self.collection:
            return self.collection.export_schema_path


class ComparableFunction:
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __call__(self, *args, **kwargs):
        return self.func(*self.args, *args, **{**self.kwargs, **kwargs})

    def __eq__(self, other):
        if isinstance(other, ComparableFunction):
            return self.func == other.func
        return self.func == other

    def __getattr__(self, name):
        return getattr(self.func, name)


def make_comparable(func, *args, **kwargs):
    return ComparableFunction(func, *args, **kwargs)


def force_columns_types(doc: Dict, force_dict: Optional[dict[str, Any]] = None) -> Dict:
    """
    Forces the specified columns to the given types in the document.

    Args:
        doc (Dict): The document in which columns are to be forced to types.
        force_dict (Optional[dict[str, Any]], optional): The dictionary of column names to types. Defaults to None.

    Returns:
        Dict: The modified document with the specified columns forced to types.
    """
    if force_dict is None:
        force_dict = {}

    # valid_types = [dict, list, int, float, str, bool, pendulum, datetime, Decimal]
    # valid_types: type: default
    valid_types = {
        dict: {},
        list: [],
        int: None,
        float: None,
        str: None,
        bool: None,
        pendulum.DateTime: None,
        datetime: None,
        Decimal: None,
    }
    cast_functions = {
        dict: dict,
        list: list,
        int: int,
        float: float,
        str: str,
        bool: bool,
        pendulum.DateTime: pendulum.parse,
        datetime: pendulum.parse,
        Decimal: lambda x: Decimal(x, context=decimal.Context(prec=24)).quantize(
            decimal.Decimal("0.00000001"), rounding=decimal.ROUND_HALF_UP
        ),
    }
    for column_name, column_type in force_dict.items():
        if not isinstance(column_type, type) or column_type not in valid_types.keys():
            raise NotImplementedError(
                f"Not implemented type '{column_type}' for column '{column_name}'"
            )

        if column_name in doc and not isinstance(doc[column_name], column_type):
            if doc[column_name] is None:
                doc[column_name] = valid_types[column_type]
            else:
                try:
                    doc[column_name] = cast_functions[column_type](doc[column_name])
                except (ValueError, TypeError):
                    doc[column_name] = None

    return doc


def remove_collection_columns(
    doc: Dict, remove_columns: Optional[Sequence[str]] = None
) -> Dict:
    """
    Removes the specified columns from the given document.

    Args:
        doc (Dict): The document from which columns are to be removed.
        remove_columns (Optional[tuple[str]], optional): The list of column names to be removed. Defaults to None.

    Returns:
        Dict: The modified document with the specified columns removed.
    """
    if remove_columns is None:
        remove_columns = []

    for column_name in remove_columns:
        # print(f"check {column_name}")
        if column_name in doc:
            # print(f"del {column_name}")
            del doc[column_name]

    return doc


def include_collection_columns(
    doc: Dict, include_columns: Optional[Sequence[str]] = None
): #-> Dict:
    """
    Removes the specified columns from the given document.

    Args:
        doc (Dict): The document from which columns are to be removed.
        include_columns (Optional[tuple[str]], optional): The list of column names to be included. Defaults to None.

    Returns:
        Dict: The modified document with the specified columns removed.
    """
    if include_columns is None:
        include_columns = []

    # with open(
    #     r"D:\python_p\Main_Dagster_DEV\dagster_shared_gf\dagster_shared_gf\dlt_shared\logs.log",
    #     "a",
    # ) as file:
    #     file.write(f"comparing {include_columns} with {str(doc.keys())}\n")
    # print(f"comparing {include_columns} with {str(doc.keys())}")
    yield {
        key: doc[key]
        for key in doc.keys()
        if key in include_columns
        or str(key).startswith("_dlt")    
        }


ColConfigs = tuple[DltResourceCollectionConfig, ...]


def process_collection_config(
    col_res: DltResource, collections_config: DltResourceCollectionConfig | None
) -> DltResource:
    """Process a collection resource with configs."""
    if not collections_config:
        return col_res

    c = collections_config
    col_res.add_limit(c.limit) if c.limit else None
    if c.table_new_name:
        col_res = col_res.apply_hints(table_name=c.table_new_name)

    if c.columns_hints:
        col_res = col_res.apply_hints(columns=c.columns_hints)

    if c.max_table_nesting:
        col_res.max_table_nesting = c.max_table_nesting

    if c.force_columns_type:

        def force_columns(doc):
            return force_columns_types(doc=doc, force_dict=c.force_columns_type)

        col_res = col_res.add_map(force_columns)

    if c.columns_to_remove:

        def remove_columns(doc):
            return remove_collection_columns(
                doc=doc, remove_columns=c.columns_to_remove
            )

        col_res = col_res.add_map(remove_columns)

    if c.columns_to_include:

        def include_columns(doc):
            return include_collection_columns(
                doc=doc, include_columns=c.columns_to_include
            )

        col_res = col_res.add_yield_map(include_columns)

    if isinstance(c.primary_key, str):
        col_res = col_res.apply_hints(
            columns={c.primary_key: {"data_type": "text", "precision": 50}}
        )
    elif isinstance(c.primary_key, tuple):
        col_res = col_res.apply_hints(
            columns={
                key: {"data_type": "text", "precision": 50} for key in c.primary_key
            }
        )

    if c.schema_contract:
        col_res = col_res.apply_hints(schema_contract=c.schema_contract)

    return col_res


# @dlt_assets(
#     dlt_source=mongodb_source,
#     name="mongodb_source_name",
#     group_name="mongodb_source_group",
#     dagster_dlt_translator=DagsterDltTranslator(),
#     dlt_pipeline=dlt_pipeline_dest_mssql_dwh.get_pipeline(
#     "mongodb_source_pipeline", "mongodb_source"
# )
# )
# def compute_collections(context: AssetExecutionContext, dlt: DagsterDltResource):

#     yield from dlt.run(context=context, dlt_source=mongodb_source)


def custom_runs_interator(
    dlt_resource: DltResource,
    collection_config: dict[str, DltResourceCollectionConfig] | None = None,
):
    if collection_config and dlt_resource.name in collection_config:
        incs = collection_config[dlt_resource.name].incrementals
        if incs:
            for i in incs:
                incremental_dlt = dlt.sources.incremental(
                    cursor_path=i.cursor_path,
                    initial_value=i.initial_value,  # .to_iso8601_string(),
                    # lag=i.lag,
                    last_value_func=make_comparable(
                        max_dt_with_lag_last_value_func, lag_days=i.lag
                    ),
                )
                yield dlt_resource.apply_hints(incremental=incremental_dlt)
        else:
            yield dlt_resource
    else:
        yield dlt_resource


def create_dlt_asset(
    dlt_resource: DltResource,
    dataset_name: str,
    dlt_t: DagsterDltTranslatorMongodb,
    collections_config_dict: dict[str, DltResourceCollectionConfig],
    pipeline_name: str,
    group_name: str | None = None,
    tags: Mapping[str, str] | None = None,
) -> AssetsDefinition:
    @asset(
        key=dlt_t.get_asset_key(dlt_resource),
        group_name=group_name,
        description=f"resource {dlt_t.get_asset_key(dlt_resource)} description {dlt_t.get_description} ",
        metadata=dlt_t.get_metadata(dlt_resource),
        deps=dlt_t.get_deps_asset_keys(dlt_resource),
        # compute_kind="dlt",
        kinds={
            "dlt",
            "mongodb",
            "sql_server",
        },
        tags=tags,
        automation_condition=dlt_t.get_automation_condition(dlt_resource),
    )
    def compute_dlt_asset(
        context: AssetExecutionContext, dlt_pipeline_dest_mssql_dwh: BaseDltPipeline
    ):
        target_pipeline_name = pipeline_name
        new_pipeline = dlt_pipeline_dest_mssql_dwh.get_pipeline(
            pipeline_name=target_pipeline_name,
            dataset_name=dataset_name,
            import_schema_path=dlt_t.get_import_schema_path(dlt_resource),
            export_schema_path=dlt_t.get_export_schema_path(dlt_resource),
        )
        # print(str(dlt_t.get_incremental_info()))
        context.log.info(
            {
                "Running dlt pipeline": target_pipeline_name,
                "resource": dlt_t.get_asset_key(dlt_resource),
                "dataset": dataset_name,
                "write_disposition": dlt_pipeline_dest_mssql_dwh.write_disposition,
                "directory": new_pipeline.pipelines_dir,
                "incremental": str(dlt_t.get_incremental_config()),
                "import_schema_path": dlt_t.get_import_schema_path(dlt_resource),
            }
        )
        extracted_resource_metadata = {}
        first_iteration = True
        for custom_run_resource in custom_runs_interator(
            dlt_resource, collection_config=collections_config_dict
        ):
            # print(custom_run_resource.columns)
            new_pipeline.activate()
            custom_run_resource.compute_table_schema()  # ? al parecerer arregla el problema de los tipos de datos
            custom_run_resource.columns
            if first_iteration and (
                dlt_pipeline_dest_mssql_dwh.write_disposition == "replace"
                or dlt_pipeline_dest_mssql_dwh.refresh is not None
                or dlt_pipeline_dest_mssql_dwh.drop_pending_packages
            ):
                new_pipeline.drop_pending_packages()
            load_info: LoadInfo = dlt_pipeline_dest_mssql_dwh.run_pipeline(
                custom_run_resource,
                new_pipeline,
                remove_config=not first_iteration,
            )
            context.log.info(
                f"Last cursor unbound value: {custom_run_resource.incremental.incremental.start_value if custom_run_resource.incremental and custom_run_resource.incremental.incremental else None}"
            )
            load_info.raise_on_failed_jobs()

            first_iteration = False

            if load_info:
                extracted_resource_metadata = merge_dlt_dagster_metadata(
                    extracted_resource_metadata,
                    dlt_pipeline_dest_mssql_dwh.extract_resource_metadata(
                        context, custom_run_resource, load_info, new_pipeline
                    ),
                )

        return MaterializeResult(
            asset_key=dlt_t.get_asset_key(dlt_resource),
            metadata=extracted_resource_metadata,
        )

    return compute_dlt_asset


def dlt_mongodb_asset_factory(
    dlt_source: DltSource,
    dataset_name: str,
    collections_config: tuple[DltResourceCollectionConfig, ...],
    base_pipeline_name: str,
    group_name: str | None = None,
) -> Generator[AssetsDefinition, None, None]:
    collections_config_dict = {c.collection_name: c for c in collections_config}
    dlt_source.root_key = True
    for resource in dlt_source.resources:
        process_collection_config(
            dlt_source.resources[resource], collections_config_dict.get(resource)
        )
        yield create_dlt_asset(
            dlt_resource=dlt_source.resources[resource],
            group_name=group_name,
            dlt_t=DagsterDltTranslatorMongodb(
                dataset_name=dataset_name,
                collection=collections_config_dict.get(resource),
            ),
            dataset_name=dataset_name,
            collections_config_dict=collections_config_dict,
            pipeline_name=base_pipeline_name + "_" + resource,
        )
