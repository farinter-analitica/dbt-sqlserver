from collections import deque
from collections.abc import Generator
import functools
from itertools import chain
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    AutomationCondition,
    MaterializeResult,
    MetadataValue,
    asset,
    get_dagster_logger,
)
from dagster_embedded_elt.dlt import DagsterDltTranslator
from dagster_shared_gf.dlt_shared.dlt_resources import (
    BaseDltPipeline,
    merge_dlt_dagster_metadata,
)
from dagster_shared_gf.dlt_shared.mongodb import mongodb_collection
from dagster_shared_gf.dlt_shared.mongodb.helpers import max_dt_with_lag_last_value_func
from dagster_shared_gf.shared_functions import get_for_current_env
from dlt.common import pendulum
from dlt.common.pipeline import LoadInfo
from dlt.extract.resource import DltResource
from pydantic import Field, dataclasses
from dlt.common.normalizers.naming.snake_case import NamingConvention
import dlt
from functools import partial, update_wrapper
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

snake_case_normalizer = NamingConvention()
logger = get_dagster_logger()

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
    lag_days: int = 1


@dataclasses.dataclass(frozen=True, config={"arbitrary_types_allowed": True})
class DltResourceCollection:
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

    def get_all_configs_dict(self) -> Mapping[str, Any]:
        return {key: value for key, value in self.__dict__.items() if value is not None}


@dataclasses.dataclass(frozen=True, config={"arbitrary_types_allowed": True})
class DltPipelineSourceConfig:
    connection_string: str
    database: str
    pipeline_base_name: str
    dataset_name: str
    dagster_group_name: str
    primary_key: str | tuple
    cursor_path: Optional[str] = None
    initial_value: pendulum.DateTime = Field(default_factory=default_date_fn)
    incrementals: Optional[tuple[DltIncrementalPartialConfig, ...]] = None
    lag_days: Optional[int] = 1
    collections: tuple[DltResourceCollection, ...] = Field(default_factory=tuple)
    dep_pipeline_cursor: Optional[str] = None

    def __post_init__(self):
        for collection in self.collections:
            if collection.primary_key is None:
                object.__setattr__(collection, "primary_key", self.primary_key)
            if collection.cursor_path is None:
                object.__setattr__(collection, "cursor_path", self.cursor_path)
            if collection.initial_value is None:
                object.__setattr__(collection, "initial_value", self.initial_value)
            if collection.incrementals is None:
                object.__setattr__(collection, "incrementals", self.incrementals)
            if collection.lag_days is None:
                object.__setattr__(collection, "lag_days", self.lag_days)
            if collection.dep_pipeline_cursor is None:
                object.__setattr__(
                    collection, "dep_pipeline_cursor", self.dep_pipeline_cursor
                )

    def get_all_configs_dict(self):
        return {key: value for key, value in self.__dict__.items() if value is not None}


@dataclasses.dataclass
class DagsterDltTranslatorMongodbCRMHN(DagsterDltTranslator):
    config: DltPipelineSourceConfig
    collection: DltResourceCollection

    def get_asset_key(self, resource: DltResource) -> AssetKey:
        """
        Para evitar duplicados en pipelines multi columnas de updated_at y created_at
        """
        asset_key_list = (
            "DL_FARINTER",
            self.get_normalized_dataset_name(),
            resource.name,
        )
        if self.collection.cursor_path:
            asset_key_list = (*asset_key_list, self.collection.cursor_path)
        return AssetKey(asset_key_list)

    def get_deps_asset_keys(self, resource: DltResource) -> Iterable[AssetKey]:
        """
        Origen

        """
        dependencies = (
            AssetKey(
                [
                    "mongo_db",
                    self.get_normalized_dataset_name(),
                    resource.name,
                ]
            ),
        )

        if self.collection.dep_pipeline_cursor:
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

    def get_collection_metadata(self, resource: DltResource) -> Mapping[str, Any]:
        collection_dict = self.collection.get_all_configs_dict()
        collection_meta = {}
        if collection_dict:
            collection_meta = {
                key: MetadataValue.text(str(value))
                for key, value in collection_dict.items()
            }
        return {
            **collection_meta
            # , **resource.explicit_args  # type: ignore
            # , **{"columns_schema": resource.columns}
        }

    def get_normalized_table_identifier(self, resource: DltResource) -> str:
        return snake_case_normalizer.normalize_table_identifier(resource.name)

    def get_normalized_column_identifier(self, column_identifier: str) -> str:
        return snake_case_normalizer.normalize_identifier(column_identifier)

    def get_pipeline_name(self, resource: DltResource) -> str:
        return f"{self.config.pipeline_base_name}_{self.get_normalized_table_identifier(resource)}"

    def get_normalized_dataset_name(self) -> str:
        return snake_case_normalizer.normalize_table_identifier(
            self.config.dataset_name
        )

    def get_normalized_cursor_path(self) -> str | None:
        return (
            snake_case_normalizer.normalize_identifier(self.collection.cursor_path)
            if self.collection.cursor_path
            else None
        )

    def get_incremental_info(self) -> tuple[DltIncrementalPartialConfig, ...]:
        collection = self.collection
        if collection.incrementals:
            return collection.incrementals
        elif collection.cursor_path:
            return (
                DltIncrementalPartialConfig(
                    cursor_path=self.collection.cursor_path,  # type: ignore
                    initial_value=self.collection.initial_value or default_date_fn(),
                    lag_days=self.collection.lag_days or 1,
                ),
            )
        else:
            return ()


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


def custom_runs_interator(
    collection: DltResourceCollection,
    dlt_source_config: DltPipelineSourceConfig,
    resource: DltResource,
) -> Generator[DltResource]:
    if collection.incrementals:
        for incremental in collection.incrementals:

            incremental_dlt = dlt.sources.incremental(
                cursor_path=incremental.cursor_path,
                primary_key=collection.primary_key,
                initial_value=incremental.initial_value,
                last_value_func=make_comparable(
                    max_dt_with_lag_last_value_func, lag_days=incremental.lag_days
                ),
            )
            # resource._hints["incremental"] = incremental_dlt
            # resource = build_collection_resource(dlt_source_config=dlt_source_config, collection=collection, incremental_def=incremental_dlt)
            yield resource.apply_hints(incremental=incremental_dlt)

    elif collection.cursor_path:
        incremental_dlt = dlt.sources.incremental(
                cursor_path=collection.cursor_path,  # type: ignore
                primary_key=collection.primary_key,
                initial_value=collection.initial_value,
                last_value_func=make_comparable(
                    max_dt_with_lag_last_value_func, lag_days=collection.lag_days
                )
            )

        yield resource.apply_hints(incremental=incremental_dlt)

    else:
        yield resource


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
        if column_name in doc:
            del doc[column_name]

    return doc


def include_collection_columns(
    doc: Dict, include_columns: Optional[Sequence[str]] = None
) -> Dict:
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

    for column_name in doc.keys():
        if column_name not in include_columns:
            del doc[column_name]

    return doc


DltPipelineSourceConfigResourceTuple = tuple[DltPipelineSourceConfig, ...]
DLTRColl = DltResourceCollection


def build_collection_resource(
    dlt_source_config: DltPipelineSourceConfig,
    collection: DLTRColl,
    incremental_def=None,
) -> DltResource:
    resource: DltResource = mongodb_collection(
        connection_url=dlt_source_config.connection_string,
        database=dlt_source_config.database,
        collection=collection.collection_name,
        limit=collection.limit,
        incremental=incremental_def,
        parallel=True,
        # data_item_format="arrow", #aparentemente no con esta combinacion de source / destino
    )

    if collection.table_new_name:
        resource = resource.apply_hints(table_name=collection.table_new_name)

    if collection.columns_hints:
        resource = resource.apply_hints(columns=collection.columns_hints)

    if collection.columns_to_remove:
        resource = resource.add_map(
            lambda doc, columns=collection.columns_to_remove: remove_collection_columns(
                doc=doc, remove_columns=columns
            )
        )

    if collection.columns_to_include:
        resource = resource.add_map(
            lambda doc,
            columns=collection.columns_to_include: include_collection_columns(
                doc=doc, include_columns=columns
            )
        )

    if isinstance(collection.primary_key, str):
        resource = resource.apply_hints(
            columns={collection.primary_key: {"data_type": "text", "precision": 50}}
        )
    elif isinstance(collection.primary_key, tuple):
        resource = resource.apply_hints(
            columns={
                key: {"data_type": "text", "precision": 50}
                for key in collection.primary_key
            }
        )

    return resource


def dlt_mongo_db_asset_factory(
    mongo_db_source_configs: DltPipelineSourceConfigResourceTuple,
) -> tuple[AssetsDefinition, ...]:
    dlt_assets_list: deque[AssetsDefinition] = deque()
    for dlt_source_config in mongo_db_source_configs:
        for collection in dlt_source_config.collections:
            # if env_str == "local":
            #     print(f"Processing collection: {collection.collection_name}")
            resource = build_collection_resource(
                dlt_source_config=dlt_source_config,
                collection=collection,
            )
            dlt_t = DagsterDltTranslatorMongodbCRMHN(
                config=dlt_source_config, collection=collection
            )
            #print(f"Processing collection: {collection.collection_name}")
            new_assets = create_dlt_asset(
                dlt_resource=resource,
                group_name=dlt_source_config.dagster_group_name,
                dlt_t=dlt_t,
                dataset_name=dlt_source_config.dataset_name,
                # tags={"dagster/storage_kind": "sqlserver"},
            )
            dlt_assets_list.append(new_assets)

    return tuple(chain(dlt_assets_list))


# def get_config_filtered(
#     dlt_source_config_resource_list: DltPipelineSourceConfigResourceTuple,
#     dlt_source_config: DltPipelineSourceConfig,
# ) -> tuple[str]:
#     return list(chain(dlt_source_config_resource_tuple[dlt_source_config]))


def create_dlt_asset(
    dlt_resource: DltResource,
    group_name,
    dlt_t: DagsterDltTranslatorMongodbCRMHN,
    dataset_name: str,
    tags: Mapping[str, str] | None = None,
) -> AssetsDefinition:
    # target_table_identifier = dlt_t.get_normalized_table_identifier(dlt_resource)
    target_pipeline_name = dlt_t.get_pipeline_name(dlt_resource)

    @asset(
        key=dlt_t.get_asset_key(dlt_resource),
        group_name=group_name,
        description=f"cursor {dlt_t.collection.cursor_path} resource {dlt_t.get_asset_key(dlt_resource)}",
        metadata=dlt_t.get_collection_metadata(dlt_resource),
        deps=dlt_t.get_deps_asset_keys(dlt_resource),
        # compute_kind="dlt",
        kinds={
            "dlt",
            "mongodb",
            "sql_server",
        },
        tags=tags,
        automation_condition=dlt_t.collection.automation_condition,
    )
    def created_dlt_assets(
        context: AssetExecutionContext, dlt_pipeline_dest_mssql_dwh: BaseDltPipeline
    ):
        new_pipeline = dlt_pipeline_dest_mssql_dwh.get_pipeline(
            pipeline_name=target_pipeline_name, dataset_name=dataset_name
        )
        print(str(dlt_t.get_incremental_info()))
        context.log.info(
            {
                "Running dlt pipeline": target_pipeline_name,
                "resource": dlt_t.get_asset_key(dlt_resource),
                "dataset": dataset_name,
                "write_disposition": dlt_pipeline_dest_mssql_dwh.write_disposition,
                "directory": new_pipeline.pipelines_dir,
                "incremental": str(dlt_t.get_incremental_info()),
            }
        )
        extracted_resource_metadata = {}
        for custom_run_resource in custom_runs_interator(
            dlt_t.collection, dlt_t.config, dlt_resource
        ):

            load_info: LoadInfo = dlt_pipeline_dest_mssql_dwh.run_pipeline(
                custom_run_resource, new_pipeline
            )
            context.log.info(
                f"Last cursor unbound value: {custom_run_resource.incremental.incremental.start_value if custom_run_resource.incremental.incremental else None}"
            )
            
            load_info.raise_on_failed_jobs()
            
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

    return created_dlt_assets
