from collections.abc import Iterable, Mapping
import os
from typing import Any, Dict, Optional, Sequence
import dlt
import dlt.extract
from dagster import (
    AssetKey,
    AssetSpec,
    AutomationCondition,
    ConfigurableResource,
    IntMetadataValue,
    MetadataValue,
)
from dagster_dlt import (
    DagsterDltResource,
    DagsterDltTranslator,
)
from dagster_dlt.translator import DltResourceTranslatorData
from dlt.common.destination.reference import Destination
from dlt.common.normalizers.naming.snake_case import (
    NamingConvention as snake_case_normalizer,
)
from dlt.common.pipeline import LoadInfo, get_dlt_pipelines_dir
from dlt.extract import DltResource
from pydantic import Field, dataclasses

from dagster_shared_gf.resources import sql_server_resources
from dagster_shared_gf.shared_variables import env_str, Tags
from dagster_shared_gf.shared_functions import (
    normalize_table_identifier,
    normalize_identifier,
)

new_pipelines_dir = os.path.join(get_dlt_pipelines_dir(), env_str)

dlt.secrets["dwh_farinter_dl"] = (
    sql_server_resources.dwh_farinter_dl.get_sqlalchemy_pyodbc_conn_string()
)

mssql_dwh_destination = dlt.destinations.mssql(
    credentials=dlt.secrets["dwh_farinter_dl"],
    # create_indexes=True, #esto tiene bug, prefiero no usarlo y ver despues si el rendimiento lo requiere crear indices en un proceso
)
fn_extract_resource_metadata = DagsterDltResource().extract_resource_metadata
ExtractedResourceMetadata = Mapping[str, Any]


@dataclasses.dataclass(config={"arbitrary_types_allowed": True})
class MyDagsterDltTranslator(DagsterDltTranslator):
    """MyDagsterDltTranslator is a custom implementation of DagsterDltTranslator.

    It defines asset specifications for MongoDB resources in Dagster pipelines.

    Args:
        dataset_name: Name of the dataset
        automation_condition: Optional automation condition for the asset
        prefix_key: Optional sequence of strings to prefix the asset key
        tags: Optional tags to apply to the asset
        group_name: Optional group name for the asset

    Methods:
        get_asset_spec: Creates AssetSpec for MongoDB resource
        get_normalized_cursor_path: Returns normalized cursor path
        get_normalized_table_identifier: Returns normalized table identifier
        get_normalized_column_identifier: Returns normalized column identifier
        get_normalized_dataset_name: Returns normalized dataset name
        remove_columns_map_fn: Utility fn, use with map, removes specified columns from document
    """

    dataset_name: str
    asset_database: str | None = None
    automation_condition: AutomationCondition | None = None
    prefix_key: Optional[Sequence[str]] = None
    tags: Optional[Tags | Mapping[str, str]] = None
    group_name: Optional[str] = None

    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        """Defines the asset spec for the MongoDB resource"""
        resource = data.resource
        destination = data.destination

        return AssetSpec(
            key=self._custom_get_asset_key(resource),
            deps=self._custom_get_deps_asset_keys(resource),
            metadata=self._default_metadata_fn(resource),
            automation_condition=self._custom_get_automation_condition()
            or self._default_automation_condition_fn(resource),
            tags={**self._default_tags_fn(resource), **self._custom_get_tags()},
            group_name=self.group_name or self._default_group_name_fn(resource),
            # Preserve the default behavior for these attributes
            description=self._default_description_fn(resource),
            owners=self._default_owners_fn(resource),
            kinds=self._default_kinds_fn(resource, destination),
        )

    def _custom_get_asset_key(self, resource: DltResource) -> AssetKey:
        """Defines asset key for a given dlt resource key and dataset name.

        If `prefix_key` is provided, the asset key is extended with the components of `prefix_key`
        followed by the resource name.
        If `asset_database` is provided, the asset key is extended with the asset database name
        followed by the dataset name and the resource name.

        Args:
            resource (DltResource): dlt resource

        Returns:
            AssetKey of Dagster asset derived from dlt resource

        """
        if self.prefix_key:
            return AssetKey([*self.prefix_key, resource.name])
        else:
            return AssetKey(
                [
                    self.asset_database or resource.source_name,
                    self.get_normalized_dataset_name(),
                    resource.name,
                ]
            )

    def _custom_get_deps_asset_keys(self, resource: DltResource) -> Iterable[AssetKey]:
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
        return dependencies

    def _custom_get_automation_condition(self) -> AutomationCondition | None:
        return self.automation_condition

    def get_normalized_cursor_path(self, resource: DltResource) -> str | None:
        return (
            normalize_identifier(resource.incremental.incremental.cursor_path)
            if resource.incremental and resource.incremental.incremental
            else None
        )

    def get_normalized_table_identifier(self, resource: DltResource) -> str:
        return snake_case_normalizer().normalize_table_identifier(resource.name)

    def get_normalized_column_identifier(self, column_identifier: str) -> str:
        return normalize_identifier(column_identifier)

    def get_normalized_dataset_name(self) -> str:
        return normalize_table_identifier(self.dataset_name)

    def remove_columns_map_fn(
        self, doc: dict, remove_columns: Optional[list[str]] = None
    ) -> Dict:
        """
        Removes the specified columns from the given document.

        Args:
            doc (Dict): The document from which columns are to be removed.
            remove_columns (Optional[list[str]], optional): The list of column names to be removed. Defaults to None.

        Returns:
            Dict: The modified document with the specified columns removed.
        """
        if remove_columns is None:
            remove_columns = []

        for column_name in remove_columns:
            if column_name in doc:
                del doc[column_name]

        return doc

    def _custom_get_tags(self) -> Mapping[str, str]:
        return self.tags or {}


class BaseDltPipeline(ConfigurableResource):
    write_disposition: str | None = Field(
        default=None,
        description="""The write disposition strategy.
- replace: Overwrite the destination table with the new data.
- append: Append the new data to the destination table.
- merge: Merge the new data with the destination table.
- skip: Do not write the new data to the destination table.""",
    )
    refresh: str | None = Field(
        default=None,
        description="""drop_sources: Drop tables and source and resource state for all sources currently being processed in run or extract methods of the pipeline. (Note: schema history is erased)
drop_resources: Drop tables and resource state for all resources being processed. Source level state is not modified. (Note: schema history is erased)
drop_data: Wipe all data and resource state for all resources being processed. Schema is not modified.""",
    )
    restore_from_destination: bool | None = Field(
        default=None,
        description="Enables the run method of the Pipeline object to restore the pipeline state and schemas from the destination",
    )
    drop_pending_packages: bool = Field(
        default=False,
        description="Drops previous pending data instead of trying to load it again.",
    )

    dev_mode: bool = Field(default=False)

    def get_pipeline(
        self,
        pipeline_name: str,
        dataset_name: str,
        import_schema_path: str | None = None,
        export_schema_path: str | None = None,
    ) -> dlt.Pipeline:
        # configure the pipeline with your destination details
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=self._get_destination(),
            dataset_name=dataset_name,
            progress="log",
            # refresh=self.refresh,  # type: ignore
            pipelines_dir=new_pipelines_dir,
            dev_mode=self.dev_mode,
            import_schema_path=import_schema_path,  # type: ignore[arg-type]
            export_schema_path=export_schema_path,  # type: ignore[arg-type]
        )
        if self.restore_from_destination is not None:
            pipeline.config.restore_from_destination = self.restore_from_destination
        return pipeline

    def run_pipeline(
        self,
        resource_data: DltResource,
        pipeline: dlt.Pipeline,
        write_disposition: str | None = None,
        refresh: str | None = None,
        drop_pending_packages: bool | None = None,
        remove_config: bool = False,
    ) -> LoadInfo:
        """
        A function to run a pipeline with the given resource data, pipeline, and write disposition.

        Args:
            resource_data (dlt.extract.DltResource): The data resource to run the pipeline with.
            pipeline (dlt.pipeline): The pipeline to run. You can use get_pipeline() to get the pipeline.
            write_disposition (Literal["replace", "append", "merge", "skip"], optional): The write disposition strategy. Defaults to configured write disposition.
            refresh (Literal["drop_sources", "drop_resources", "drop_data"], optional): The refresh strategy. Defaults to configured refresh.
            remove_config (bool, optional): If True, the write_disposition and refresh configurations at resource level are ignored. Defaults to False.

        Returns:
            dlt.common.pipeline.LoadInfo: Information about the load operation.
        """
        self.validate_dlt_config(write_disposition=write_disposition, refresh=refresh)

        if remove_config:
            write_disposition = None
            refresh = None
            drop_pending_packages = False
        else:
            if write_disposition is None:
                write_disposition = self.write_disposition  # type: ignore
            if refresh is None:
                refresh = self.refresh
            if drop_pending_packages is None:
                drop_pending_packages = self.drop_pending_packages
        if (
            dlt_pipeline_dest_mssql_dwh.write_disposition == "replace"
            or dlt_pipeline_dest_mssql_dwh.refresh is not None
            or dlt_pipeline_dest_mssql_dwh.drop_pending_packages
        ):
            pipeline.drop_pending_packages()

        load_info: LoadInfo = pipeline.run(
            resource_data,
            write_disposition=write_disposition,  # type: ignore
            refresh=refresh,  # type: ignore
        )
        # load_info.raise_on_failed_jobs()
        # extracted_resource_metadata = self.extract_resource_metadata( resource_data, load_info)

        return load_info

    def _get_destination(self) -> Destination | None:
        return None

    def validate_dlt_config(self, **kwargs) -> None:
        if "write_disposition" in kwargs:
            if kwargs["write_disposition"] is not None and kwargs[
                "write_disposition"
            ] not in ["replace", "append", "merge", "skip"]:
                raise ValueError(
                    "write_disposition must be one of replace, append, merge or skip"
                )
        if "refresh" in kwargs:
            if kwargs["refresh"] is not None and kwargs["refresh"] not in [
                "drop_sources",
                "drop_resources",
                "drop_data",
            ]:
                raise ValueError(
                    "refresh must be one of drop_sources, drop_resources or drop_data"
                )

    def setup_for_execution(
        self, context: sql_server_resources.InitResourceContext
    ) -> None:
        self.validate_dlt_config(**context.resource_config)
        return super().setup_for_execution(context)

    def extract_resource_metadata(
        self,
        context,
        resource_data: DltResource,
        load_info: LoadInfo,
        dlt_pipeline: dlt.Pipeline,
    ) -> ExtractedResourceMetadata:
        if dlt_pipeline.last_trace.last_normalize_info:
            return fn_extract_resource_metadata(
                context,
                resource=resource_data,
                load_info=load_info,
                dlt_pipeline=dlt_pipeline,
            )
        else:
            # Extract metadata from load_info instead
            metadata = {}
            if load_info:
                # Basic load info that should be available
                metadata["load_id"] = MetadataValue.text(
                    str(getattr(load_info, "load_id", "unknown"))
                )

                # Try to get any available job information safely
                if hasattr(load_info, "has_failed_jobs"):
                    metadata["has_failed_jobs"] = MetadataValue.bool(
                        load_info.has_failed_jobs
                    )

                # Set extraction status
                metadata["extraction_status"] = MetadataValue.text(
                    "completed_without_normalization_info"
                )
                metadata["fallback_reason"] = MetadataValue.text(
                    "last_normalize_info was None"
                )

            # Add basic pipeline info
            metadata.update(
                {
                    "pipeline_name": MetadataValue.text(dlt_pipeline.pipeline_name),
                    "dataset_name": MetadataValue.text(dlt_pipeline.dataset_name),
                    "destination": MetadataValue.text(str(dlt_pipeline.destination)),
                    "resource_name": MetadataValue.text(resource_data.name),
                    "write_disposition": MetadataValue.text(
                        str(resource_data.write_disposition)
                    ),
                }
            )

            return metadata


class DltPipelineDestMssqlDwh(BaseDltPipeline):
    def _get_destination(self) -> Destination:
        return mssql_dwh_destination


dlt_pipeline_dest_mssql_dwh = DltPipelineDestMssqlDwh()


def merge_dlt_dagster_metadata(metdadata_1: Mapping, metadata_2: Mapping) -> dict:
    rows_loaded_1 = metdadata_1.get("rows_loaded", None) if metdadata_1 else None
    rows_loaded_2 = metadata_2.get("rows_loaded", None) if metadata_2 else None
    rows_loaded_int_1: int = (
        rows_loaded_1.value
        if isinstance(rows_loaded_1, IntMetadataValue) and rows_loaded_1.value
        else 0
    )
    rows_loaded_int_2: int = (
        rows_loaded_2.value
        if isinstance(rows_loaded_2, IntMetadataValue) and rows_loaded_2.value
        else 0
    )
    jobs_1 = metdadata_1.get("jobs", []) if metdadata_1 else []
    jobs_2 = metadata_2.get("jobs", []) if metadata_2 else []
    new_meta = {}
    if rows_loaded_1 is not None or rows_loaded_2 is not None:
        new_meta["rows_loaded"] = MetadataValue.int(
            rows_loaded_int_1 + rows_loaded_int_2
        )  # type: ignore
    new_meta["jobs"] = list([*jobs_1, *jobs_2])  # type: ignore
    return {**metdadata_1, **metadata_2, **new_meta}  # type: ignore


if __name__ == "__main__":
    assert dlt_pipeline_dest_mssql_dwh.get_pipeline("test_pipeline", "test_dataset")
