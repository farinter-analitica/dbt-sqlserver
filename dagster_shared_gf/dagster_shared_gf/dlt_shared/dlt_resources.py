from collections.abc import Mapping
import os
from typing import Any, Dict, Literal, Optional, Sequence

import dlt
import dlt.extract
from dagster import (
    AssetKey,
    AutomationCondition,
    ConfigurableResource,
    IntMetadataValue,
    MetadataValue,
)
from dagster_embedded_elt.dlt import (
    DagsterDltResource,
    DagsterDltTranslator,
)
from dlt.common.destination.reference import Destination
from dlt.common.normalizers.naming.snake_case import (
    NamingConvention as snake_case_normalizer,
)
from dlt.common.pipeline import LoadInfo, get_dlt_pipelines_dir
from dlt.extract import DltResource
from pydantic import Field, dataclasses

from dagster_shared_gf.resources import sql_server_resources
from dagster_shared_gf.shared_variables import env_str

new_pipelines_dir = os.path.join(get_dlt_pipelines_dir(), env_str)

dlt.secrets["dwh_farinter_dl"] = (
    sql_server_resources.dwh_farinter_dl.get_sqlalchemy_url().render_as_string(hide_password=False)
)

mssql_dwh_destination = dlt.destinations.mssql(
    credentials=dlt.secrets["dwh_farinter_dl"],
    create_indexes=True,
)
fn_extract_resource_metadata = DagsterDltResource().extract_resource_metadata
ExtractedResourceMetadata = Mapping[str, Any]


@dataclasses.dataclass(config={"arbitrary_types_allowed": True})
class MyDagsterDltTranslator(DagsterDltTranslator):
    automation_condition: AutomationCondition | None = None
    prefix_key: Optional[Sequence[str]] = None

    def get_asset_key(self, resource: DltResource) -> AssetKey:
        """Defines asset key for a given dlt resource key and dataset name.

        By default, the asset key is a concatenation of the source name and the resource name.
        If `prefix_key` is provided, the asset key is extended with the components of `prefix_key`
        followed by the resource name.

        Args:
            resource (DltResource): dlt resource

        Returns:
            AssetKey of Dagster asset derived from dlt resource

        """
        if self.prefix_key:
            return AssetKey([*self.prefix_key, resource.name])
        else:
            return AssetKey([resource.source_name, resource.name])

    def get_automation_condition(
        self, resource: DltResource
    ) -> AutomationCondition | None:
        default_automation_condition = super().get_automation_condition(resource)
        if self.automation_condition and default_automation_condition:
            return self.automation_condition | default_automation_condition
        elif self.automation_condition:
            return self.automation_condition
        else:
            return default_automation_condition

    def get_normalized_table_identifier(self, resource: DltResource) -> str:
        return snake_case_normalizer().normalize_table_identifier(resource.name)

    def get_normalized_column_identifier(self, column_identifier: str) -> str:
        return snake_case_normalizer().normalize_identifier(column_identifier)

    def remove_columns(
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


class BaseDltPipeline(ConfigurableResource):
    write_disposition: Literal["replace", "append", "merge", "skip"] = Field(
        default="merge"
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

    dev_mode: bool = Field(default=False)

    def get_pipeline(self, pipeline_name: str, dataset_name: str, import_schema_path: str|None = None, export_schema_path: str|None = None) -> dlt.Pipeline:
        # configure the pipeline with your destination details
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=self._get_destination(),
            dataset_name=dataset_name,
            progress="log",
            refresh=self.refresh, # type: ignore
            pipelines_dir=new_pipelines_dir,
            dev_mode=self.dev_mode,
            import_schema_path=import_schema_path, # type: ignore[arg-type]
            export_schema_path=export_schema_path, # type: ignore[arg-type]
        )
        if self.restore_from_destination is not None:
            pipeline.config.restore_from_destination = self.restore_from_destination
        return pipeline

    def run_pipeline(
        self,
        resource_data: DltResource,
        pipeline: dlt.Pipeline,
        write_disposition: Literal["replace", "append", "merge", "skip"] | None = None,
    ) -> LoadInfo:
        """
        A function to run a pipeline with the given resource data, pipeline, and write disposition.

        Args:
            resource_data (dlt.extract.DltResource): The data resource to run the pipeline with.
            pipeline (dlt.pipeline): The pipeline to run. You can use get_pipeline() to get the pipeline.
            write_disposition (Literal["replace", "append", "merge", "skip"], optional): The write disposition strategy. Defaults to configured write disposition.

        Returns:
            dlt.common.pipeline.LoadInfo: Information about the load operation.
        """
        if write_disposition is None:
            write_disposition = self.write_disposition
        load_info: LoadInfo = pipeline.run(
            resource_data,
            write_disposition=write_disposition,
            refresh=self.refresh, # type: ignore
        )
        # oad_info.raise_on_failed_jobs()
        # extracted_resource_metadata = self.extract_resource_metadata( resource_data, load_info)

        return load_info

    def _get_destination(self) -> Destination | None:
        return None

    def extract_resource_metadata(
        self,
        context,
        resource_data: DltResource,
        load_info: LoadInfo,
        dlt_pipeline: dlt.Pipeline,
    ) -> ExtractedResourceMetadata:
        return fn_extract_resource_metadata(
            context,
            resource=resource_data,
            load_info=load_info,
            dlt_pipeline=dlt_pipeline,
        )


class DltPipelineDestMssqlDwh(BaseDltPipeline):
    def _get_destination(self) -> Destination:
        return mssql_dwh_destination


dlt_pipeline_dest_mssql_dwh = DltPipelineDestMssqlDwh()

if __name__ == "__main__":
    assert dlt_pipeline_dest_mssql_dwh.get_pipeline("test_pipeline", "test_dataset")


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
