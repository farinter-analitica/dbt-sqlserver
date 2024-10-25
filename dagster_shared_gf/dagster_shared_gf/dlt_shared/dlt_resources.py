from dagster import ConfigurableResource
from dagster_shared_gf.resources import sql_server_resources
from dagster_shared_gf.shared_variables import env_var
from dagster_embedded_elt.dlt import DagsterDltResource
from typing import Literal, Any, Dict
import dlt, os

from pydantic import Field
from dlt.common.destination.reference import Destination
from dlt.common.pipeline import LoadInfo, get_dlt_pipelines_dir
from dlt.extract import DltResource

new_pipelines_dir = os.path.join(get_dlt_pipelines_dir(), env_var)

connection_url_dest:str = sql_server_resources.dwh_farinter_dl.get_sqlalchemy_url()

mssql_destination = dlt.destinations.mssql(credentials=connection_url_dest.render_as_string(hide_password=False), create_indexes=True)
fn_extract_resource_metadata = DagsterDltResource().extract_resource_metadata
ExtractedResourceMetadata = Dict[str , Any]
class BaseDltPipeline(ConfigurableResource):
    write_disposition: Literal["replace", "append", "merge", "skip"] = Field(default="merge")
    refresh: str | None = Field(default=None, 
                                description=f"""drop_sources: Drop tables and source and resource state for all sources currently being processed in run or extract methods of the pipeline. (Note: schema history is erased)
drop_resources: Drop tables and resource state for all resources being processed. Source level state is not modified. (Note: schema history is erased)
drop_data: Wipe all data and resource state for all resources being processed. Schema is not modified.""")
    restore_from_destination: bool | None = Field(default=None, description="Enables the run method of the Pipeline object to restore the pipeline state and schemas from the destination")

    def get_pipeline(self, pipeline_name: str, dataset_name: str) -> dlt.Pipeline:

        # configure the pipeline with your destination details
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=self._get_destination(),
            dataset_name=dataset_name,
            progress="log",
            refresh=self.refresh,
            pipelines_dir=new_pipelines_dir
        )
        if self.restore_from_destination is not None:
            pipeline.config.restore_from_destination  = self.restore_from_destination
        return pipeline
    
    def run_pipeline(self, 
                     resource_data: DltResource,
                     pipeline: dlt.Pipeline, 
                     write_disposition:Literal["replace", "append", "merge", "skip"] | None = None) -> LoadInfo:
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
        load_info: LoadInfo = pipeline.run(resource_data, write_disposition=write_disposition)
        #oad_info.raise_on_failed_jobs()
        #extracted_resource_metadata = self.extract_resource_metadata( resource_data, load_info)

        return load_info
    
    def _get_destination(self) -> Destination | None:
        return None
    
    def extract_resource_metadata(self, context, resource_data: DltResource, load_info: LoadInfo, dlt_pipeline: dlt.Pipeline) -> ExtractedResourceMetadata:
        return fn_extract_resource_metadata(context, resource=resource_data, load_info=load_info, dlt_pipeline=dlt_pipeline)

class DltPipelineDestMssql(BaseDltPipeline):
    def _get_destination(self) -> Destination:
        return mssql_destination

dlt_pipeline_dest_mssql = DltPipelineDestMssql()

if __name__ == "__main__":
    assert dlt_pipeline_dest_mssql.get_pipeline("test_pipeline", "test_dataset")


