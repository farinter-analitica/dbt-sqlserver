from dagster import ConfigurableResource, Config
from dagster_shared_gf.resources import sql_server_resources
from dagster_embedded_elt.dlt import DagsterDltResource
from typing import Literal, Any, Dict
import dlt

from pydantic import Field, PrivateAttr
from dlt.common.destination.reference import Destination
from dlt.common.pipeline import LoadInfo
from dlt.extract import DltResource

connection_url_dest:str = sql_server_resources.dwh_farinter_dl.get_sqlalchemy_url()

mssql_destination = dlt.destinations.mssql(credentials=connection_url_dest.render_as_string(hide_password=False))
fn_extract_resource_metadata = DagsterDltResource().extract_resource_metadata
extracted_resource_metadata = Dict[str , Any]
class BaseDltPipeline(ConfigurableResource):
    write_disposition: str = Field(default="merge")

    def get_pipeline(self, pipeline_name: str, dataset_name: str) -> dlt.pipeline:

        # configure the pipeline with your destination details
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=self._get_destination(),
            dataset_name=dataset_name,
            progress="log",
        )
        return pipeline
    
    
    def run_pipeline(self, 
                     resource_data: DltResource,
                     pipeline: dlt.pipeline, 
                     write_disposition:Literal["replace", "append", "merge", "skip"] | None = None) -> extracted_resource_metadata:
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
        load_info = pipeline.run(resource_data, write_disposition=write_disposition)
        extracted_resource_metadata = self.extract_resource_metadata( resource_data, load_info)

        return extracted_resource_metadata
    
    def _get_destination(self) -> Destination | None:
        return None
    
    def extract_resource_metadata(self, resource_data: DltResource, load_info: LoadInfo) -> Any:
        return fn_extract_resource_metadata( resource=resource_data, load_info=load_info)

class DltPipelineDestMssql(BaseDltPipeline):
    def _get_destination(self) -> Destination:
        return mssql_destination

dlt_pipeline_dest_mssql = DltPipelineDestMssql()

if __name__ == "__main__":
    assert dlt_pipeline_dest_mssql.get_pipeline("test_pipeline", "test_dataset")


