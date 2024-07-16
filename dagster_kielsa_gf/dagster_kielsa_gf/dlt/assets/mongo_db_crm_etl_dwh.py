import dlt
from dlt.common import pendulum
from dlt.common.data_writers import TDataItemFormat
from dlt.common.pipeline import LoadInfo
from dlt.common.typing import TDataItems
from dlt.extract.resource import DltResource
import dlt.extract
from dlt.pipeline.pipeline import Pipeline
from dagster_shared_gf.dlt_shared.mongodb import mongodb, mongodb_collection
from dlt.sources.credentials import ConnectionStringCredentials

from dagster_shared_gf.dlt_shared.sql_database import sql_database, sql_table, Table
from sqlalchemy.engine import URL, create_engine
from dagster_shared_gf.shared_functions import get_for_current_env
from dagster_shared_gf.resources import sql_server_resources
from dagster_shared_gf import shared_variables as shared_vars
from dagster import EnvVar, SourceAsset, asset, AssetExecutionContext, AssetsDefinition, AssetKey
from dagster_embedded_elt.dlt import dlt_assets, DagsterDltResource, DagsterDltTranslator
from typing import Dict, List, Union, Iterable, Any, Mapping
from pydantic import dataclasses, Field
from dataclasses import asdict
from itertools import chain

env_str = shared_vars.env_str
# Set environment variables

connection_str_source:str = EnvVar("DAGSTER_SECRET_MONGODB_CRM_HN_CONN_URL").get_value()
connection_url_dest:str = sql_server_resources.dwh_farinter_dl.get_sqlalchemy_url()
mssql_destination = dlt.destinations.mssql(credentials=connection_url_dest.render_as_string(hide_password=False))



@dataclasses.dataclass(frozen=True, config={"arbitrary_types_allowed": True})
class DltSourceConfig:
    cursor_path: str
    primary_key: str | tuple
    pipeline_name: str
    initial_value: pendulum.DateTime = Field(default_factory=lambda: get_for_current_env( {"dev": pendulum.now().subtract(years=2), "prd": pendulum.now().subtract(years=5) } ))

    def all_configs(self):
        dataclass_var_value_dict = asdict(self)
        return dataclass_var_value_dict
    
    
DltSourceConfigResourceList = Dict[DltSourceConfig, List[str]]

read_source_config_updated_at: DltSourceConfigResourceList = {
    DltSourceConfig(cursor_path="updated_at", primary_key="_id", pipeline_name="mongo_crm_hn_updated_at"): [
        "crm_email",
        #"crm_portfolio", # no tiene nada
        "crm_sms",
        #"crm_campaign_log", # no tiene nada
        "crm_incident", 
        "crm_call", 
        "constantcontactcampaigns",
    ],
    DltSourceConfig(cursor_path="UpdatedAt", primary_key="_id", pipeline_name="mongo_crm_hn_updatedat"): [
        "crmCall",
        "crm_list",
    ],
}

read_source_config_multi_column: DltSourceConfigResourceList = {
    DltSourceConfig(cursor_path="updated_at", primary_key="_id", pipeline_name="mongo_crm_hn_multi_updated_at", initial_value=pendulum.now().subtract(months=2)): [
       # "crm_person",  ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
        "crm_message", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
        "crm_campaign", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
    ],
    DltSourceConfig(cursor_path="created_at", primary_key="_id", pipeline_name="mongo_crm_hn_multi_created_at"): [
       # "crm_person",  ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
        "crm_message", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
        "crm_campaign", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
        "campaignSchedule", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
    ],
    DltSourceConfig(cursor_path="EndDate", primary_key="_id", pipeline_name="mongo_crm_hn_multi_enddate"): [
        "campaignSchedule", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
    ]
}

all_mongo_db_source_configs: List[DltSourceConfigResourceList] = [read_source_config_updated_at, read_source_config_multi_column]


def get_config_filtered(dlt_source_config_resource_list: DltSourceConfigResourceList, dlt_source_config: DltSourceConfig) -> List[str]:
    return list(chain(dlt_source_config_resource_list[dlt_source_config]))

def create_dlt_asset(dlt_source, dlt_pipeline, name, group_name, dlt_dagster_translator) -> dlt_assets:
    @dlt_assets(
                dlt_source=dlt_source,
                dlt_pipeline=dlt_pipeline,
                name=name,
                group_name=group_name,
                dlt_dagster_translator=dlt_dagster_translator,
            )
    def created_dlt_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
        yield from dlt.run(context=context, write_disposition="merge")

    return created_dlt_assets

@dataclasses.dataclass
class DagsterDltTranslatorMongodbCRMHN(DagsterDltTranslator):
    config: DltSourceConfig
    def get_asset_key(self, resource: DltResource) -> AssetKey:
        """
        Para evitar duplicados en pipelines multi columnas de updated_at y created_at
        """
        return AssetKey([f"dlt_{self.config.pipeline_name}", f"{resource.name}"])          
    def get_deps_asset_keys(self, resource: DltResource) -> Iterable[AssetKey]:
        """
        Para evitar duplicados en pipelines multi columnas de updated_at y created_at

        """
        return [AssetKey([f"mongo_db_crm_hn", f"{resource.name}"]) ]
    def get_metadata(self, resource: DltResource) -> Mapping[str, Any]:
        return self.config.all_configs()

def dlt_asset_factory(mongo_db_source_configs: List[DltSourceConfigResourceList]) -> List[AssetsDefinition]:
    dlt_assets_list: List[AssetsDefinition] = []
    for dlt_source_config in mongo_db_source_configs:
        for config, collections in dlt_source_config.items():
                  
            source = mongodb(
                connection_url=connection_str_source,
                database="pro01",
                incremental=dlt.sources.incremental(config.cursor_path
                                                    ,primary_key=config.primary_key
                                                    , initial_value=config.initial_value),
                ).with_resources(*collections)
            dlt_pipeline = dlt.pipeline(
                pipeline_name=config.pipeline_name,
                destination=mssql_destination,
                dataset_name="mongo_db_crm_hn",
            )

            new_assets = create_dlt_asset(dlt_source=source, dlt_pipeline=dlt_pipeline
                    ,name=config.pipeline_name
                    ,group_name="dlt_mongo_db_crm_hn_etl_dwh"
                    ,dlt_dagster_translator=DagsterDltTranslatorMongodbCRMHN(config=config)
                    )
            dlt_assets_list.append(new_assets
            )

    return list(chain(dlt_assets_list))


all_mongo_db_hn_assets = dlt_asset_factory(all_mongo_db_source_configs)

all_mongo_db_hn_source_assets = list(
    SourceAsset(key, group_name="dlt_mongo_db_crm_hn_etl_dwh", tags={"dagster/storage_kind": "mongodb"}) for key in set(chain.from_iterable(dlt_assets.dependency_keys for dlt_assets in all_mongo_db_hn_assets))
    )
#print(mongodbdb_source_assets)

if __name__ == "__main__":

    def load_select_collection_updated_at(pipeline: Pipeline|None = None) -> LoadInfo:
        """Use the mongodb source to reflect an entire database schema and load select tables from it.

        This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
        """
        dlt_source_config = DltSourceConfig(cursor_path="updated_at", primary_key="_id")
        if pipeline is None:
            # Create a pipeline
            pipeline = dlt.pipeline(
                pipeline_name="mongo_crm_hn_updated_at",
                destination=mssql_destination,
                dataset_name="mongo_crm_hn",
            )
        collections: List[str] = get_config_filtered(read_source_config_updated_at
                                                    ,dlt_source_config)
        # Configure the source to load a few select collections incrementally
        sources = mongodb(connection_url=connection_str_source
            ,database="pro01"
            ,incremental=dlt.sources.incremental(dlt_source_config.cursor_path
                                                ,primary_key=dlt_source_config.primary_key
                                                , initial_value=pendulum.now().subtract(years=2))
            ).with_resources(
            *collections
        )

        info = pipeline.run(sources, write_disposition="merge")

        return info


    def load_select_collections_multi_updated_at(pipeline: Pipeline|None = None) -> LoadInfo:
        """Use the mongodb source to reflect an entire database schema and load select tables from it.

        This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
        """
        dlt_source_config = DltSourceConfig(cursor_path="updated_at", primary_key="_id")
        if pipeline is None:
            # Create a pipeline
            pipeline = dlt.pipeline(
                pipeline_name="mongo_crm_hn_multi_updated_at",
                destination=mssql_destination,
                dataset_name="mongo_crm_hn",
            )
        collections: List[str] = get_config_filtered(read_source_config_multi_column
                                                    ,dlt_source_config)

        # Configure the source to load a few select collections incrementally
        sources = mongodb(connection_url=connection_str_source
            ,database="pro01"
            ,incremental=dlt.sources.incremental(dlt_source_config.cursor_path
                                                ,primary_key=dlt_source_config.primary_key
                                                , initial_value=pendulum.now().subtract(months=2))
            ).with_resources(
            *collections
        )

        info = pipeline.run(sources, write_disposition="merge")

        return info

    def load_select_collections_multi_created_at(pipeline: Pipeline|None = None) -> LoadInfo:
        """Use the mongodb source to reflect an entire database schema and load select tables from it.

        This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
        """
        dlt_source_config = DltSourceConfig(cursor_path="created_at", primary_key="_id")
        if pipeline is None:
            # Create a pipeline
            pipeline = dlt.pipeline(
                pipeline_name="mongo_crm_hn_multi_created_at",
                destination=mssql_destination,
                dataset_name="mongo_crm_hn",
                progress=dlt.progress.log(log_period=1,)
            )
        collections: List[str] = get_config_filtered(read_source_config_multi_column
                                                    ,dlt_source_config)

        # Configure the source to load a few select collections incrementally
        sources = mongodb(connection_url=connection_str_source
            ,database="pro01"
            ,incremental=dlt.sources.incremental(dlt_source_config.cursor_path
                                                ,primary_key=dlt_source_config.primary_key
                                                , initial_value=pendulum.now().subtract(months=2)
                                                
                                                )
            ).with_resources(
            *collections
        )

        info = pipeline.run(sources, write_disposition="merge")

        return info

    print(load_select_collections_multi_created_at())
