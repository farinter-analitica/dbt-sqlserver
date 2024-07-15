import dlt
from dlt.common import pendulum
from dlt.common.data_writers import TDataItemFormat
from dlt.common.pipeline import LoadInfo
from dlt.common.typing import TDataItems
from dlt.pipeline.pipeline import Pipeline

from dagster_shared_gf.dlt_shared.mongodb import mongodb, mongodb_collection

from dlt.sources.credentials import ConnectionStringCredentials
import dlt, os
from dagster_shared_gf.dlt_shared.sql_database import sql_database, sql_table, Table
from sqlalchemy.engine import URL, create_engine
from dagster_shared_gf.shared_functions import get_for_current_env
from dagster_shared_gf.resources import sql_server_resources
from dagster_shared_gf import shared_variables as shared_vars
from dagster import EnvVar
from typing import Dict, List
from pydantic.dataclasses import dataclass
from itertools import chain



env_str = shared_vars.env_str
# Set environment variables

connection_str_source = EnvVar("DAGSTER_SECRET_MONGODB_CRM_HN_CONN_URL").get_value()
connection_url_dest = sql_server_resources.dwh_farinter_dl.get_sqlalchemy_url()

mssql_destination = dlt.destinations.mssql(credentials=connection_url_dest.render_as_string(hide_password=False))

@dataclass(frozen=True)
class DltSourceConfig:
    cursor_path: str
    primary_key: str | tuple

    @classmethod
    def all_configs(self):
        return self.__dataclass_fields__.keys()


DltSourceConfigResourceList = Dict[DltSourceConfig, List[str]]


read_source_config_updated_at: DltSourceConfigResourceList = {
    DltSourceConfig(cursor_path="updated_at", primary_key="_id"): [
        "crm_email",
        #"crm_portfolio", # no tiene nada
        "crm_sms",
        #"crm_campaign_log", # no tiene nada
        "crm_incident", 
        "crm_call", 
    ],
    DltSourceConfig(cursor_path="UpdatedAt", primary_key="_id"): [
        "crmCall",
    ],
}

read_source_config_multi_column: DltSourceConfigResourceList = {
    DltSourceConfig(cursor_path="updated_at", primary_key="_id"): [
        "crm_person",  ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
        "crm_message", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
        "crm_campaign", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
    ],
    DltSourceConfig(cursor_path="created_at", primary_key="_id"): [
        "crm_person",  ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
        "crm_message", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
        "crm_campaign", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
    ],
}


def get_config_filtered(dlt_source_config_resource_list: DltSourceConfigResourceList, dlt_source_config: DltSourceConfig) -> List[str]:
    return dlt_source_config_resource_list[dlt_source_config]

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

if __name__ == "__main__":
    print(load_select_collections_multi_created_at())
