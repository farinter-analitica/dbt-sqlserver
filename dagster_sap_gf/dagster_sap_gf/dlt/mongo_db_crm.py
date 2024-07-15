import dlt
from dlt.common import pendulum
from dlt.common.data_writers import TDataItemFormat
from dlt.common.pipeline import LoadInfo
from dlt.common.typing import TDataItems
from dlt.pipeline.pipeline import Pipeline

# As this pipeline can be run as standalone script or as part of the tests, we need to handle the import differently.
try:
    from .mongodb import mongodb, mongodb_collection  # type: ignore
except ImportError:
    from dagster_sap_gf.dlt.mongodb import mongodb, mongodb_collection

from dlt.sources.credentials import ConnectionStringCredentials
import dlt, os
from dagster_sap_gf.dlt.sql_database import sql_database, sql_table, Table
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
p_server = sql_server_resources.p_server
p_user = sql_server_resources.p_user
p_password: EnvVar = sql_server_resources.p_password
p_driver = sql_server_resources.p_driver

# new_mongodb = mongodb(connection_url="mongodb://admin_qa:F4r1nt3r2022@172.16.2.195:27017/?authSource=admin", database="pro01")
new_mongodb = mongodb

connection_str_source = "mongodb://admin_qa:F4r1nt3r2022@172.16.2.195:27017/?authSource=admin"
connection_url_dest = URL.create("mssql"
                            , username=p_user
                            , password=p_password.get_value()
                            , host=p_server
                            #, port=1433
                            , database="DL_FARINTER"                            
                            , query={"driver": p_driver
                                , "TrustServerCertificate": "yes"})

mssql_destination = dlt.destinations.mssql(credentials=connection_url_dest.render_as_string(hide_password=False))

@dataclass(frozen=True)
class DltSourceConfig:
    cursor_path: str
    primary_key: str | tuple


read_source_config = Dict[DltSourceConfig, List[str]] 
read_source_config = {
    DltSourceConfig(cursor_path="updated_at", primary_key="_id"): [
        "crm_email",
        #"crm_portfolio", # no tiene nada
        "crm_sms",
    ],
    DltSourceConfig(cursor_path="created_at", primary_key="_id"): [
        "crm_campaign_log", ##pedir que actualicen de ser necesario para que funcione con updated_at
        "crm_person",  ##pedir que actualicen de ser necesario para que funcione con updated_at
        "crm_message", ##pedir que actualicen de ser necesario para que funcione con updated_at
        "crm_campaign", ##pedir que actualicen de ser necesario para que funcione con updated_at
    ],
}

def get_config_filtered(DltSourceConfig: DltSourceConfig) -> List[str]:
    return read_source_config[DltSourceConfig]

def load_select_collection_updated_at(pipeline: Pipeline|None = None) -> LoadInfo:
    """Use the mongodb source to reflect an entire database schema and load select tables from it.

    This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
    """
    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="mongo_crm_hn_updated_at",
            destination=mssql_destination,
            dataset_name="mongo_crm_hn",
        )

    collections: List[str] = get_config_filtered(DltSourceConfig(cursor_path="updated_at", primary_key="_id"))
    # Configure the source to load a few select collections incrementally
    sources = new_mongodb(connection_url=connection_str_source
        ,database="pro01"
        ,incremental=dlt.sources.incremental("updated_at",primary_key="_id")
        ).with_resources(
        *collections
    )

    info = pipeline.run(sources, write_disposition="merge")

    return info


def load_select_collections_created_at(pipeline: Pipeline|None = None) -> LoadInfo:
    """Use the mongodb source to reflect an entire database schema and load select tables from it.

    This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
    """
    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="mongo_crm_hn_created_at",
            destination=mssql_destination,
            dataset_name="mongo_crm_hn",
        )
    collections: List[str] = get_config_filtered(DltSourceConfig(cursor_path="created_at", primary_key="_id"))

    # Configure the source to load a few select collections incrementally
    sources = new_mongodb(connection_url=connection_str_source
        ,database="pro01"
        ,incremental=dlt.sources.incremental("created_at",primary_key="_id")
        ).with_resources(
        *collections
    )

    info = pipeline.run(sources, write_disposition="merge")

    return info

if __name__ == "__main__":
    print(load_select_collections_created_at())
