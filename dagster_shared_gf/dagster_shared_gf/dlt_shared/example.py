
from dlt.sources.credentials import ConnectionStringCredentials
import dlt, os
from sql_database import sql_database, sql_table, Table
from sqlalchemy.engine import URL, create_engine
from dagster_shared_gf.shared_functions import get_for_current_env
from dagster_shared_gf.resources import sql_server_resources
from dagster_shared_gf import shared_variables as shared_vars
from dagster import EnvVar


env_str = shared_vars.env_str
# Set environment variables
p_server = sql_server_resources.p_server
p_user = sql_server_resources.p_user
p_password: EnvVar = sql_server_resources.p_password
p_driver = sql_server_resources.p_driver

connection_url_source = URL.create("mssql+pyodbc"
                            , username=p_user
                            , password=p_password.get_value()
                            , host=p_server
                            #, port=1433
                            , database="DL_FARINTER"                            
                            , query={"driver": p_driver
                                , "TrustServerCertificate": "yes"})
connection_url_dest = URL.create("mssql"
                            , username=p_user
                            , password=p_password.get_value()
                            , host=p_server
                            #, port=1433
                            , database="DL_FARINTER"                            
                            , query={"driver": p_driver
                                , "TrustServerCertificate": "yes"})
                     
source_engine = create_engine(connection_url_source)

mssql_destination = dlt.destinations.mssql(credentials=connection_url_dest.render_as_string(hide_password=False))

sql_alchemy_source = sql_database(
    source_engine,
    table_names=["DL_SAP_T001"],

    #backend="pyarrow",
    #table_adapter_callback=_double_as_decimal_adapter
).with_resources("DL_SAP_T001")

sql_alchemy_table = sql_table(credentials=source_engine, table="DL_SAP_T001",schema="dbo")

pipeline = dlt.pipeline(
    pipeline_name="sap",  destination=mssql_destination, dataset_name="sap_data"
)

info = pipeline.run(sql_alchemy_table)
print(info)
