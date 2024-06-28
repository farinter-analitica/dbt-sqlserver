from dagster import ConfigurableResource, EnvVar, asset, Definitions
import os
import pyodbc
from dagster_shared_gf import shared_variables as shared_vars

env_str = shared_vars.env_str

# Set environment variables
p_server = {"dev": os.environ.get("DAGSTER_DEV_DWH_FARINTER_SQL_SERVER")
            , "prd": os.environ.get("DAGSTER_PRD_DWH_FARINTER_SQL_SERVER")}.get(env_str)
p_user = {"dev": os.environ.get("DAGSTER_DEV_DWH_FARINTER_USERNAME")
          , "prd": os.environ.get("DAGSTER_PRD_DWH_FARINTER_USERNAME")}.get(env_str)
p_password = {"dev": EnvVar("DAGSTER_SECRET_DEV_DWH_FARINTER_PASSWORD")
             , "prd": EnvVar("DAGSTER_SECRET_PRD_DWH_FARINTER_PASSWORD")}.get(env_str)
p_driver = os.environ.get('DAGSTER_SQL_SERVER_ODBC_DRIVER')


class SQLServerResource(ConfigurableResource):
    server: str
    databases: list  # List of databases
    user: str
    password: str
    trust_server_certificate: str = 'no'  # 'yes' or 'no', default should be no for public IPs.
    default_database: str   # Default database


    def get_connection(self,  database: str = ""):
        if database == "":
            database = self.default_database
        if database not in self.databases:
            raise ValueError(f"Database {database} is not in the allowed list.")
        
        connection_string = (
            f"DRIVER={p_driver};"
            f"SERVER={self.server};"
            f"DATABASE={database};"
            f"UID={self.user};"
            f"PWD={self.password};"
            f"TrustServerCertificate={self.trust_server_certificate};"
        )
        return pyodbc.connect(connection_string)

    def query(self, query: str, database: str = ""):
        with self.get_connection(database) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()

    def execute_and_commit(self, query: str, database: str = ""):
        with self.get_connection(database) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()


dwh_farinter = SQLServerResource(
    server= p_server,
    databases= ["BI_FARINTER", "ADM_FARINTER", "DL_FARINTER", "IA_FARINTER", "CRM_FARINTER"],
    user=p_user,
    password=p_password,
    trust_server_certificate='yes',
    default_database="DL_FARINTER"
)

dwh_farinter_adm = SQLServerResource(
    server= dwh_farinter.server,
    databases= dwh_farinter.databases,
    user=dwh_farinter.user,
    password=dwh_farinter.password,
    trust_server_certificate=dwh_farinter.trust_server_certificate,
    default_database="ADM_FARINTER"

    )

dwh_farinter_dl = SQLServerResource(
    server= dwh_farinter.server,
    databases= dwh_farinter.databases,
    user=dwh_farinter.user,
    password=dwh_farinter.password,
    trust_server_certificate=dwh_farinter.trust_server_certificate,
    default_database="DL_FARINTER"
    )
