from dagster import ConfigurableResource, EnvVar #, asset, Definitions
import os
import pyodbc

class SQLServerResource(ConfigurableResource):
    server: str
    database: str
    user: str
    password: str
    trust_server_certificate: str = 'no' # 'yes' or 'no', default should be no for public ips.

    def get_connection(self):
        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
            f"TrustServerCertificate={self.trust_server_certificate};"
        )
        return pyodbc.connect(connection_string)

    def query(self, query: str):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()

    def execute_and_commit(self, query: str):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()

dwh_adm_farinter = SQLServerResource(
    server= os.environ.get('DEV_SQL_SERVER'),
    database= "ADM_FARINTER",
    user=os.environ.get('DEV_SQL_USERNAME'),
    password=EnvVar('DEV_SQL_PASSWORD'),
    trust_server_certificate='yes'
    )

dwh_dl_farinter = SQLServerResource(
    server= os.environ.get('DEV_SQL_SERVER'),
    database= "DL_FARINTER",
    user=os.environ.get('DEV_SQL_USERNAME'),
    password=EnvVar('DEV_SQL_PASSWORD'),
    trust_server_certificate='yes'
    )