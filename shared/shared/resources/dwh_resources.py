import os
import pyodbc
from dagster import ConfigurableResource, EnvVar

class dwh_resource(ConfigurableResource):
    database: str

    def get_dwh_config(self):
        current_env = os.environ.get('CURRENT_ENV', 'DEV')
        databases = [
            'DL_FARINTER',
            'BI_FARINTER',
            'AN_FARINTER',
            'ADM_FARINTER'
            # Add more databases as needed
        ]
        # Check if database exists
        if self.database not in databases:
            raise ValueError(f"Unknown database: {self.database}, add if exists in resources.")
            
        if current_env == 'DEV':
            return {
                'server': os.environ.get('DEV_SQL_SERVER'),
                'username': os.environ.get('DEV_SQL_USERNAME'),
                'password': EnvVar('DEV_SQL_PASSWORD'),
            }
        elif current_env == 'PRD':
            return {
                'server': os.environ.get('PRD_SQL_SERVER'),
                'username': os.environ.get('PRD_SQL_USERNAME'),
                'password': EnvVar('PRD_SQL_PASSWORD'),
            }
        else:
            raise ValueError(f"Unknown environment: {current_env}")

    def get_connection(self):
        config = self.get_dwh_config()
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={config['server']};"
            f"DATABASE={self.database};"
            f"UID={config['username']};"
            f"PWD={config['password']}"
        )
        connection = pyodbc.connect(conn_str)
        return connection

