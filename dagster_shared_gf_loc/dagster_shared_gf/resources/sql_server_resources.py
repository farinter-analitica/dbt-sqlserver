from dagster import ConfigurableResource, EnvVar, asset, Definitions
from typing import List
import os
import pyodbc
from dagster_shared_gf import shared_variables as shared_vars
from dagster_shared_gf.shared_functions import get_for_current_env
import contextlib

env_str = shared_vars.env_str

# Set environment variables
p_server = get_for_current_env({"dev": os.environ.get("DAGSTER_DEV_DWH_FARINTER_SQL_SERVER")
            , "prd": os.environ.get("DAGSTER_PRD_DWH_FARINTER_SQL_SERVER")})
p_user = get_for_current_env({"dev": os.environ.get("DAGSTER_DEV_DWH_FARINTER_USERNAME")
          , "prd": os.environ.get("DAGSTER_PRD_DWH_FARINTER_USERNAME")})
p_password = get_for_current_env({"dev": EnvVar("DAGSTER_SECRET_DEV_DWH_FARINTER_PASSWORD")
             , "prd": EnvVar("DAGSTER_SECRET_PRD_DWH_FARINTER_PASSWORD")})
p_driver = os.environ.get('DAGSTER_SQL_SERVER_ODBC_DRIVER')


class SQLServerResource(ConfigurableResource):
    server: str
    databases: list  # List of databases
    user: str
    password: str
    trust_server_certificate: str = 'no'  # 'yes' or 'no', default should be no for public IPs.
    default_database: str   # Default database

    def __post_init__(self):
        if self.trust_server_certificate not in ['yes', 'no']:
            raise ValueError("trust_server_certificate must be 'yes' or 'no'")
        if not self.databases:
            raise ValueError("databases list cannot be empty")
        if self.default_database not in self.databases:
            raise ValueError(f"default_database {self.default_database} is not in the allowed list of databases")

    @contextlib.contextmanager
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
        conn = pyodbc.connect(connection_string)
        
        try:
            yield conn
        finally:
            if conn:
                self.close_connection(conn)

    def close_connection(self, connection: pyodbc.Connection):
        try:
            connection.close()
        except pyodbc.Error as e:
            # Add proper logging here
            raise RuntimeError(f"Error closing connection: {e}")
        
    def query(self, query: str, connection: pyodbc.Connection = None, database: str = "", fetch_one: bool = False) -> List[pyodbc.Row]:
        """
        Executes a SQL query on a database connection and returns the result as a list of rows, this doesn't work well for single valued queries.

        Args:
            query (str): The SQL query to execute.
            connection (pyodbc.Connection, optional): The database connection to use. If not provided, a new connection
                will be created using the default database.
            database (str, optional): The name of the database to connect to. If not provided, the default database
                will be used.

        Returns:
            List[pyodbc.Row]: A list of rows returned by the query.

        Raises:
            RuntimeError: If there is an error executing the query.

        Example:
            >>> query = "SELECT * FROM table"
            >>> connection = pyodbc.connect(connection_string)
            >>> result = query(query, connection)
            >>> print(result)
            [('value1',), ('value2',), ...]
        """
        try:
            if connection is None:
                with self.get_connection(database) as conn:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    return cursor.fetchall()
            else:
                cursor = connection.cursor()
                cursor.execute(query)
                if fetch_one:
                    return cursor.fetchone()
                else:
                    return cursor.fetchall()
        except pyodbc.DatabaseError as opex:           
            if opex.args[0] == '42S02':
                #print("Table does not exist error handling")
                self.get_resource_context().log.info(f"Table does not exist error handling. Returning None to caller.")
                return None
                # Handle the error specific to table not existing
            else:
                #print(f"An unexpected database error occurred: {str(opex)}")
                self.get_resource_context().log.info(f"An unexpected database error occurred: {str(opex)}. Returning None to caller.")
                return None
        except pyodbc.Error as e:
            self.get_resource_context().log.error(f"An unexpected error occurred. Returning None to caller.")
            e.__traceback__ = None
            raise e
            
#            print(f"An unexpected error occurred: {str(e)}")

    def execute_and_commit(self, query: str, connection: pyodbc.Connection = None, database: str = ""):
        """
        Executes a SQL query on a database connection and commits the changes.

        Args:
            query (str): The SQL query to execute.
            connection (pyodbc.Connection, optional): The database connection to use. If not provided, a new connection
                will be created using the default database.
            database (str, optional): The name of the database to connect to. If not provided, the default database
                will be used.

        Raises:
            RuntimeError: If there is an error executing and committing the query.

        Example:
            >>> query = "INSERT INTO table (column1, column2) VALUES ('value1', 'value2')"
            >>> connection = pyodbc.connect(connection_string)
            >>> execute_and_commit(query, connection)
        """
        try:
            if connection is None:
                with self.get_connection(database) as conn:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    conn.commit()
            else:
                cursor = connection.cursor()
                cursor.execute(query)
                connection.commit()
                
        except pyodbc.Error as e:
            # Add proper logging here
            self.get_resource_context().log.error(f"Error executing and committing query.")
            e.__traceback__ = None
            raise e


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
