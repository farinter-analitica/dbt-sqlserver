from dagster import ConfigurableResource, EnvVar, asset, Definitions
from typing import List, Any
import os
import psycopg2
from dagster_shared_gf import shared_variables as shared_vars
from dagster_shared_gf.shared_functions import get_for_current_env
import contextlib

Connection = Any
Row = Any
# Set environment variables
p_server = get_for_current_env({"local": "172.16.2.235","dev": "localhost","prd": "localhost"})
p_user = get_for_current_env({"dev": os.environ.get("DAGSTER_PG_USERNAME")})
p_password = get_for_current_env({"dev": EnvVar("DAGSTER_PG_PASSWORD")})

class PostgreSQLResource(ConfigurableResource):
    server: str
    databases: list  # List of databases
    user: str
    password: str | None 
    default_database: str   # Default database

    def __post_init__(self):
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
        
        connection_dict = {
            "host":self.server,
            "database":database,
            "user":self.user,
            }
        if self.password:
            connection_dict["password"] = self.password
        conn = psycopg2.connect(**connection_dict)
        
        try:
            yield conn
        finally:
            if conn:
                self.close_connection(conn)

    def close_connection(self, connection: Connection):
        try:
            connection.close()
        except psycopg2.Error as e:
            # Add proper logging here
            raise RuntimeError(f"Error closing connection: {e}")
        
    def query(self, query: str, connection: Connection = None, database: str = "") -> List[Row]:
        """
        Executes a SQL query on a database connection and returns the result as a list of rows.

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
                return cursor.fetchall()
        except psycopg2.DatabaseError as opex:           
            if opex.args[0] == '42S02':
                #print("Table does not exist error handling")
                self.get_resource_context().log.info(f"Table does not exist error handling. Returning None to caller.")
                return None
                # Handle the error specific to table not existing
            else:
                #print(f"An unexpected database error occurred: {str(opex)}")
                self.get_resource_context().log.info(f"An unexpected database error occurred: {str(opex)}. Returning None to caller.")
                return None
        except psycopg2.Error as e:
            self.get_resource_context().log.error(f"An unexpected error occurred:: {str(e)}. Returning None to caller.")
#            print(f"An unexpected error occurred: {str(e)}")

    def execute_and_commit(self, query: str, connection: Connection = None, database: str = ""):
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
                
        except psycopg2.Error as e:
            # Add proper logging here
            self.get_resource_context().log.error(f"Error executing and committing query: {str(e.args)}.")

db_analitica_etl = PostgreSQLResource(
    server= p_server,
    databases= ["analitica"],
    user=p_user,
    password=p_password,
    default_database="analitica"
)












# Another way:
# class PostgresResource(ConfigurableResource):
#     def __init__(self, context: InitResourceContext):
#         self.host = context.resource_config['host']
#         self.user = context.resource_config['user']
#         self.password = context.resource_config['password']
#         self.db_name = context.resource_config['db_name']

#     def connect(self):
#         return psycopg2.connect(
#             host=self.host,
#             user=self.user,
#             password=self.password,
#             dbname=self.db_name
#         )

#
# Example usage in an asset
#@asset(required_resource_keys={'postgres'})
# def my_asset(context):
#     postgres_resource = context.resources.postgres
#     with postgres_resource.connect() as conn:
#         # Perform database operations using the connection
#         ...