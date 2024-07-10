from dagster import ConfigurableResource, EnvVar, asset, Definitions
from typing import List, Literal
import os
import pyodbc
from dagster_shared_gf import shared_variables as shared_vars
from dagster_shared_gf.shared_functions import get_for_current_env
import contextlib
import base64

Row = pyodbc.Row

def encode_password(input_str: str) -> str:
    # Convert the string to bytes
    input_bytes = input_str.encode('utf-8')
    # Perform base64 encoding
    encoded_bytes = base64.urlsafe_b64encode(input_bytes)
    # Convert the bytes back to a string
    return encoded_bytes.decode('utf-8')

def decode_password(encoded_str: str) -> str:
    # Convert the encoded string back to bytes
    encoded_bytes = encoded_str.encode('utf-8')
    # Perform base64 decoding
    decoded_bytes = base64.urlsafe_b64decode(encoded_bytes)
    # Convert the bytes back to the original string
    return decoded_bytes.decode('utf-8')

env_str = shared_vars.env_str

# Set environment variables
p_server = get_for_current_env({"dev": os.environ.get("DAGSTER_DEV_DWH_FARINTER_SQL_SERVER")
            , "prd": os.environ.get("DAGSTER_PRD_DWH_FARINTER_SQL_SERVER")})
p_user = get_for_current_env({"dev": os.environ.get("DAGSTER_DEV_DWH_FARINTER_USERNAME")
          , "prd": os.environ.get("DAGSTER_PRD_DWH_FARINTER_USERNAME")})
p_password = get_for_current_env({"dev": EnvVar("DAGSTER_SECRET_DEV_DWH_FARINTER_PASSWORD")
             , "prd": EnvVar("DAGSTER_SECRET_PRD_DWH_FARINTER_PASSWORD")})
p_driver = os.environ.get('DAGSTER_SQL_SERVER_ODBC_DRIVER')


class SQLServerBaseResource:
    server: str
    databases: list[str]  # List of databases
    user: str
    password: str
    default_database: str   # Default database
    trust_server_certificate: str = 'no'  # 'yes' or 'no', default should be no for public IPs.
    allow_any_database: bool = False  # Allow any database to be used, default should be False.

    def __post_init__(self):
        if self.trust_server_certificate not in ['yes', 'no']:
            raise ValueError("trust_server_certificate must be 'yes' or 'no'")
        if not self.allow_any_database and not self.databases:
            raise ValueError("databases list cannot be empty")
        if not self.allow_any_database and self.default_database not in self.databases:
            raise ValueError(f"default_database {self.default_database} is not in the allowed list of databases")
        self.password = str(self.password.encode('utf-8'))
        
    @classmethod
    def log_event(self,type: Literal["info", "warning", "error"], message: str):
        print(f"{type}: {message}")

    @contextlib.contextmanager
    def get_connection(self,  database: str = "", autocommit: bool=False):
        """
        A context manager that provides a connection to a SQL Server database, beware, by default is non autocommit.

        Args:
            database (str, optional): The name of the database to connect to. If not provided, the default database will be used.
            autocommit (bool, optional): Whether to enable autocommit. Defaults to False.

        Yields:
            pyodbc.Connection: A connection to the SQL Server database.

        Raises:
            ValueError: If the specified database is not in the allowed list.

        Returns:
            None

        """
        if database == "":
            database = self.default_database
        if not self.allow_any_database and  database not in self.databases:
            raise ValueError(f"Database {database} is not in the allowed list.")
        
        connection_string = (
            f"DRIVER={p_driver};"
            f"SERVER={self.server};"
            f"DATABASE={database};"
            f"UID={self.user};"
            f"PWD={bytes(self.password,encoding='utf-8').decode('utf-8')};"
            f"TrustServerCertificate={self.trust_server_certificate};"
        )
        conn = None
        try:
            conn = pyodbc.connect(connection_string, autocommit=autocommit)
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
        
    def query(self, query: str, connection: pyodbc.Connection = None, database: str = "", fetch_one: bool = False, autocommit: bool = True) -> List[Row]:
        """
        Executes a SQL query on a database connection and returns the result as a list of rows or single row,
        this doesn't work well for single valued queries, instead use fetch_one = True to get a single value/row.
        beware, by default is autocommit when using auto generated connection instead of a provided connection.

        Args:
            query (str): The SQL query to execute.
            connection (pyodbc.Connection, optional): The database connection to use. If not provided, a new connection
                will be created using the default database.
            database (str, optional): The name of the database to connect to. If not provided, the default database
                will be used.
            fetch_one (bool, optional): Whether to fetch only one row. Defaults to False.
            autocommit (bool, optional): Whether to enable autocommit. Defaults to True.

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
                with self.get_connection(database=database, autocommit = autocommit) as conn:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    if fetch_one:
                        return [cursor.fetchone()]
                    else:
                        return cursor.fetchall()
            else:
                cursor = connection.cursor()
                cursor.execute(query)
                if fetch_one:
                    return [cursor.fetchone()]
                else:
                    return cursor.fetchall()
        except pyodbc.DatabaseError as opex:           
            if opex.args[0] == '42S02':
                #print("Table does not exist error handling")
                self.log_event('info',f"Table does not exist error handling. Returning None to caller.")
                return None
                # Handle the error specific to table not existing
            else:
                #print(f"An unexpected database error occurred: {str(opex)}")
                self.log_event('error', f"An unexpected database error occurred: {str(opex)}. Returning None to caller.")
                return None
        except pyodbc.Error as e:
            self.log_event('error', f"Error executing and committing query: {str(e.args)}.")
            e.__traceback__ = None
            raise e
            
#            print(f"An unexpected error occurred: {str(e)}")

    def execute_and_commit(self, query: str, connection: pyodbc.Connection = None, database: str = ""):
        """
        Executes an SQL query on a database connection, 
        beware, by default is autocommit when using auto generated connection instead of a provided connection.

        Args:
            query (str): The SQL query to execute.
            connection (pyodbc.Connection, optional): The database connection to use. If not provided, a new connection
                will be created using the default database.
            database (str, optional): The name of the database to connect to. If not provided, the default database
                will be used.
            fetch_one (bool, optional): Whether to fetch only one row. Defaults to False.
            autocommit (bool, optional): Whether to enable autocommit. Defaults to True.

        Raises:
            RuntimeError: If there is an error executing and committing the query.

        Example:
            >>> query = "INSERT INTO table (column1, column2) VALUES ('value1', 'value2')"
            >>> connection = SQLServerResource.connect(connection_string)
            >>> execute_and_commit(query, connection)
        """
        try:
            if connection is None:
                with self.get_connection(database=database, autocommit = True) as new_conn:
                    cursor = new_conn.cursor()
                    cursor.execute(query)
                    if not new_conn.autocommit:
                        new_conn.commit()
            else:
                existing_conn = connection
                cursor = existing_conn.cursor()
                cursor.execute(query)
                if not existing_conn.autocommit:
                    existing_conn.commit()
                
        except pyodbc.Error as e:
            # Add proper logging here
            self.log_event('error', f"Error executing and committing query: {str(e.args)}.")
            e.__traceback__ = None
            raise e
        
class SQLServerNonRuntimeResource(SQLServerBaseResource):
    def __init__(self, server: str, databases: List[str], user: str, password: str, default_database: str, trust_server_certificate: str = 'no', allow_any_database: bool = False):
        self.server = server
        self.databases = databases
        self.user = user
        self.password = password
        self.default_database = default_database
        self.trust_server_certificate = trust_server_certificate
        self.allow_any_database = allow_any_database

class SQLServerResource(SQLServerBaseResource, ConfigurableResource):
    @classmethod
    def log_event(self, type: Literal['info'] | Literal['warning'] | Literal['error'], message: str):
        if type == "info":
            self.get_resource_context().log.info(f"An unexpected error occurred. Returning None to caller.")
        elif type == "warning":
            self.get_resource_context().log.warning(f"An unexpected error occurred. Returning None to caller.")
        elif type == "error":
            self.get_resource_context().log.error(f"An unexpected error occurred. Returning None to caller.")


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

        
dwh_farinter_database_admin = SQLServerNonRuntimeResource(
    server= p_server,
    databases= ["no_database_specified"],
    user=p_user,
    password=p_password.get_value(),
    trust_server_certificate='yes',
    default_database="no_database_specified",
    allow_any_database=True
)