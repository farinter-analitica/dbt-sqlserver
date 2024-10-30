import base64
import contextlib
import os
from typing import Any, Generator, List, Literal
import urllib.parse

import pyodbc
import sqlalchemy
from dagster import ConfigurableResource, EnvVar, InitResourceContext
from pydantic import Field

from dagster_shared_gf import shared_variables as shared_vars
from dagster_shared_gf.load_env_run import load_env_vars
from dagster_shared_gf.shared_functions import get_for_current_env

Row = pyodbc.Row
encode_url = urllib.parse.quote

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
    username: str
    password: str
    default_database: str   # Default database
    driver: str = p_driver
    trust_server_certificate: str = Field(default="no", description="Trust server certificate", pattern="^(yes|no)$")  # 'yes' or 'no', default should be no for public IPs.
    allow_any_database: bool = False  # Allow any database to be used, default should be False.

    @classmethod
    def log_event(self,type: Literal["info", "warning", "error"], message: str):
        print(f"{type}: {message}")

    @contextlib.contextmanager
    def get_connection(self,  database: str | None = None
                       , autocommit: bool=False
                       , engine: Literal["pyodbc", "sqlalchemy"] = "pyodbc"
                       ) -> Generator[pyodbc.Connection | sqlalchemy.Connection, None, None]:
        """
        A context manager that provides a connection to a SQL Server database, beware, by default is non autocommit.
        Pyodbc engine is used by default, sqlalchemy can only return connections and will fail on query and execute if provided.
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

        connection_string = self.get_connection_string(database=database, engine=engine)

        conn = None
        try:
            if engine == "pyodbc":
                conn = pyodbc.connect(connection_string, autocommit=autocommit)
            elif engine == "sqlalchemy":
                conn = sqlalchemy.create_engine(connection_string
                                                , isolation_level="AUTOCOMMIT" if autocommit else None
                                                , poolclass=sqlalchemy.pool.NullPool
                                                ).connect()
            yield conn
        finally:
            try:
                if engine == "pyodbc":
                    if not conn.autocommit:
                        conn.commit()
                    
                if engine == "sqlalchemy":
                    if conn.in_transaction():
                        conn.commit()

            except Exception as e:
                self.log_event('warning', f"Error committing transaction: {e}")

            if conn:
                self.close_connection(conn)

    def get_connection_string(self, database: str | None = None
                                                     , engine: Literal["pyodbc", "sqlalchemy", "arrow-odbc"] = "pyodbc"
            ) -> str | sqlalchemy.URL:
        """
        Generates a connection string for the specified database.

        Args:
            database (str, optional): The name of the database to connect to. Defaults to None.
            engine (Literal["pyodbc", "sqlalchemy", "arrow-odbc"], optional): The database engine to use. Defaults to "pyodbc".

        Raises:
            ValueError: If the specified database is not in the allowed list.

        Returns:
            str | sqlalchemy.URL: The connection string or URL.

        """

        connection_instance: str | sqlalchemy.URL  = None
        if database is None or database == "":
            database = self.default_database
        if not self.allow_any_database and  database not in self.databases:
            raise ValueError(f"Database {database} is not in the allowed list.")
        if engine == "pyodbc":
            connection_instance = (
                f"DRIVER={self.driver};"
                f"SERVER={self.server};"
                f"DATABASE={database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                f"TrustServerCertificate={self.trust_server_certificate};"
            )
        elif engine == "sqlalchemy":
            connection_instance = sqlalchemy.URL.create(
                "mssql+pyodbc",
                username=self.username,
                password=self.password,
                host=self.server,
                database=database,
                query={"driver": self.driver, "TrustServerCertificate": self.trust_server_certificate},
            )
        elif engine == "arrow-odbc":
            connection_instance = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={{{self.server}}};"
                f"DATABASE={{{database}}};"
                f"UID={{{self.username}}};"
                f"PWD={{{self.password}}};"
                f"TrustServerCertificate={{{self.trust_server_certificate}}};"
            )

        return connection_instance

    def close_connection(self, connection: pyodbc.Connection | sqlalchemy.Connection):
        try:
            connection.close()
        except Exception as e:
            # Add proper logging here
            raise RuntimeError(f"Error closing connection: {e}")
        
    def cursor_fetch_first_result(self, cursor: pyodbc.Cursor, fetch_val: bool = False):
        result = None
        try:
            result=cursor.fetchval() if fetch_val else cursor.fetchall()
        except pyodbc.ProgrammingError as e:
            self.log_event('info', "Skipping non rs message: {}".format(e))
            while cursor.nextset():
                try:
                    result = cursor.fetchval() if fetch_val else cursor.fetchall()
                except pyodbc.ProgrammingError as e:
                    self.log_event('info', "Skipping non rs message: {}".format(e))
                continue
            
        return result

    def query(self, query: str, connection: pyodbc.Connection | None = None, database: str = "", fetch_val: bool = False, autocommit: bool = True) -> List[Row] | Any:
        """
        Executes a SQL query on a database connection and returns the result as a list of rows or single row,
        this doesn't work well for return values, use a select and optional fetch_val = true instead.
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
                with self.get_connection(database=database, autocommit = autocommit) as new_conn:
                    cursor = new_conn.cursor()
                    cursor.execute(query)
                    return self.cursor_fetch_first_result(cursor=cursor, fetch_val=fetch_val)
            else:
                cursor = connection.cursor()
                cursor.execute(query)
                return self.cursor_fetch_first_result(cursor=cursor, fetch_val=fetch_val)
        except pyodbc.DatabaseError as opex:           
            if opex.args[0] == '42S02':
                #print("Table does not exist error handling")
                self.log_event('info',"Table does not exist error handling. Returning None to caller.")
                return None
                # Handle the error specific to table not existing
            else:
                #print(f"An unexpected database error occurred: {str(opex)}")
                self.log_event('error', f"An unexpected database error occurred: {str(opex)}.")
                raise opex
        except pyodbc.Error as e:
            self.log_event('error', f"Error executing and committing query: {str(e.args)}.")
            raise e
            
#            print(f"An unexpected error occurred: {str(e)}")

    def _execute_query_and_commit(self, conn: pyodbc.Connection | sqlalchemy.engine.Connection, query: str, autocommit: bool):
        if isinstance(conn, pyodbc.Connection):
            cursor = conn.cursor()
            cursor.execute(query)
            if not conn.autocommit:
                try: 
                    conn.commit()
                except Exception:
                    pass
        elif isinstance(conn, sqlalchemy.engine.Connection):
            conn.execute(sqlalchemy.text(query))
            if not autocommit and conn.in_transaction():
                conn.commit()

    def execute_and_commit(self, query: str, connection: pyodbc.Connection | sqlalchemy.engine.Connection | None = None,
                        database: str = "", autocommit: bool = True, engine: Literal["pyodbc", "sqlalchemy"] = "pyodbc"):
        """
        Executes an SQL query on a database connection.

        Args:
            query (str): The SQL query to execute.
            connection (pyodbc.Connection | sqlalchemy.engine.Connection, optional): The database connection to use.
                If not provided, a new connection will be created using the specified engine.
            database (str, optional): The name of the database to connect to. If not provided, the default database
                will be used.
            autocommit (bool, optional): Whether to enable autocommit. Defaults to True.
            engine (Literal["pyodbc", "sqlalchemy"], optional): The database engine to use. Defaults to "pyodbc".

        Raises:
            Exception: If there is an error executing and committing the query.
        """
        try:
            if connection is None:
                with self.get_connection(database=database, autocommit=autocommit, engine=engine) as conn:
                    self._execute_query_and_commit(conn, query, autocommit)
            else:
                self._execute_query_and_commit(connection, query, autocommit)
                
        except pyodbc.Error as e:
            # Add proper logging here
            self.log_event('error', f"Error executing and committing query: {str(e.args)}.")
            e.__traceback__ = None
            raise e
    

    def text(self, string: str) -> sqlalchemy.sql.text:
        """Text type that can be used in SQLAlchemy expressions to execute queries."""
        return sqlalchemy.text(string)

    def get_arrow_odbc_conn_string(self, database: str | None = None) -> str:
        """Get the Arrow ODBC connection string for the SQL Server resource."""
        return self.get_connection_string(engine="arrow-odbc", database=database)

    def get_sqlalchemy_url(self) -> sqlalchemy.URL:
        """Get the SQLAlchemy URL for the SQL Server resource."""
        try:
            v_password: str = str(self.password)
        except RuntimeError:
            v_password: str = self.password.get_value()
        return sqlalchemy.URL.create("mssql"
                            , username=self.username
                            , password= v_password
                            , host=self.server
                            #, port=1433
                            , database=self.default_database                         
                            , query={"driver": self.driver
                                , "TrustServerCertificate": self.trust_server_certificate})
    def get_sqlalchemy_conn(self, database: str | None = None
                       , autocommit: bool=False) -> sqlalchemy.Connection:
        return self.get_connection(database=database, autocommit=autocommit, engine="sqlalchemy")

    def get_pyodbc_conn(self, database: str | None = None
                       , autocommit: bool=False) -> pyodbc.Connection:
        return self.get_connection(database=database, autocommit=autocommit, engine="pyodbc")

class SQLServerNonRuntimeResource(SQLServerBaseResource):
    def __init__(self, server: str, databases: List[str], username: str, password: str, default_database: str, trust_server_certificate: str = 'no', allow_any_database: bool = False):
        self.server = server
        self.databases = databases
        self.username = username
        self.password = password
        self.default_database = default_database
        self.trust_server_certificate = trust_server_certificate
        self.allow_any_database = allow_any_database

# setup_for_execution  init with context
class SQLServerResource(SQLServerBaseResource, ConfigurableResource):
    _context: InitResourceContext = None
    def setup_for_execution(self, context: InitResourceContext):
        self._context = context

    def log_event(self, type: Literal['info', 'warning', 'error'], message: str):
        if not hasattr(self, '_context') or self._context is None:
            raise ValueError("Context has not been set. Call setup_for_execution first.")
        
        if type == "info":
            self._context.log.info(message)
        elif type == "warning":
            self._context.log.warning(message)
        elif type == "error":
            self._context.log.error(message)


dwh_farinter = SQLServerResource(
    server= p_server,
    databases= ["BI_FARINTER", "ADM_FARINTER", "DL_FARINTER", "IA_FARINTER", "CRM_FARINTER"],
    username=p_user,
    password=p_password,
    trust_server_certificate='yes',
    default_database="DL_FARINTER"
)

dwh_farinter_adm = SQLServerResource(
    server= dwh_farinter.server,
    databases= dwh_farinter.databases,
    username=dwh_farinter.username,
    password=dwh_farinter.password,
    trust_server_certificate=dwh_farinter.trust_server_certificate,
    default_database="ADM_FARINTER"

    )

dwh_farinter_dl = SQLServerResource(
    server= dwh_farinter.server,
    databases= dwh_farinter.databases,
    username=dwh_farinter.username,
    password=dwh_farinter.password,
    trust_server_certificate=dwh_farinter.trust_server_certificate,
    default_database="DL_FARINTER"
    )

dwh_farinter_bi = SQLServerResource(
    server= dwh_farinter.server,
    databases= dwh_farinter.databases,
    username=dwh_farinter.username,
    password=dwh_farinter.password,
    trust_server_certificate=dwh_farinter.trust_server_certificate,
    default_database="BI_FARINTER"
    )


dwh_farinter_database_admin = SQLServerNonRuntimeResource(
    server= p_server,
    databases= ["no_database_specified"],
    username=p_user,
    password=p_password.get_value(),
    trust_server_certificate='yes',
    default_database="no_database_specified",
    allow_any_database=True
)

dwh_farinter_dl_prd = SQLServerResource(
    server= os.getenv('DAGSTER_PRD_DWH_FARINTER_SQL_SERVER'),
    databases= dwh_farinter_dl.databases,
    username=os.getenv('DAGSTER_PRD_DWH_FARINTER_USERNAME'),
    password=EnvVar('DAGSTER_SECRET_PRD_DWH_FARINTER_PASSWORD'),
    trust_server_certificate=dwh_farinter_dl.trust_server_certificate,
    default_database=dwh_farinter_dl.default_database
)

dwh_farinter_prd_replicas_ldcom = SQLServerResource(
    server= os.getenv('DAGSTER_PRD_DWH_FARINTER_SQL_SERVER'),
    databases= ["REP_LDCOM_HN", "REP_LDCOM_NI", "REP_LDCOM_CR", "REP_LDCOM_GT", "REP_LDCOM_SV", "REP_LDCOM_ARB"],
    username=os.getenv('DAGSTER_PRD_DWH_FARINTER_USERNAME'),
    password=EnvVar('DAGSTER_SECRET_PRD_DWH_FARINTER_PASSWORD'),
    trust_server_certificate='yes',
    default_database="REP_LDCOM_HN"
)

LDCOM_SQLSERVER_HOSTS = {
    "HN": r"172.16.2.25",
    "NI": r"172.16.2.42",
    "CR": r"172.16.2.52",
    "GT": r"172.16.2.62",
    "SV": r"172.16.2.72",
    "CR_ARB": r"172.16.2.37",
    "SQLLDSUBS": r"172.16.2.125\SQLLDSUBS",
}
LDCOM_SQLSERVER_DATABASES = {
    "HN": ["LDCOM_KIELSA", "LDFAS_KIELSA"],
    "NI": ["LDCOM_KIELSA_NIC", "LDFAS_KIELSA_NIC"],
    "CR": ["LDCOM_KIELSA_CR", "LDFAS_KIELSA_CR"],
    "GT": ["LDCOM_KIELSA_GT", "LDFAS_KIELSA_GT"],
    "SV": ["LDCOM_KIELSA_SALVADOR", "LDFAS_KIELSA_SALVADOR"],
    "CR_ARB": ["LDCOM_KIELSA_CR", "LDFAS_KIELSA_CR"],
    "SQLLDSUBS": ["LDCOMREPHN", "LDCOMREPNIC","LDCOMREPSLV","LDCOMREPGT","LDCOMREPCR", "LDCOMREPARBCR",
                  "LDFASREPHN","LDFASREPNIC","LDFASREPSLV","LDFASREPGT","LDFASREPCR", "LDFASREPARBCR",
                  "SITEPLUS","RECETAS","KPP_DB"],
}

if os.getenv("DAGSTER_LDCOM_PRD_USERNAME", None) is None:
    load_env_vars()

ldcom_hn_prd_sqlserver = SQLServerResource(
    server= LDCOM_SQLSERVER_HOSTS["HN"],
    databases= LDCOM_SQLSERVER_DATABASES["HN"],
    username=os.getenv('DAGSTER_LDCOM_PRD_USERNAME'),
    password=EnvVar('DAGSTER_SECRET_LDCOM_PRD_PASSWORD'),
    trust_server_certificate='yes',
    default_database="LDCOM_KIELSA",
)

ldcom_ni_prd_sqlserver = SQLServerResource(
    server= LDCOM_SQLSERVER_HOSTS["NI"],
    databases= LDCOM_SQLSERVER_DATABASES["NI"],
    username=os.getenv('DAGSTER_LDCOM_PRD_USERNAME'),
    password=EnvVar('DAGSTER_SECRET_LDCOM_PRD_PASSWORD'),
    trust_server_certificate='yes',
    default_database="LDCOM_KIELSA_NIC",
)

ldcom_cr_prd_sqlserver = SQLServerResource(
    server= LDCOM_SQLSERVER_HOSTS["CR"],
    databases= LDCOM_SQLSERVER_DATABASES["CR"],
    username=os.getenv('DAGSTER_LDCOM_PRD_USERNAME'),
    password=EnvVar('DAGSTER_SECRET_LDCOM_PRD_PASSWORD'),
    trust_server_certificate='yes',
    default_database="LDCOM_KIELSA_CR",
)

ldcom_cr_arb_prd_sqlserver = SQLServerResource(
    server= LDCOM_SQLSERVER_HOSTS["CR_ARB"],
    databases= LDCOM_SQLSERVER_DATABASES["CR_ARB"],
    username=os.getenv('DAGSTER_LDCOM_PRD_USERNAME'),
    password=EnvVar('DAGSTER_SECRET_LDCOM_PRD_PASSWORD'),
    trust_server_certificate='yes',
    default_database="LDCOM_KIELSA_CR",
)


ldcom_gt_prd_sqlserver = SQLServerResource(
    server= LDCOM_SQLSERVER_HOSTS["GT"],
    databases= LDCOM_SQLSERVER_DATABASES["GT"],
    username=os.getenv('DAGSTER_LDCOM_PRD_USERNAME'),
    password=EnvVar('DAGSTER_SECRET_LDCOM_PRD_PASSWORD'),
    trust_server_certificate='yes',
    default_database="LDCOM_KIELSA_GT",
)

ldcom_sv_prd_sqlserver = SQLServerResource(
    server= LDCOM_SQLSERVER_HOSTS["SV"],
    databases= LDCOM_SQLSERVER_DATABASES["SV"],
    username=os.getenv('DAGSTER_LDCOM_PRD_USERNAME'),
    password=EnvVar('DAGSTER_SECRET_LDCOM_PRD_PASSWORD'),
    trust_server_certificate='yes',
    default_database="LDCOM_KIELSA_SALVADOR",
)

siteplus_sqlldsubs_sqlserver = SQLServerResource(
    server= LDCOM_SQLSERVER_HOSTS["SQLLDSUBS"],
    databases= LDCOM_SQLSERVER_DATABASES["SQLLDSUBS"],
    username=os.getenv('DAGSTER_LDCOM_PRD_USERNAME'),
    password=EnvVar('DAGSTER_SECRET_LDCOM_PRD_PASSWORD'),
    trust_server_certificate='yes',
    default_database="SITEPLUS",
)



if __name__ == "__main__":
    x=SQLServerNonRuntimeResource(
            server="server",
            databases=[],
            username="user",
            password="password",
            default_database="default_database",
            trust_server_certificate="yes",
            allow_any_database=False
        )

    from unittest.mock import MagicMock, patch
    # from dagster_shared_gf.resources.sql_server_resources import (
    #     SQLServerNonRuntimeResource,
    #     SQLServerNonRuntimeResource,
    #     SQLServerResource,
    # )

    # Test the SQLServerNonRuntimeResource class
    def test_sql_server_base_resource():
        resource = SQLServerNonRuntimeResource(
            server="test_server",
            databases=["test_database"],
            username="test_username",
            password="test_password",
            default_database="test_default_database",
        )
        assert resource.server == "test_server"
        assert resource.databases == ["test_database"]
        assert resource.username == "test_username"
        assert resource.password == "test_password"
        assert resource.default_database == "test_default_database"

    # Test the SQLServerNonRuntimeResource class
    def test_sql_server_non_runtime_resource():
        resource = SQLServerNonRuntimeResource(
            server="test_server",
            databases=["test_database"],
            username="test_username",
            password="test_password",
            default_database="test_default_database",
        )
        assert resource.server == "test_server"
        assert resource.databases == ["test_database"]
        assert resource.username == "test_username"
        assert resource.password == "test_password"
        assert resource.default_database == "test_default_database"

    # Test the SQLServerResource class
    def test_sql_server_resource():
        resource = SQLServerResource(
            server="test_server",
            databases=["test_database"],
            username="test_username",
            password="test_password",
            default_database="test_default_database",
            trust_server_certificate="yes",
            allow_any_database=False,
        )
        assert resource._context is None

        # Test the setup_for_execution method
        context = MagicMock()  # Mock context object
        resource.setup_for_execution(context)
        assert resource._context is context

        # Test the log_event method
        resource.log_event("info", "Test message")

    # Test the get_connection method
    @patch('pyodbc.connect')
    def test_get_connection(mock_connect):
        mock_conn = MagicMock()  # Mock connection object
        mock_connect.return_value = mock_conn  # Return the mock connection object

        resource = SQLServerNonRuntimeResource(
            server="test_server",
            databases=["test_database"],
            username="test_username",
            password="test_password",
            default_database="test_default_database",
            allow_any_database=True
        )
        with resource.get_connection() as connection:
            assert connection is mock_conn  # Assert that the connection is the mock connection object
            mock_connect.assert_called_once()  # Assert that the mock connection was called once
        

    # Test the query method
    @patch('pyodbc.connect')
    def test_query(mock_connect):
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        mock_cursor.fetchall.return_value = []
        mock_connect.return_value.cursor.return_value = mock_cursor

        resource = SQLServerNonRuntimeResource(
            server="test_server",
            databases=["test_default_database"],
            username="test_username",
            password="test_password",
            default_database="test_default_database",
        )
        query_result = resource.query("SELECT * FROM test_table")
        assert query_result == []
        mock_cursor.execute.assert_called_once()
        mock_cursor.fetchall.assert_called_once()

    # Test the execute_and_commit method
    @patch('pyodbc.connect')
    def test_execute_and_commit(mock_connect):
        mock_cursor = MagicMock(spec=pyodbc.Cursor)
        mock_cursor.execute.return_value = None
        mock_conn = MagicMock(spec=pyodbc.Connection)   
        mock_connect.return_value = mock_conn

        mock_connect.return_value.cursor.return_value = mock_cursor

        mock_conn.autocommit = False

        resource = SQLServerNonRuntimeResource(
            server="test_server",
            databases=["test_default_database"],
            username="test_username",
            password="test_password",
            default_database="test_default_database",
        )
        resource.execute_and_commit("INSERT INTO test_table (column1) VALUES ('value1')", autocommit=False)
        mock_cursor.execute.assert_called_once()
        assert mock_connect.return_value.commit.call_count == 2
    # Test the get_sqlalchemy_url method
    def test_get_sqlalchemy_url():
        resource = SQLServerNonRuntimeResource(
            server="test_server",
            databases=["test_default_database"],
            username="test_username",
            password="test_password",
            default_database="test_default_database",
        )
        url = resource.get_sqlalchemy_url()
        assert url is not None

    # Test the get_sqlalchemy_conn method
    @patch('sqlalchemy.create_engine')
    def test_get_sqlalchemy_conn(mock_create_engine):
        mock_conn = MagicMock(spec=sqlalchemy.Connection)
        mock_create_engine.return_value.connect.return_value = mock_conn
        
        resource = SQLServerNonRuntimeResource(
            server="test_server",
            databases=["test_default_database"],
            username="test_username",
            password="test_password",
            default_database="test_default_database",
        )
        with resource.get_sqlalchemy_conn() as conn:
            assert conn is mock_conn
            mock_create_engine.assert_called_once()

    # Test the get_pyodbc_conn method
    @patch('pyodbc.connect')
    def test_get_pyodbc_conn(mock_connect):
        mock_connect.return_value = MagicMock()

        mock_connect.return_value.cursor.return_value = MagicMock()

        resource = SQLServerNonRuntimeResource(
            server="test_server",
            databases=["test_default_database"],
            username="test_username",
            password="test_password",
            default_database="test_default_database",
        )
        with resource.get_pyodbc_conn() as conn:
            assert conn is not None
            mock_connect.assert_called_once()

    # Test the get_arrow_odbc_conn_string method
    def test_get_arrow_odbc_conn_string():
        resource = SQLServerNonRuntimeResource(
            server="test_server",
            databases=["test_default_database"],
            username="test_username",
            password="test_password",
            default_database="test_default_database",
            trust_server_certificate="yes",
            allow_any_database=False
        )
        expected_conn_string = (
                f"DRIVER={{{resource.driver}}};"
                f"SERVER={{{resource.server}}};"
                f"DATABASE={{{resource.default_database}}};"
                f"UID={{{resource.username}}};"
                f"PWD={{{resource.password}}};"
                f"TrustServerCertificate={{{resource.trust_server_certificate}}};"
            )
        conn_string = resource.get_arrow_odbc_conn_string()
        assert conn_string == expected_conn_string, f"Expected connection string: {expected_conn_string}, but got: {conn_string}"
    # Test
    test_sql_server_base_resource()
    test_sql_server_non_runtime_resource()
    test_sql_server_resource()
    test_get_connection()
    test_query()
    test_execute_and_commit()
    test_get_sqlalchemy_url()
    test_get_sqlalchemy_conn()
    test_get_pyodbc_conn()
    test_get_arrow_odbc_conn_string()
    #print(dwh_farinter_dl.get_arrow_odbc_conn_string())