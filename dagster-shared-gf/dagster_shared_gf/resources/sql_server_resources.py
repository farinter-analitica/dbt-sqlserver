import base64
import contextlib
from enum import Enum
from typing import Any, Generator, Literal, Type, Union
import urllib.parse

import pymssql
import pyodbc
import sqlalchemy
from dagster import ConfigurableResource, EnvVar, InitResourceContext
from pydantic import Field

from dagster_shared_gf import shared_variables as shared_vars
from dagster_shared_gf.config import get_dagster_config
from dagster_shared_gf.shared_functions import get_for_current_env
import sqlalchemy.exc
from sqlalchemy.engine.interfaces import DBAPICursor

Row = tuple[Any, ...] | sqlalchemy.Row
encode_url = urllib.parse.quote


class EngineType(str, Enum):
    """Database engine types supported by the SQL Server resource."""

    PYMSSQL = "pymssql"  # Standard connection using pymssql
    SQLALCHEMY = "sqlalchemy"  # SQLAlchemy ORM connection
    ARROW_ODBC = "arrow-odbc"  # Apache Arrow ODBC connection
    PYODBC = "pyodbc"  # Standard connection using pyodbc
    SQLALCHEMY_PYODBC = "sqlalchemy_pyodbc"  # SQLAlchemy ORM connection


# For type hints
ENGINES = Union[
    Literal[
        EngineType.PYMSSQL,
        EngineType.SQLALCHEMY,
        EngineType.ARROW_ODBC,
        EngineType.PYODBC,
        EngineType.SQLALCHEMY_PYODBC,
    ],
    Literal["pymssql", "sqlalchemy", "arrow-odbc", "pyodbc", "sqlalchemy_pyodbc"],
]

Connection = Union[pymssql.Connection, sqlalchemy.Connection, pyodbc.Connection]


def encode_password(input_str: str) -> str:
    # Convert the string to bytes
    input_bytes = input_str.encode("utf-8")
    # Perform base64 encoding
    encoded_bytes = base64.urlsafe_b64encode(input_bytes)
    # Convert the bytes back to a string
    return encoded_bytes.decode("utf-8")


def decode_password(encoded_str: str) -> str:
    # Convert the encoded string back to bytes
    encoded_bytes = encoded_str.encode("utf-8")
    # Perform base64 decoding
    decoded_bytes = base64.urlsafe_b64decode(encoded_bytes)
    # Convert the bytes back to the original string
    return decoded_bytes.decode("utf-8")


env_str = shared_vars.env_str

# Set environment variables desde la configuración centralizada
cfg = get_dagster_config()
p_server = get_for_current_env(
    {
        "dev": cfg.dagster_dev_dwh_farinter_sql_server,
        "prd": cfg.dagster_prd_dwh_farinter_sql_server,
    }
)
p_user = get_for_current_env(
    {
        "dev": cfg.dagster_dev_dwh_farinter_username,
        "prd": cfg.dagster_prd_dwh_farinter_username,
    }
)
p_password = get_for_current_env(
    {
        "dev": cfg.dagster_secret_dev_dwh_farinter_password,
        "prd": cfg.dagster_secret_prd_dwh_farinter_password,
    }
)
p_driver = cfg.dagster_sql_server_odbc_driver


class SQLServerResource(ConfigurableResource):
    server: str
    databases: list[str]  # list of databases
    username: str
    password: str
    default_database: str  # Default database
    driver: str = p_driver if p_driver is not None else ""
    trust_server_certificate: str = Field(
        default="no", description="Trust server certificate", pattern="^(yes|no)$"
    )  # 'yes' or 'no', default should be no for public IPs.
    allow_any_database: bool = (
        False  # Allow any database to be used, default should be False.
    )
    _context: InitResourceContext | None = None

    def get_password(self) -> str:
        password: str | None = self.password
        if self.password and isinstance(password, EnvVar):
            password = password.get_value()
        return password if password else ""

    @contextlib.contextmanager
    def get_connection(
        self,
        database: str | None = None,
        autocommit: bool = False,
        engine: ENGINES = EngineType.SQLALCHEMY,
        timeout: int = 30,
        connection_attrs: dict | None = None,
    ) -> Generator[Connection, None, None]:
        """
        A context manager that provides a connection to a SQL Server database, beware, by default is non autocommit.
        Pymssql engine is used by default, sqlalchemy can only return connections and will fail on query and execute if provided.
        Args:
            database (str, optional): The name of the database to connect to. If not provided, the default database will be used.
            autocommit (bool, optional): Whether to enable autocommit. Defaults to False.
            engine (ENGINES, optional): The database engine to use. Defaults to EngineType.SQLALCHEMY.
            timeout (int, optional): Connection timeout in seconds. Defaults to 30.
            connection_attrs (dict, optional): Additional connection attributes or tags.

        Yields:
            Union[pymssql.Connection, sqlalchemy.Connection, Any]: A connection to the SQL Server database.

        Raises:
            ValueError: If the specified database is not in the allowed list.
            ConnectionError: If unable to establish a connection to the database.

        Returns:
            None

        """

        if engine not in EngineType:
            raise ValueError(f"Engine {engine} is not supported.")

        connection_string = self.get_connection_string(database=database, engine=engine)
        connection_attrs = connection_attrs or {}  # Default to empty dict if None

        conn: Connection | None = None
        try:
            if engine == EngineType.PYMSSQL:
                try:
                    # Add timeout parameter and any connection attributes
                    conn_options = {"autocommit": autocommit, "timeout": timeout}
                    conn_options.update(connection_attrs)
                    conn = pymssql.connect(str(connection_string), **conn_options)
                    self._assert_connection_type(conn, pymssql.Connection)
                except pymssql.Error as e:
                    self.log_event(
                        "error", f"Failed to establish PYMSSQL connection: {e}"
                    )
                    raise ConnectionError(f"Could not connect to database: {e}")

            elif engine == EngineType.SQLALCHEMY:
                try:
                    # Use QueuePool instead of NullPool for better connection reuse
                    engine_options = {
                        "isolation_level": "AUTOCOMMIT" if autocommit else None,
                        "poolclass": sqlalchemy.pool.QueuePool,
                        "pool_size": 5,
                        "max_overflow": 10,
                        "pool_timeout": timeout,
                        "connect_args": connection_attrs,
                    }

                    engine_instance = sqlalchemy.create_engine(
                        connection_string, **engine_options
                    )
                    conn = engine_instance.connect()
                    self._assert_connection_type(conn, sqlalchemy.Connection)
                except sqlalchemy.exc.SQLAlchemyError as e:
                    self.log_event(
                        "error", f"Failed to establish SQLAlchemy connection: {e}"
                    )
                    raise ConnectionError(f"Could not connect to database: {e}")

            if not conn:
                raise ConnectionError("Connection is None")

            yield conn
        finally:
            if conn:
                try:
                    if engine in (EngineType.PYMSSQL, EngineType.PYODBC) and isinstance(
                        conn, (pymssql.Connection, pyodbc.Connection)
                    ):
                        if not conn.autocommit:
                            conn.commit()
                    elif engine == EngineType.SQLALCHEMY and isinstance(
                        conn, sqlalchemy.Connection
                    ):
                        if not conn.closed:
                            conn.commit()
                except Exception as e:
                    self.log_event("warning", f"Error committing transaction: {e}")

                self.close_connection(conn)

    def _assert_connection_type(self, connection: Connection, type: Type):
        if not isinstance(connection, type):
            raise TypeError(f"Connection must be a {type.__name__}.")

        return connection

    def get_connection_string(
        self,
        database: str | None = None,
        engine: ENGINES = EngineType.SQLALCHEMY,
    ) -> str | sqlalchemy.URL:
        """
        Generates a connection string for the specified database.

        Args:
            database (str, optional): The name of the database to connect to. Defaults to None.
            engine (ENGINES, optional): The database engine to use. Defaults to EngineType.SQLALCHEMY.

        Raises:
            ValueError: If the specified database is not in the allowed list.

        Returns:
            str | sqlalchemy.URL: The connection string or URL.

        """

        connection_instance: str | sqlalchemy.URL = ""
        if database is None or database == "":
            database = self.default_database
        if not self.allow_any_database and database not in self.databases:
            raise ValueError(f"Database {database} is not in the allowed list.")
        if engine == EngineType.PYMSSQL:
            connection_instance = (
                f"server={self.server};"
                f"database={database};"
                f"user={self.username};"
                f"password={self.get_password()};"
            )
        elif engine == EngineType.SQLALCHEMY:
            connection_instance = sqlalchemy.URL.create(
                "mssql+pymssql",
                username=self.username,
                password=self.get_password(),
                host=self.server,
                database=database,
            )
        elif engine == EngineType.ARROW_ODBC:
            # Ej. Driver={PostgreSQL};Server=localhost;Port=5432;Database=test;Uid=usr;Pwd="
            connection_instance = (
                f"Driver={{{self.driver}}};"
                f"Server={{{self.server}}};"
                f"Database={{{database}}};"
                f"Uid={{{self.username}}};"
                f"Pwd={{{self.get_password()}}};"
                f"TrustServerCertificate={{{self.trust_server_certificate}}};"
            )
        elif engine == EngineType.PYODBC:
            connection_instance = (
                f"DRIVER={self.driver};"
                f"SERVER={self.server};"
                f"DATABASE={database};"
                f"UID={self.username};"
                f"PWD={self.get_password()};"
                f"TrustServerCertificate={self.trust_server_certificate};"
            )
        elif engine == EngineType.SQLALCHEMY_PYODBC:
            connection_instance = sqlalchemy.URL.create(
                "mssql+pyodbc",
                username=self.username,
                password=self.get_password(),
                host=self.server,
                database=database,
                query={
                    "driver": self.driver,
                    "TrustServerCertificate": self.trust_server_certificate,
                },
            )
        return connection_instance

    def get_sqlalchemy_engine(self) -> sqlalchemy.Engine:
        return sqlalchemy.create_engine(self.get_sqlalchemy_url())

    def close_connection(self, connection: Connection):
        try:
            connection.close()
        except Exception as e:
            # Add proper logging here
            raise RuntimeError(f"Error closing connection: {e}")

    def _cursor_fetch_first_result(
        self, cursor: pymssql.Cursor | DBAPICursor, fetch_val: bool = False
    ):
        """Fetch the first valid result set from the cursor, skipping any non‐result sets."""
        if cursor.description is not None:
            if fetch_val:
                row = cursor.fetchone()
                return row[0] if row else None
            else:
                return cursor.fetchall()
        else:
            self.log_event("info", "Skipping non-result set (no description available)")

        while cursor.nextset():
            if cursor.description is not None:
                if fetch_val:
                    row = cursor.fetchone()
                    return row[0] if row else None
                else:
                    return cursor.fetchall()
            else:
                self.log_event(
                    "info", "Skipping non-result set (no description available)"
                )
        raise ValueError("No result sets found.")

    def _ensure_text(
        self, query_obj: Union[str, sqlalchemy.TextClause]
    ) -> sqlalchemy.TextClause:
        """Ensure the query is wrapped in text() if it's a string."""
        return sqlalchemy.text(query_obj) if isinstance(query_obj, str) else query_obj

    def query(
        self,
        query: str,
        connection: Connection | None = None,
        database: str = "",
        fetch_val: bool = False,
        autocommit: bool = True,
        engine: ENGINES = EngineType.SQLALCHEMY,
    ) -> list[Row] | Any:
        """
        Executes a SQL query on a database connection and returns the result as a list of rows or single row.
        [docstring truncated for brevity]
        """
        if engine not in EngineType:
            raise ValueError(f"Engine {engine} is not supported.")
        try:
            # Helper to extract results using pymssql cursor from SQLAlchemy result
            def _fetch_from_sqlalchemy(conn: sqlalchemy.Connection) -> Any:
                # Obtain a raw DBAPI connection and cursor, then execute the query
                raw_conn = conn.engine.raw_connection()
                cursor = raw_conn.cursor()
                # Execute the SQL text on the DBAPI cursor
                sql_text = self._ensure_text(query).text
                cursor.execute(sql_text)
                # Ensure we have a usable cursor
                if cursor is None:
                    raise ValueError(
                        f"Cursor is not available or implemented. Cursor: {cursor}"
                    )
                return self._cursor_fetch_first_result(
                    cursor=cursor, fetch_val=fetch_val
                )

            if connection is None:
                with self.get_connection(
                    database=database, autocommit=autocommit, engine=engine
                ) as new_conn:
                    if isinstance(new_conn, pymssql.Connection):
                        cursor = new_conn.cursor()
                        cursor.execute(query)
                        return self._cursor_fetch_first_result(
                            cursor=cursor, fetch_val=fetch_val
                        )
                    elif isinstance(new_conn, sqlalchemy.Connection):
                        return _fetch_from_sqlalchemy(new_conn)
            else:
                if isinstance(connection, pymssql.Connection):
                    cursor = connection.cursor()
                    cursor.execute(query)
                    return self._cursor_fetch_first_result(
                        cursor=cursor, fetch_val=fetch_val
                    )
                elif isinstance(connection, sqlalchemy.Connection):
                    return _fetch_from_sqlalchemy(connection)
        except pymssql.DatabaseError as opex:
            if opex.args[0] == "42S02":
                self.log_event(
                    "info",
                    "Table does not exist error handling. Returning None to caller.",
                )
                return None
            else:
                self.log_event(
                    "error", f"An unexpected database error occurred: {str(opex)}."
                )
                raise opex
        except pymssql.Error as e:
            self.log_event(
                "error", f"Error executing and committing query: {str(e.args)}."
            )
            raise e
        except sqlalchemy.exc.SQLAlchemyError as e:
            self.log_event("error", f"Error executing and committing query: {str(e)}.")
            raise e

    def _execute_query_and_commit(
        self,
        conn: Connection,
        query: str,
        autocommit: bool,
    ):
        if isinstance(conn, pymssql.Connection):
            cursor = conn.cursor()
            cursor.execute(query)
            if not conn.autocommit:
                try:
                    conn.commit()
                except Exception:
                    pass
        elif isinstance(conn, sqlalchemy.Connection):
            conn.execute(self._ensure_text(query))
            if not autocommit and conn.in_transaction():
                conn.commit()

    def execute_and_commit(
        self,
        query: str,
        connection: Connection | None = None,
        database: str = "",
        autocommit: bool = True,
        engine: ENGINES = EngineType.SQLALCHEMY,
    ):
        """
        Executes an SQL query on a database connection.

        Args:
            query (str): The SQL query to execute.
            connection (Connection, optional): The database connection to use.
                If not provided, a new connection will be created using the specified engine.
            database (str, optional): The name of the database to connect to. If not provided, the default database
                will be used.
            autocommit (bool, optional): Whether to enable autocommit. Defaults to True.
            engine (ENGINES, optional): The database engine to use. Defaults to EngineType.SQLALCHEMY.
            If you provide a connection, engine will be ignored.

        Raises:
            Exception: If there is an error executing and committing the query.
        """
        if engine not in EngineType:
            raise ValueError(f"Engine {engine} is not supported.")
        try:
            if connection is None:
                with self.get_connection(
                    database=database, autocommit=autocommit, engine=engine
                ) as conn:
                    self._execute_query_and_commit(conn, query, autocommit)
            else:
                self._execute_query_and_commit(connection, query, autocommit)

        except pymssql.Error as e:
            # Add proper logging here
            self.log_event(
                "error", f"Error executing and committing query: {str(e.args)}."
            )
            e.__traceback__ = None
            raise e

    def text(self, string: str) -> sqlalchemy.TextClause:
        """Text type that can be used in SQLAlchemy expressions to execute queries."""
        return sqlalchemy.text(string)

    def get_arrow_odbc_conn_string(self, database: str | None = None) -> str:
        """Get the Arrow ODBC connection string for the SQL Server resource."""
        conn_string = self.get_connection_string(
            engine=EngineType.ARROW_ODBC, database=database
        )
        return str(conn_string)

    def get_sqlalchemy_pyodbc_conn_string(self, database: str | None = None) -> str:
        """Get the pyodbc connection string for the SQL Server resource."""
        conn_string = self.get_connection_string(
            engine=EngineType.SQLALCHEMY_PYODBC, database=database
        )
        if isinstance(conn_string, sqlalchemy.URL):
            return str(conn_string.render_as_string(hide_password=False))
        return str(conn_string)

    def get_sqlalchemy_url(self) -> sqlalchemy.URL:
        """Get the SQLAlchemy URL for the SQL Server resource."""
        return sqlalchemy.URL.create(
            "mssql+pymssql",
            username=self.username,
            password=self.get_password(),
            host=self.server,
            # , port=1433
            database=self.default_database,
        )

    @contextlib.contextmanager
    def get_sqlalchemy_conn(
        self, database: str | None = None, autocommit: bool = False
    ) -> Generator[sqlalchemy.Connection, None, None]:
        with self.get_connection(
            database=database, autocommit=autocommit, engine=EngineType.SQLALCHEMY
        ) as conn:
            if not isinstance(conn, sqlalchemy.Connection):
                raise TypeError("Expected SQLAlchemy connection")
            yield conn

    @contextlib.contextmanager
    def get_pymssql_conn(
        self, database: str | None = None, autocommit: bool = False
    ) -> Generator[pymssql.Connection, None, None]:
        with self.get_connection(
            database=database, autocommit=autocommit, engine=EngineType.PYMSSQL
        ) as conn:
            if not isinstance(conn, pymssql.Connection):
                raise TypeError("Expected pymssql connection")
            yield conn

    def setup_for_execution(self, context: InitResourceContext):
        self._context = context

    def log_event(self, type: Literal["info", "warning", "error"], message: str):
        if not hasattr(self, "_context") or self._context is None:
            raise ValueError(
                "Context has not been set. Call setup_for_execution first."
            )
        context = self._context
        if hasattr(context, "log") and context.log:
            if type == "info":
                context.log.info(message)
            elif type == "warning":
                context.log.warning(message)
            elif type == "error":
                context.log.error(message)
        else:
            raise ValueError("Context does not have a attributes.")


class SQLServerNonRuntimeResource(SQLServerResource):
    def log_event(self, type: Literal["info", "warning", "error"], message: str):
        print(f"{type}: {message}")


DWH_FARINTER_DATABASES = [
    "BI_FARINTER",
    "ADM_FARINTER",
    "DL_FARINTER",
    "IA_FARINTER",
    "CRM_FARINTER",
]

dwh_farinter = SQLServerResource(
    server=p_server,
    databases=DWH_FARINTER_DATABASES,
    username=p_user,
    password=p_password,
    trust_server_certificate="yes",
    default_database="DL_FARINTER",
)

dwh_farinter_adm = SQLServerResource(
    server=p_server,
    databases=DWH_FARINTER_DATABASES,
    username=p_user,
    password=p_password,
    trust_server_certificate="yes",
    default_database="ADM_FARINTER",
)

dwh_farinter_dl = SQLServerResource(
    server=p_server,
    databases=DWH_FARINTER_DATABASES,
    username=p_user,
    password=p_password,
    trust_server_certificate="yes",
    default_database="DL_FARINTER",
)

dwh_farinter_bi = SQLServerResource(
    server=p_server,
    databases=DWH_FARINTER_DATABASES,
    username=p_user,
    password=p_password,
    trust_server_certificate="yes",
    default_database="BI_FARINTER",
)

dwh_farinter_ia = SQLServerResource(
    server=p_server,
    databases=DWH_FARINTER_DATABASES,
    username=p_user,
    password=p_password,
    trust_server_certificate="yes",
    default_database="IA_FARINTER",
)

dwh_farinter_sms_kielsa = SQLServerResource(
    server=p_server,
    databases=[
        "SMS_KIELSA",
    ],
    username=p_user,
    password=p_password,
    trust_server_certificate="yes",
    default_database="SMS_KIELSA",
)

dwh_farinter_database_admin = SQLServerNonRuntimeResource(
    server=p_server,
    databases=["no_database_specified"],
    username=p_user,
    password=p_password,
    trust_server_certificate="yes",
    default_database="no_database_specified",
    allow_any_database=True,
)

dwh_farinter_dl_prd = SQLServerResource(
    server=cfg.dagster_prd_dwh_farinter_sql_server or "ENV NOT FOUND",
    databases=dwh_farinter_dl.databases,
    username=cfg.dagster_prd_dwh_farinter_username or "ENV NOT FOUND",
    password=cfg.dagster_secret_prd_dwh_farinter_password,
    trust_server_certificate=dwh_farinter_dl.trust_server_certificate,
    default_database=dwh_farinter_dl.default_database,
)

dwh_farinter_prd_replicas_ldcom = SQLServerResource(
    server=cfg.dagster_prd_dwh_farinter_sql_server or "ENV NOT FOUND",
    databases=[
        "REP_LDCOM_HN",
        "REP_LDCOM_NI",
        "REP_LDCOM_CR",
        "REP_LDCOM_GT",
        "REP_LDCOM_SV",
        "REP_LDCOM_ARB",
    ],
    username=cfg.dagster_prd_dwh_farinter_username or "ENV NOT FOUND",
    password=cfg.dagster_secret_prd_dwh_farinter_password,
    trust_server_certificate="yes",
    default_database="REP_LDCOM_HN",
)

LDCOM_SQLSERVER_HOSTS = {
    "HN": r"172.16.2.25",
    "NI": r"172.16.2.42",
    "CR": r"172.16.2.52",
    "GT": r"172.16.2.63",
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
    "SQLLDSUBS": [
        "LDCOMREPHN",
        "LDCOMREPNIC",
        "LDCOMREPSLV",
        "LDCOMREPGT",
        "LDCOMREPCR",
        "LDCOMREPARBCR",
        "LDFASREPHN",
        "LDFASREPNIC",
        "LDFASREPSLV",
        "LDFASREPGT",
        "LDFASREPCR",
        "LDFASREPARBCR",
        "SITEPLUS",
        "RECETAS",
        "KPP_DB",
    ],
}

ldcom_hn_prd_sqlserver = SQLServerResource(
    server=LDCOM_SQLSERVER_HOSTS["HN"],
    databases=LDCOM_SQLSERVER_DATABASES["HN"],
    username=cfg.dagster_ldcom_prd_username or "ENV NOT FOUND",
    password=cfg.dagster_secret_ldcom_prd_password,
    trust_server_certificate="yes",
    default_database="LDCOM_KIELSA",
)

ldcom_ni_prd_sqlserver = SQLServerResource(
    server=LDCOM_SQLSERVER_HOSTS["NI"],
    databases=LDCOM_SQLSERVER_DATABASES["NI"],
    username=cfg.dagster_ldcom_prd_username or "ENV NOT FOUND",
    password=cfg.dagster_secret_ldcom_prd_password,
    trust_server_certificate="yes",
    default_database="LDCOM_KIELSA_NIC",
)

ldcom_cr_prd_sqlserver = SQLServerResource(
    server=LDCOM_SQLSERVER_HOSTS["CR"],
    databases=LDCOM_SQLSERVER_DATABASES["CR"],
    username=cfg.dagster_ldcom_prd_username or "ENV NOT FOUND",
    password=cfg.dagster_secret_ldcom_prd_password,
    trust_server_certificate="yes",
    default_database="LDCOM_KIELSA_CR",
)

ldcom_cr_arb_prd_sqlserver = SQLServerResource(
    server=LDCOM_SQLSERVER_HOSTS["CR_ARB"],
    databases=LDCOM_SQLSERVER_DATABASES["CR_ARB"],
    username=cfg.dagster_ldcom_prd_username or "ENV NOT FOUND",
    password=cfg.dagster_secret_ldcom_prd_password,
    trust_server_certificate="yes",
    default_database="LDCOM_KIELSA_CR",
)


ldcom_gt_prd_sqlserver = SQLServerResource(
    server=LDCOM_SQLSERVER_HOSTS["GT"],
    databases=LDCOM_SQLSERVER_DATABASES["GT"],
    username=cfg.dagster_ldcom_prd_username or "ENV NOT FOUND",
    password=cfg.dagster_secret_ldcom_prd_password,
    trust_server_certificate="yes",
    default_database="LDCOM_KIELSA_GT",
)

ldcom_sv_prd_sqlserver = SQLServerResource(
    server=LDCOM_SQLSERVER_HOSTS["SV"],
    databases=LDCOM_SQLSERVER_DATABASES["SV"],
    username=cfg.dagster_ldcom_prd_username or "ENV NOT FOUND",
    password=cfg.dagster_secret_ldcom_prd_password,
    trust_server_certificate="yes",
    default_database="LDCOM_KIELSA_SALVADOR",
)

siteplus_sqlldsubs_sqlserver = SQLServerResource(
    server=LDCOM_SQLSERVER_HOSTS["SQLLDSUBS"],
    databases=LDCOM_SQLSERVER_DATABASES["SQLLDSUBS"],
    username=cfg.dagster_ldcom_prd_username or "ENV NOT FOUND",
    password=cfg.dagster_secret_ldcom_prd_password,
    trust_server_certificate="yes",
    default_database="SITEPLUS",
)


if __name__ == "__main__":
    x = SQLServerNonRuntimeResource(
        server="server",
        databases=[],
        username="user",
        password="password",
        default_database="default_database",
        trust_server_certificate="yes",
        allow_any_database=False,
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
    @patch("pymssql.connect")
    def test_get_connection(mock_connect):
        mock_conn = MagicMock(spec=pymssql.Connection)  # Mock connection object
        mock_connect.return_value = mock_conn  # Return the mock connection object

        resource = SQLServerNonRuntimeResource(
            server="test_server",
            databases=["test_database"],
            username="test_username",
            password="test_password",
            default_database="test_default_database",
            allow_any_database=True,
        )
        with resource.get_connection(engine=EngineType.PYMSSQL) as connection:
            assert (
                connection is mock_conn
            )  # Assert that the connection is the mock connection object
            mock_connect.assert_called_once()  # Assert that the mock connection was called once

    # Test the query method
    @patch("pymssql.connect")
    def test_query(mock_connect):
        mock_conn = MagicMock(spec=pymssql.Connection)  # Mock connection object
        mock_connect.return_value = mock_conn  # Return the mock connection object
        mock_cursor = MagicMock(spec=pymssql.Cursor)
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
        query_result = resource.query(
            "SELECT * FROM test_table", engine=EngineType.PYMSSQL
        )
        assert query_result == []
        mock_cursor.execute.assert_called_once()
        mock_cursor.fetchall.assert_called_once()

    # Test the execute_and_commit method
    @patch("pymssql.connect")
    def test_execute_and_commit(mock_connect):
        mock_cursor = MagicMock(spec=pymssql.Cursor)
        mock_cursor.execute.return_value = None
        mock_conn = MagicMock(spec=pymssql.Connection)
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
        resource.execute_and_commit(
            "INSERT INTO test_table (column1) VALUES ('value1')",
            autocommit=False,
            engine=EngineType.PYMSSQL,
        )
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
    @patch("sqlalchemy.create_engine")
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

    # Test the get_pymssql_conn method
    @patch("pymssql.connect")
    def test_get_pymssql_conn(mock_connect):
        mock_conn = MagicMock(spec=pymssql.Connection)  # Mock connection object
        mock_connect.return_value = mock_conn  # Return the mock connection object
        mock_connect.return_value.cursor.return_value = MagicMock(spec=pymssql.Cursor)

        resource = SQLServerNonRuntimeResource(
            server="test_server",
            databases=["test_default_database"],
            username="test_username",
            password="test_password",
            default_database="test_default_database",
        )
        with resource.get_pymssql_conn() as conn:
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
            allow_any_database=False,
        )
        expected_conn_string = (
            f"Driver={{{resource.driver}}};"
            f"Server={{{resource.server}}};"
            f"Database={{{resource.default_database}}};"
            f"Uid={{{resource.username}}};"
            f"Pwd={{{resource.password}}};"
            f"TrustServerCertificate={{{resource.trust_server_certificate}}};"
        )
        conn_string = resource.get_arrow_odbc_conn_string()
        assert conn_string == expected_conn_string, (
            f"Expected connection string: {expected_conn_string}, but got: {conn_string}"
        )

    # Test
    test_sql_server_base_resource()
    test_sql_server_non_runtime_resource()
    test_sql_server_resource()
    test_get_connection()
    test_query()
    test_execute_and_commit()
    test_get_sqlalchemy_url()
    test_get_sqlalchemy_conn()
    test_get_pymssql_conn()
    test_get_arrow_odbc_conn_string()
    # print(dwh_farinter_dl.get_arrow_odbc_conn_string())
