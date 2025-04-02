import contextlib
import os
from typing import Any, Sequence, Union, Generator

import sqlalchemy
from sqlalchemy import text, create_engine, TextClause, Row
from sqlalchemy.engine import Connection as SQLAConnection
from sqlalchemy.pool import QueuePool
from dagster import ConfigurableResource, EnvVar, get_dagster_logger

from dagster_shared_gf.load_env_run import load_env_vars
from dagster_shared_gf.shared_functions import get_for_current_env

Connection = SQLAConnection
Row_Tuple = Row
dagster_logger = get_dagster_logger(name="Independent")

if not os.environ.get("DAGSTER_PG_USERNAME"):
    load_env_vars()
# Set environment variables
p_server = get_for_current_env(
    {"local": "172.16.2.235", "dev": "localhost", "prd": "localhost"}
)
p_user = get_for_current_env({"dev": os.environ.get("DAGSTER_PG_USERNAME")})
p_password = get_for_current_env({"dev": EnvVar("DAGSTER_PG_PASSWORD")})


class PostgreSQLResource(ConfigurableResource):
    server: str
    databases: list  # List of databases
    user: str
    password: str | None
    default_database: str  # Default database
    independent_instance: bool = False
    """Allow to run without a dagster instance"""

    def __post_init__(self):
        if not self.databases:
            raise ValueError("databases list cannot be empty")
        if self.default_database not in self.databases:
            raise ValueError(
                f"default_database {self.default_database} is not in the allowed list of databases"
            )

    def get_engine(self, database: str | None = None) -> sqlalchemy.engine.Engine:
        """Create and return a SQLAlchemy engine for the specified database."""
        if not database:
            database = self.default_database

        if database not in self.databases:
            raise ValueError(f"Database {database} is not in the allowed list.")

        conn_str = f"postgresql://{self.user}"
        if self.password:
            conn_str += f":{self.password}"
        conn_str += f"@{self.server}/{database}"

        return create_engine(
            conn_str,
            poolclass=QueuePool,
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_size=5,
            max_overflow=10,
        )

    @contextlib.contextmanager
    def get_connection(
        self, database: str | None = None
    ) -> Generator[Connection, None, None]:
        """Get a connection to the specified database."""
        if not database:
            database = self.default_database

        engine = self.get_engine(database)
        conn = engine.connect()

        try:
            yield conn
        finally:
            if conn:
                self.close_connection(conn)

    def close_connection(self, connection: Connection):
        """Close the provided connection."""
        try:
            if not connection.closed:
                connection.close()
        except Exception as e:
            msg = f"Error closing connection: {e}"
            self.custom_logging.error(msg)
            raise RuntimeError(msg)

    def _ensure_text(self, query_obj: Union[str, Any]) -> Any:
        """Ensure the query is wrapped in text() if it's a string."""
        if isinstance(query_obj, str):
            return text(query_obj)
        return query_obj

    def query(
        self,
        query: Union[str, TextClause],
        connection: Connection | None = None,
        database: str = "",
    ) -> Sequence[Row_Tuple] | None:
        """
        Executes a SQL query on a database connection and returns the result as a list of rows.
        Automatically converts string queries to SQLAlchemy text objects.

        Args:
            query: The SQL query to execute (str or SQLAlchemy query object)
            connection: The database connection to use (optional)
            database: The name of the database to connect to (optional)

        Returns:
            List of result rows or None if an error occurs
        """
        query_obj = self._ensure_text(query)

        try:
            if connection is None:
                with self.get_connection(database) as conn:
                    result = conn.execute(query_obj)
                    return result.all()
            else:
                result = connection.execute(query_obj)
                return result.all()

        except sqlalchemy.exc.ProgrammingError as e:
            if "relation" in str(e) and "does not exist" in str(e):
                msg = "Table does not exist error handling. Returning None to caller."
                self.custom_logging.error(msg)
                return None
            else:
                msg = f"An unexpected database error occurred: {str(e)}. Returning None to caller."
                self.custom_logging.error(msg)
                return None
        except Exception as e:
            msg = f"An unexpected error occurred: {str(e)}. Returning None to caller."
            self.custom_logging.error(msg)
            return None

    def execute_and_commit(
        self,
        query: Union[str, Any],
        connection: Connection | None = None,
        database: str = "",
    ):
        """
        Executes a SQL query on a database connection and commits the changes.
        Automatically converts string queries to SQLAlchemy text objects.

        Args:
            query: The SQL query to execute (str or SQLAlchemy query object)
            connection: The database connection to use (optional)
            database: The name of the database to connect to (optional)
        """
        query_obj = self._ensure_text(query)

        try:
            if connection is None:
                with self.get_connection(database) as conn:
                    conn.execute(query_obj)
                    conn.commit()
            else:
                connection.execute(query_obj)
                if not connection.in_transaction():
                    connection.commit()

        except Exception as e:
            msg = f"Error executing and committing query: {str(e)}."
            self.custom_logging.error(msg)
            raise

    @property
    def custom_logging(self):
        """Return the appropriate logger based on instance configuration."""
        if self.independent_instance:
            return dagster_logger
        else:
            return get_dagster_logger()


db_analitica_etl = PostgreSQLResource(
    server=p_server,
    databases=["analitica"],
    user=p_user,
    password=p_password,
    default_database="analitica",
)

db_nocodb_data_gf = PostgreSQLResource(
    server=get_for_current_env({"dev": os.environ.get("NOCODB_PG_FARINTER_HOST")}),
    databases=["nocodb_data_gf"],
    user=get_for_current_env({"dev": os.environ.get("NOCODB_PG_FARINTER_USERNAME")}),
    password=get_for_current_env(
        {"dev": os.environ.get("NOCODB_PG_FARINTER_SECRET_PASSWORD")}
    ),
    default_database="nocodb_data_gf",
)

db_independent_analitica_etl = PostgreSQLResource(
    server=p_server,
    databases=["analitica"],
    user=p_user,
    password=p_password.get_value(),
    default_database="analitica",
    independent_instance=True,
)

# Another way:
# class PostgresResource(ConfigurableResource):
#     def __init__(self, context: InitResourceContext):
#         self.host = context.resource_config['host']
#         self.user = context.resource_config['user']
#         self.password = context.resource_config['password']
#         self.db_name = context.resource_config['db_name']

#     def connect(self):
#         return psycopg.connect(
#             host=self.host,
#             user=self.user,
#             password=self.password,
#             dbname=self.db_name
#         )

#
# Example usage in an asset
# @asset(required_resource_keys={'postgres'})
# def my_asset(context):
#     postgres_resource = context.resources.postgres
#     with postgres_resource.connect() as conn:
#         # Perform database operations using the connection
#         ...
