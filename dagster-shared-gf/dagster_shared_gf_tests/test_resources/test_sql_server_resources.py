import pymssql
from unittest.mock import patch, MagicMock

from dagster_shared_gf.resources.sql_server_resources import (
    encode_password,
    decode_password,
    dwh_farinter_database_admin,
    EngineType,
    Row,
)
import sqlalchemy


# Test encoding and decoding functions
def test_encode_password():
    assert encode_password("test_password") == "dGVzdF9wYXNzd29yZA=="


def test_decode_password():
    assert decode_password("dGVzdF9wYXNzd29yZA==") == "test_password"


# Mock the pymssql connection and cursor
@patch("pymssql.connect")
def test_get_connection(mock_connect):
    mock_conn = MagicMock(spec=pymssql.Connection)
    mock_connect.return_value = mock_conn

    with dwh_farinter_database_admin.get_connection(
        database="DL_FARINTER", engine=EngineType.PYMSSQL
    ) as conn:
        assert conn == mock_conn
        mock_connect.assert_called_once()


@patch("sqlalchemy.create_engine")
def test_get_connection_sqlalchemy(mock_create_engine):
    mock_engine = MagicMock(spec=sqlalchemy.Engine)
    mock_conn = MagicMock(spec=sqlalchemy.Connection)
    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value = mock_conn
    mock_conn.closed = False

    with dwh_farinter_database_admin.get_connection(database="DL_FARINTER") as conn:
        assert conn == mock_conn
        mock_create_engine.assert_called_once()
        mock_engine.connect.assert_called_once()


@patch("sqlalchemy.create_engine")
def test_query_sqlalchemy(mock_create_engine):
    mock_engine = MagicMock(spec=sqlalchemy.Engine)
    mock_conn = MagicMock(spec=sqlalchemy.Connection)
    mock_raw_conn = MagicMock()
    mock_cursor = MagicMock(spec=pymssql.Cursor)

    # Set up the chain of mocks
    mock_conn.engine = mock_engine
    mock_engine.raw_connection.return_value = mock_raw_conn
    mock_raw_conn.cursor.return_value = mock_cursor

    # Configure the basic mock setup
    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value = mock_conn

    # Create the mock row that will be returned
    mock_row = MagicMock(spec=Row)

    # Set up cursor to return our mock row
    mock_cursor.description = ["column1"]
    mock_cursor.execute.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [mock_row]
    mock_cursor.fetchone.return_value = (1,)
    # Run the test
    result = dwh_farinter_database_admin.query(
        "SELECT * FROM test_table", database="DL_FARINTER"
    )
    assert result == [mock_row]

    scalar_result = dwh_farinter_database_admin.query(
        "SELECT 1", database="DL_FARINTER", fetch_val=True
    )
    assert scalar_result == 1


@patch("sqlalchemy.create_engine")
def test_execute_and_commit_sqlalchemy(mock_create_engine):
    mock_engine = MagicMock()
    mock_conn = MagicMock(spec=sqlalchemy.Connection)
    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value = mock_conn

    # First test - auto created connection with autocommit=True (default)
    dwh_farinter_database_admin.execute_and_commit(
        "INSERT INTO test_table (col) VALUES ('value')"
    )
    mock_conn.execute.assert_called_once()
    # Autocommit should be enabled via isolation_level="AUTOCOMMIT" when engine created

    # Reset mock
    mock_conn.reset_mock()

    # Second test - provided connection with autocommit=False
    mock_conn.in_transaction.return_value = True
    dwh_farinter_database_admin.execute_and_commit(
        "INSERT INTO test_table (col) VALUES ('value')",
        connection=mock_conn,
        autocommit=False,
    )
    mock_conn.execute.assert_called_once()
    mock_conn.commit.assert_called_once()


@patch("pymssql.connect")
def test_query(mock_connect):
    mock_conn = MagicMock(spec=pymssql.Connection)
    mock_cursor = MagicMock(spec=pymssql.Cursor)
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_row = MagicMock(spec=Row)
    mock_any = 1
    mock_cursor.fetchall.return_value = [mock_row]
    mock_cursor.fetchone.return_value = (mock_any,)

    result = dwh_farinter_database_admin.query(
        "SELECT * FROM test_table", database="DL_FARINTER", engine=EngineType.PYMSSQL
    )
    assert result == [mock_row]
    mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table")

    result_one = dwh_farinter_database_admin.query(
        "SELECT 1", database="DL_FARINTER", fetch_val=True, engine=EngineType.PYMSSQL
    )
    assert result_one == mock_any
    mock_cursor.execute.assert_called_with("SELECT 1")


@patch("pymssql.connect")
def test_execute_and_commit(mock_connect):
    mock_conn = MagicMock(spec=pymssql.Connection)
    mock_cursor = MagicMock(spec=pymssql.Cursor)
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    dwh_farinter_database_admin.execute_and_commit(
        "INSERT INTO test_table (col) VALUES ('value')",
        connection=mock_conn,
        engine=EngineType.PYMSSQL,
    )
    mock_cursor.execute.assert_called_once_with(
        "INSERT INTO test_table (col) VALUES ('value')"
    )
    mock_conn.commit.assert_not_called()

    mock_conn.autocommit = True
    dwh_farinter_database_admin.execute_and_commit(
        "INSERT INTO test_table (col) VALUES ('value')",
        connection=mock_conn,
        engine=EngineType.PYMSSQL,
    )
    mock_cursor.execute.assert_called_with(
        "INSERT INTO test_table (col) VALUES ('value')"
    )
    mock_conn.commit.assert_not_called()

    mock_conn.autocommit = False
    dwh_farinter_database_admin.execute_and_commit(
        "INSERT INTO test_table (col) VALUES ('value')",
        connection=mock_conn,
        engine=EngineType.PYMSSQL,
    )
    mock_cursor.execute.assert_called_with(
        "INSERT INTO test_table (col) VALUES ('value')"
    )
    mock_conn.commit.assert_called_once()


def test_cursor_fetch_first_result():
    """Test the cursor_fetch_first_result function."""
    # Test case 1: Fetch value - Valid description
    cursor = MagicMock(spec=pymssql.Cursor)
    cursor.description = ["column1"]  # Add this line
    cursor.fetchone.return_value = (1,)
    result = dwh_farinter_database_admin._cursor_fetch_first_result(
        cursor, fetch_val=True
    )
    assert result == 1

    # Test case 2: Fetch all - Valid description
    cursor = MagicMock(spec=pymssql.Cursor)
    cursor.description = ["column1"]  # Add this line
    cursor.fetchall.return_value = [(1, 2), (3, 4)]
    result = dwh_farinter_database_admin._cursor_fetch_first_result(cursor)
    assert result == [(1, 2), (3, 4)]

    # Test case 3: Fetch value with multiple result sets
    cursor = MagicMock(spec=pymssql.Cursor)
    cursor.description = ["column1"]  # Add this line
    cursor.fetchone.side_effect = [(1,), (2,)]
    result = dwh_farinter_database_admin._cursor_fetch_first_result(
        cursor, fetch_val=True
    )
    assert result == 1

    # Test case 4: Fetch all with multiple result sets
    cursor = MagicMock(spec=pymssql.Cursor)
    cursor.description = ["column1"]  # Add this line
    cursor.fetchall.side_effect = [
        [(1, 2), (3, 4)],
        [(5, 6), (7, 8)],
    ]
    result = dwh_farinter_database_admin._cursor_fetch_first_result(cursor)
    assert result == [(1, 2), (3, 4)]

    # Test case 5: Skip non-result set messages
    cursor = MagicMock(spec=pymssql.Cursor)
    # Set up description to be None initially, then valid after nextset
    cursor.description = None
    cursor.nextset.side_effect = [True, False]

    # After nextset() is called, change description to be valid
    def side_effect_nextset(*args, **kwargs):
        cursor.description = ["column1"]
        return True

    cursor.nextset.side_effect = side_effect_nextset
    cursor.fetchone.return_value = ("test4",)

    result = dwh_farinter_database_admin._cursor_fetch_first_result(
        cursor, fetch_val=True
    )
    assert result == "test4"

    # Test case 6: Skip multiple non-result set messages
    cursor = MagicMock(spec=pymssql.Cursor)

    # Set up a more complex side effect for nextset
    # First call sets description to None, second call sets it to valid
    def first_nextset(*args, **kwargs):
        cursor.description = None
        return True

    def second_nextset(*args, **kwargs):
        cursor.description = ["column1"]
        return True

    cursor.description = None
    cursor.nextset.side_effect = [first_nextset(), second_nextset()]
    cursor.fetchone.return_value = ("test5",)

    result = dwh_farinter_database_admin._cursor_fetch_first_result(
        cursor, fetch_val=True
    )
    assert result == "test5"
