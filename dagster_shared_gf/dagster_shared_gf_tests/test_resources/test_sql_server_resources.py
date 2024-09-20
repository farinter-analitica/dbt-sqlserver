import pytest
import pyodbc, os
from unittest.mock import patch, MagicMock

from typing import Any
from dagster_shared_gf.resources.sql_server_resources import encode_password, decode_password, SQLServerResource, SQLServerNonRuntimeResource, dwh_farinter, dwh_farinter_adm, dwh_farinter_dl, dwh_farinter_database_admin
import dagster_shared_gf.resources.sql_server_resources as sql_server_resources
# Test encoding and decoding functions
def test_encode_password():
    assert encode_password("test_password") == "dGVzdF9wYXNzd29yZA=="

def test_decode_password():
    assert decode_password("dGVzdF9wYXNzd29yZA==") == "test_password"

# Mock the pyodbc connection and cursor
@patch('pyodbc.connect')
def test_get_connection(mock_connect):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    with dwh_farinter_database_admin.get_connection(database='DL_FARINTER') as conn:
        assert conn == mock_conn
        mock_connect.assert_called_once()


@patch('pyodbc.connect')
def test_query(mock_connect):
    mock_conn = MagicMock(spec=pyodbc.Connection)
    mock_cursor = MagicMock(spec=pyodbc.Cursor)
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_row = MagicMock(spec=pyodbc.Row)
    mock_any = MagicMock(spec=Any)
    mock_cursor.fetchall.return_value = [mock_row]
    mock_cursor.fetchval.return_value = mock_any

    result = dwh_farinter_database_admin.query("SELECT * FROM test_table", database='DL_FARINTER')
    assert result == [mock_row]
    mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table")

    result_one = dwh_farinter_database_admin.query("SELECT 1", database='DL_FARINTER', fetch_val=True)
    assert result_one == mock_any
    mock_cursor.execute.assert_called_with("SELECT 1")

@patch('pyodbc.connect')
def test_execute_and_commit(mock_connect):
    mock_conn = MagicMock(spec=pyodbc.Connection)
    mock_cursor = MagicMock(spec=pyodbc.Cursor)
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    dwh_farinter_database_admin.execute_and_commit("INSERT INTO test_table (col) VALUES ('value')")
    mock_cursor.execute.assert_called_once_with("INSERT INTO test_table (col) VALUES ('value')")
    mock_conn.commit.assert_not_called()

    mock_conn.autocommit = True
    dwh_farinter_database_admin.execute_and_commit("INSERT INTO test_table (col) VALUES ('value')", connection=mock_conn)
    mock_cursor.execute.assert_called_with("INSERT INTO test_table (col) VALUES ('value')")
    mock_conn.commit.assert_not_called()

    mock_conn.autocommit = False
    dwh_farinter_database_admin.execute_and_commit("INSERT INTO test_table (col) VALUES ('value')", connection=mock_conn)
    mock_cursor.execute.assert_called_with("INSERT INTO test_table (col) VALUES ('value')")
    mock_conn.commit.assert_called_once()
# More tests can be added here as needed
def test_cursor_fetch_first_result():
    """Test the cursor_fetch_first_result function."""
    # Test case 1: Fetch value
    cursor = MagicMock(spec=pyodbc.Cursor)
    cursor.fetchval.return_value = 1
    result = dwh_farinter_database_admin.cursor_fetch_first_result(cursor, fetch_val=True)
    assert result == 1

    # Test case 2: Fetch all
    cursor = MagicMock(spec=pyodbc.Cursor)
    cursor.fetchall.return_value = [(1, 2), (3, 4)]
    result = dwh_farinter_database_admin.cursor_fetch_first_result(cursor)
    assert result == [(1, 2), (3, 4)]

    # Test case 3: Fetch value with multiple result sets
    cursor = MagicMock(spec=pyodbc.Cursor)
    cursor.fetchval.side_effect = [1, 2]
    result = dwh_farinter_database_admin.cursor_fetch_first_result(cursor, fetch_val=True)
    assert result == 1

    # Test case 4: Fetch all with multiple result sets
    cursor = MagicMock(spec=pyodbc.Cursor)
    cursor.fetchall.side_effect = [
        [(1, 2), (3, 4)],
        [(5, 6), (7, 8)],
    ]
    result = dwh_farinter_database_admin.cursor_fetch_first_result(cursor)
    assert result == [(1, 2), (3, 4)]

    # Test case 5: Skip non-result set messages
    cursor = MagicMock(spec=pyodbc.Cursor)
    cursor.nextset.side_effect = iter([True, False, False])
    cursor.fetchval.side_effect = iter([
        pyodbc.ProgrammingError("Non-result set message"),
        'test4',
    ])
    result = dwh_farinter_database_admin.cursor_fetch_first_result(cursor, fetch_val=True)
    assert result == 'test4'

    # Test case 6: Skip multiple non-result set messages
    cursor = MagicMock(spec=pyodbc.Cursor)
    cursor.nextset.side_effect = iter([True, True, False])
    cursor.fetchval.side_effect = iter([
        pyodbc.ProgrammingError("Non-result set message 1"),
        pyodbc.ProgrammingError("Non-result set message 2"),
        'test5',
    ])
    result = dwh_farinter_database_admin.cursor_fetch_first_result(cursor, fetch_val=True)
    assert result == 'test5'