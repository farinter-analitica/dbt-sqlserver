import pytest
import pyodbc, os
from unittest.mock import patch, MagicMock

from dagster_shared_gf.resources.sql_server_resources import encode_password, decode_password, SQLServerResource, SQLServerNonRuntimeResource, dwh_farinter, dwh_farinter_adm, dwh_farinter_dl, dwh_farinter_database_admin

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
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_row = MagicMock(spec=pyodbc.Row)
    mock_cursor.fetchall.return_value = [mock_row]
    mock_cursor.fetchone.return_value = mock_row

    result = dwh_farinter_database_admin.query("SELECT * FROM test_table", database='DL_FARINTER')
    assert result == [mock_row]
    mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table")

    result_one = dwh_farinter_database_admin.query("SELECT * FROM test_table", database='DL_FARINTER', fetch_one=True)
    assert result_one == [mock_row]
    mock_cursor.execute.assert_called_with("SELECT * FROM test_table")

@patch('pyodbc.connect')
def test_execute_and_commit(mock_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
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
