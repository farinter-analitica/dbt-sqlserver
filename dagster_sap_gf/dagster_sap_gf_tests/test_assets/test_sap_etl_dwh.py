import pytest
from datetime import datetime
from pathlib import Path
from dagster import build_op_context
from dagster_sap_gf.assets.sap_etl_dwh import DL_SAP_T001  # Adjust the import based on your actual module structure


@pytest.fixture
def mock_sqlserver_resource(mocker):
    mock_resource = mocker.Mock()
    mock_connection = mocker.Mock()
    mock_enter = mocker.Mock(return_value=mock_connection)
    mock_exit = mocker.Mock()
    mock_resource.get_connection.return_value.__enter__ = mock_enter
    mock_resource.get_connection.return_value.__exit__ = mock_exit
    mock_resource.query.return_value = [(datetime(2022, 1, 1, 0, 0),)]
    return mock_resource

@pytest.fixture
def mock_context():
    return build_op_context()

def test_DL_SAP_T001(mock_sqlserver_resource, mock_context, mocker):
    table = "DL_SAP_T001"
    database = "DL_FARINTER"
    schema = "dbo"
    sql_content = "SELECT * FROM table WHERE last_date_updated = '{last_date_updated}'"

    mock_open = mocker.patch("builtins.open", mocker.mock_open(read_data=sql_content))
    mock_path = mocker.patch("pathlib.Path.joinpath", return_value=Path("/mock/path/to/sqlfile.sql"))

    # Execute the asset
    DL_SAP_T001(
        context=mock_context,
        dwh_farinter_dl=mock_sqlserver_resource
    )

    # Assert that get_connection was called with the correct database
    mock_sqlserver_resource.get_connection.assert_called_once_with(database)

    # Assert that the query to get the last update date was executed
    last_date_updated_query = f"""SELECT MAX(Fecha_Actualizado) FROM [{database}].[{schema}].[{table}]"""
    mock_sqlserver_resource.query.assert_called_once_with(query=last_date_updated_query, connection=mock_sqlserver_resource.get_connection().__enter__())

    # Assert the correct SQL query was executed
    last_date_updated = "2022-01-01"
    expected_query = sql_content.format(last_date_updated=last_date_updated)
    mock_sqlserver_resource.execute_and_commit.assert_called_once_with(expected_query, connection=mock_sqlserver_resource.get_connection().__enter__())