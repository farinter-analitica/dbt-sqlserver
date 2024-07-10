import pytest
from datetime import datetime
from pathlib import Path
from dagster import build_op_context
from dagster_sap_gf.assets.sap_etl_dwh import DL_SAP_T001  # Adjust the import based on your actual module structure
from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_dl

@pytest.fixture
def mock_sqlserver_resource(mocker):
    mock_resource = mocker.Mock()
    mock_connection = mocker.Mock()
    mock_enter = mocker.Mock(return_value=mock_connection)
    mock_exit = mocker.Mock()
    mock_resource.get_connection.return_value.__enter__ = mock_enter
    mock_resource.get_connection.return_value.__exit__ = mock_exit
    mock_resource.query.return_value = [(datetime(2022, 1, 1, 0, 0).isoformat(),)]
    return mock_resource

@pytest.fixture
def mock_context():
    return build_op_context()

def test_DL_SAP_T001(mock_sqlserver_resource, mock_context, mocker):
    context = mock_context
    DL_SAP_T001(context, dwh_farinter_dl=dwh_farinter_dl)