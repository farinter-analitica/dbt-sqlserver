import pytest
from datetime import datetime, date
from unittest.mock import MagicMock, patch
from pathlib import Path
from dagster import build_asset_context, materialize_to_memory
from dagster_sap_gf.assets.sap_etl_dwh import DL_SAP_T001, SQLServerResource  # Adjust the import based on your actual module structure
import dagster_shared_gf.resources.sql_server_resources as sql_server_resources
#import dagster_sap_gf.assets.sap_etl_dwh as sap_etl_dwh

from dagster import ResourceDefinition
## @asset(
#     key_prefix=["DL_FARINTER"],
#     tags=tags_repo.Replicas.tag,
#     compute_kind="sqlserver"
# )
# def DL_SAP_T001(context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource) -> None: 
#     table = "DL_SAP_T001"
#     database = "DL_FARINTER"
#     schema = "dbo"
#     sql_file_path = file_path.joinpath(f"sap_etl_dwh_sql/{table}.sql").resolve()
    
#     try:
#         with open(sql_file_path, encoding="utf8") as procedure:
#             final_query = procedure.read()
#     except IOError as e:
#         context.log.error(f"Error reading SQL file: {e}")
#         return

#     try:
#         with dwh_farinter_dl.get_connection(database) as conn:
#             last_date_updated_query: str = f"""SELECT MAX(Fecha_Actualizado) FROM [{database}].[{schema}].[{table}]"""
#             last_date_updated_result: List[tuple] = dwh_farinter_dl.query(query=last_date_updated_query, connection=conn)
#             last_date_updated: date = date(1900, 1, 1)
#             if last_date_updated_result and last_date_updated_result[0][0] is not None:
#                 try:
#                     last_date_updated = datetime.fromisoformat(str(last_date_updated_result[0][0])).date()
#                 except ValueError as e:
#                     context.log.error(f"Error converting date: {e}, defaulting to {last_date_updated}.")
#             final_query = final_query.format(last_date_updated=last_date_updated.isoformat())
#             dwh_farinter_dl.execute_and_commit(final_query, connection=conn)
#     except Exception as e:
#         context.log.error(f"Error during database operation: {e}")
        

#mock the calls to the resource


# def test_DL_SAP_T001(mocker):
#     mocked_dwh_farinter_dl = mocker.patch.object(sql_server_resources, 'dwh_farinter_dl', autospec=True)
#     mock_context = build_asset_context( )
#     log_error_spy = mocker.spy(mock_context.log, 'error')

#     def test_error_on_query_date():
#         mocked_dwh_farinter_dl.query.return_value = [('dsfsdf',)]

#         DL_SAP_T001(context=mock_context, dwh_farinter_dl=mocked_dwh_farinter_dl)

#         log_error_spy.assert_called_once()    
#         mocked_dwh_farinter_dl.get_connection.assert_called_once()
#         assert log_error_spy.called and log_error_spy.call_args[0][0].startswith("Error converting date: ") , "Bad query return value not logged correctly."

#         mocker.resetall()
#     def test_success_on_query_date():
#         mocked_dwh_farinter_dl.query.return_value = [('2022-01-01',)]

#         DL_SAP_T001(context=mock_context, dwh_farinter_dl=mocked_dwh_farinter_dl)

#         mocked_dwh_farinter_dl.execute_and_commit.assert_called_once()
#         mocker.resetall()
#     test_error_on_query_date()
    
#     test_success_on_query_date()

class Test_DL_SAP_T001:
    @pytest.fixture
    def setup_mocks(self, mocker):
        self.mocked_dwh_farinter_dl = mocker.patch.object(sql_server_resources, 'dwh_farinter_dl', autospec=True)
        self.mock_context = build_asset_context()
        self.log_error_spy = mocker.spy(self.mock_context.log, 'error')

    def test_error_on_query_date(self, setup_mocks):
        self.mocked_dwh_farinter_dl.query.return_value = [('dsfsdf',)]

        DL_SAP_T001(context=self.mock_context, dwh_farinter_dl=self.mocked_dwh_farinter_dl)

        self.log_error_spy.assert_called_once()
        self.mocked_dwh_farinter_dl.get_connection.assert_called_once()
        assert self.log_error_spy.called and self.log_error_spy.call_args[0][0].startswith("Error converting date:"), "Bad query return value not logged correctly."

    def test_success_on_query_date(self, setup_mocks):
        self.mocked_dwh_farinter_dl.query.return_value = [('2022-01-01',)]

        DL_SAP_T001(context=self.mock_context, dwh_farinter_dl=self.mocked_dwh_farinter_dl)

        self.mocked_dwh_farinter_dl.execute_and_commit.assert_called_once()
