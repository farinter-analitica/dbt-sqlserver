import pytest
from dagster import build_op_context
from unittest.mock import MagicMock
from dagster_kielsa_gf.sensors import comprobar_max_val_replicas_sql_server

class TestComprobarMaxValReplicasSQLServer:
    
    @pytest.fixture(autouse=True)
    def setup_mocks(self, mocker):
        self.sql_server_a_mock = mocker.MagicMock()
        self.sql_server_b_mock = mocker.MagicMock()

        self.conn_a_mock = mocker.MagicMock()
        self.conn_b_mock = mocker.MagicMock()

        self.sql_server_a_mock.get_connection.return_value.__enter__.return_value = self.conn_a_mock
        self.sql_server_a_mock.get_connection.return_value.__exit__.return_value = None
        
        self.sql_server_b_mock.get_connection.return_value.__enter__.return_value = self.conn_b_mock
        self.sql_server_b_mock.get_connection.return_value.__exit__.return_value = None

        self.context = build_op_context()

    def test_comprobar_max_val_replicas_sql_server(self, mocker):
        column = "test_column"
        table = "test_table"
        
        # Mock the results for SQLServer A
        results_a = [(100,)]
        self.sql_server_a_mock.query.return_value = results_a

        # Mock the results for SQLServer B
        results_b = [(200,)]
        self.sql_server_b_mock.query.return_value = results_b

        result = comprobar_max_val_replicas_sql_server(
            sql_server_a=self.sql_server_a_mock,
            sql_server_b=self.sql_server_b_mock,
            column=column,
            table=table
        )

        self.sql_server_a_mock.get_connection.assert_called_once()
        self.sql_server_b_mock.get_connection.assert_called_once()
        
        self.sql_server_a_mock.query.assert_called_once_with(
            query=f"SELECT MAX({column}) FROM {table}",
            connection=self.conn_a_mock
        )
        self.sql_server_b_mock.query.assert_called_once_with(
            query=f"SELECT MAX({column}) FROM {table}",
            connection=self.conn_b_mock
        )

        assert result == {"max_val_a": 100, "max_val_b": 200}

    def test_comprobar_max_val_replicas_sql_server_no_results(self, mocker):
        column = "test_column"
        table = "test_table"
        
        # Mock the results for SQLServer A and B as empty
        results_a = [(None,)]
        results_b = [(None,)]
        self.sql_server_a_mock.query.return_value = results_a
        self.sql_server_b_mock.query.return_value = results_b

        result = comprobar_max_val_replicas_sql_server(
            sql_server_a=self.sql_server_a_mock,
            sql_server_b=self.sql_server_b_mock,
            column=column,
            table=table
        )

        assert result == {"max_val_a": None, "max_val_b": None}

if __name__ == "__main__":
    pytest.main()
