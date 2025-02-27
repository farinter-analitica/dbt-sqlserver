import pytest
from datetime import datetime, timedelta
from dagster import build_asset_context, build_op_context
from unittest.mock import patch, MagicMock
from dagster_kielsa_gf.job_control_replicas import (
    obtener_max_datetime_val_replicas_sql_server,
    es_delta_max_datetime_superado,
    obtener_servidores_de_replica_en_alerta,
    enviar_alertas_si_aplica,
    ParServidoresReplicaSQLServer
)
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.resources.correo_e import EmailSenderResource
import inspect

class TestDagsterJobControlReplicas:
    @pytest.fixture
    def mock_sql_server_resource(self):
        with patch('dagster_shared_gf.resources.sql_server_resources.SQLServerResource') as MockSQLServerResource:
            mock_resource = MockSQLServerResource.return_value
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = [[datetime.now().isoformat()]]
            mock_cursor.fetchone.return_value = [datetime.now().isoformat()]
            mock_conn.cursor.return_value = mock_cursor
            mock_conn.__enter__.return_value = mock_conn  # Ensure context manager behavior
            mock_conn.__exit__.return_value = None
            mock_resource.get_connection.return_value = mock_conn
            mock_resource.server = "mock_server"  # Ensure server attribute is a string

            def mock_query(query, connection=None, database="", fetch_val=False, autocommit=True):
                cursor = connection.cursor()
                cursor.execute(query)
                if fetch_val:
                    return cursor.fetchone()
                return cursor.fetchall()

            mock_resource.query.side_effect = mock_query
            yield mock_resource

            # Teardown code
            mock_conn.__exit__.assert_called()  # Ensure the connection's exit method was called

    @pytest.fixture
    def mock_email_sender_resource(self):
        with patch('dagster_shared_gf.resources.correo_e.EmailSenderResource') as MockEmailSenderResource:
            yield MockEmailSenderResource.return_value

    def test_obtener_max_datetime_val_replicas_sql_server(self, mock_sql_server_resource):
        context = build_op_context()
        max_val_origen, max_val_replica, logs = obtener_max_datetime_val_replicas_sql_server(
            context,
            mock_sql_server_resource,
            mock_sql_server_resource,
            "test_origen",
            "test_replica",
            "test_column_origen",
            "test_column_replica"
        )

        assert max_val_origen is not None
        assert max_val_replica is not None
        assert logs["max_val_origen"] == "Consulta exitosa"
        assert logs["max_val_replica"] == "Consulta exitosa"

    def test_es_delta_max_datetime_superado(self):
        referencia = datetime.now()
        comprobado = referencia - timedelta(hours=2)
        assert es_delta_max_datetime_superado(referencia, comprobado)

        comprobado = referencia - timedelta(minutes=30)
        assert not es_delta_max_datetime_superado(referencia, comprobado)

        assert es_delta_max_datetime_superado(None, None)
        assert es_delta_max_datetime_superado(referencia, None)
        assert not es_delta_max_datetime_superado(None, referencia)

    def test_obtener_servidores_de_replica_en_alerta(self, mock_sql_server_resource):
        context = build_op_context()

        mock_sql_server_resource.query.side_effect = [
            [[datetime.now().isoformat()]],  # Query result for origen
            [[(datetime.now() - timedelta(hours=2)).isoformat()]],  # Query result for replica
        ]
    # Get the function to inspect its arguments
        func_args = inspect.signature(obtener_servidores_de_replica_en_alerta).parameters
        args_mapping = {
                    arg: mock_sql_server_resource
                    for arg, param in func_args.items()
                    if param.annotation == SQLServerResource and arg != "context"
        }        
        resultado = obtener_servidores_de_replica_en_alerta(
            context,
            **args_mapping
        )

        # Ensure that the mocked server is used in the result
        assert "mock_server" in resultado

    def test_enviar_alertas_si_aplica(self, mock_email_sender_resource):
        context = build_op_context(
            resources={
                "enviador_correo_e_analitica_farinter": mock_email_sender_resource
            }
        )
        servidores_en_alerta = {"mock_server": [{"test": "data"}]}

        enviar_alertas_si_aplica(context, servidores_en_alerta)
        assert mock_email_sender_resource.send_email.called

    def test_enviar_alertas_si_aplica_no_alert(self, mock_email_sender_resource):
        context = build_op_context(
            resources={
                "enviador_correo_e_analitica_farinter": mock_email_sender_resource
            }
        )
        servidores_en_alerta = {}

        enviar_alertas_si_aplica(context, servidores_en_alerta)
        assert not mock_email_sender_resource.send_email.called
