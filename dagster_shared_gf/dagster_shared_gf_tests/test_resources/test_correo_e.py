import pytest
from datetime import datetime, timedelta
from dagster import build_sensor_context
from dagster_shared_gf.resources.correo_e import EmailSenderResource, _example_for_tests

# Define a fixture for the EmailSenderResource with mock values
@pytest.fixture
def email_sender_resource():
    return EmailSenderResource(
        email_from="test@example.com",
        email_password="test_password",
        smtp_host="smtp.example.com",
        smtp_port=465,
        smtp_type="SSL"
    )

# Mock get_max_column_value function to control the test output
def mock_get_max_column_value(server: str, database: str, table: str, column: str) -> datetime:
    if server == "server_a":
        return datetime.now() - timedelta(days=2)
    elif server == "server_b":
        return datetime.now()

def mock_get_max_column_value_no_alert(server: str, database: str, table: str, column: str) -> datetime:
    if server == "server_a":
        return datetime.now() - timedelta(days=1)
    elif server == "server_b":
        return datetime.now() - timedelta(hours=23)

class TestReplicasLdcomStatus:
    @pytest.fixture(autouse=True)
    def setup_mocks(self, mocker):
        self.mock_get_max_column_value = mocker.patch(
            'dagster_shared_gf.resources.correo_e.get_max_column_value',
            side_effect=mock_get_max_column_value
        )
        self.mock_send_email = mocker.patch.object(
            EmailSenderResource,
            'send_email',
            return_value=None
        )


    def test_replicas_ldcom_status(self, email_sender_resource):
        context = build_sensor_context(resources={"enviador_correo_e_analitica_farinter": email_sender_resource})

        result = _example_for_tests(context, enviador_correo_e_analitica_farinter=email_sender_resource)
        
        assert result == 1  # Ensure the sensor sends an email

        # Verify get_max_column_value was called twice, once for each server
        assert self.mock_get_max_column_value.call_count == 2
        self.mock_get_max_column_value.assert_any_call("server_a", "example_db", "example_table", "example_column")
        self.mock_get_max_column_value.assert_any_call("server_b", "example_db", "example_table", "example_column")

        # Verify send_email was called with the expected arguments
        self.mock_send_email.assert_called_once()

    @pytest.fixture
    def setup_mocks_no_alert(self, mocker):
        self.mock_get_max_column_value = mocker.patch(
            'dagster_shared_gf.resources.correo_e.get_max_column_value',
            side_effect=mock_get_max_column_value_no_alert
        )
        self.mock_send_email = mocker.patch.object(
            EmailSenderResource,
            'send_email',
            return_value=None
        )


    def test_replicas_ldcom_status_no_alert(self, email_sender_resource, setup_mocks_no_alert):
        context = build_sensor_context(resources={"enviador_correo_e_analitica_farinter": email_sender_resource})

        result = _example_for_tests(context, enviador_correo_e_analitica_farinter=email_sender_resource)
        
        assert result == 0  # Ensure the sensor does not send an email

        # Verify get_max_column_value was called twice, once for each server
        assert self.mock_get_max_column_value.call_count == 2
        self.mock_get_max_column_value.assert_any_call("server_a", "example_db", "example_table", "example_column")
        self.mock_get_max_column_value.assert_any_call("server_b", "example_db", "example_table", "example_column")

        # Verify send_email was not called
        self.mock_send_email.assert_not_called()
