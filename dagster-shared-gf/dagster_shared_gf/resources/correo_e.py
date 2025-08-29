import os
import smtplib
import ssl

##from dagster._utils.alert import send_email_via_ssl, send_email_via_starttls
from datetime import datetime
from typing import Callable, Optional, Sequence, TypeVar
from dagster_shared_gf.config import get_dagster_config
import dagster as dg

T = TypeVar("T")

cfg = get_dagster_config()

EMAIL_MESSAGE = """From: {email_from}
To: {email_to}
MIME-Version: 1.0
Content-type: text/html; charset=UTF-8
Subject: {email_subject}

{email_body}

<!-- this ensures Gmail doesn't trim the email -->
<span style="opacity: 0"> {randomness} </span>
"""


def send_email_via_ssl(
    email_from: str,
    email_password: str,
    email_to: Sequence[str],
    message: str,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
):
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_host, smtp_port, context=context) as server:
        server.login(smtp_user, email_password)
        server.sendmail(email_from, email_to, message.encode("utf-8"))


def send_email_via_starttls(
    email_from: str,
    email_password: str,
    email_to: Sequence[str],
    message: str,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
):
    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls(context=context)
        server.login(smtp_user, email_password)
        server.sendmail(email_from, email_to, message.encode("utf-8"))


class EmailSenderResource(dg.ConfigurableResource):
    email_from: str
    email_password: str
    smtp_host: str
    smtp_port: int = 465
    smtp_user: Optional[str] = None
    smtp_type: str = "SSL"

    def send_email(
        self, email_to: Sequence[str] | set[str], email_subject: str, email_body: str
    ):
        # email_message = MIMEText(email_body,'html', 'utf-8')
        # email_message['From'] = self.email_from
        # email_message['To'] = ",".join(email_to)
        # email_message['Subject'] = email_subject
        email_to = list(email_to)

        if isinstance(self.email_password, dg.EnvVar):
            email_password = self.email_password.get_value()
        else:
            email_password = self.email_password
        if not email_password:
            raise ValueError("Email_password is required but not set")

        email_message = EMAIL_MESSAGE.format(
            email_to=",".join(email_to),
            email_from=self.email_from,
            email_subject=email_subject,
            email_body=f"<pre><code>{email_body}</code></pre>",
            # email_body=email_body.replace("\n", "<br>").replace(" ", "&nbsp;"),
            # email_body=quopri.encodestring(email_body.encode("utf-8")).decode("utf-8"),
            randomness=datetime.now(),
        )
        if self.smtp_type == "SSL":
            send_email_via_ssl(
                email_from=self.email_from,
                email_password=email_password,
                email_to=email_to,
                message=email_message,
                smtp_host=self.smtp_host,
                smtp_port=self.smtp_port,
                smtp_user=self.smtp_user or self.email_from,
            )
        elif self.smtp_type == "STARTTLS":
            send_email_via_starttls(
                email_from=self.email_from,
                email_password=email_password,
                email_to=email_to,
                message=email_message,  # email_body.replace("\n", "<br>").replace(" ", "&nbsp;"),
                smtp_host=self.smtp_host,
                smtp_port=self.smtp_port,
                smtp_user=self.smtp_user or self.email_from,
            )
        else:
            raise Exception(f'smtp_type "{self.smtp_type}" is not supported.')


class ValidatedEnvVar(str):
    """A string-like class that validates environment variable exists at runtime."""

    _env_var_name: str

    def __new__(cls, env_var_name: str):
        value = os.getenv(env_var_name)
        if not value:
            raise ValueError(
                f"Environment variable '{env_var_name}' is required but not set"
            )
        instance = str.__new__(cls, value)
        return instance


class LazyEmailSenderResource(EmailSenderResource):
    _factory: Callable[[], EmailSenderResource]
    _instance: Optional[EmailSenderResource] = None

    def __init__(self, factory: Callable[[], EmailSenderResource]):
        object.__setattr__(self, "_factory", factory)
        object.__setattr__(self, "_instance", None)

    def _get_instance(self) -> EmailSenderResource:
        if self._instance is None:
            object.__setattr__(self, "_instance", self._factory())
        if self._instance is None:
            raise ValueError("EmailSenderResource instance could not be created")
        return self._instance

    def __getattribute__(self, name):
        if name in ("_factory", "_instance", "_get_instance", "__class__"):
            return object.__getattribute__(self, name)
        return getattr(self._get_instance(), name)


enviador_correo_e_analitica_farinter: EmailSenderResource = LazyEmailSenderResource[
    EmailSenderResource
](
    lambda: EmailSenderResource(
        email_from=ValidatedEnvVar("DAGSTER_EMAIL_ADDRESS"),
        email_password=cfg.dagster_secret_email_password or "NOT-SET",
        smtp_host="mail.farinter.com",
        smtp_port=26,
        smtp_type="STARTTLS",
    )
)


def get_max_column_value(
    server: str, database: str, table: str, column: str
) -> datetime:
    # Placeholder function to simulate retrieving the maximum column value from a database.
    # Replace this with actual database query logic.
    import random
    from datetime import timedelta

    return datetime.now() - timedelta(days=random.randint(0, 5))


def check_max_value_difference(max_value_a: datetime, max_value_b: datetime) -> bool:
    return (max_value_b - max_value_a).days <= 1


@dg.sensor(minimum_interval_seconds=3600)
def example_for_tests(
    context: dg.SensorEvaluationContext,
    enviador_correo_e_analitica_farinter: EmailSenderResource,
):
    # Configuration for the databases and tables
    server_a = "server_a"
    server_b = "server_b"
    database = "example_db"
    table = "example_table"
    column = "example_column"

    max_value_a = get_max_column_value(server_a, database, table, column)
    max_value_b = get_max_column_value(server_b, database, table, column)

    if not check_max_value_difference(max_value_a, max_value_b):
        email_sender = enviador_correo_e_analitica_farinter
        email_subject = "Max Value Check Alert"
        email_body = f"""
        The maximum value of column {column} in table {table} on server B ({max_value_b}) is more than 1 day ahead of the corresponding value on server A ({max_value_a}).
        """
        email_to = ["brian.padilla@farinter.com"]  # Replace with actual recipients

        email_sender.send_email(email_to, email_subject, email_body)
        return dg.SensorResult()

    return dg.SensorResult(
        skip_reason=dg.SkipReason(
            "No alert needed, values are within the expected range."
        )
    )


if __name__ == "__main__":
    # Obtener configuración cacheada (la carga del .env ya la maneja get_dagster_config)

    os.environ["DAGSTER_TEST_MODE"] = "1"

    print(cfg.dagster_email_address)

    # Simular contexto de sensor
    # Usar un contexto real de Dagster o un mock mínimo
    try:
        from dagster import build_sensor_context

        context = build_sensor_context()
    except ImportError:
        # Fallback mínimo si no está disponible
        class DummyContext:
            pass

        context = DummyContext()

    print("\n[TEST] --- Simulación: valores iguales, no debe enviar correo ---")
    # Monkeypatch get_max_column_value para devolver el mismo valor
    from datetime import datetime

    def mock_get_max_column_value_same(server, database, table, column):
        return datetime(2024, 1, 1, 0, 0, 0)

    original_func = get_max_column_value
    globals()["get_max_column_value"] = mock_get_max_column_value_same

    result = example_for_tests(
        context,
        enviador_correo_e_analitica_farinter=enviador_correo_e_analitica_farinter,
    )
    print("[TEST] example_for_tests() returned:", result)

    print("\n[TEST] --- Simulación: valores diferentes, debe enviar correo ---")

    def mock_get_max_column_value_diff(server, database, table, column):
        if server == "server_a":
            return datetime(2024, 1, 1, 0, 0, 0)
        else:
            return datetime(2024, 1, 3, 0, 0, 0)

    globals()["get_max_column_value"] = mock_get_max_column_value_diff
    result = example_for_tests(
        context,
        enviador_correo_e_analitica_farinter=enviador_correo_e_analitica_farinter,
    )
    print("[TEST] example_for_tests() returned:", result)

    # Restaurar función original
    globals()["get_max_column_value"] = original_func
