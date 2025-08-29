import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Optional
from dagster import EnvVar
from dagster._config.field_utils import IntEnvVar

# La carga del .env se realiza mediante la función `load_env_file` definida abajo.
from dotenv import load_dotenv


class CachedIntEnvVar(IntEnvVar):
    _value: Optional[int] = None

    @classmethod
    def create(cls, name: str) -> "CachedIntEnvVar":
        value = os.getenv(name)
        raw = int(value) if value is not None else None
        var = CachedIntEnvVar(0)
        var.name = name
        # store cached raw value to avoid triggering IntEnvVar.__int__ which raises
        var._value = raw
        return var

    def get_value(self, default: Optional[int] = None) -> Optional[int]:
        return self._value if self._value is not None else default


class CachedEnvVar(EnvVar):
    """Wrapper que resuelve y cachea el valor de una variable de entorno.

    Uso: definir secrets en `DagsterSettings` como `CachedEnvVar("MY_SECRET")`.
    - `get_value()` devuelve el valor cacheado.
    - `__str__` y `__repr__` no exponen el valor para evitar fugas en logs.
    """

    @classmethod
    def int(cls, name: str) -> "CachedIntEnvVar":
        return CachedIntEnvVar.create(name=name)

    def __init__(self, name: str, default: Optional[str] = None):
        self.name = name
        # resolver y cachear inmediatamente (no exponer mediante str)
        self._value: Optional[str] = os.getenv(self.name, default)

    def get_value(self, default: Optional[str] = None) -> Optional[str]:
        return self._value or default


@dataclass(frozen=True)
class DagsterSettings:
    """Objeto centralizado de settings para los paquetes Dagster.

    Todos los valores basados en variables de entorno deben exponerse como
    atributos aquí. El código que use la configuración debe acceder a los
    atributos (p.ej. settings.graphql_port) y no usar directamente
    os.environ ni os.getenv.
    """

    instance_current_env: str = field(
        default_factory=lambda: os.getenv("INSTANCE_CURRENT_ENV", "dev")
    )
    graphql_port: int = field(
        default_factory=lambda: int(
            os.getenv(
                "DAGSTER_GRAPHQL_PORT", os.getenv("DAGSTER_WEBSERVER_PORT", "3000")
            )
        )
    )
    dagster_webserver_host: str = field(
        default_factory=lambda: os.getenv("DAGSTER_WEBSERVER_HOST", "localhost")
    )
    dagster_home: Optional[str] = field(
        default_factory=lambda: os.getenv("DAGSTER_HOME")
    )

    # Agregar otras configuraciones comunes aquí según sea necesario, p. ej.
    # credenciales de base de datos, SMTP, etc. Mantener los nombres en
    # estilo atributo (snake_case).
    # --- Configuración de bases de datos (Postgres / SQL Server / LDCOM) ---
    dagster_dev_dwh_farinter_sql_server: Optional[str] = field(
        default_factory=lambda: os.getenv("DAGSTER_DEV_DWH_FARINTER_SQL_SERVER")
    )
    dagster_prd_dwh_farinter_sql_server: Optional[str] = field(
        default_factory=lambda: os.getenv("DAGSTER_PRD_DWH_FARINTER_SQL_SERVER")
    )
    dagster_dev_dwh_farinter_username: Optional[str] = field(
        default_factory=lambda: os.getenv("DAGSTER_DEV_DWH_FARINTER_USERNAME")
    )
    dagster_dev_dwh_farinter_password: Optional[str] = field(
        default_factory=lambda: os.getenv("DAGSTER_DEV_DWH_FARINTER_PASSWORD")
    )
    dagster_prd_dwh_farinter_username: Optional[str] = field(
        default_factory=lambda: os.getenv("DAGSTER_PRD_DWH_FARINTER_USERNAME")
    )
    dagster_prd_dwh_farinter_password: Optional[str] = field(
        default_factory=lambda: os.getenv("DAGSTER_PRD_DWH_FARINTER_PASSWORD")
    )
    dagster_sql_server_odbc_driver: Optional[str] = field(
        default_factory=lambda: os.getenv("DAGSTER_SQL_SERVER_ODBC_DRIVER")
    )

    # Secrets / passwords
    dagster_pg_password: CachedEnvVar = field(
        default_factory=lambda: CachedEnvVar("DAGSTER_PG_PASSWORD")
    )
    nocodb_pg_farinter_secret_password: CachedEnvVar = field(
        default_factory=lambda: CachedEnvVar("NOCODB_PG_FARINTER_SECRET_PASSWORD")
    )
    auroraqa_pg_farinter_secret_password: CachedEnvVar = field(
        default_factory=lambda: CachedEnvVar("AURORAQA_PG_FARINTER_SECRET_PASSWORD")
    )
    dagster_secret_analitica_farinternet_password: CachedEnvVar = field(
        default_factory=lambda: CachedEnvVar(
            "DAGSTER_SECRET_ANALITICA_FARINTERNET_PASSWORD"
        )
    )

    # Postgres / Nocodb / Aurora
    dagster_pg_username: Optional[str] = field(
        default_factory=lambda: os.getenv("DAGSTER_PG_USERNAME")
    )
    nocodb_pg_farinter_host: Optional[str] = field(
        default_factory=lambda: os.getenv("NOCODB_PG_FARINTER_HOST")
    )
    nocodb_pg_farinter_username: Optional[str] = field(
        default_factory=lambda: os.getenv("NOCODB_PG_FARINTER_USERNAME")
    )
    auroraqa_pg_farinter_host: Optional[str] = field(
        default_factory=lambda: os.getenv("AURORAQA_PG_FARINTER_HOST")
    )
    auroraqa_pg_farinter_username: Optional[str] = field(
        default_factory=lambda: os.getenv("AURORAQA_PG_FARINTER_USERNAME")
    )

    # SMB / NAS / redes
    dagster_dev_dwh_farinter_ip: Optional[str] = field(
        default_factory=lambda: os.getenv("DAGSTER_DEV_DWH_FARINTER_IP")
    )
    dagster_prd_dwh_farinter_ip: Optional[str] = field(
        default_factory=lambda: os.getenv("DAGSTER_PRD_DWH_FARINTER_IP")
    )
    dagster_analitica_farinternet_username: Optional[str] = field(
        default_factory=lambda: os.getenv("DAGSTER_ANALITICA_FARINTERNET_USERNAME")
    )
    # Valor genérico para proyectos/legacy que usan DAGSTER_DWH_FARINTER_IP
    dagster_dwh_farinter_ip: Optional[str] = field(
        default_factory=lambda: os.getenv("DAGSTER_DWH_FARINTER_IP")
    )
    # Path/variable usada por sling
    sling_home_dir: Optional[str] = field(
        default_factory=lambda: os.getenv("SLING_HOME_DIR")
    )

    # Email / SMTP
    dagster_email_address: Optional[str] = field(
        default_factory=lambda: os.getenv("DAGSTER_EMAIL_ADDRESS")
    )
    dagster_secret_email_password: Optional[str] = field(
        default_factory=lambda: CachedEnvVar("DAGSTER_SECRET_EMAIL_PASSWORD")
    )

    # Cached EnvVar instances for common secrets (so code can use cfg.<attr> where needed)
    dagster_secret_dev_dwh_farinter_password: CachedEnvVar = field(
        default_factory=lambda: CachedEnvVar("DAGSTER_SECRET_DEV_DWH_FARINTER_PASSWORD")
    )
    dagster_secret_prd_dwh_farinter_password: CachedEnvVar = field(
        default_factory=lambda: CachedEnvVar("DAGSTER_SECRET_PRD_DWH_FARINTER_PASSWORD")
    )
    dagster_secret_ldcom_prd_password: CachedEnvVar = field(
        default_factory=lambda: CachedEnvVar("DAGSTER_SECRET_LDCOM_PRD_PASSWORD")
    )
    dagster_secret_analitica_su_password: CachedEnvVar = field(
        default_factory=lambda: CachedEnvVar("DAGSTER_SECRET_ANALITICA_SU_PASSWORD")
    )

    # LDCOM / replicas
    dagster_ldcom_prd_username: Optional[str] = field(
        default_factory=lambda: os.getenv("DAGSTER_LDCOM_PRD_USERNAME")
    )

    # DBT related
    dagster_dbt_parse_project_on_load: Optional[str] = field(
        default_factory=lambda: os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD")
    )

    @property
    def graphql_endpoint(self) -> str:
        return f"http://{self.dagster_webserver_host}:{self.graphql_port}/graphql"

    # Helpers para obtener el valor real de secrets cuando sea necesario.
    def get_secret(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """Devuelve el valor resuelto de un secret definido como CachedEnvVar.

        name: nombre del atributo en el objeto DagsterSettings, no el nombre de la
        variable de entorno. Ejemplo: cfg.get_secret('dagster_pg_password')
        """
        attr = getattr(self, name, None)
        if isinstance(attr, CachedEnvVar):
            return attr.get_value(default=default)
        # Si no es CachedEnvVar, intentar devolver tal cual (compatibilidad)
        return attr


@lru_cache(maxsize=1)
def get_dagster_config() -> DagsterSettings:
    """Devuelve una instancia cacheada de `DagsterSettings`.

    Usar esta función en todo el código para que las lecturas sean consistentes
    y estén cacheadas.
    """
    # Cargar el .env una sola vez desde la función centralizada.
    try:
        load_dotenv()
    except Exception as e:
        # No interrumpir si no existe .env; solo informar en modo debug.
        print(f"Error cargando .env: {e}")
    return DagsterSettings()


def get_env_var(name: str, default: Optional[str] = None) -> Optional[str]:
    """Ayudante para acceder a variables de entorno crudas (migración/pruebas).

    Preferir el uso de los atributos del objeto devuelto por
    `get_dagster_config()`.
    """
    return os.getenv(name, default)


def load_env_file(
    joinpath_str: list[str] | None = None, file_name: str = ".env"
) -> None:
    """Carga variables de entorno desde un archivo .env relativo a este paquete.

    Replica el comportamiento previo de `load_env_vars` pero centraliza la
    carga en este módulo de configuración.
    """
    if joinpath_str is None:
        joinpath_str = ["..", ".."]

    base_os_path = os.path.dirname(__file__)
    workspace_dir = Path(base_os_path).joinpath(*joinpath_str).resolve()
    env_path = workspace_dir.joinpath(file_name)

    if env_path.exists():
        # load_dotenv acepta un Path
        load_dotenv(env_path, override=True)


__all__ = ["DagsterSettings", "get_dagster_config", "get_env_var", "load_env_file"]
