from threading import Lock

from dagster import AssetSelection, Config, RunConfig, AutomationCondition
from dagster._core.definitions.asset_spec import AssetExecutionType  # to use shared
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,  # to use shared
)

# from dlt.common.normalizers.naming.snake_case import NamingCo1nvention
from dagster._utils.tags import normalize_tags

from dagster_shared_gf.shared_functions import dagster_instance_current_env
import polars as pl
from pydantic import Field

# dlt_snake_case_normalizer = NamingConvention()

env_str: str = dagster_instance_current_env.env
shared_class_holder = [UnresolvedAssetJobDefinition, AssetExecutionType]
default_timezone_teg: str = "America/Tegucigalpa"


class SingletonMeta(type):
    """
    This is a thread-safe implementation of Singleton.
    """

    _instances = {}

    _lock: Lock = Lock()
    """
    We now have a lock object that will be used to synchronize threads during
    first access to the Singleton.
    """

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        # Now, imagine that the program has just been launched. Since there's no
        # Singleton instance yet, multiple threads can simultaneously pass the
        # previous conditional and reach this point almost at the same time. The
        # first of them will acquire lock and will proceed further, while the
        # rest will wait here.
        with cls._lock:
            # The first thread to acquire the lock, reaches this conditional,
            # goes inside and creates the Singleton instance. Once it leaves the
            # lock block, a thread that might have been waiting for the lock
            # release may then enter this section. But since the Singleton field
            # is already initialized, the thread won't create a new object.
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class Tags(dict[str, str]):
    """
    A class that represents a single or multiple tags, inheriting from dict, with some extra methods.
    Optionally automation condition to each schedule tag (starts with 'periodo/').

    Example:
        Tags({"periodo/diario": ""})
        Tags(key="periodo/diario", value="")
    """

    def __init__(
        self,
        dict_tags: dict[str, str] | None = None,
        key: str | None = None,
        value: str | None = None,
        automation_condition: AutomationCondition | None = None,
    ):
        if key is not None and value is not None:
            tags = {key: value}
        elif dict_tags is not None:
            tags = dict_tags
        else:
            raise ValueError(
                "Tag must be initialized with either a key-value pair or dict"
            )

        normalize_tags(tags, strict=True)
        super().__init__(tags)

        self._is_all_schedule = all("periodo" in k for k in self.keys())
        self._is_schedule = self._is_all_schedule or any(
            "periodo" in k for k in self.keys()
        )
        self._automation = automation_condition
        self._frozen = True

    def __setattr__(self, key, value):
        if hasattr(self, "_frozen"):
            raise AttributeError(f"Cannot modify frozen tag {self}")
        super().__setattr__(key, value)

    @property
    def key(self) -> str:
        """
        Returns the single key if there's only one item.
        """
        if len(self) == 1:
            return next(iter(self.keys()))
        raise ValueError("Tag contains more than one item.")

    @property
    def value(self) -> str:
        """
        Returns the single value if there's only one item.
        """
        if len(self) == 1:
            return next(iter(self.values()))
        raise ValueError("Tag contains more than one item.")

    @property
    def tag(self) -> dict[str, str]:
        """
        Returns the tag dictionary if there's only one item.
        """
        if len(self) == 1:
            return dict(self)
        raise ValueError("Tag contains more than one item.")

    @property
    def is_schedule(self) -> bool:
        """
        Returns true if tags contains any schedule tag.
        """
        return self._is_schedule

    @property
    def is_all_schedule(self) -> bool:
        """
        Returns true if all tags contains schedule tags.
        """
        return self._is_all_schedule


class TagsRepositoryGF(metaclass=SingletonMeta):
    Hourly: Tags = Tags(key="periodo/por_hora", value="")
    """{"periodo/por_hora": ""}"""

    AutomationHourly: Tags = Tags(key="automation/periodo_por_hora", value="")
    """{"automation/periodo_por_hora": ""}"""

    Replicas: Tags = Tags(key="replicas_sap", value="")
    """{"replicas_sap": ""}"""

    UniquePeriod: Tags = Tags(key="periodo_unico/si", value="")
    """{"periodo_unico/si": ""}"""

    Daily: Tags = Tags(key="periodo/diario", value="")
    """{"periodo/diario": ""}"""

    AutomationDaily: Tags = Tags(key="automation/periodo_diario", value="")
    """{"automation/periodo_diario": ""}"""

    SmbDataRepository: Tags = Tags(key="smb_data_repository/data_repo", value="")
    """{"smb_data_repository/data_repo": ""}"""

    Monthly: Tags = Tags(key="periodo/mensual", value="")
    """{"periodo/mensual": ""} inicio del mes generalmente o cualquier dia. """

    MonthlyEnd: Tags = Tags(key="periodo/mensual_fin", value="")
    """{"periodo/mensual_fin": ""} fin del mes."""

    AutomationMonthlyStart: Tags = Tags(
        key="automation/periodo_mensual_inicio", value=""
    )
    """{"automation/periodo_mensual_inicio": ""} inicio del mes."""

    MonthlyStart: Tags = Tags(key="periodo/mensual_inicio", value="")
    """{"periodo/mensual_inicio": ""} inicio del mes."""

    AutomationMonthlyEnd: Tags = Tags(key="automation/periodo_mensual_fin", value="")
    """{"automation/periodo_mensual_fin": ""} fin del mes."""

    HourlyAdditional: Tags = Tags(key="periodo/por_hora_adicional", value="")
    """{"periodo/por_hora_adicional": ""}"""

    AutomationHourlyAdditional: Tags = Tags(
        key="automation/periodo_por_hora_adicional", value=""
    )
    """{"automation/periodo_por_hora_adicional": ""}"""

    AutomationOnlyParticionado: Tags = Tags(
        key="automation_only/particionado", value=""
    )
    """{"automation_only/particionado": ""} Usar para definir solo por automatizacion no por jobs."""

    DetenerCarga: Tags = Tags(key="detener_carga/si", value="")
    """{"detener_carga/si": ""} Usar para detener la carga por cualquier motivo."""

    Automation: Tags = Tags(key="automation/si", value="")
    """{"automation/si": ""} Usar para definir que tiene automatizacion pero puede usarse en jobs."""

    AutomationOnly: Tags = Tags(key="automation_only", value="")
    """{"automation_only": ""} Usar para definir solo por automatizacion no por jobs."""

    Weekly: Tags = Tags(key="periodo/semanal", value="")
    """{"periodo/semanal": ""} Domingos generalmente o cualquier dia."""

    Weekly7: Tags = Tags(key="periodo/semanal_7", value="")
    """{"periodo/semanal_7": ""} Domingos"""

    AutomationWeekly7: Tags = Tags(key="automation/periodo_semanal_7", value="")
    """{"automation/periodo_semanal_7": ""}"""

    Weekly1: Tags = Tags(key="periodo/semanal_1", value="")
    """{"periodo/semanal_1": ""} Lunes"""

    AutomationWeekly1: Tags = Tags(key="automation/periodo_semanal_1", value="")
    """{"automation/periodo_semanal_1": ""} Lunes"""

    MultipleTags: Tags = Tags({"multiple/tags": "", "automation_only": ""})
    """{"multiple/tags": "", "automation_only": ""}"""

    def get_schedule_tags(self) -> tuple[Tags, ...]:
        """
        Returns all tags that are schedule-related (contains 'periodo').
        """
        return tuple(
            getattr(self, name)
            for name in dir(self)
            if isinstance(getattr(self, name), Tags) and getattr(self, name).is_schedule
        )

    def get_automation_tags(self) -> tuple[Tags, ...]:
        """
        Returns all tags class groups that are automation-related (start with 'automation').
        """
        return tuple(
            getattr(self, name)
            for name in dir(self)
            if isinstance(getattr(self, name), Tags)
            and any(k.startswith("automation") for k in getattr(self, name).keys())
        )

    def get_automation_tags_keys(self) -> tuple[str, ...]:
        """
        Returns all keys of individual tags that are automation-related (start with 'automation/' or 'automation_only/particionado').
        """
        automation_tags = self.get_automation_tags()
        return tuple(
            key
            for tag in automation_tags
            for key in tag.keys()
            if key.startswith("automation")
        )

    def get_unselected_for_jobs_tags(self) -> tuple[Tags, ...]:
        """
        Returns all tags that are not selected for jobs (start with 'detener_carga/si' or 'automation_only').
        """
        return tuple(
            getattr(self, name)
            for name in dir(self)
            if isinstance(getattr(self, name), Tags)
            and any(
                k.startswith("detener_carga/si") or k.startswith("automation_only")
                for k in getattr(self, name).keys()
            )
        )


def get_execution_config(max_concurrent: int) -> RunConfig:
    return RunConfig(
        execution={"config": {"multiprocess": {"max_concurrent": max_concurrent}}}
    )


tags_repo = TagsRepositoryGF()

if __name__ == "__main__":
    print(tags_repo.AutomationHourly.key)


##
class ExcelProcessConfig(Config):
    """
    Config class for Excel loading.
    """

    expected_columns: dict[str, str] = Field(
        description="Columns New Name : Column File Name", default_factory=dict
    )
    polars_schema: pl.Schema = Field(
        description="polars_schema", default_factory=pl.Schema
    )
    exclude_colums: tuple[str, ...] = Field(
        description="Exclude columns", default_factory=tuple
    )
    blanks_allowed: bool = Field(description="Allow blanks", default=True)
    blanks_on_type_error: bool = Field(
        description="Convert type error to blanks", default=False
    )

    excel_sheet_name: str = Field(description="Excel sheet name", default="carga")

    loaded_files_folder: str = Field(
        description="Loaded files folder", default="cargados"
    )
    excel_primary_keys: tuple[str, ...] = Field(
        description="Primary keys", default_factory=tuple
    )

    final_table_primary_keys: tuple[str, ...] = Field(
        description="Primary keys", default_factory=tuple
    )

    fill_nulls: bool = Field(description="Fill nulls", default=False)


class NullsException(BaseException):
    pass


class FileException(BaseException):
    pass


class ErrorsOccurred(BaseException):
    pass


seleccion_no_programar: AssetSelection = (
    AssetSelection.tag(
        key=tags_repo.DetenerCarga.key, value=tags_repo.DetenerCarga.value
    )
    | AssetSelection.tag(
        key=tags_repo.AutomationOnly.key, value=tags_repo.AutomationOnly.value
    )
    | AssetSelection.tag(
        key=tags_repo.AutomationOnlyParticionado.key,
        value=tags_repo.AutomationOnlyParticionado.value,
    )
)
