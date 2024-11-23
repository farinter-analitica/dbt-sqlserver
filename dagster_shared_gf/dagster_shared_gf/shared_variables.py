from typing import Optional
from threading import Lock

from dagster import RunConfig
from dagster._core.definitions.asset_spec import AssetExecutionType  # to use shared
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,  # to use shared
)
# from dlt.common.normalizers.naming.snake_case import NamingConvention

from dagster_shared_gf.shared_functions import dagster_instance_current_env

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
    A class that represents a single tag, inheriting from dict.
    """

    def __init__(
        self, key: Optional[str] = None, value: Optional[str] = None, **kwargs
    ):
        if key is not None and value is not None:
            super().__init__({key: value})
        elif kwargs:
            super().__init__(kwargs)
        else:
            raise ValueError(
                "Tag must be initialized with either a key-value pair or kwargs"
            )
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


class TagsRepositoryGF(metaclass=SingletonMeta):
    Hourly: Tags = Tags(key="periodo/por_hora", value="")
    """{"periodo/por_hora": ""}"""

    Replicas: Tags = Tags(key="replicas_sap", value="")
    """{"replicas_sap": ""}"""

    UniquePeriod: Tags = Tags(key="periodo_unico/si", value="")
    """{"periodo_unico/si": ""}"""

    Daily: Tags = Tags(key="periodo/diario", value="")
    """{"periodo/diario": ""}"""

    SmbDataRepository: Tags = Tags(key="smb_data_repository/data_repo", value="")
    """{"smb_data_repository/data_repo": ""}"""

    Monthly: Tags = Tags(key="periodo/mensual", value="")
    """{"periodo/mensual": ""}"""

    HourlyAdditional: Tags = Tags(key="periodo/por_hora_adicional", value="")
    """{"periodo/por_hora_adicional": ""}"""

    Partitioned: Tags = Tags(key="particionado/si", value="")
    """{"particionado/si": ""}"""

    DetenerCarga: Tags = Tags(key="detener_carga/si", value="")
    """{"detener_carga/si": ""}"""

    Automation: Tags = Tags(key="automation/si", value="")
    """{"automation/si": ""}"""


def get_execution_config(max_concurrent: int) -> dict:
    return {"config": {"multiprocess": {"max_concurrent": max_concurrent}}}


tags_repo = TagsRepositoryGF()

if __name__ == "__main__":
    print(tags_repo.Hourly.key)
