from dagster_shared_gf.shared_functions import dagster_instance_current_env
from dagster._core.definitions.unresolved_asset_job_definition import UnresolvedAssetJobDefinition #to use shared
from dagster._core.definitions.asset_spec import AssetExecutionType #to use shared
from pydantic import Field
from typing import Any, Mapping, Annotated, Union, Dict, Optional
from dataclasses import dataclass, field
import types
from dlt.common.normalizers.naming.snake_case import NamingConvention

dlt_snake_case_normalizer = NamingConvention()

env_str:str = dagster_instance_current_env.env
shared_class_holder = [UnresolvedAssetJobDefinition, AssetExecutionType]
@dataclass
class TagsRepositoryGF:
    """
    Repository for tags.

    This class provides a way to define and retrieve tags for different purposes.
    Each tag is represented by a class that inherits from the _base_tag_class.
    These classes are dynamically generated based on the tags defined in the `tags` dictionary.

    To get a tag mapping, you can either call the tag class directly, or access the `tag` attribute.
    For example:

    ```python
    tags_repo = TagsRepositoryGF()
    hourly_tag = tags_repo.Hourly()
    print(hourly_tag.tag)  # prints {"periodo": "por_hora"}
    print(tags_repo.Hourly.tag)  # also prints {"periodo": "por_hora"}
    ```

    The `tags` dictionary should be defined in the TagsRepositoryGF class.
    """
    @dataclass
    class _base_tag_class:
        """Base class for all tags"""
        key: str
        value: str
        tag: Mapping[str, str] = field(init=False, default=None)

        def __post_init__(self):
            self.tag = {self.key: self.value}

        def __new__(cls, *args, **kwargs):
            instance = super().__new__(cls)
            instance.key = cls.key
            instance.value = cls.value
            instance.tag = {cls.key: cls.value}
            return instance.tag

        # def __init__(self, *args, **kwargs):
        #     pass

        def __call__(self):
            return self.tag

        @classmethod
        def __init_subclass__(cls, key: str, value: str, **kwargs):
            super().__init_subclass__(**kwargs)
            cls.key = key
            cls.value = value
            cls.tag = {cls.key: cls.value}

    class Hourly(_base_tag_class, key="periodo/por_hora", value=""):
        """{"periodo/por_hora": ""}"""

    class Replicas(_base_tag_class, key="replicas_sap", value=""):
        """{"replicas_sap": ""}"""

    class HourlyUnique(_base_tag_class, key="periodo_unico/por_hora", value=""):
        """{"periodo_unico/por_hora": ""}"""

    class Daily(_base_tag_class, key="periodo/diario", value=""):
        """{"periodo/diario": ""}"""

    class DailyUnique(_base_tag_class, key="periodo_unico/diario", value=""):
        """{"periodo_unico/diario": ""}"""

    class SmbDataRepository(_base_tag_class, key="smb_data_repository/data_repo", value=""):
        """{"smb_data_repository/data_repo": ""}"""


if __name__ == "__main__":
    print(TagsRepositoryGF.Hourly())
