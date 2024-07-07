from dagster_shared_gf.shared_functions import dagster_instance_current_env
from pydantic import Field
from typing import Any, Mapping, Annotated, Union, Dict, Optional
from dataclasses import dataclass, field
import types
env_str:str = dagster_instance_current_env.env

asdasd = Annotated[str, Field(default=env_str)]

@dataclass
class TagsRepositoryGF:
    """
    Repository for tags.

    This class provides a way to define and retrieve tags for different purposes.
    Each tag is represented by a class that inherits from [_base_tag_class](dagster_shared_gf/dagster_shared_gf/shared_variables.py).
    These classes are dynamically generated based on the tags defined in the `tags` dictionary.

    To get a tag mapping, you can either call the tag class directly, or access the [tag](dagster_shared_gf/dagster_shared_gf/shared_variables.py) attribute.
    For example:

    ```python
    tags_repo = TagsRepositoryGF()
    hourly_tag = tags_repo.Hourly()
    print(hourly_tag.tag)  # prints {"periodo": "por_hora"}
    print(tags_repo.Hourly.tag)  # also prints {"periodo": "por_hora"}
    ```

    The `tags` dictionary should be defined in the [TagsRepositoryGF](dagster_shared_gf/dagster_shared_gf/shared_variables.py) class.
    """
    @dataclass
    class _base_tag_class():
        """Base class for all tags"""
        tag: Optional[Mapping[str, str]] = None
        key: Optional[str] = None
        value: Optional[str]    = None
        @classmethod
        def set_tags(self, key, value):
            self.key = key
            self.value = value
            self.tag = {key: value}
            self.__name__ = str(self.tag)
        # def __call__(self) -> Mapping[str, str]:
        #     return self.tag
        def __new__(self, key:Optional[str]=None, value:Optional[str]=None):
            if self.tag is None:
                if key is not None and value is not None:
                    self.set_tags(key, value)
                return self
            else:
                return self.tag
    class Hourly(_base_tag_class):
        """{"periodo": "por_hora"}"""
        tag: Mapping[str, str] = {"periodo": "por_hora"}
        key: str = next(iter(tag.keys()))
        value: str = next(iter(tag.values()))
    hourly2 = _base_tag_class(key="periodo",value= "por_hora")
    class Replicas(_base_tag_class):
        """{"replicas_sap": "true"}"""
        tag: Mapping[str, str] = {"replicas_sap": "true"}
        key: str = next(iter(tag.keys()))
        value: str = next(iter(tag.values()))
    class HourlyUnique(_base_tag_class):
        """{"periodo_unico": "por_hora"}"""
        tag: Mapping[str, str] = {"periodo_unico": "por_hora"}
        key: str = next(iter(tag.keys()))
        value: str = next(iter(tag.values()))
    class Daily(_base_tag_class):
        """{"periodo": "diario"}"""
        tag: Mapping[str, str] = {"periodo": "diario"}
        key: str = next(iter(tag.keys()))
        value: str = next(iter(tag.values()))
    class DailyUnique(_base_tag_class):
        """{"periodo_unico": "diario"}"""
        tag: Mapping[str, str] = {"periodo_unico": "diario"}
        key: str = next(iter(tag.keys()))
        value: str = next(iter(tag.values()))


if __name__ == "__main__":
    # Example usage:
    my_tag: Mapping[str, str] = TagsRepositoryGF.Hourly()
    print(my_tag)  # Output: {"color": "blue"}
    yet_another_tag: Mapping[str, str] = TagsRepositoryGF.hourly2()
    print(yet_another_tag)  # Output: {"color": "blue"}
    my_second_tag: Mapping[str, str] = TagsRepositoryGF.Daily()
    my_another_tag: Mapping[str, str] = TagsRepositoryGF.HourlyUnique()
    print(my_tag)  # Output: "color"
    #print(my_tag.value)  # Output: "blue"
    all_tags = my_tag | my_another_tag | my_second_tag
    print(all_tags)  # Output: {"color": "blue", "weight": "100", "size": "big"}
    # Example usage and assertions
    assert TagsRepositoryGF.Hourly() == {"periodo": "por_hora"}
    assert TagsRepositoryGF.Replicas() == {"replicas_sap": "true"}
    assert TagsRepositoryGF.HourlyUnique() == {"periodo_unico": "por_hora"}
    assert TagsRepositoryGF.Daily() == {"periodo": "diario"}
    assert TagsRepositoryGF.DailyUnique() == {"periodo_unico": "diario"}

    # Combining tags
    combined_tags = {**TagsRepositoryGF.Hourly(), **TagsRepositoryGF.Daily(), **TagsRepositoryGF.HourlyUnique()}
    assert combined_tags == {"periodo": "diario", "periodo_unico": "por_hora"}



