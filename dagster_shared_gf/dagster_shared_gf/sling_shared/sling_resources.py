from dataclasses import dataclass
from typing import Any, Literal, Mapping, Sequence, Iterable, Optional
from dagster import AssetKey, AssetSpec, AutomationCondition
from dagster_shared_gf.shared_variables import Tags
from dagster_sling import DagsterSlingTranslator, SlingResource


@dataclass
class MyDagsterSlingTranslator(DagsterSlingTranslator):
    schema_name: str | None = None
    asset_database: str | None = None
    automation_condition: AutomationCondition | None = None
    prefix_key: Optional[Sequence[str]] = None
    source_prefix_key: Optional[Sequence[str]] = None
    tags: Optional[Tags | Mapping[str, str]] = None
    group_name: Optional[str] = None

    def get_asset_spec(self, stream_definition: Mapping[str, Any]) -> AssetSpec:
        """Defines the asset spec for the Sling resource"""

        return AssetSpec(
            key=self._custom_get_asset_key(stream_definition),
            deps=self._custom_get_deps_asset_keys(stream_definition),
            metadata=self._default_metadata_fn(stream_definition),
            automation_condition=self._custom_get_automation_condition(),
            # or self._default_automation_condition_fn(resource),
            tags={
                **self._default_tags_fn(stream_definition),
                **self._custom_get_tags(),
            },
            group_name=self.group_name
            or self._default_group_name_fn(stream_definition),
            # Preserve the default behavior for these attributes
            description=self._default_description_fn(stream_definition),
            # owners=self._default_owners_fn(resource),
            kinds=self._default_kinds_fn(stream_definition),
        )

    def _custom_get_asset_key(self, stream_definition: Mapping[str, Any]) -> AssetKey:
        """
        Defines asset key for a given stream definition.

        If `prefix_key` is provided, the asset key is extended with the components of `prefix_key`
        followed by the resource name.
        If `asset_database` is provided, the asset key is extended with the asset database name
        followed by the schema name and the resource name.

        Args:
            stream_definition: A dictionary representing the stream definition

        Returns:
            AssetKey: The Dagster AssetKey for the replication stream
        """
        resource_name = stream_definition["name"].replace(".", "_")
        sanitized_name = self.sanitize_stream_name(resource_name)

        if self.prefix_key:
            return AssetKey([*self.prefix_key, sanitized_name])
        else:
            components = []
            if self.asset_database:
                components.append(self.asset_database)
            if self.schema_name:
                components.append(self.schema_name)
            components.append(sanitized_name)
            return AssetKey(components)

    def _custom_get_deps_asset_keys(
        self, stream_definition: Mapping[str, Any]
    ) -> Iterable[AssetKey]:
        """
        Defines dependencies for a given stream definition.

        By default, creates a dependency on a source asset with the same name but in a different
        location (e.g., "mongodb" database).

        Args:
            stream_definition: A dictionary representing the stream definition

        Returns:
            Iterable[AssetKey]: The dependencies for this asset
        """
        resource_name = stream_definition["name"].replace(".", "_")
        sanitized_name = self.sanitize_stream_name(resource_name)

        # Create a dependency on a source asset in "mongodb" database
        if self.source_prefix_key:
            return [AssetKey([*self.source_prefix_key, sanitized_name])]
        else:
            return self._default_deps_fn(stream_definition)

    def _custom_get_automation_condition(self) -> Optional[AutomationCondition]:
        """
        Returns the custom automation condition if specified.

        Returns:
            Optional[AutomationCondition]: The automation condition for the assets
        """
        return self.automation_condition

    def _custom_get_tags(self) -> Mapping[str, str]:
        """
        Returns custom tags to apply to all assets.

        Returns:
            Mapping[str, str]: The tags to apply to the assets
        """
        return self.tags or {}


class MySlingResource(SlingResource):
    default_mode: Literal["incremental", "full-refresh"] = "incremental"
