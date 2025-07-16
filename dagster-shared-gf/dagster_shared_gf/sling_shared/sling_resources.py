from dataclasses import dataclass
from typing import Any, Literal, Mapping, Iterable, Optional
import dagster as dg
from dagster_shared_gf.shared_variables import Tags
from dagster_sling import DagsterSlingTranslator, SlingResource


@dataclass
class MyDagsterSlingTranslator(DagsterSlingTranslator):
    schema_name: str | None = None
    asset_database: str | None = None
    automation_condition: dg.AutomationCondition | None = None
    prefix_key: Optional[Iterable[str]] = None
    source_prefix_key: Optional[Iterable[str]] = None
    tags: Optional[Tags | Mapping[str, str]] = None
    group_name: Optional[str] = None
    deps: Optional[Iterable[dg.AssetKey]] = None

    def get_asset_spec(self, stream_definition: Mapping[str, Any]) -> dg.AssetSpec:
        """Defines the asset spec for the Sling resource"""

        default_spec = super().get_asset_spec(stream_definition)
        extra_deps = self.deps or []

        new_spec = default_spec.replace_attributes(
            key=self._custom_get_asset_key(stream_definition),
            deps=[*self._custom_get_deps_asset_keys(stream_definition), *extra_deps],
            automation_condition=self._custom_get_automation_condition(),
            # or self._default_automation_condition_fn(resource),
            tags={
                **default_spec.tags,
                **self._custom_get_tags(),
            },
            group_name=self.group_name or default_spec.group_name,
        )

        return new_spec

    def _custom_get_asset_key(
        self, stream_definition: Mapping[str, Any]
    ) -> dg.AssetKey:
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
            return dg.AssetKey([*self.prefix_key, sanitized_name])
        else:
            components = []
            if self.asset_database:
                components.append(self.asset_database)
            if self.schema_name:
                components.append(self.schema_name)
            components.append(sanitized_name)
            return dg.AssetKey(components)

    def _custom_get_deps_asset_keys(
        self, stream_definition: Mapping[str, Any]
    ) -> Iterable[dg.AssetKey]:
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
            return [dg.AssetKey([*self.source_prefix_key, sanitized_name])]
        else:
            return self._default_deps_fn(stream_definition)

    def _custom_get_automation_condition(self) -> Optional[dg.AutomationCondition]:
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
