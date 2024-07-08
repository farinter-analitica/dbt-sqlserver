from typing import Mapping, Any, Sequence
from dagster import SourceAsset, load_assets_from_modules
from dagster_dbt import DagsterDbtTranslator, DbtCliResource
from dagster_shared_gf.resources.dbt_resources import dbt_resource, dbt_manifest

from dagster._core.definitions.utils import is_valid_definition_tag_key

#replace get_tags with get_source_tags
class MyDbtSourceTranslator(DagsterDbtTranslator):
    def get_tags(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, str]:
        """A function that takes a dictionary representing properties of a dbt resource, and
        returns the Dagster tags for that resource.

        Copy from dagster_dbt.DagsterDbtTranslator.get_tags modified
        """
        tags = dbt_resource_props.get("config", {}).get("tags", [])
        return {tag: "" for tag in tags if is_valid_definition_tag_key(tag)}

def build_dbt_sources(manifest: Mapping[str, Any], dagster_dbt_translator: DagsterDbtTranslator) -> Sequence[SourceAsset]:
    """
    This function builds a list of SourceAsset objects based on the manifest and dbt_cli_resource.
    It filters the source assets based on certain conditions related to the dbt resources.
    Filter conditions: Not already imported into Dagster.
    Returns a list of SourceAsset objects.
    """

    return [
        SourceAsset(
            key=dagster_dbt_translator.get_asset_key(dbt_resource_props),
            group_name=dagster_dbt_translator.get_group_name(dbt_resource_props),
            tags=dagster_dbt_translator.get_tags(dbt_resource_props),
            description=dagster_dbt_translator.get_description(dbt_resource_props),
            metadata=dagster_dbt_translator.get_metadata(dbt_resource_props),
            freshness_policy=dagster_dbt_translator.get_freshness_policy(dbt_resource_props),  
        )
        for dbt_resource_props in manifest["sources"].values()
        if "dagster_sap_gf" in dagster_dbt_translator.get_tags(dbt_resource_props).keys()
    ]

source_assets: Sequence[SourceAsset] = build_dbt_sources(dbt_manifest, MyDbtSourceTranslator())
all_assets = source_assets

if __name__ == "__main__":
    #print(source_assets)
    #print([asset.tags.keys() for asset in source_assets])
    print([asset.tags for asset in source_assets])