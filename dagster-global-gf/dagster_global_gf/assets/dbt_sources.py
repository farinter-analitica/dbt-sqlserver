from typing import Mapping, Any, Sequence
from dagster import AssetSpec
from dagster_dbt import DagsterDbtTranslator
from dagster_shared_gf.resources.dbt_resources import (
    get_dbt_manifest,
    MyDbtSourceTranslator,
)


def build_dbt_sources(
    manifest: Mapping[str, Any], dagster_dbt_translator: DagsterDbtTranslator
) -> Sequence[AssetSpec]:
    """
    This function builds a list of AssetSpec objects based on the manifest and dbt_cli_resource.
    It filters the source assets based on certain conditions related to the dbt resources.
    Filter conditions: Not already imported into Dagster.
    Returns a list of AssetSpec objects.
    """
    # List of tags to check
    required_tags = ["dagster_global_gf/dbt"]
    return [
        AssetSpec(
            key=dagster_dbt_translator.get_asset_key(dbt_resource_props),
            group_name=dagster_dbt_translator.get_group_name(dbt_resource_props),
            tags=dagster_dbt_translator.get_tags(dbt_resource_props),
            description=dagster_dbt_translator.get_description(dbt_resource_props),
            metadata=dagster_dbt_translator.get_metadata(dbt_resource_props),
            freshness_policy=dagster_dbt_translator.get_freshness_policy(
                dbt_resource_props
            ),  # type: ignore
        )
        for dbt_resource_props in manifest["sources"].values()
        if any(
            tag in dagster_dbt_translator.get_tags(dbt_resource_props).keys()
            for tag in required_tags
        )
    ]


source_assets: Sequence[AssetSpec] = build_dbt_sources(
    get_dbt_manifest(), MyDbtSourceTranslator()
)

if __name__ == "__main__":
    # print(source_assets)
    # print([asset.tags.keys() for asset in source_assets])
    print([asset.key for asset in source_assets])
