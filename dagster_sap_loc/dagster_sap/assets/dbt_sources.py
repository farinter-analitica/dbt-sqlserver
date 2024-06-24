from typing import Mapping, Any, Sequence
from dagster import SourceAsset
from dagster_dbt import DagsterDbtTranslator, DbtCliResource
from dagster_shared_gf.resources.dbt_resources import dbt_resource, dbt_manifest

def build_dbt_sources(manifest: Mapping[str, Any], dbt_cli_resource: DbtCliResource) -> Sequence[SourceAsset]:
    dagster_dbt_translator = DagsterDbtTranslator(dbt_cli_resource)

    return [
        SourceAsset(
            key=dagster_dbt_translator.get_asset_key(dbt_resource_props),
            group_name=dagster_dbt_translator.get_group_name(dbt_resource_props)
        )
        for dbt_resource_props in manifest["sources"].values()
    ]

source_assets: Sequence[SourceAsset] = build_dbt_sources(dbt_manifest, dbt_resource)
