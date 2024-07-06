from typing import Mapping, Any, Sequence
from dagster import SourceAsset
from dagster_dbt import DagsterDbtTranslator
from dagster_shared_gf.resources.dbt_resources import dbt_manifest

from typing import Any, Mapping, Sequence
from dagster import SourceAsset
from dagster_dbt import DagsterDbtTranslator

def build_dbt_sources_base(manifest: Mapping[str, Any], dagster_dbt_translator: DagsterDbtTranslator) -> Sequence[SourceAsset]:
    return [
        SourceAsset(
            key=dagster_dbt_translator.get_asset_key(dbt_resource_props),
            group_name=dagster_dbt_translator.get_group_name(dbt_resource_props),
            metadata=dagster_dbt_translator.get_metadata(dbt_resource_props),
            description=dbt_resource_props.get("description", ""),
        )
        for dbt_resource_props in manifest["sources"].values()
    ]

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
        if "dagster_kielsa_gf" not in dagster_dbt_translator.get_tags(dbt_resource_props).items()
        #if "dwh_kielsa" in dbt_resource_props.get("fqn", [])[1]
        # if asset_key exists dont include:  "meta": {
        #         "dagster": {
        #             "group": "dbt_dwh_kielsa_mart_datos_maestros",
        #             "asset_key": [
        #                 "extrac_ldcom_dl_kielsa_sucursal_asset"
        #             ]
        #and dbt_resource_props.get("meta", {}).get("dagster", {}).get("asset_key", None) is None
    ]

source_assets: Sequence[SourceAsset] = build_dbt_sources(dbt_manifest, DagsterDbtTranslator())

if __name__ == "__main__":
    #print(source_assets)
    print([asset.tags.keys() for asset in source_assets])