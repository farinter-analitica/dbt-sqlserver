from typing import Mapping, Any, Sequence
from dagster import SourceAsset
from dagster_dbt import DagsterDbtTranslator, DbtCliResource
from dagster_shared_gf.resources.dbt_resources import dbt_resource, dbt_manifest

def build_dbt_sources(manifest: Mapping[str, Any], dbt_cli_resource: DbtCliResource) -> Sequence[SourceAsset]:
    """
    This function builds a list of SourceAsset objects based on the manifest and dbt_cli_resource.
    It filters the source assets based on certain conditions related to the dbt resources.
    Filter conditions: Not already imported into Dagster.
    Returns a list of SourceAsset objects.
    """
    dagster_dbt_translator = DagsterDbtTranslator(dbt_cli_resource)

    return [
        SourceAsset(
            key=dagster_dbt_translator.get_asset_key(dbt_resource_props),
            group_name=dagster_dbt_translator.get_group_name(dbt_resource_props)
        )
        for dbt_resource_props in manifest["sources"].values()
        if "dwh_kielsa" in dbt_resource_props.get("fqn", [])[1]
        # if asset_key exists dont include:  "meta": {
        #         "dagster": {
        #             "group": "dbt_dwh_kielsa_mart_datos_maestros",
        #             "asset_key": [
        #                 "extrac_ldcom_dl_kielsa_sucursal_asset"
        #             ]
        #and dbt_resource_props.get("meta", {}).get("dagster", {}).get("asset_key", None) is None
    ]

source_assets: Sequence[SourceAsset] = build_dbt_sources(dbt_manifest, dbt_resource)
