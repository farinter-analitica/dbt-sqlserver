from typing import Mapping, Any, Sequence
from dagster import AssetKey, SourceAsset, load_assets_from_modules
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
        if dbt_resource_props["resource_type"] == "source":
            tags = dbt_resource_props.get("config", {}).get("tags", [])
            return {tag: "" for tag in tags if is_valid_definition_tag_key(tag)}
        else:
            return super().get_tags(dbt_resource_props)
    
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        """
        A function that takes a dictionary representing properties of a dbt resource, and
        returns the Dagster asset key for that resource.

        Note that a dbt resource is unrelated to Dagster's resource concept, and simply represents
        a model, seed, snapshot or source in a given dbt project. You can learn more about dbt
        resources and the properties available in this dictionary here:
        https://docs.getdbt.com/reference/artifacts/manifest-json#resource-details

        """
        if dbt_resource_props["resource_type"] == "source":
            configured_database = dbt_resource_props.get("database")
            configured_schema = dbt_resource_props.get("schema")
            configured_name = dbt_resource_props["name"]
            if configured_schema is not None and configured_database is not None:
                components = [configured_database , configured_schema , configured_name]
                return AssetKey(components)
            else:
                return super().get_asset_key(dbt_resource_props)
        else:
            return super().get_asset_key(dbt_resource_props)


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
        if "dagster_kielsa_gf" in dagster_dbt_translator.get_tags(dbt_resource_props).keys()
    ]
        #if "dwh_kielsa" in dbt_resource_props.get("fqn", [])[1]
        # if asset_key exists dont include:  "meta": {
        #         "dagster": {
        #             "group": "dbt_dwh_kielsa_mart_datos_maestros",
        #             "asset_key": [
        #                 "extrac_ldcom_dl_kielsa_sucursal_asset"
        #             ]
        #and dbt_resource_props.get("meta", {}).get("dagster", {}).get("asset_key", None) is None
    

source_assets: Sequence[SourceAsset] = [] #build_dbt_sources(dbt_manifest, MyDbtSourceTranslator()) #empezo a funcionar en nueva version directamente en assets
all_assets: Sequence[SourceAsset]  = source_assets

if __name__ == "__main__":
    #print(source_assets)
    #print([asset.tags.keys() for asset in source_assets])
    print([asset.tags for asset in source_assets])