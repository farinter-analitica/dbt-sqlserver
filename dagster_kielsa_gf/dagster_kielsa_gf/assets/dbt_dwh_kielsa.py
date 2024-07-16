from dagster import AssetExecutionContext, load_assets_from_current_module, load_asset_checks_from_current_module, build_last_update_freshness_checks, AssetChecksDefinition,AssetKey
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator
from typing import Sequence, List, Mapping, Dict, Any
from datetime import timedelta
from dagster_shared_gf.shared_variables import TagsRepositoryGF
from dagster_shared_gf.shared_functions import filter_assets_by_tags
from dagster_shared_gf.resources.dbt_resources import dbt_manifest_path

tags_repo = TagsRepositoryGF
class MyDbtSourceTranslator(DagsterDbtTranslator):
    def get_tags(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, str]:
        """A function that takes a dictionary representing properties of a dbt resource, and
        returns the Dagster tags for that resource.

        Copy from dagster_dbt.DagsterDbtTranslator.get_tags modified
        """
        if dbt_resource_props["resource_type"] == "source":
            tags = dbt_resource_props.get("config", {}).get("tags", [])
            return {tag: "" for tag in tags if super().is_valid_definition_tag_key(tag)}
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
        if dbt_resource_props["resource_type"] in ["model", "source"]:
            configured_database = (
                dbt_resource_props.get("database")
                if dbt_resource_props["resource_type"] == "model"
                else dbt_resource_props.get("source_name")
            )
            configured_schema = dbt_resource_props.get("schema")
            configured_name = dbt_resource_props["name"]
            if configured_schema is not None and configured_database is not None:
                components = [configured_database , configured_schema , configured_name]
                return AssetKey(components)
            else:
                return super().get_asset_key(dbt_resource_props)
        else:
            return super().get_asset_key(dbt_resource_props)

@dbt_assets(manifest=dbt_manifest_path, select="tag:dagster_kielsa_gf", dagster_dbt_translator=MyDbtSourceTranslator())
def dbt_dwh_kielsa_mart_datos_maestros_assets(context: AssetExecutionContext, dbt_resource: DbtCliResource):
    yield from dbt_resource.cli(["build"], context=context).stream()


all_assets = load_assets_from_current_module()

all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(all_assets, tags=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"),
    lower_bound_delta=timedelta(hours=26),
    deadline_cron="0 9 * * 1-6",
)
#print(filter_assets_by_tags(all_assets, tags=hourly_tag, filter_type="any_tag_matches"), "\n")
all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(all_assets, tags=tags_repo.Hourly.tag, filter_type="any_tag_matches"),
    lower_bound_delta=timedelta(hours=13),
    deadline_cron="0 10-16 * * 1-6",
)

all_asset_checks: Sequence[AssetChecksDefinition] = load_asset_checks_from_current_module()
all_asset_freshness_checks = all_assets_non_hourly_freshness_checks + all_assets_hourly_freshness_checks
