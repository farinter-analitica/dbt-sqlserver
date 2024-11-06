from dagster import (asset
                     , AssetChecksDefinition 
                     , AssetExecutionContext 
                     , AssetsDefinition
                     , build_last_update_freshness_checks
                     , load_assets_from_current_module
                     , load_asset_checks_from_current_module
                     )
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource 
from dagster_shared_gf.shared_variables import env_str, tags_repo
from dagster_shared_gf.shared_functions import filter_assets_by_tags, get_all_instances_of_class
import dagster_sap_gf.assets.dbt_dwh_sap as dbt_dwh_sap
from pathlib import Path
from typing import List, Dict, Any, Mapping, Sequence, Union
from datetime import datetime, date, timedelta
from pydantic import Field

##








##

if not __name__ == '__main__':
    all_assets = load_assets_from_current_module(group_name="smb_etl_dwh")

    all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
        assets=filter_assets_by_tags(all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"),
        lower_bound_delta=timedelta(hours=26),
        deadline_cron="0 9 * * 1-6",
    )
    all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = build_last_update_freshness_checks(
        assets=filter_assets_by_tags(all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="any_tag_matches"),
        lower_bound_delta=timedelta(hours=13),
        deadline_cron="0 10-16 * * 1-6",
    )

    all_asset_checks: Sequence[AssetChecksDefinition] = load_asset_checks_from_current_module()
    all_asset_freshness_checks = all_assets_non_hourly_freshness_checks + all_assets_hourly_freshness_checks
