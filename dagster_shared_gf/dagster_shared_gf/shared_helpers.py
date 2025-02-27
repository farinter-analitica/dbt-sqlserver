from collections import deque
from datetime import timedelta
from typing import Sequence
from dagster import (
    AssetChecksDefinition,
    AssetsDefinition,
    build_last_update_freshness_checks,
)
from dagster_shared_gf.shared_constants import hourly_freshness_lbound_per_environ
from dagster_shared_gf.shared_functions import filter_assets_by_tags
from dagster_shared_gf.shared_variables import tags_repo


def get_unique_source_assets(all_assets, source_assets):
    """
    Extracts unique source assets that don't overlap with existing asset keys.

    Args:
        all_assets: List of all asset definitions
        source_assets: List of source assets to filter

    Returns:
        List of source assets that don't have keys overlapping with all_assets
    """
    # Extract the asset keys from the AssetsDefinition instances
    all_asset_keys = {
        key
        for asset in all_assets
        if isinstance(asset, AssetsDefinition)
        for key in asset.keys
    }

    # Return source assets that don't overlap with existing keys
    return [
        source_asset
        for source_asset in source_assets
        if source_asset.key not in all_asset_keys
    ]


def create_freshness_checks_for_assets(all_assets):
    all_asset_defs = deque()
    for asset in all_assets:
        if isinstance(asset, AssetsDefinition):
            all_asset_defs.append(asset)

    all_asset_defs_hourly_tag = filter_assets_by_tags(
        all_asset_defs,
        tags_to_match=tags_repo.Hourly.tag,
        filter_type="any_tag_matches",
    )

    all_asset_defs_non_hourly_tag = tuple(
        asset
        for asset in filter_assets_by_tags(
            all_asset_defs,
            tags_to_match=tags_repo.Hourly.tag,
            filter_type="exclude_if_any_tag",
        )
        if asset not in all_asset_defs_hourly_tag
    )

    all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = (
        build_last_update_freshness_checks(
            assets=all_asset_defs_hourly_tag,
            lower_bound_delta=hourly_freshness_lbound_per_environ,
            deadline_cron="0 10-16 * * 1-6",
        )
    )

    all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
        assets=all_asset_defs_non_hourly_tag,
        lower_bound_delta=timedelta(hours=26),
        deadline_cron="0 9 * * 1-6",
    )

    return (
        *all_assets_hourly_freshness_checks,
        *all_assets_non_hourly_freshness_checks,
    )
