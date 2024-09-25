from dagster import (
    define_asset_job,
    AssetSelection,
    asset,
    job,
    AssetKey,
    JobDefinition,
    ConfigMapping,
    ConfigSchema,
)
from dagster_shared_gf.shared_functions import get_all_instances_of_class
from dagster_shared_gf.shared_variables import (
    env_str,
    UnresolvedAssetJobDefinition,
    TagsRepositoryGF as tags_repo,
)
from typing import List, Any, Mapping

ExecutorConfig = ConfigSchema
workflows_run_config_secuential: ExecutorConfig = {
    "execution": {"config": {"multiprocess": {"max_concurrent": 1}}}
}

shared_daily_assets: AssetSelection = AssetSelection.tag(
    key=tags_repo.Daily.key, value=tags_repo.Daily.value
)
shared_daily_assets_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="shared_daily_assets_job",
    selection=shared_daily_assets,
    tags=tags_repo.Daily.tag,
)

shared_hourly_assets: AssetSelection = AssetSelection.tag(
    key=tags_repo.Hourly.key, value=tags_repo.Hourly.value
)
shared_hourly_assets = shared_hourly_assets | (
    shared_daily_assets.upstream().required_multi_asset_neighbors()
    - AssetSelection.tag(
        key=tags_repo.UniquePeriod.key,
        value=tags_repo.UniquePeriod.value,
    )
)
shared_hourly_assets_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="shared_hourly_assets_job",
    selection=shared_hourly_assets,
    tags=tags_repo.Hourly.tag
    | {
        "dagster/max_runtime": (100 * 60)
    },  # max 100 minutes in seconds, then mark it as failed.)
)

shared_assets_not_scheduled: AssetSelection = (
    shared_daily_assets
    - shared_hourly_assets
)
shared_assets_not_scheduled_job = define_asset_job(
    name="shared_assets_not_scheduled_job",
    selection=shared_assets_not_scheduled,
    tags=tags_repo.Daily.tag,
)


all_jobs = get_all_instances_of_class(
    class_type_list=[JobDefinition, UnresolvedAssetJobDefinition]
)

__all__ = list(map(lambda x: x.name, all_jobs))

if __name__ == "__main__":
    print(shared_assets_not_scheduled)
