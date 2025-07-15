import warnings

from dagster import (
    AssetsDefinition,
    AssetSpec,
    Definitions,
    JobDefinition,
)

from dagster_kielsa_gf import defs
from dagster_shared_gf.shared_variables import (
    AssetExecutionType,
    UnresolvedAssetJobDefinition,
    tags_repo,
)

defs: Definitions = defs  ##import your defs
asset_spec: AssetSpec
warnings.filterwarnings("ignore", message="validators are deprecated")


def test_all_assets_automated():
    # Inspect the Definitions object to find assets without jobs
    all_assets_specs = set()
    all_auto_stop_assets_specs = set()
    # all_assets = list(asset_spec.key for asset_spec in defs.get_all_asset_specs())
    for asset_spec in defs.get_all_asset_specs():
        if (
            defs.resolve_assets_def(asset_spec.key).execution_type
            == AssetExecutionType.MATERIALIZATION
        ):
            all_assets_specs.add(asset_spec.key)
            if any(
                tag_no_auto.key in asset_spec.tags
                for tag_no_auto in tags_repo.get_unselected_for_jobs_tags()
            ):
                all_auto_stop_assets_specs.add(asset_spec.key)
        # print(defs.resolve_assets_def(asset_spec.key).execution_type)
    print("All assets: ", str(len(all_assets_specs)))

    # print(all_assets)
    # print(defs.get_implicit_job_def_for_assets(all_assets))
    job_auto_stop_assigned_assets = set()
    all_jobs = defs.jobs or []
    all_assets = defs.assets
    all_assets = (
        [asset for asset in all_assets if isinstance(asset, AssetsDefinition)]
        if all_assets
        else []
    )
    for job_def in all_jobs:
        if isinstance(job_def, JobDefinition):
            if job_def.asset_selection:
                materialized_assets = {
                    asset_key
                    for asset_key in job_def.asset_selection
                    if defs.resolve_assets_def(asset_key).execution_type
                    == AssetExecutionType.MATERIALIZATION
                }
                job_auto_stop_assigned_assets.update(materialized_assets)
        elif isinstance(job_def, UnresolvedAssetJobDefinition):
            try:
                resolved_assets = job_def.selection.resolve(all_assets)  # type: ignore
            except Exception as e:
                print("Error resolving assets for job: ", job_def.name)
                resolved_assets = set()
                raise e
            materialized_assets = {
                asset_key
                for asset_key in resolved_assets
                if defs.resolve_assets_def(asset_key).execution_type
                == AssetExecutionType.MATERIALIZATION
            }
            job_auto_stop_assigned_assets.update(materialized_assets)

    job_auto_stop_assigned_assets.update(all_auto_stop_assets_specs)

    non_job_assigned_assets = all_assets_specs - job_auto_stop_assigned_assets

    assert_message = f"""
        All assets: {str(len(all_assets_specs))}
        Non-job assigned assets: {str(len(non_job_assigned_assets))}
        List of non-job assigned assets: {non_job_assigned_assets}
        Job, auto or stopped assigned assets: {str(len(job_auto_stop_assigned_assets))}
        Non-job, auto or stopped assigned assets: {str(len(non_job_assigned_assets))}
        """
    # for asset_key in non_job_assigned_assets:
    #     asset_spec = next((a for a in defs.get_all_asset_specs() if a.key == asset_key), None)
    #     print(f"Asset: {asset_key}, \
    #           tags: {getattr(asset_spec, 'tags', None)}, \
    #           execution type: {defs.resolve_assets_def(asset_key).execution_type}, \
    #           atr status: {defs.resolve_assets_def(asset_key).automation_conditions_by_key.get(asset_key, '')[:50]}")

    assert len(non_job_assigned_assets) == 0, assert_message

    # Print non-job assigned assets
    # print("Non-job assigned assets:")

    # print(asset_key.metadata for asset_key in defs.get_all_asset_specs() if asset_key in non_job_assigned_assets)
    # Optionally, define a job with the non-job assigned assets for testing purposes
    # non_job_assigned_assets_selection = AssetSelection.assets(*non_job_assigned_assets)

    # non_job_assigned_assets_job = define_asset_job(
    #     name="non_job_assigned_assets_job",
    #     selection=non_job_assigned_assets_selection
    # )

    # # Update the Definitions object with the new job
    # defs = Definitions(
    #     assets=[asset1, asset2, asset3],
    #     jobs=[job_with_assets, non_job_assigned_assets_job]
    # )


if __name__ == "__main__":
    test_all_assets_automated()
