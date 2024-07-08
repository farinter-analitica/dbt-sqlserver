from dagster_kielsa_gf import defs
from dagster import asset, define_asset_job, Definitions, AssetSelection, AssetSpec, JobDefinition
from dagster._core.definitions.unresolved_asset_job_definition import UnresolvedAssetJobDefinition
from dagster._core.definitions.asset_spec import AssetExecutionType
from typing import Sequence

defs: Definitions = defs ##import your defs
asset_spec: AssetSpec 


def test_all_assets_assigned_to_a_job():
    # Inspect the Definitions object to find assets without jobs
    all_assets_specs = set()
    #all_assets = list(asset_spec.key for asset_spec in defs.get_all_asset_specs())
    for asset_spec in defs.get_all_asset_specs():
        if defs.get_assets_def(asset_spec.key).execution_type == AssetExecutionType.MATERIALIZATION:
            all_assets_specs.add(asset_spec.key) 
        #print(defs.get_assets_def(asset_spec.key).execution_type)
    print("All assets: ", str(len(all_assets_specs)))



    #print(all_assets)
    #print(defs.get_implicit_job_def_for_assets(all_assets))

    job_assigned_assets = set()
    all_jobs:  Sequence[JobDefinition | UnresolvedAssetJobDefinition] = defs.jobs
    for job_def in all_jobs:
        if hasattr(job_def, 'asset_selection'):
            if job_def.asset_selection:
                for asset_key in job_def.asset_selection:
                    if defs.get_assets_def(asset_key).execution_type == AssetExecutionType.MATERIALIZATION:
                        job_assigned_assets.update(asset_key)
        elif hasattr(job_def, 'selection'):
            if job_assigned_assets.update(job_def.selection.resolve(defs.assets)):
                for asset_key in job_assigned_assets.update(job_def.selection.resolve(defs.assets)):
                    if defs.get_assets_def(asset_key).execution_type == AssetExecutionType.MATERIALIZATION:
                        job_assigned_assets.update(asset_key)
        
    print("Job assigned assets: ", str(len(job_assigned_assets)))

    non_job_assigned_assets = all_assets_specs - job_assigned_assets
    print("Non-job assigned assets: ", str(len(non_job_assigned_assets)))

    assert_message =f"""Non-job assigned assets: {str(len(non_job_assigned_assets))}
        List of non-job assigned assets: {non_job_assigned_assets}
        """
    assert len(non_job_assigned_assets) == 0 , assert_message

    # Print non-job assigned assets
    # print("Non-job assigned assets:")
    # for asset_key in non_job_assigned_assets:
    #     print(asset_key)




        #print(asset_key.metadata for asset_key in defs.get_all_asset_specs() if asset_key in non_job_assigned_assets)
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
    test_all_assets_assigned_to_a_job()