from dagster import AssetSelection, JobDefinition, RunConfig, define_asset_job

from dagster_sap_gf.assets.dbt_dwh_sap import MyDbtConfig
from dagster_shared_gf.shared_functions import get_all_instances_of_class
from dagster_shared_gf.shared_variables import (
    UnresolvedAssetJobDefinition,
    get_execution_config,
    tags_repo,
)
from dagster_shared_gf.utils import clean_storage

clean_storage_job = clean_storage.clean_storage_graph.to_job(name="clean_storage_job")

# Define the job and add to definitions on main __init__.py

execution_run_config_secuential = get_execution_config(max_concurrent=1)
execution_run_config_2 = get_execution_config(max_concurrent=2)
execution_run_config_default = get_execution_config(max_concurrent=4)


# dbt_dwh_sap_mart_datos_maestros_job = define_asset_job(name="dbt_dwh_sap_mart_datos_maestros_job"
#                                                             , selection=AssetSelection.groups("dbt_dwh_sap_mart_datos_maestros"))

dbt_dwh_sap_marts_job = define_asset_job(
    name="dbt_dwh_sap_marts_job",
    selection=AssetSelection.groups(
        "dbt_dwh_sap_mart_datos_maestros",
        "dbt_dwh_sap_mart_finanzas",
        "dbt_dwh_sap_mart_mm",
    ),
    config=execution_run_config_default,
)


dbt_dwh_sap_etl_dwh_job = define_asset_job(
    name="dbt_dwh_sap_etl_dwh_job",
    selection=AssetSelection.groups("sap_etl_dwh").tag_string("dagster_sap_gf/dbt"),
    config=execution_run_config_default,
)

dbt_dwh_sap_etl_dwh_full_refresh_job = define_asset_job(
    name="dbt_dwh_sap_etl_dwh_full_refresh_job",
    selection=AssetSelection.groups("sap_etl_dwh").tag_string("dagster_sap_gf/dbt"),
    config=RunConfig(
        {"dbt_sap_etl_dwh_assets": MyDbtConfig(full_refresh=True)},
        execution=execution_run_config_default.execution,
    ),
)


sap_etl_dwh_all_downstream_assets: AssetSelection = AssetSelection.groups(
    "sap_etl_dwh"
).tag(key=tags_repo.Daily.key, value=tags_repo.Daily.value)
sap_etl_dwh_all_downstream_assets = sap_etl_dwh_all_downstream_assets | (
    sap_etl_dwh_all_downstream_assets.downstream()
    - (
        sap_etl_dwh_all_downstream_assets.tag(
            key=tags_repo.UniquePeriod.key, value=tags_repo.UniquePeriod.value
        )
    )
)
sap_etl_dwh_all_downstream_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="sap_etl_dwh_all_downstream_job",
    selection=sap_etl_dwh_all_downstream_assets,
    tags={
        "dagster/max_runtime": (4 * 60 * 60)
    }  # max 4 hours in seconds, then mark it as failed.
    | tags_repo.Daily.tag,
    config=execution_run_config_default,
)

# Definir assets que tenga la etiqueta por_hora y todos los dependientes que no tengan la etiqueta de periodo unico
sap_dwh_hourly_assets: AssetSelection = AssetSelection.tag(
    key=tags_repo.Hourly.key, value=tags_repo.Hourly.value
)
sap_dwh_hourly_assets = sap_dwh_hourly_assets | (
    sap_dwh_hourly_assets.upstream()
    - (
        sap_dwh_hourly_assets.tag(
            key=tags_repo.UniquePeriod.key, value=tags_repo.UniquePeriod.value
        )
    )
)
sap_dwh_hourly_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="sap_dwh_hourly_job",
    selection=sap_dwh_hourly_assets,
    tags={
        "dagster/max_runtime": (100 * 60)
    }  # max 100 minutes in seconds, then mark it as failed.
    | tags_repo.Hourly.tag,
    config=execution_run_config_default,
)

dbt_dwh_sap_marts_all_orphan_assets: AssetSelection = (
    AssetSelection.groups(
        "dbt_dwh_sap_mart_datos_maestros", "dbt_dwh_sap_mart_finanzas"
    )
    - sap_etl_dwh_all_downstream_assets
)
dbt_dwh_sap_marts_all_orphan_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="dbt_dwh_sap_marts_all_orphan_job",
    selection=dbt_dwh_sap_marts_all_orphan_assets,
    config=execution_run_config_default,
)


all_jobs: tuple[JobDefinition | UnresolvedAssetJobDefinition,...] = get_all_instances_of_class(
    class_type_list=[JobDefinition, UnresolvedAssetJobDefinition], namespace=globals()
)

if __name__ == "__main__":
    print(globals())
    for job in all_jobs:
        print(job.name)
