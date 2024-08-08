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
# Define the job and add to definitions on main __init__.py
ldcom_etl_dwh_job = define_asset_job(
    name="ldcom_etl_dwh_job", selection=AssetSelection.groups("ldcom_etl_dwh")
)

examples_assets_job = define_asset_job(
    name="examples_assets_job",
    selection=AssetSelection.groups(
        "examples",  # con directorio de proyecto
        "dbt_examples",  # con directorio de proyecto
        "dbt_second_group_test",  # con directorio de proyecto y tambien directamente en snapshots
        "dbt_first_group",  # grupo definido y usado directamente en snapshots
        "dbt_first_model",  # grupo no definido pero usado directamente en models
    ),
)
dbt_dwh_kielsa_marts_assets: AssetSelection = AssetSelection.groups(
    "dbt_dwh_kielsa_mart_datos_maestros",
    "dbt_dwh_kielsa_mart_kpp",
    "dbt_dwh_kielsa_mart_existencias",
    "dbt_dwh_kielsa_mart_ventas",
)
dbt_dwh_kielsa_marts_job = define_asset_job(
    name="dbt_dwh_kielsa_marts_job", selection=dbt_dwh_kielsa_marts_assets,
    tags=tags_repo.Daily.tag,
)

kielsa_etl_dwh_all_downstream_assets: AssetSelection = AssetSelection.groups(
    "ldcom_etl_dwh", "recetas_libros_etl_dwh"
).downstream()
kielsa_etl_dwh_all_downstream_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="kielsa_etl_dwh_all_downstream_job",
    selection=kielsa_etl_dwh_all_downstream_assets,
    tags=tags_repo.Daily.tag,
)

dlt_dwh_kielsa_assets: AssetSelection = AssetSelection.groups(
    "dlt_mongo_db_crm_hn_etl_dwh"
)
dlt_dwh_kielsa_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="dlt_dwh_kielsa_job",
    selection=dlt_dwh_kielsa_assets,
    config={"execution": {"config": {"multiprocess": {"max_concurrent": 2}}}},
    tags=tags_repo.Daily.tag,
)

kielsa_hourly_assets: AssetSelection = AssetSelection.tag(
    key=tags_repo.Hourly.key, value=tags_repo.Hourly.value
).upstream().required_multi_asset_neighbors() - AssetSelection.tag(
    key=tags_repo.DailyUnique.key,
    value=tags_repo.DailyUnique.value,
)
kielsa_hourly_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="kielsa_hourly_job",
    selection=kielsa_hourly_assets,
    tags=tags_repo.Hourly.tag
    | {
        "dagster/max_runtime": (100 * 60)
    },  # max 100 minutes in seconds, then mark it as failed.)
)

kielsa_olap_kielsa_general_temp_dev_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="kielsa_olap_kielsa_general_temp_dev_job",
    selection=AssetSelection.assets(
        AssetKey(["DWH_TABULAR","SSAS","olap_tabular_kielsa_general_ejecucion"])).upstream() \
        - AssetSelection.tag(key=tags_repo.DailyUnique.key,value=tags_repo.DailyUnique.value),
    tags=tags_repo.Hourly.tag
    | {
        "dagster/max_runtime": (100 * 60)
    },  # max 100 minutes in seconds, then mark it as failed.)
)

dlt_dwh_kielsa_all_downstream_assets: AssetSelection = AssetSelection.groups(
    "dlt_mongo_db_crm_hn_etl_dwh"
).downstream()
dlt_dwh_kielsa_all_downstream_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="dlt_dwh_kielsa_all_downstream_job",
    selection=dlt_dwh_kielsa_all_downstream_assets,
    config={"execution": {"config": {"multiprocess": {"max_concurrent": 2}}}},
    tags=tags_repo.Daily.tag,
)

dbt_dwh_kielsa_marts_assets_not_in_downstream: AssetSelection = (
    dbt_dwh_kielsa_marts_assets
    - kielsa_etl_dwh_all_downstream_assets
    - dlt_dwh_kielsa_all_downstream_assets
)
dbt_dwh_kielsa_marts_orphan_assets_job = define_asset_job(
    name="dbt_dwh_kielsa_marts_orphan_assets_job",
    selection=dbt_dwh_kielsa_marts_assets_not_in_downstream,
    tags=tags_repo.Daily.tag,
)

knime_workflows_start_of_month_assets: AssetSelection = AssetSelection.assets(
    AssetKey(["knime_wf", env_str, "DWHFP_SalidaExportarAExcel"])
).downstream()  ##Schedule differently
knime_workflows_start_of_month_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="knime_workflows_start_of_month_job",
    selection=knime_workflows_start_of_month_assets,
    config=workflows_run_config_secuential,
    tags=tags_repo.Monthly.tag,
)

knime_workflows_all_downstream_assets: AssetSelection = (
    AssetSelection.groups("knime_workflows").downstream()
    - knime_workflows_start_of_month_assets
)
knime_workflows_all_downstream_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="knime_workflows_all_downstream_job",
    selection=knime_workflows_all_downstream_assets,
    config=workflows_run_config_secuential,
    tags=tags_repo.Daily.tag,
)


all_jobs = get_all_instances_of_class(
    class_type_list=[JobDefinition, UnresolvedAssetJobDefinition]
)

__all__ = list(map(lambda x: x.name, all_jobs))

if __name__ == "__main__":
    print(dbt_dwh_kielsa_marts_assets_not_in_downstream)
