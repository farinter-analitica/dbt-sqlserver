from dagster import (
    AssetKey,
    AssetSelection,
    JobDefinition,
    define_asset_job,
    RunConfig,
)

from dagster_shared_gf.shared_functions import get_all_instances_of_class
from dagster_shared_gf.shared_variables import (
    get_execution_config,
    UnresolvedAssetJobDefinition,
    tags_repo,
)

execution_config_secuential = get_execution_config(max_concurrent=1)
execution_run_config_2 = get_execution_config(max_concurrent=2)
execution_run_config_default = get_execution_config(max_concurrent=4)

## jobsss
# Define the job and add to definitions on main __init__.py
ldcom_etl_dwh_job = define_asset_job(
    name="ldcom_etl_dwh_job",
    selection=AssetSelection.groups("ldcom_etl_dwh"),
    config=RunConfig(execution=execution_run_config_default),
)
# Define the job and add to definitions on main __init__.py
ldcom_etl_dwh_job = define_asset_job(
    name="ldcom_etl_dwh_job",
    selection=AssetSelection.groups("ldcom_etl_dwh"),
    config=RunConfig(execution=execution_run_config_default),
)


seleccion_no_programar: AssetSelection = AssetSelection.tag(
    key=tags_repo.DetenerCarga.key, value=tags_repo.DetenerCarga.value
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
    config=RunConfig(execution=execution_run_config_default),
)
dbt_dwh_kielsa_marts_assets: AssetSelection = (
    AssetSelection.groups(
        "dbt_dwh_kielsa_mart_datos_maestros",
        "dbt_dwh_kielsa_mart_kpp",
        "dbt_dwh_kielsa_mart_existencias",
        "dbt_dwh_kielsa_mart_ventas",
    )
    - seleccion_no_programar
)
dbt_dwh_kielsa_marts_job = define_asset_job(
    name="dbt_dwh_kielsa_marts_job",
    selection=dbt_dwh_kielsa_marts_assets,
    tags=tags_repo.Daily.tag,
    config=RunConfig(execution=execution_run_config_default),
)

kielsa_etl_dwh_all_downstream_assets: AssetSelection = (
    AssetSelection.groups("ldcom_etl_dwh", "recetas_libros_etl_dwh").downstream()
    - AssetSelection.tag(
        key=tags_repo.Partitioned.key, value=tags_repo.Partitioned.value
    )
    - seleccion_no_programar
)
kielsa_etl_dwh_all_downstream_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="kielsa_etl_dwh_all_downstream_job",
    selection=kielsa_etl_dwh_all_downstream_assets,
    tags=tags_repo.Daily.tag,
    config=RunConfig(execution=execution_run_config_default),
)

dlt_dwh_kielsa_assets: AssetSelection = (
    AssetSelection.groups("dlt_mongo_db_crm_hn_etl_dwh") - seleccion_no_programar
)
dlt_dwh_kielsa_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="dlt_dwh_kielsa_job",
    selection=dlt_dwh_kielsa_assets,
    config=RunConfig(execution=execution_run_config_2),
    tags=tags_repo.Daily.tag,
)


# Definir assets que tengan la etiqueta por_hora y todos los dependientes que no tengan la etiqueta de periodo unico
kielsa_hourly_assets: AssetSelection = (
    AssetSelection.tag(key=tags_repo.Hourly.key, value=tags_repo.Hourly.value)
    - seleccion_no_programar
)
kielsa_hourly_assets = (
    kielsa_hourly_assets
    | (
        kielsa_hourly_assets.upstream().required_multi_asset_neighbors()
        - AssetSelection.tag(
            key=tags_repo.UniquePeriod.key,
            value=tags_repo.UniquePeriod.value,
        )
    )
    - seleccion_no_programar
)
kielsa_hourly_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="kielsa_hourly_job",
    selection=kielsa_hourly_assets,
    config=RunConfig(execution=execution_run_config_default),
    tags=tags_repo.Hourly.tag
    | {
        "dagster/max_runtime": (100 * 60)
    },  # max 100 minutes in seconds, then mark it as failed.)
)

# Definir assets que tengan la etiqueta mensual y todos los dependientes que no tengan la etiqueta de periodo unico
kielsa_start_of_month_assets: AssetSelection = AssetSelection.tag(
    key=tags_repo.Monthly.key, value=tags_repo.Monthly.value
)
kielsa_start_of_month_assets = (
    kielsa_start_of_month_assets
    | (
        kielsa_start_of_month_assets.upstream().required_multi_asset_neighbors()
        - AssetSelection.tag(
            key=tags_repo.UniquePeriod.key,
            value=tags_repo.UniquePeriod.value,
        )
    )
    - seleccion_no_programar
)
kielsa_start_of_month_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="kielsa_start_of_month_job",
    selection=kielsa_start_of_month_assets,
    config=RunConfig(execution=execution_run_config_default),
    tags=tags_repo.Monthly.tag
    | {
        "dagster/max_runtime": (100 * 60)
    },  # max 100 minutes in seconds, then mark it as failed.)
)

# Definir assets que tengan la etiqueta por_hora adicional (para ejecutar en medio del otro job) y todos los dependientes que no tengan la etiqueta de periodo unico
kielsa_hourly_additional_assets: AssetSelection = AssetSelection.tag(
    key=tags_repo.HourlyAdditional.key, value=tags_repo.HourlyAdditional.value
)
kielsa_hourly_additional_assets = (
    kielsa_hourly_additional_assets
    | (
        kielsa_hourly_additional_assets.upstream().required_multi_asset_neighbors()
        - AssetSelection.tag(
            key=tags_repo.UniquePeriod.key,
            value=tags_repo.UniquePeriod.value,
        )
    )
    - seleccion_no_programar
)
kielsa_hourly_additional_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="kielsa_hourly_additional_job",
    selection=kielsa_hourly_additional_assets,
    config=RunConfig(execution=execution_run_config_default),
    tags=tags_repo.Hourly.tag
    | tags_repo.HourlyAdditional.tag
    | {
        "dagster/max_runtime": (45 * 60)
    },  # max 45 minutes in seconds, then mark it as failed.)
)

# Migrado a PRD
# kielsa_olap_kielsa_general_temp_dev_job: UnresolvedAssetJobDefinition = (
#     define_asset_job(
#         name="kielsa_olap_kielsa_general_temp_dev_job",
#         selection=AssetSelection.assets(
#             AssetKey(["DWH_TABULAR", "SSAS", "olap_tabular_kielsa_general_ejecucion"])
#         ).upstream()
#         & kielsa_hourly_assets,
#         config=RunConfig(execution=execution_run_config_default),
#         tags=tags_repo.Hourly.tag
#         | {
#             "dagster/max_runtime": (100 * 60)
#         },  # max 100 minutes in seconds, then mark it as failed.)
#     )
# )

dlt_dwh_kielsa_all_downstream_assets: AssetSelection = (
    AssetSelection.groups("dlt_mongo_db_crm_hn_etl_dwh").downstream()
    - seleccion_no_programar
)
dlt_dwh_kielsa_all_downstream_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="dlt_dwh_kielsa_all_downstream_job",
    selection=dlt_dwh_kielsa_all_downstream_assets,
    config=RunConfig(execution=execution_run_config_2),
    tags=tags_repo.Daily.tag,
)

dbt_dwh_kielsa_marts_assets_not_in_downstream: AssetSelection = (
    dbt_dwh_kielsa_marts_assets
    - kielsa_etl_dwh_all_downstream_assets
    - dlt_dwh_kielsa_all_downstream_assets
) - seleccion_no_programar
dbt_dwh_kielsa_marts_orphan_assets_job = define_asset_job(
    name="dbt_dwh_kielsa_marts_orphan_assets_job",
    selection=dbt_dwh_kielsa_marts_assets_not_in_downstream,
    config=RunConfig(execution=execution_run_config_default),
    tags=tags_repo.Daily.tag,
)

knime_workflows_start_of_month_assets: AssetSelection = (
    AssetSelection.groups("knime_workflows")
    & AssetSelection.tag(key=tags_repo.Monthly.key, value=tags_repo.Monthly.value)
).downstream() - seleccion_no_programar  ##Schedule differently
knime_workflows_start_of_month_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="knime_workflows_start_of_month_job",
    selection=knime_workflows_start_of_month_assets,
    config=RunConfig(execution=execution_config_secuential),
    tags=tags_repo.Monthly.tag,
)

knime_workflows_all_downstream_assets: AssetSelection = (
    AssetSelection.groups("knime_workflows").downstream()
    - knime_workflows_start_of_month_assets
) - seleccion_no_programar
knime_workflows_all_downstream_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="knime_workflows_all_downstream_job",
    selection=knime_workflows_all_downstream_assets,
    config=RunConfig(execution=execution_config_secuential),
    tags=tags_repo.Daily.tag,
)

smb_etl_dwh_kielsa_all_downstream_assets: AssetSelection = AssetSelection.groups(
    "smb_etl_dwh"
).downstream()
smb_etl_dwh_kielsa_all_downstream_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="smb_etl_dwh_kielsa_all_downstream_job",
    selection=smb_etl_dwh_kielsa_all_downstream_assets,
    config=RunConfig(execution=execution_run_config_default),
    # , tags= {"dagster/max_runtime": (4*60*60)} # max 4 hours in seconds, then mark it as failed.
)


all_jobs = get_all_instances_of_class(
    class_type_list=[JobDefinition, UnresolvedAssetJobDefinition],
    namespace=globals(),
)

if __name__ == "__main__":
    print(dbt_dwh_kielsa_marts_assets_not_in_downstream)
