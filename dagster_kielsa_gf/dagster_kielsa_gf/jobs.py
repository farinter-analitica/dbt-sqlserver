from dagster import (
    AssetSelection,
    JobDefinition,
    define_asset_job,
)

from dagster_shared_gf.shared_functions import get_all_instances_of_class
from dagster_shared_gf.shared_variables import (
    UnresolvedAssetJobDefinition,
    get_execution_config,
    seleccion_no_programar,
    tags_repo,
)
from dagster_shared_gf.utils import clean_storage

clean_storage_job = clean_storage.clean_storage_job

execution_config_secuential = get_execution_config(max_concurrent=1)
execution_run_config_2 = get_execution_config(max_concurrent=2)
execution_run_config_default = get_execution_config(max_concurrent=4)

## jobsss
# Define the job and add to definitions on main __init__.py
ldcom_etl_dwh_job = define_asset_job(
    name="ldcom_etl_dwh_job",
    selection=AssetSelection.groups("ldcom_etl_dwh"),
    config=execution_run_config_default,
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
    config=execution_run_config_default,
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
    tags=tags_repo.Daily.tag | {"dagster/max_runtime": (23 * 60 * 60)},
    run_tags=tags_repo.Daily.tag
    | {
        "dagster/max_runtime": (23 * 60 * 60)
    },  # max 23 hours in seconds, then mark it as failed.)
    config=execution_run_config_default,
)

knime_workflows_daily_assets: AssetSelection = (
    AssetSelection.groups("knime_workflows")
    & AssetSelection.tag(key=tags_repo.Daily.key, value=tags_repo.Daily.value)
) - seleccion_no_programar


knime_workflows_daily_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="knime_workflows_daily_job",
    selection=knime_workflows_daily_assets,
    config=execution_config_secuential,
    tags=tags_repo.Daily.tag,
)

dlt_dwh_kielsa_daily_assets: AssetSelection = (
    AssetSelection.groups("dlt_mongo_db_crm_hn_etl_dwh")
    & AssetSelection.tag(key=tags_repo.Daily.key, value=tags_repo.Daily.value)
    - seleccion_no_programar
)
dlt_dwh_kielsa_daily_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="dlt_dwh_kielsa_daily_job",
    selection=dlt_dwh_kielsa_daily_assets,
    config=execution_run_config_2,
    tags=tags_repo.Daily.tag,
    description="Este Job solo ejecuta los assets del grupo dlt_mongo_db_crm_hn_etl_dwh\n\
    pero no sus dependientes ya que podrian estar ligados con otros grupos, usar automation unica para esos assets.\n\
El job solo ejecuta 2 procesos maximo al mismo tiempo.\n\
    ",
)

kielsa_daily_downstream_assets: AssetSelection = AssetSelection.tag(
    key=tags_repo.Daily.key, value=tags_repo.Daily.value
)
kielsa_daily_downstream_assets = (
    (
        kielsa_daily_downstream_assets
        | (
            kielsa_daily_downstream_assets.downstream()
            - kielsa_daily_downstream_assets.downstream().tag(
                key=tags_repo.UniquePeriod.key, value=tags_repo.UniquePeriod.value
            )
        )
    )
    - seleccion_no_programar
    - knime_workflows_daily_assets
    - dlt_dwh_kielsa_daily_assets
)
kielsa_daily_downstream_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="kielsa_daily_downstream_job",
    selection=kielsa_daily_downstream_assets,
    tags=tags_repo.Daily.tag | {"dagster/max_runtime": (18 * 60 * 60)},
    run_tags=tags_repo.Daily.tag
    | {
        "dagster/max_runtime": (18 * 60 * 60)
    },  # max 18 hours in seconds, then mark it as failed.)
    config=execution_run_config_default,
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
    config=execution_run_config_default,
    tags=tags_repo.Hourly.tag
    | {
        "dagster/max_runtime": (100 * 60)
    },  # max 100 minutes in seconds, then mark it as failed.)
    run_tags=tags_repo.Hourly.tag | {"dagster/max_runtime": (100 * 60)},
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
    config=execution_run_config_default,
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
    config=execution_run_config_default,
    tags=tags_repo.Hourly.tag
    | tags_repo.HourlyAdditional.tag
    | {
        "dagster/max_runtime": (45 * 60)
    },  # max 45 minutes in seconds, then mark it as failed.)
)

dbt_dwh_kielsa_marts_assets_not_in_downstream: AssetSelection = (
    dbt_dwh_kielsa_marts_assets
    - kielsa_daily_downstream_assets
    - dlt_dwh_kielsa_daily_assets
) - seleccion_no_programar

smb_etl_dwh_kielsa_assets: AssetSelection = AssetSelection.groups("smb_etl_dwh")
smb_etl_dwh_kielsa_job: UnresolvedAssetJobDefinition = define_asset_job(
    name="smb_etl_dwh_kielsa_all_downstream_job",
    selection=smb_etl_dwh_kielsa_assets,
    config=execution_run_config_default,
    # , tags= {"dagster/max_runtime": (4*60*60)} # max 4 hours in seconds, then mark it as failed.
)

all_jobs = get_all_instances_of_class(
    class_type_list=[JobDefinition, UnresolvedAssetJobDefinition],
    namespace=globals(),
)

if __name__ == "__main__":
    print(dbt_dwh_kielsa_marts_assets_not_in_downstream)
