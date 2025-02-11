from dagster import (
    AssetSelection,
    AssetsDefinition,
    Definitions,
    AutomationConditionSensorDefinition as ACS,
)
import dagster_kielsa_gf.dlt_defs.definitions as dlt_defs
import dagster_kielsa_gf.gobernor.jobs_gobernor as gobernor_defs

from dagster_kielsa_gf import job_control_replicas, jobs
from dagster_kielsa_gf.assets import (
    analysis_services,
    cliente_general,
    dbt_dwh_kielsa,
    dbt_example,
    dbt_sources,
    examples,
    knime_asset_factory,
    ldcom_etl_dwh,
    ldcom_etl_dwh_sp,
    recetas_libros_etl_dwh,
    smb_etl_dwh,
    recomendacion_cliente,
)
from dagster_kielsa_gf.schedules import all_schedules
from dagster_kielsa_gf.sensors import all_sensors
from dagster_shared_gf import all_shared_resources
from dagster_shared_gf.shared_variables import tags_repo

all_assets = (
    *examples.all_assets,
    *dbt_example.all_assets,
    *dbt_dwh_kielsa.all_assets,
    *ldcom_etl_dwh_sp.all_assets,
    *knime_asset_factory.all_assets,
    *recetas_libros_etl_dwh.all_assets,
    *analysis_services.all_assets,
    *ldcom_etl_dwh.all_assets,
    *smb_etl_dwh.all_assets,
    *cliente_general.all_assets,
    *recomendacion_cliente.all_assets,
)
all_asset_checks = (
    *dbt_example.all_asset_checks,
    *dbt_dwh_kielsa.all_asset_checks,
    *ldcom_etl_dwh_sp.all_asset_checks,
    *knime_asset_factory.all_asset_checks,
    *recetas_libros_etl_dwh.all_asset_checks,
    *analysis_services.all_asset_checks,
    *ldcom_etl_dwh.all_asset_checks,
    *smb_etl_dwh.all_asset_checks,
    *recomendacion_cliente.all_asset_checks,
)

# Extract the asset keys from the AssetsDefinition instances
all_asset_keys = set()
for asset in all_assets:
    if type(asset) is AssetsDefinition:
        all_asset_keys.update(asset.keys)

dbt_sources_assets: list = [
    source_asset
    for source_asset in dbt_sources.source_assets
    if source_asset.key not in all_asset_keys
]


all_resources = all_shared_resources

asset_selection_hora = AssetSelection.tag(
    key=tags_repo.Hourly.key, value=tags_repo.Hourly.value
)

asset_selection_dia = (
    AssetSelection.tag(key=tags_repo.Daily.key, value=tags_repo.Daily.value)
    - asset_selection_hora
)
asset_selection_semana = (
    AssetSelection.tag(key=tags_repo.Weekly.key, value=tags_repo.Weekly.value)
    - asset_selection_dia
    - asset_selection_hora
)

asset_selection_mes = (
    AssetSelection.tag(key=tags_repo.Monthly.key, value=tags_repo.Monthly.value)
    - asset_selection_semana
    - asset_selection_dia
    - asset_selection_hora
)

asset_selection_restante = (
    AssetSelection.all()
    - asset_selection_mes
    - asset_selection_semana
    - asset_selection_dia
    - asset_selection_hora
)

defs = Definitions.merge(
    # dlt_defs.defs, #antes todos los subrepos
    Definitions(
        assets=(
            *all_assets,
            *dbt_sources_assets,
            *dlt_defs.all_assets,
        ),
        asset_checks=(*all_asset_checks, *dlt_defs.all_asset_checks),
        resources=all_resources | dlt_defs.all_resources,
        jobs=(*jobs.all_jobs, *job_control_replicas.all_jobs),
        schedules=all_schedules,
        sensors=(
            *all_sensors,
            ACS(
                "automation_condition_sensor",
                target=asset_selection_dia,
                use_user_code_server=True,
                minimum_interval_seconds=60 * 5,
                tags=tags_repo.Daily,
                run_tags=tags_repo.Daily,
            ),
            ACS(
                "automation_condition_sensor_slow",
                target=asset_selection_mes | asset_selection_semana | asset_selection_restante,
                use_user_code_server=True,
                minimum_interval_seconds=60 * 60,
                tags=tags_repo.Monthly | tags_repo.Weekly,
                run_tags=tags_repo.Monthly | tags_repo.Weekly,
            ),
            ACS(
                "automation_condition_sensor_hourly",
                target=AssetSelection.tag(
                    key=tags_repo.Hourly.key, value=tags_repo.Hourly.value
                )
                & AssetSelection.groups("recetas_libros_etl_dwh"),
                use_user_code_server=True,
                minimum_interval_seconds=60,
                tags=tags_repo.Hourly,
                run_tags=tags_repo.Hourly,
            ),
        ),
    ),
    gobernor_defs.defs,  # De ultimo ya que puede gobernar los demas subrepos
)

# @repository
# def dagster_kielsa_gf_repo():
#     return defs
