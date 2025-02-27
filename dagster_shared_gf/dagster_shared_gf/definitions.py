import warnings

from dagster import (
    AssetSelection,
    Definitions,
    ExperimentalWarning,
    FilesystemIOManager,
    InMemoryIOManager,
    build_sensor_for_freshness_checks,
)
from dagster import (
    AutomationConditionSensorDefinition as ACS,
)
from dagster_polars import PolarsParquetIOManager

from dagster_shared_gf import assets as assets_repo
from dagster_shared_gf.assets import dbt_sources
from dagster_shared_gf.jobs import all_jobs
from dagster_shared_gf.resources import (
    correo_e,
    dbt_resources,
    postgresql_resources,
    smb_resources,
    sql_server_resources,
)
from dagster_shared_gf.schedules import all_schedules
from dagster_shared_gf.shared_constants import (
    running_default_sensor_status,
    hourly_freshness_seconds_per_environ,
)
from dagster_shared_gf.shared_helpers import (
    get_unique_source_assets,
    create_freshness_checks_for_assets,
)
from dagster_shared_gf.shared_variables import tags_repo

warnings.filterwarnings("ignore", category=ExperimentalWarning)

all_assets = assets_repo.all_assets
dbt_sources_assets: list = get_unique_source_assets(
    assets_repo.all_assets, dbt_sources.source_assets
)

all_shared_resources = {
    "dwh_farinter": sql_server_resources.dwh_farinter,
    "dwh_farinter_adm": sql_server_resources.dwh_farinter_adm,
    "dwh_farinter_dl": sql_server_resources.dwh_farinter_dl,
    "dwh_farinter_dl_prd": sql_server_resources.dwh_farinter_dl_prd,
    "dwh_farinter_bi": sql_server_resources.dwh_farinter_bi,
    "dwh_farinter_prd_replicas_ldcom": sql_server_resources.dwh_farinter_prd_replicas_ldcom,
    "ldcom_hn_prd_sqlserver": sql_server_resources.ldcom_hn_prd_sqlserver,
    "ldcom_ni_prd_sqlserver": sql_server_resources.ldcom_ni_prd_sqlserver,
    "ldcom_cr_prd_sqlserver": sql_server_resources.ldcom_cr_prd_sqlserver,
    "ldcom_cr_arb_prd_sqlserver": sql_server_resources.ldcom_cr_arb_prd_sqlserver,
    "ldcom_gt_prd_sqlserver": sql_server_resources.ldcom_gt_prd_sqlserver,
    "ldcom_sv_prd_sqlserver": sql_server_resources.ldcom_sv_prd_sqlserver,
    "siteplus_sqlldsubs_sqlserver": sql_server_resources.siteplus_sqlldsubs_sqlserver,
    #
    "dbt_resource": dbt_resources.dbt_resource,
    #
    "db_analitica_etl": postgresql_resources.db_analitica_etl,
    #
    "smb_resource_analitica_nasgftgu02": smb_resources.smb_resource_analitica_nasgftgu02,
    #
    "smb_resource_staging_dagster_dwh": smb_resources.smb_resource_staging_dagster_dwh,
    #
    "enviador_correo_e_analitica_farinter": correo_e.enviador_correo_e_analitica_farinter,
    #
    "in_memory_io_manager": InMemoryIOManager(),
    "filesystem_io_manager": FilesystemIOManager(),
    "polars_parquet_io_manager": PolarsParquetIOManager(),
}

asset_selection_hora = AssetSelection.tag(
    key=tags_repo.Hourly.key, value=tags_repo.Hourly.value
)

asset_selection_dia = (
    AssetSelection.tag(key=tags_repo.Daily.key, value=tags_repo.Daily.value)
    - asset_selection_hora
)
asset_selection_semana = (
    (
        AssetSelection.tag(key=tags_repo.Weekly.key, value=tags_repo.Weekly.value)
        | AssetSelection.tag(key=tags_repo.Weekly1.key, value=tags_repo.Weekly1.value)
        | AssetSelection.tag(key=tags_repo.Weekly7.key, value=tags_repo.Weekly7.value)
    )
    - asset_selection_dia
    - asset_selection_hora
)

asset_selection_mes = (
    (
        AssetSelection.tag(key=tags_repo.Monthly.key, value=tags_repo.Monthly.value)
        | AssetSelection.tag(
            key=tags_repo.MonthlyStart.key, value=tags_repo.MonthlyStart.value
        )
        | AssetSelection.tag(
            key=tags_repo.MonthlyEnd.key, value=tags_repo.MonthlyEnd.value
        )
    )
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

all_shared_sensors = (
    ACS(
        "automation_condition_sensor",
        target=asset_selection_dia,
        use_user_code_server=True,
        minimum_interval_seconds=60 * 5,
        tags=tags_repo.Daily,
        run_tags=tags_repo.Daily,
        default_status=running_default_sensor_status,
    ),
    ACS(
        "automation_condition_sensor_slow",
        target=asset_selection_mes | asset_selection_semana | asset_selection_restante,
        use_user_code_server=True,
        minimum_interval_seconds=60 * 60,
        tags=tags_repo.Monthly | tags_repo.Weekly,
        run_tags=tags_repo.Monthly | tags_repo.Weekly,
        default_status=running_default_sensor_status,
    ),
    ACS(
        "automation_condition_sensor_hourly",
        target=AssetSelection.tag(
            key=tags_repo.Hourly.key, value=tags_repo.Hourly.value
        ),
        use_user_code_server=True,
        minimum_interval_seconds=60,
        tags=tags_repo.Hourly,
        run_tags=tags_repo.Hourly,
        default_status=running_default_sensor_status,
    ),
)

all_freshness_checks = create_freshness_checks_for_assets(all_assets)

all_assets_freshness_checks_sensor = build_sensor_for_freshness_checks(
    freshness_checks=all_freshness_checks,
    default_status=running_default_sensor_status,
    minimum_interval_seconds=hourly_freshness_seconds_per_environ,
    name="all_assets_freshness_checks_sensor",
)

defs = Definitions(
    assets=(*assets_repo.all_assets, *dbt_sources_assets),
    jobs=(*all_jobs,),
    schedules=(*all_schedules,),
    asset_checks=(*assets_repo.all_asset_checks,),
    sensors=(*all_shared_sensors, all_assets_freshness_checks_sensor),
    resources=all_shared_resources,
)
