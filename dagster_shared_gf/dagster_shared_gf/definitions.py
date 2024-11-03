import warnings

from dagster import (
    Definitions,
    ExperimentalWarning,
    FilesystemIOManager,
    InMemoryIOManager,
)
from dagster_polars import PolarsParquetIOManager

from dagster_shared_gf.assets import dbt_dwh_global, dbt_sources, dias_festivos
from dagster_shared_gf.jobs import all_jobs
from dagster_shared_gf.resources import (
    correo_e,
    dbt_resources,
    postgresql_resources,
    smb_resources,
    sql_server_resources,
)
from dagster_shared_gf.schedules import all_schedules

warnings.filterwarnings("ignore", category=ExperimentalWarning)

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

defs = Definitions(
    assets=dias_festivos.all_assets
    + dbt_dwh_global.all_assets
    + dbt_sources.all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    asset_checks=dias_festivos.all_asset_checks
    + dias_festivos.all_asset_freshness_checks,
    resources=all_shared_resources,
)
