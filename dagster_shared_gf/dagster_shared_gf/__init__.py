import warnings
from dagster import ExperimentalWarning, Definitions, InMemoryIOManager, FilesystemIOManager
warnings.filterwarnings("ignore", category=ExperimentalWarning)

from dagster_shared_gf.resources import (sql_server_resources,
                                         dbt_resources,
                                         postgresql_resources,
                                         smb_resources,
                                         correo_e,

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
    "ldcom_gt_prd_sqlserver": sql_server_resources.ldcom_gt_prd_sqlserver,
    "ldcom_sv_prd_sqlserver": sql_server_resources.ldcom_sv_prd_sqlserver,


    "dbt_resource": dbt_resources.dbt_resource,

    "db_analitica_etl": postgresql_resources.db_analitica_etl,

    "smb_resource_analitica_nasgftgu02": smb_resources.smb_resource_analitica_nasgftgu02,


    "enviador_correo_e_analitica_farinter": correo_e.enviador_correo_e_analitica_farinter,


    "in_memory_io_manager": InMemoryIOManager(),
    "filesystem_io_manager": FilesystemIOManager(),
}

defs = Definitions(
    resources= all_shared_resources,
)
