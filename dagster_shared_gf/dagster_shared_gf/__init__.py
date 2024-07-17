import warnings
from dagster import ExperimentalWarning, Definitions
warnings.filterwarnings("ignore", category=ExperimentalWarning)

from dagster_shared_gf.resources import (sql_server_resources,
                                         dbt_resources,
                                         postgresql_resources,
                                         smb_resources,
                                         )

all_shared_resources = {"dwh_farinter" : sql_server_resources.dwh_farinter
                        , "dwh_farinter_adm" : sql_server_resources.dwh_farinter_adm
                        , "dwh_farinter_dl" : sql_server_resources.dwh_farinter_dl
                        , "dwh_farinter_bi" : sql_server_resources.dwh_farinter_bi
                        , "dbt_resource" : dbt_resources.dbt_resource
                        , "db_analitica_etl" : postgresql_resources.db_analitica_etl
                        , "smb_resource_analitica_nasgftgu02" : smb_resources.smb_resource_analitica_nasgftgu02
                        }

defs = Definitions(
    resources= all_shared_resources
)

