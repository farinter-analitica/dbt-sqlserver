import warnings
from dagster import ExperimentalWarning, Definitions
warnings.filterwarnings("ignore", category=ExperimentalWarning)

from dagster_shared_gf.resources import (sql_server_resources,
                                         dbt_resources,
                                         postgresql_resources)

all_shared_resources = {"dwh_farinter" : sql_server_resources.dwh_farinter
                        , "dwh_farinter_adm" : sql_server_resources.dwh_farinter_adm
                        , "dwh_farinter_dl" : sql_server_resources.dwh_farinter_dl
                        , "dbt_resource" : dbt_resources.dbt_resource
                        , "db_analitica_etl" : postgresql_resources.db_analitica_etl}

defs = Definitions(
    resources= all_shared_resources
)

