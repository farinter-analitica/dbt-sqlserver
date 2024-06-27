from dagster import Definitions, load_assets_from_modules, ExperimentalWarning


from dagster_shared_gf.resources import sql_server_resources
from dagster_shared_gf.resources import dbt_resources

all_shared_resources = {"dwh_farinter" : sql_server_resources.dwh_farinter
                        , "dwh_farinter_adm" : sql_server_resources.dwh_farinter_adm
                        , "dwh_farinter_dl" : sql_server_resources.dwh_farinter_dl
                        , "dbt_resource" : dbt_resources.dbt_resource}

defs = Definitions(
    resources= all_shared_resources
)

