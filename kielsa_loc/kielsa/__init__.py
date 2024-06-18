from dagster import Definitions, load_assets_from_modules

from .assets import all_assets
from .resources import dwh_resources
from .resources import sql_server_resources

all_resourses = [dwh_resources.dwh_resource]

defs = Definitions(
    assets=all_assets,
    resources= {"dwh_adm_farinter" : sql_server_resources.dwh_adm_farinter
                ,"dwh_dl_farinter" : sql_server_resources.dwh_dl_farinter
                }
)
