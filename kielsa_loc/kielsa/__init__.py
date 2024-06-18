from dagster import Definitions, load_assets_from_modules

from .assets import examples, kielsa_general

examples = load_assets_from_modules([examples], group_name="examples")
kielsa_general = load_assets_from_modules([kielsa_general], group_name="kielsa_general")

all_assets = examples + kielsa_general

from .resources import sql_server_resources

all_resourses = [sql_server_resources.dwh_farinter,sql_server_resources.dwh_farinter_adm,sql_server_resources.dwh_farinter_dl]

defs = Definitions(
    assets=all_assets,
    resources= {"dwh_farinter_adm" : sql_server_resources.dwh_farinter_adm
                ,"dwh_farinter_dl" : sql_server_resources.dwh_farinter_dl
                ,"dwh_farinter" : sql_server_resources.dwh_farinter
                }
)
