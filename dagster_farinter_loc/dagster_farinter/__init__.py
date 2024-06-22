import os

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from .assets import examples, kielsa_general, dbt_example

examples = load_assets_from_modules([examples], group_name="examples")
kielsa_general = load_assets_from_modules([kielsa_general], group_name="kielsa_general")
dbt_example = load_assets_from_modules([dbt_example] #, group_name="dbt_examples" #group name already on the dbt models
                                       )

all_assets = examples + kielsa_general + dbt_example

from .resources import sql_server_resources
from .constants import dbt_resource

all_resourses = [sql_server_resources.dwh_farinter,sql_server_resources.dwh_farinter_adm,sql_server_resources.dwh_farinter_dl]

defs = Definitions(
    assets=all_assets,
    resources= {"dwh_farinter_adm" : sql_server_resources.dwh_farinter_adm
                ,"dwh_farinter_dl" : sql_server_resources.dwh_farinter_dl
                ,"dwh_farinter" : sql_server_resources.dwh_farinter
                ,"dbt_resource" : dbt_resource
                }
)
