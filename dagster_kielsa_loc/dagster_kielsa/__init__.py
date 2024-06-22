import os

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from .assets import examples, kielsa_general, dbt_example

examples = load_assets_from_modules([examples], group_name="examples")
kielsa_general = load_assets_from_modules([kielsa_general], group_name="kielsa_general")
dbt_example = load_assets_from_modules([dbt_example] #, group_name="dbt_examples" #group name already on the dbt models
                                       )

all_assets = examples + kielsa_general + dbt_example

from dagster_shared_gf import all_shared_resources

dagster_kielsa_resources = all_shared_resources

defs = Definitions(
    assets=all_assets,
    resources= dagster_kielsa_resources
)

