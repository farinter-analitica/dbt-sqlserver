import os

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from .assets import examples, kielsa_general, dbt_example, dbt_dwh_kielsa_mart_datos_maestros, dbt_sources

examples = load_assets_from_modules([examples], group_name="examples")
kielsa_general = load_assets_from_modules([kielsa_general], group_name="kielsa_general")
dbt_example = load_assets_from_modules([dbt_example] #, group_name="dbt_examples" #group name already on the dbt models
                                       )
dbt_dwh_kielsa_mart_assets = load_assets_from_modules([dbt_dwh_kielsa_mart_datos_maestros])
dbt_sources_assets = dbt_sources.source_assets

all_assets = examples + kielsa_general + dbt_example + dbt_sources_assets + dbt_dwh_kielsa_mart_assets

from dagster_shared_gf import all_shared_resources
import dagster_kielsa.jobs as jobs
import dagster_kielsa.schedules as schedules

dagster_kielsa_resources = all_shared_resources
all_jobs = [*jobs.__all__]
all_schedules = [*schedules.__all__]

defs = Definitions(
    assets=all_assets,
    resources= dagster_kielsa_resources,
    jobs=all_jobs,
    schedules=all_schedules
)

