from dagster import load_assets_from_modules, load_asset_checks_from_modules
from dagster_kielsa_gf.assets import examples, dbt_dwh_kielsa, dotacion_personal

from dagster_kielsa_gf.assets.control_incentivos import assets as assets_incentivos

all_assets = (
    *load_assets_from_modules([examples], group_name="examples"),
    *load_assets_from_modules([dbt_dwh_kielsa, assets_incentivos, dotacion_personal]),
)
all_asset_checks = load_asset_checks_from_modules([examples, dbt_dwh_kielsa])
