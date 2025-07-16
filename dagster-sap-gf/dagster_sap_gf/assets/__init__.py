from dagster import load_assets_from_modules, load_asset_checks_from_modules
from dagster_sap_gf.assets import examples, dbt_dwh_sap

all_assets = (
    *load_assets_from_modules([examples], group_name="examples"),
    *load_assets_from_modules([dbt_dwh_sap]),
)
all_asset_checks = load_asset_checks_from_modules([examples, dbt_dwh_sap])
