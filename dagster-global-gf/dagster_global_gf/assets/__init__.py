from dagster import (
    AssetSpec,
    load_assets_from_modules,
    load_asset_checks_from_modules,
    AssetChecksDefinition,
    AssetsDefinition,
)
from dagster_global_gf.assets import (
    dbt_sources,
    dias_festivos,
    dbt_dwh_global,
    etl_dwh_sp,
    examples,
)

web_api_externo = load_assets_from_modules(
    [dias_festivos], group_name="web_api_externo"
)

example_assets = load_assets_from_modules([examples], group_name="examples")

all_assets = (
    *load_assets_from_modules([dbt_sources, dbt_dwh_global, etl_dwh_sp]),
    *web_api_externo,
    *example_assets,
)
all_asset_checks = load_asset_checks_from_modules(
    [dbt_sources, dias_festivos, dbt_dwh_global, etl_dwh_sp, examples]
)

if __name__ == "__main__":
    print(
        [
            assetdef.keys
            for assetdef in all_assets
            if isinstance(assetdef, AssetsDefinition)
        ]
    )
    print([assetdef.key for assetdef in all_assets if isinstance(assetdef, AssetSpec)])
    print(
        [
            assetchk.keys
            for assetchk in all_asset_checks
            if isinstance(assetchk, AssetChecksDefinition)
        ]
    )
