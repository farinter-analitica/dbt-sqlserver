from dagster import load_assets_from_modules, load_asset_checks_from_modules
from dagster_kielsa_gf.assets.recomendaciones import articulo, cliente_articulo

all_assets = load_assets_from_modules(
    [articulo, cliente_articulo], group_name="recomendaciones"
)
all_asset_checks = load_asset_checks_from_modules([articulo, cliente_articulo])
