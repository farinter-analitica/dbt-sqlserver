from dagster import load_asset_checks_from_modules, load_assets_from_modules
from dagster_kielsa_gf.assets.control_demanda import (
    forecast_demanda,
    limpiar_demanda,
    personal_necesario_proyectado,
)

all_assets = load_assets_from_modules(
    [limpiar_demanda, forecast_demanda, personal_necesario_proyectado]
)
all_asset_checks = load_asset_checks_from_modules(
    [limpiar_demanda, forecast_demanda, personal_necesario_proyectado]
)
