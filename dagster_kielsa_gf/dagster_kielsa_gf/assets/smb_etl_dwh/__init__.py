from dagster import (
    load_assets_from_modules,
    load_asset_checks_from_modules,
)
from . import kielsa_metas_excel
from . import kielsa_horarios

all_assets = (
    *load_assets_from_modules((kielsa_metas_excel,), group_name="smb_etl_dwh"),
    *load_assets_from_modules((kielsa_horarios,), group_name="smb_etl_dwh"),
)
all_asset_checks = (
    *load_asset_checks_from_modules((kielsa_metas_excel,)),
    *load_asset_checks_from_modules((kielsa_horarios,)),
)
