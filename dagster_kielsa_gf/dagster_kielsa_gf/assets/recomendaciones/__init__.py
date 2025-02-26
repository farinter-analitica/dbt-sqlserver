from . import articulo, cliente_articulo

all_assets = (*articulo.all_assets, *cliente_articulo.all_assets)
all_asset_checks = (*articulo.all_asset_checks, *cliente_articulo.all_asset_checks)
