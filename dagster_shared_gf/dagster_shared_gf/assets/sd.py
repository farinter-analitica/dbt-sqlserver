from dagster import asset, load_assets_from_current_module, AssetExecutionContext

@asset(key_prefix=["dagster_k", "assets"], compute_kind="python")
def asset_1(context: AssetExecutionContext):
    X=1
    X+=1

@asset(key_prefix=["dagster_k", "assets"], compute_kind="python" , deps=[asset_1])
def asset_2(context: AssetExecutionContext):
    X=2
    X+=1

all_assets = tuple(load_assets_from_current_module())

print(all_assets)