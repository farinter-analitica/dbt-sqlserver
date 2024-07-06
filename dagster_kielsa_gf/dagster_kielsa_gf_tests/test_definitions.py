from dagster_kielsa_gf import defs
from dagster_kielsa_gf import assets 
from dagster import load_assets_from_package_module

def test_all_assets_loaded():
    assert defs.get_all_asset_specs().__len__()==load_assets_from_package_module(assets).__len__() , "All assets should be loaded"