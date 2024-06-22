from dagster_sap import all_assets
from dagster_sap import assets 
from dagster import load_assets_from_package_module

def test_all_assets_loaded():
    assert all_assets.__len__()==load_assets_from_package_module(assets).__len__() , "All assets should be loaded"