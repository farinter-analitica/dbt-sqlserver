from dagster import load_assets_from_modules
import my_assets

all_assets = load_assets_from_modules(my_assets, group_name="test")