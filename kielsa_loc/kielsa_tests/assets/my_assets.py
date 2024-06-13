from dagster import build_op_context, build_init_resource_context
from assets.my_assets import my_data_asset
from shared_loc import DWHConfig

def test_my_data_asset():
    resource_config = DWHConfig(database='DL_FARINTER')
    resource_context = build_init_resource_context(resources={'dwh': resource_config})
    dwh = resource_config.get_connection()

    context = build_op_context(resources={'dwh': dwh})
    result = my_data_asset(context)
    
    assert result
