from dagster import build_init_resource_context
from kielsa.resources.dwh_resources import dwh_resource

def test_dwh_resource():
    resource_config = dwh_resource(database='DL_FARINTER')
    context = build_init_resource_context(resources={'dwh': resource_config})
    resource = resource_config.get_connection()
    print("passed")

    assert resource is not None
