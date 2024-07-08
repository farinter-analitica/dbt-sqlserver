from dagster import define_asset_job, ScheduleDefinition
import pytest
from dagster_shared_gf.shared_functions import *

# Example class and function definitions for testing

test_job = define_asset_job(name="dbt_dwh_sap_marts_job")

my_schedule = ScheduleDefinition(
    name="test",
    cron_schedule="14 1 * * *",  # 10:01 AM every day
    execution_timezone="America/Tegucigalpa",
    job=test_job
)
def my_schedule_function():
    pass

def another_function():
    pass

another_variable = 0

#print(test_job)
# Filter instances of ScheduleDefinition
#print(get_all_instances_of_class([ScheduleDefinition]))

# Filter variables created by the function define_asset_job
#print(get_variables_created_by_function(define_asset_job))

# Test get_all_instances_of_class
def test_get_all_instances_of_class():
    instances = get_all_instances_of_class([ScheduleDefinition])
    assert len(instances) == 1, f"Expected 1 instances, got {len(instances)}"
    assert my_schedule in instances, f"Expected 1 instances, got {len(instances)}"

# Test get_variables_created_by_function
def test_get_variables_created_by_function():
    variables = get_variables_created_by_function(define_asset_job)
    assert len(variables) == 1, f"Expected 1 variables, got {len(variables)}"
    assert test_job in variables, "Expected test_job to be in variables"



class MockAsset:
    def __init__(self, tags):
        self.tags_by_key = {'key1': tags}
        self.keys = ['key1']

@pytest.fixture
def asset1():
    return MockAsset({'tag1': 'value1', 'tag2': 'value2'})

@pytest.fixture
def asset2():
    return MockAsset({'tag2': 'value2'})

@pytest.fixture
def asset3():
    return MockAsset({'tag3': 'value3'})

@pytest.fixture
def assets_definitions(asset1, asset2, asset3):
    return [asset1, asset2, asset3]

def test_all_tags_match(assets_definitions, asset1):
    tags = {'tag1': 'value1', 'tag2': 'value2'}
    result = filter_assets_by_tags(assets_definitions, tags, "all_tags_match")
    assert result == [asset1]

def test_any_tag_matches(assets_definitions, asset1):
    tags = {'tag1': 'value1'}
    result = filter_assets_by_tags(assets_definitions, tags, "any_tag_matches")
    assert result == [asset1]

def test_exclude_if_all_tags(assets_definitions, asset2, asset3):
    tags = {'tag1': 'value1', 'tag2': 'value2'}
    result = filter_assets_by_tags(assets_definitions, tags, "exclude_if_all_tags")
    assert result == [asset2, asset3]

def test_exclude_if_any_tag(assets_definitions, asset3):
    tags = {'tag2': 'value2'}
    result = filter_assets_by_tags(assets_definitions, tags, "exclude_if_any_tag")
    assert result == [asset3]

print("All tests passed!")