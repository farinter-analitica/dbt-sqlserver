from abc import ABCMeta
from typing import Any
from dagster import define_asset_job, ScheduleDefinition, JobDefinition, AssetsDefinition
import pytest

from dagster_shared_gf.shared_functions import *
from dagster_shared_gf.shared_variables import UnresolvedAssetJobDefinition

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
    variables = get_all_instances_of_class(class_type_list=[UnresolvedAssetJobDefinition])
    assert len(variables) == 1, f"Expected 1 variables, got {len(variables)}"
    assert test_job in variables, "Expected test_job to be in variables"



class MockAssetsDefinition:
    def __init__(self, keys, tags_by_key):
        self.keys = keys
        self.tags_by_key = tags_by_key

@pytest.fixture
def assets_definitions():
    return [
        MockAssetsDefinition(keys=["key1"], tags_by_key={"key1": {"tag1": "value1", "tag2": "value2"}}),
        MockAssetsDefinition(keys=["key2"], tags_by_key={"key2": {"tag1": "value1"}}),
        MockAssetsDefinition(keys=["key3"], tags_by_key={"key3": {"tag3": "value3"}}),
        MockAssetsDefinition(keys=["key4"], tags_by_key={"key4": {"tag2": "value2"}}),
    ]

@pytest.fixture
def patch_isinstance(mocker):
    mocker.patch('dagster_shared_gf.shared_functions.isinstance', side_effect=lambda obj, cls: isinstance(obj, MockAssetsDefinition) if cls.__name__ == 'AssetsDefinition' else isinstance(obj, cls))



def test_all_tags_match(patch_isinstance, assets_definitions):
    tags_to_match = {"tag1": "value1"}
    filtered = filter_assets_by_tags(assets_definitions, tags_to_match, "all_tags_match")
    assert len(filtered) == 2

def test_any_tag_matches(patch_isinstance, assets_definitions):
    tags_to_match = {"tag1": "value1"}
    filtered = filter_assets_by_tags(assets_definitions, tags_to_match, "any_tag_matches")
    assert len(filtered) == 2

def test_exclude_if_all_tags(patch_isinstance, assets_definitions):
    tags_to_match = {"tag1": "value1"}
    filtered = filter_assets_by_tags(assets_definitions, tags_to_match, "exclude_if_all_tags")
    assert len(filtered) == 2

def test_exclude_if_any_tag(patch_isinstance, assets_definitions):
    tags_to_match = {"tag1": "value1"}
    filtered = filter_assets_by_tags(assets_definitions, tags_to_match, "exclude_if_any_tag")
    assert len(filtered) == 2

def test_no_matching_tags(patch_isinstance, assets_definitions):
    tags_to_match = {"tag4": "value4"}
    filtered = filter_assets_by_tags(assets_definitions, tags_to_match, "all_tags_match")
    assert len(filtered) == 0

def test_empty_assets_list(patch_isinstance):
    tags_to_match = {"tag1": "value1"}
    filtered = filter_assets_by_tags([], tags_to_match, "all_tags_match")
    assert len(filtered) == 0

print("All tests passed!")