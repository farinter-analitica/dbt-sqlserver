from dagster import define_asset_job, ScheduleDefinition

from dagster_shared_gf.shared_functions import get_all_instances_of_class, get_variables_created_by_function

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

print("All tests passed!")