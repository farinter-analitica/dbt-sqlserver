from dagster import sensor, RunRequest, DagsterRunStatus, SensorDefinition, AutoMaterializeSensorDefinition
from dagster_kielsa_gf.jobs import *
from dagster_shared_gf.shared_functions import (get_all_instances_of_class)

@sensor(job=dbt_dwh_kielsa_marts_job)
def upstream_completion_sensor(context):
    # Check for the most recent successful run of the upstream job
    last_run = context.instance.get_runs(
        filters={"job_name": "upstream_job", "status": DagsterRunStatus.SUCCESS},
        limit=1,
    )
    if last_run:
        # Trigger the downstream job
        yield RunRequest(run_key=None)



from dagster import AssetSelection, AutoMaterializeSensorDefinition, Definitions,  AutoMaterializePolicy, AutoMaterializeRule

my_custom_auto_materialize_sensor = AutoMaterializeSensorDefinition(
    "my_custom_auto_materialize_sensor",
    asset_selection=AssetSelection.all(include_sources=True),
    minimum_interval_seconds=60 * 15,
)


all_sensors = get_all_instances_of_class([SensorDefinition]) + get_all_instances_of_class([AutoMaterializeSensorDefinition])

__all__ = list(map(lambda x: x.name, all_sensors) )