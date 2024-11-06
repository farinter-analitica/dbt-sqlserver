from dagster import (
    sensor,
    RunRequest,
    DagsterRunStatus,
    DefaultSensorStatus,
    SensorDefinition,
    build_sensor_for_freshness_checks,
)
import dagster_kielsa_gf.jobs as jobs
from dagster_kielsa_gf.assets import (
    dbt_dwh_kielsa,
    knime_asset_factory,
    ldcom_etl_dwh_sp,
    recetas_libros_etl_dwh,
)
from dagster_shared_gf.shared_functions import (
    get_all_instances_of_class,
    get_for_current_env,
)
from dagster_shared_gf import shared_variables as shared_vars
from dagster_shared_gf import shared_failed_sensors

env_str:str=shared_vars.env_str

default_timezone: str = "America/Tegucigalpa"
running_default_sensor_status: DefaultSensorStatus = get_for_current_env({"local":DefaultSensorStatus.STOPPED
                                                                              ,"dev":DefaultSensorStatus.RUNNING
                                                                              ,"prd":DefaultSensorStatus.RUNNING})
stopped_default_sensor_status: DefaultSensorStatus = get_for_current_env({"local":DefaultSensorStatus.STOPPED
                                                                              ,"dev":DefaultSensorStatus.STOPPED
                                                                              ,"prd":DefaultSensorStatus.STOPPED})
@sensor(job=jobs.dbt_dwh_kielsa_marts_job, default_status=stopped_default_sensor_status)
def upstream_completion_sensor(context):
    # Check for the most recent successful run of the upstream job
    last_run = context.instance.get_runs(
        filters={"job_name": "upstream_job", "status": DagsterRunStatus.SUCCESS},
        limit=1,
    )
    if last_run:
        # Trigger the downstream job
        yield RunRequest(run_key=None)

# shared sensors
failed_asset_notification_sensor = shared_failed_sensors.failed_asset_notification_sensor

all_asset_freshness_checks = (*dbt_dwh_kielsa.all_asset_freshness_checks, *ldcom_etl_dwh_sp.all_asset_freshness_checks, *recetas_libros_etl_dwh.all_asset_freshness_checks , *knime_asset_factory.all_asset_freshness_checks)
freshness_checks_sensor = build_sensor_for_freshness_checks(
    freshness_checks=all_asset_freshness_checks,
    default_status=running_default_sensor_status,
    minimum_interval_seconds=30 * 60,  # 5 minutes
    )

all_sensors = get_all_instances_of_class([SensorDefinition]) 

