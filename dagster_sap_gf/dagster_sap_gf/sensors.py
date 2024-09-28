from dagster import SensorDefinition, DefaultSensorStatus, build_sensor_for_freshness_checks
from dagster_sap_gf.jobs import *
from dagster_shared_gf.shared_functions import (get_all_instances_of_class, get_for_current_env)
from dagster_shared_gf import shared_variables as shared_vars
from dagster_sap_gf.assets import (dbt_dwh_sap
        , sap_etl_dwh 
    )
from dagster_shared_gf import failed_sensors
#cron: minute hour day month day_of_week, example daily at midnight: 0 0 * * *
#cron example daily at midnight mon-fri with numbers: 0 0 * * 1-5
#cron template: hour minute day month day_of_week
env_str:str=shared_vars.env_str

default_timezone: str = "America/Tegucigalpa"
running_default_sensor_status: DefaultSensorStatus = get_for_current_env({"local":DefaultSensorStatus.STOPPED
                                                                              ,"dev":DefaultSensorStatus.RUNNING
                                                                              ,"prd":DefaultSensorStatus.RUNNING})
stopped_default_sensor_status: DefaultSensorStatus = get_for_current_env({"local":DefaultSensorStatus.STOPPED
                                                                              ,"dev":DefaultSensorStatus.STOPPED
                                                                              ,"prd":DefaultSensorStatus.STOPPED})

#shared sensors
failed_asset_notification_sensor = failed_sensors.failed_asset_notification_sensor

all_asset_freshness_checks = sap_etl_dwh.all_asset_freshness_checks + dbt_dwh_sap.all_asset_freshness_checks
freshness_checks_sensor = build_sensor_for_freshness_checks(
    freshness_checks=all_asset_freshness_checks,
    default_status=running_default_sensor_status,
    minimum_interval_seconds=30 * 60,  # 5 minutes
    )

all_sensors = get_all_instances_of_class([SensorDefinition])

__all__ = list(map(lambda x: x.name, all_sensors) )