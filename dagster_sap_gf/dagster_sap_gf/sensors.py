from datetime import timedelta
from dagster import SensorDefinition, DefaultSensorStatus
from dagster_shared_gf.shared_functions import (
    get_all_instances_of_class,
    get_for_current_env,
)
from dagster_shared_gf import shared_variables as shared_vars
from dagster_shared_gf import shared_failed_sensors

# cron: minute hour day month day_of_week, example daily at midnight: 0 0 * * *
# cron example daily at midnight mon-fri with numbers: 0 0 * * 1-5
# cron template: hour minute day month day_of_week
env_str: str = shared_vars.env_str

default_timezone: str = "America/Tegucigalpa"
running_default_sensor_status: DefaultSensorStatus = get_for_current_env(
    {
        "local": DefaultSensorStatus.STOPPED,
        "dev": DefaultSensorStatus.RUNNING,
        "prd": DefaultSensorStatus.RUNNING,
    }
)
stopped_default_sensor_status: DefaultSensorStatus = get_for_current_env(
    {
        "local": DefaultSensorStatus.STOPPED,
        "dev": DefaultSensorStatus.STOPPED,
        "prd": DefaultSensorStatus.STOPPED,
    }
)
only_dev_running_default_sensor_status: DefaultSensorStatus = get_for_current_env(
    {
        "local": DefaultSensorStatus.STOPPED,
        "dev": DefaultSensorStatus.RUNNING,
        "prd": DefaultSensorStatus.STOPPED,
    }
)
only_prd_running_default_sensor_status: DefaultSensorStatus = get_for_current_env(
    {
        "local": DefaultSensorStatus.STOPPED,
        "dev": DefaultSensorStatus.STOPPED,
        "prd": DefaultSensorStatus.RUNNING,
    }
)
hourly_freshness_seconds_per_environ: int = get_for_current_env(
    {
        "dev": 60 * 60 * 6,  # 6 horas
        "prd": 60 * 60 * 1,
    }
)
hourly_freshness_lbound_per_environ: timedelta = get_for_current_env(
    {
        "dev": timedelta(hours=30),  # 30 horas
        "prd": timedelta(hours=13),
    }
)


# shared sensors
failed_asset_notification_sensor = (
    shared_failed_sensors.failed_asset_notification_sensor
)


all_sensors = get_all_instances_of_class([SensorDefinition])
