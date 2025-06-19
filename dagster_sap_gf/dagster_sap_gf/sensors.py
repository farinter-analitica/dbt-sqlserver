from dagster import SensorDefinition
from dagster_shared_gf.shared_functions import (
    get_all_instances_of_class,
)
from dagster_shared_gf import shared_variables as shared_vars
from dagster_shared_gf import shared_failed_sensors
# from dagster_shared_gf import shared_constants

# cron: minute hour day month day_of_week, example daily at midnight: 0 0 * * *
# cron example daily at midnight mon-fri with numbers: 0 0 * * 1-5
# cron template: hour minute day month day_of_week
env_str: str = shared_vars.env_str

default_timezone: str = "America/Tegucigalpa"
# shared sensors
failed_asset_notification_sensor = (
    shared_failed_sensors.failed_asset_notification_sensor
)


all_sensors = get_all_instances_of_class([SensorDefinition])
