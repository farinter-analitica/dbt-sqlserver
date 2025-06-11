import dagster_kielsa_gf as current_location
from datetime import datetime
from pathlib import Path

from dagster import (
    AssetSelection,
    RunRequest,
    SensorEvaluationContext,
    AssetExecutionContext,
    SensorResult,
    SkipReason,
    define_asset_job,
    sensor,
    get_dagster_logger,
)
from dagster_sling import (
    sling_assets,
)
from dagster_shared_gf.shared_dagster_api import reload_code_location
from dagster_shared_gf.automation import automation_daily_delta_2_cron
from dagster_shared_gf.resources.postgresql_resources import db_nocodb_data_gf
from dagster_shared_gf.shared_functions import (
    get_for_current_env,
    calculate_file_checksum,
)
from dagster_shared_gf.shared_variables import tags_repo
from dagster_shared_gf.sling_shared.sling_resources import (
    MyDagsterSlingTranslator,
    MySlingResource,
)
from dagster_shared_gf.sling_shared.generate_yaml import (
    generate_sling_yaml_from_source,
    is_file_cache_valid,
)
from dagster_shared_gf.shared_constants import (
    running_default_sensor_status,
)
from dagster_shared_gf.load_env_run import load_env_vars, os
import yaml
from dagster_kielsa_gf.sling_defs.sling_nocodb_schema_control import (
    create_timestamp_triggers,
)


logger = get_dagster_logger("sling_nocodb_data_gf")

if not os.environ.get("SLING_HOME_DIR") or not os.environ.get(
    "DAGSTER_DWH_FARINTER_IP"
):
    load_env_vars()


PARENT_PATH = Path(__file__).parent
REPLICATION_CONFIG_NAME = ".sling_nocodb_data_gf.yaml"

REPLICATION_CONFIG_PATH = PARENT_PATH / REPLICATION_CONFIG_NAME
REPLICATION_CONFIG_DICT = {}
with open(REPLICATION_CONFIG_PATH, "r") as file:
    REPLICATION_CONFIG_DICT = yaml.safe_load(file)


def generate_nocodb_data_gf_sling_yaml():
    source: str = "NOCODB_DATA_GF"
    target: str = "DAGSTER_DWH_FARINTER"
    defaults = {
        "mode": "incremental",
        "object": "nocodb_data_gf.{stream_schema}_{stream_table}",
        "target_options": {"column_casing": "snake", "adjust_column_type": True},
        "source_options": {"flatten": True},
    }
    yaml_path = REPLICATION_CONFIG_PATH

    replication_config_generated: str | None = None
    try:
        if not is_file_cache_valid(
            ".sling_nocodb_data_gf.yaml", directory=PARENT_PATH, seconds_threshold=60
        ):
            replication_config_generated = generate_sling_yaml_from_source(
                engine=db_nocodb_data_gf.get_engine(),
                source_schema="kielsa",
                output_filename=".sling_nocodb_data_gf.yaml",
                output_dir=PARENT_PATH,
                source=source,
                target=target,
                defaults=defaults,
            )
    except Exception as e:
        logger.error(f"Error generating replication config: {e}")

    if replication_config_generated:
        yaml_path = Path(replication_config_generated)

    return yaml_path


@sling_assets(
    replication_config=REPLICATION_CONFIG_DICT,
    dagster_sling_translator=MyDagsterSlingTranslator(
        asset_database="DL_FARINTER",
        schema_name="nocodb_data_gf",
        tags=tags_repo.AutomationDaily,
        automation_condition=automation_daily_delta_2_cron,
        group_name="nocodb_data_gf",
    ),
)
def nocodb_data_gf(context: AssetExecutionContext, sling: MySlingResource):
    # context.log.info(f"{len(replication_config.keys())=}")
    # Esperar un tiempo promedio (60) en el que las personas terminan de llenar un campo.
    # Menos 30 de inicializacion. # Espera descontinuada por pasos de integración.
    global REPLICATION_CONFIG_DICT

    hash_actual = calculate_file_checksum(REPLICATION_CONFIG_PATH)
    yaml_path = generate_nocodb_data_gf_sling_yaml()

    if calculate_file_checksum(yaml_path) != hash_actual:
        with open(yaml_path, "r") as file:
            REPLICATION_CONFIG_DICT = yaml.safe_load(file)

        context.log.info("Replication config changed, reloading code location")
        reload_code_location(
            host="localhost",
            port=int(os.environ.get("DAGSTER_GRAPHQL_PORT", 9300)),
            location_name=current_location.__name__,
        )
        context.log.info("Reloaded code location, checking nocodb schema triggers")
        create_timestamp_triggers(
            context=context.op_execution_context,
            db_nocodb_data_gf=db_nocodb_data_gf,
            schema_name="kielsa",
        )

    if sling.default_mode == "full-refresh":
        REPLICATION_CONFIG_DICT["defaults"]["mode"] = "full-refresh"

    yield from sling.replicate(context=context, stream=True)


# Define a job that will materialize the nocodb_data_gf assets
nocodb_data_gf_job = define_asset_job(
    name="nocodb_data_gf_job",
    selection=AssetSelection.groups("nocodb_data_gf"),
    tags=tags_repo.Daily | {"by_sensor_job": ""},
)


@sensor(
    name="nocodb_data_gf_change_sensor",
    minimum_interval_seconds=get_for_current_env(
        {"dev": 60 * 60 * 8, "prd": 60 * 2}
    ),  # Check every 2 minutes
    target=nocodb_data_gf_job,
    default_status=running_default_sensor_status,
)
def nocodb_data_gf_change_sensor(context: SensorEvaluationContext):
    """
    Sensor that monitors the PostgreSQL database for changes and triggers
    the nocodb_data_gf asset when changes are detected.
    """
    # Get the last run timestamp from context
    last_run_timestamp = context.cursor or None

    # Query to check for recent changes in the database using PostgreSQL 14 compatible functions
    query = """
    SELECT 
        MAX(GREATEST(last_vacuum, last_autovacuum, last_analyze, last_autoanalyze)) as last_maintenance,
        SUM(n_tup_ins + n_tup_upd + n_tup_del + n_live_tup + n_dead_tup) as n_mod_tup
    FROM 
        pg_stat_user_tables;
    """

    try:
        # Execute the query
        result = db_nocodb_data_gf.query(query)

        if not result:
            return SkipReason("No tables found or error querying database")

        last_maintenance_dt = result[0][0]
        if last_maintenance_dt is not None:
            last_maintenance = last_maintenance_dt.strftime("%Y%m%d_%H%M%S")
        else:
            last_maintenance = "unknown"
        n_mod_tup = str(result[0][1])

        # Create a signature of the current state using hash
        state_string = f"{last_maintenance=}_{n_mod_tup=}"
        current_time = datetime.now().isoformat()
        # If this is the first run or there are new changes
        if not last_run_timestamp or state_string != last_run_timestamp.split("|")[0]:
            context.log.info(f"Detected changes {state_string}")

            # Create a run request for the asset
            run_request = RunRequest(
                run_key=f"nocodb_data_gf_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                job_name=nocodb_data_gf_job.name,
                tags={
                    "source": "nocodb_change_sensor",
                    "detected_change": f"{state_string}",
                },
            )

            return SensorResult(
                run_requests=[run_request],
                cursor=f"{state_string}|{current_time}",
            )

        last_run_time = (
            last_run_timestamp.split("|")[1] if "|" in last_run_timestamp else "unknown"
        )
        return SkipReason(f"No new changes detected since {last_run_time}")

    except Exception as e:
        context.log.error(f"Error in nocodb_data_gf_change_sensor: {str(e)}")
        return SkipReason(f"Error checking for changes: {str(e)}")


if __name__ == "__main__":
    from dagster import instance_for_test, materialize, build_sensor_context
    import sys

    # Determine what to test based on command line argument
    test_mode = "asset"  # Default to testing the asset
    if len(sys.argv) > 1 and sys.argv[1] == "sensor":
        test_mode = "sensor"

    with instance_for_test() as instance:
        if test_mode == "asset":
            print("Testing asset materialization...")
            result = materialize(
                assets=[nocodb_data_gf],
                # run_config={
                #     "ops": {
                #         nocodb_data_gf.op.name: {
                #             "config": {"default_mode": "full-refresh"}
                #         }
                #     }
                # },
                instance=instance,
                resources={"sling": MySlingResource(default_mode="full-refresh")},
            )
            print(result.all_events)
        else:
            print("Testing sensor evaluation...")
            # Create a sensor context
            context = build_sensor_context(instance=instance)

            # Evaluate the sensor
            sensor_result = nocodb_data_gf_change_sensor(context)

            if isinstance(sensor_result, SensorResult):
                run_requests = sensor_result.run_requests or []
                print(f"Sensor triggered with {len(run_requests)} run requests")
                for request in run_requests:
                    print(f"  Run key: {request.run_key}")
                    print(f"  Tags: {request.tags}")
                print(f"New cursor: {sensor_result.cursor}")
            elif isinstance(sensor_result, SkipReason):
                print(f"Sensor skipped: {str(sensor_result)}")

            # Re-evaluate the sensor
            sensor_result = nocodb_data_gf_change_sensor(context)

            if isinstance(sensor_result, SensorResult):
                run_requests = sensor_result.run_requests or []
                print(f"Sensor triggered with {len(run_requests)} run requests")
                for request in run_requests:
                    print(f"  Run key: {request.run_key}")
                    print(f"  Tags: {request.tags}")
                print(f"New cursor: {sensor_result.cursor}")
            elif isinstance(sensor_result, SkipReason):
                print(f"Sensor skipped: {str(sensor_result)}")
