from datetime import datetime
from pathlib import Path

import dagster as dg
from dagster_sling import (
    sling_assets,
)
from dagster_shared_gf.shared_dagster_api import reload_code_location
from dagster_shared_gf.automation import automation_daily_delta_2_cron
from dagster_shared_gf.resources.postgresql_resources import db_nocodb_data_gf
from dagster_shared_gf.shared_functions import (
    get_for_current_env,
    calculate_file_checksum,
    get_current_location_name,
    start_job_by_name,
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
from dagster_shared_gf.config import get_dagster_config
import os
import yaml
from dagster_kielsa_gf.sling_defs.sling_nocodb_schema_control import (
    create_timestamp_triggers,
)


logger = dg.get_dagster_logger("sling_nocodb_data_gf")

cfg = get_dagster_config()
if not cfg.sling_home_dir or not cfg.dagster_dwh_farinter_ip:
    # Si no están configuradas, la instancia ya cargó el .env en get_dagster_config()
    raise ValueError(
        "Faltan variables de entorno necesarias (sling_home_dir, dagster_dwh_farinter_ip) para la configuración de SLING."
    )


PARENT_PATH = Path(__file__).parent
REPLICATION_CONFIG_NAME = ".sling_nocodb_data_gf.yaml"

REPLICATION_CONFIG_PATH = PARENT_PATH / REPLICATION_CONFIG_NAME
SOURCE_SCHEMAS = ["kielsa", "grupo_farinter", "kielsa_incentivo"]
TARGET_SCHEMA = "nocodb_data_gf"


def get_replication_config_dict(path: Path) -> dict:
    if not os.path.exists(path):
        return {
            "streams": {
                "placeholder": {
                    "disabled": False,
                    "primary_key": ["id"],
                    "columns": [{"id": "INTEGER"}],
                }
            }
        }

    with open(path, "r") as file:
        replication_config_dict = yaml.safe_load(file)

    if isinstance(replication_config_dict, dict):
        return replication_config_dict

    raise ValueError(
        f"Expected replication config to be a dictionary, got {type(replication_config_dict).__name__} in {path}"
    )


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
            REPLICATION_CONFIG_NAME, directory=PARENT_PATH, seconds_threshold=60
        ):
            replication_config_generated = generate_sling_yaml_from_source(
                engine=db_nocodb_data_gf.get_engine(),
                source_schemas=SOURCE_SCHEMAS,
                output_filename=REPLICATION_CONFIG_NAME,
                output_dir=PARENT_PATH,
                source=source,
                target=target,
                defaults=defaults,
                target_schema=TARGET_SCHEMA,
            )
    except Exception as e:
        logger.error(f"Error generating replication config: {e}")

    if replication_config_generated:
        yaml_path = Path(replication_config_generated)

    return yaml_path


@sling_assets(
    replication_config=get_replication_config_dict(REPLICATION_CONFIG_PATH),
    dagster_sling_translator=MyDagsterSlingTranslator(
        asset_database="DL_FARINTER",
        schema_name=TARGET_SCHEMA,
        tags=tags_repo.AutomationDaily,
        automation_condition=automation_daily_delta_2_cron,
        group_name=TARGET_SCHEMA,
    ),
)
def nocodb_data_gf(
    context: dg.OpExecutionContext,
    sling: MySlingResource,
):
    # context.log.info(f"{len(replication_config.keys())=}")
    # Esperar un tiempo promedio (60) en el que las personas terminan de llenar un campo.
    # Menos 30 de inicializacion. # Espera descontinuada por pasos de integración.
    replication_config = get_replication_config_dict(REPLICATION_CONFIG_PATH)

    if sling.default_mode == "full-refresh":
        replication_config["defaults"]["mode"] = "full-refresh"

    yield from sling.replicate(
        context=context,
        replication_config=replication_config,
        stream=True,
        dagster_sling_translator=MyDagsterSlingTranslator(
            asset_database="DL_FARINTER",
            schema_name=TARGET_SCHEMA,
        ),
    )


# Use define_asset_job for asset-backed jobs

nocodb_data_gf_job = dg.define_asset_job(
    name="nocodb_data_gf_job",
    selection=dg.AssetSelection.groups(TARGET_SCHEMA),  # no sabemos las llaves finales
    tags=tags_repo.Daily | {"by_sensor_job": ""},
)


# Update hook to use assetbacked job
@dg.op(
    tags=tags_repo.Disparado,
    ins={"Nothing": dg.In(dg.Nothing)},
    out={"Nothing": dg.Out(dg.Nothing)},
)
def lanzar_nocodb_data_gf(context: dg.OpExecutionContext):
    # Directly execute the asset job
    current_location_name = get_current_location_name(context)
    run_id = start_job_by_name(
        job_name=nocodb_data_gf_job.name, location_name=current_location_name
    )
    context.log.info(f"nocodb_data_gf_job ejecutado, éxito: {run_id}")


@dg.op(tags=tags_repo.Disparado, out={"Nothing": dg.Out(dg.Nothing)})
def nocodb_data_reload_op(context: dg.OpExecutionContext):
    if os.path.exists(REPLICATION_CONFIG_PATH):
        hash_actual = calculate_file_checksum(REPLICATION_CONFIG_PATH)
    else:
        hash_actual = ""
    context.log.info("Verificando triggers de esquema nocodb")
    for schema in SOURCE_SCHEMAS:
        create_timestamp_triggers(
            context=context,
            db_nocodb_data_gf=db_nocodb_data_gf,
            schema_name=schema,
            # create_timestamp_if_not_exists=True,
        )
    yaml_path = generate_nocodb_data_gf_sling_yaml()

    if calculate_file_checksum(yaml_path) != hash_actual:
        context.log.info(
            "La configuración de replicación cambió, recargando ubicación de código"
        )
        current_location_name = get_current_location_name(context)
        if current_location_name:
            context.log.info(
                f"Recargando ubicación de código '{current_location_name}' debido a cambios en la configuración de replicación"
            )
            reload_code_location(
                host="localhost",
                port=cfg.graphql_port,
                location_name=current_location_name,
            )
        else:
            context.log.error(
                "No se encontró el nombre de la ubicación actual, no se puede recargar"
            )

    else:
        context.log.info(
            "La configuración de replicación está actualizada, no es necesario recargar"
        )


@dg.graph(
    name="nocodb_data_gf_reload_graph",
    tags=tags_repo.Daily | {"by_sensor_job": ""},
)
def nocodb_data_gf_reload_graph():
    recargado = nocodb_data_reload_op()
    lanzar_nocodb_data_gf(recargado)


@dg.job(
    name="nocodb_data_gf_reload_job",
    tags=tags_repo.Daily | {"by_sensor_job": ""},
    resource_defs={"sling": MySlingResource()},
)
def nocodb_data_gf_reload_job():
    nocodb_data_gf_reload_graph()


@dg.sensor(
    name="nocodb_data_gf_change_sensor",
    minimum_interval_seconds=get_for_current_env(
        {"dev": 60 * 60 * 8, "prd": 60 * 2}
    ),  # Check every 2 minutes
    target=nocodb_data_gf_reload_job,
    default_status=running_default_sensor_status,
)
def nocodb_data_gf_change_sensor(context: dg.SensorEvaluationContext):
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
            return dg.SkipReason("No tables found or error querying database")

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
            run_request = dg.RunRequest(
                run_key=f"nocodb_data_gf_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                job_name=nocodb_data_gf_reload_job.name,
                tags={
                    "source": "nocodb_change_sensor",
                    "detected_change": f"{state_string}",
                },
            )

            return dg.SensorResult(
                run_requests=[run_request],
                cursor=f"{state_string}|{current_time}",
            )

        last_run_time = (
            last_run_timestamp.split("|")[1] if "|" in last_run_timestamp else "unknown"
        )
        return dg.SkipReason(f"No new changes detected since {last_run_time}")

    except Exception as e:
        context.log.error(f"Error in nocodb_data_gf_change_sensor: {str(e)}")
        return dg.SkipReason(f"Error checking for changes: {str(e)}")


if __name__ == "__main__":
    from dagster import instance_for_test, materialize, build_sensor_context
    import sys

    # Mock para reload_code_location en modo test
    if "pytest" in sys.modules or "unittest" in sys.modules or __debug__:

        def _mock_reload_code_location(*args, **kwargs):
            print("[MOCK] reload_code_location called with:", args, kwargs)

        # Sobrescribe en el módulo actual
        globals()["reload_code_location"] = _mock_reload_code_location

    # Determine what to test based on command line argument
    test_mode = "asset"  # Default to testing the asset
    if len(sys.argv) > 1:
        if sys.argv[1] == "sensor":
            test_mode = "sensor"
        elif sys.argv[1] == "job":
            test_mode = "job"
        elif sys.argv[1] == "yaml":
            test_mode = "yaml"

    if test_mode == "yaml":
        print("Verifying all SOURCE_SCHEMAS are present in the generated YAML...")
        yaml_path = generate_nocodb_data_gf_sling_yaml()
        try:
            with open(yaml_path, "r") as f:
                yaml_data = yaml.safe_load(f)
            # Collect all schemas present in the YAML (assuming structure: streams -> schema.table)
            found_schemas = set()
            streams = (
                yaml_data.get("streams", {}) if isinstance(yaml_data, dict) else {}
            )
            for stream_key in streams.keys():
                if "." in stream_key:
                    schema = stream_key.split(".")[0]
                    found_schemas.add(schema)
            missing = [s for s in SOURCE_SCHEMAS if s not in found_schemas]
            if not missing:
                print(f"All schemas present in YAML: {SOURCE_SCHEMAS}")
            else:
                print(f"Missing schemas in YAML: {missing}")
                print(f"Schemas found: {sorted(found_schemas)}")
        except Exception as e:
            print(f"Error reading or parsing YAML: {e}")
    else:
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
            elif test_mode == "job":
                print("Testing nocodb_data_gf_reload_job execution...")

                result = nocodb_data_gf_reload_job.execute_in_process(
                    instance=instance,
                    # resources={"sling": MySlingResource(default_mode="full-refresh")},
                )
                print(result.success)
                print(result.events_for_node("nocodb_data_reload_op"))
            else:
                print("Testing sensor evaluation...")
                # Create a sensor context
                context = build_sensor_context(instance=instance)

                # Evaluate the sensor
                sensor_result = nocodb_data_gf_change_sensor(context)

                if isinstance(sensor_result, dg.SensorResult):
                    run_requests = sensor_result.run_requests or []
                    print(f"Sensor triggered with {len(run_requests)} run requests")
                    for request in run_requests:
                        print(f"  Run key: {request.run_key}")
                        print(f"  Tags: {request.tags}")
                    print(f"New cursor: {sensor_result.cursor}")
                elif isinstance(sensor_result, dg.SkipReason):
                    print(f"Sensor skipped: {str(sensor_result)}")

                # Re-evaluate the sensor
                sensor_result = nocodb_data_gf_change_sensor(context)

                if isinstance(sensor_result, dg.SensorResult):
                    run_requests = sensor_result.run_requests or []
                    print(f"Sensor triggered with {len(run_requests)} run requests")
                    for request in run_requests:
                        print(f"  Run key: {request.run_key}")
                        print(f"  Tags: {request.tags}")
                    print(f"New cursor: {sensor_result.cursor}")
                elif isinstance(sensor_result, dg.SkipReason):
                    print(f"Sensor skipped: {str(sensor_result)}")
