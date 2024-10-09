from ast import With
from http.client import EXPECTATION_FAILED
from platform import node
import time
from dagster import (
    AssetChecksDefinition,
    AssetExecutionContext,
    JobDefinition,
    job,
    sensor,
    AssetKey,
    DefaultSensorStatus,
    EventLogRecord,
    SensorEvaluationContext,
    AssetSelection,
    asset,
    Definitions,
    EventRecordsFilter,
    DagsterEventType,
    SensorResult,
    RunShardedEventsCursor,
)
from dagster_shared_gf.resources.correo_e import EmailSenderResource
from dagster_shared_gf.shared_functions import (
    get_all_instances_of_class,
    get_for_current_env,
)
from dagster_shared_gf.shared_variables import env_str
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from more_itertools import side_effect

running_default_schedule_status: DefaultSensorStatus = get_for_current_env(
    {
        "local": DefaultSensorStatus.STOPPED,
        "dev": DefaultSensorStatus.RUNNING,
        "prd": DefaultSensorStatus.RUNNING,
    }
)
stopped_default_schedule_status: DefaultSensorStatus = get_for_current_env(
    {
        "local": DefaultSensorStatus.STOPPED,
        "dev": DefaultSensorStatus.STOPPED,
        "prd": DefaultSensorStatus.STOPPED,
    }
)
only_prd_default_schedule_status: DefaultSensorStatus = get_for_current_env(
    {
        "local": DefaultSensorStatus.STOPPED,
        "dev": DefaultSensorStatus.STOPPED,
        "prd": DefaultSensorStatus.RUNNING,
    }
)
only_dev_default_schedule_status: DefaultSensorStatus = get_for_current_env(
    {
        "local": DefaultSensorStatus.STOPPED,
        "dev": DefaultSensorStatus.RUNNING,
        "prd": DefaultSensorStatus.STOPPED,
    }
)

# Default email if asset owner is not provided
DEFAULT_EMAILS = get_for_current_env({"local": ["brian.padilla@farinter.com"], "dev": ["brian.padilla@farinter.com","edwin.martinez@farinter.com", "david.saravia@grupobrasilsv.com"], "prd": ["brian.padilla@farinter.com","edwin.martinez@farinter.com", "david.saravia@grupobrasilsv.com"]})
def get_asset_owners(asset_key: AssetKey, context: SensorEvaluationContext):
    """
    Fetch asset owners from context. This simulates looking up asset metadata.
    Uses context to fetch dynamic data for owners.
    """
    asset_metadata = context.repository_def.assets_defs_by_key.get(asset_key).owners_by_key.get(asset_key)
    return asset_metadata if asset_metadata else DEFAULT_EMAILS

def get_downstream_lineage_with_owners(asset_key: AssetKey, job: JobDefinition, context: SensorEvaluationContext):
    """
    Retrieve downstream lineage and their owners from the asset graph.
    Returns a dictionary with downstream assets as keys and their respective owners as values.
    """
    
    selection: AssetSelection = AssetSelection.keys(asset_key).downstream() 
    downstream_assets = selection.resolve(context.repository_def.asset_graph) 
    downstream_owners = defaultdict(list)
    
    for downstream_asset in downstream_assets:
        downstream_owners[downstream_asset] = get_asset_owners(downstream_asset, context)

    return downstream_owners

def create_email_body(asset_key: AssetKey, downstream_owners: dict[AssetKey, list[str]]):
    
    """
    Create the email body to be sent when an asset fails.

    Args:
        asset_key (AssetKey): The key of the asset that failed.
        downstream_owners (dict[AssetKey, list[str]]): A dictionary with downstream assets as keys and their respective owners as values.

    Returns:
        str: The email body to be sent.
    """
    downstream_message = ""
    for downstream_asset, owners in downstream_owners.items():
        downstream_message += f"- {downstream_asset.to_user_string()}: {f' ,'.join(owners)}\n"
    
    email_body = (
        f"Se ha producido un fallo en la materialización del activo: {asset_key}.\n"
        f"Debido a este fallo, los siguientes activos descendentes no se ejecutarán:\n"
        f"{downstream_message}\n"
        f"Por favor, revise el error y tome las medidas necesarias.\n"
    )
    return email_body

@sensor(
    default_status=running_default_schedule_status,
    minimum_interval_seconds=get_for_current_env({"local": 60*5, "dev": 60*5, "prd": 60*5}),  # Adjust based on your frequency needs
)
def failed_asset_notification_sensor(context: SensorEvaluationContext, enviador_correo_e_analitica_farinter: EmailSenderResource):
    # Get the failed events since the last cursor (or from the beginning)
    int_cursor: int = 0
    if context.cursor:
        int_cursor = int(context.cursor)
        event_datetime = context.instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                event_type=DagsterEventType.STEP_FAILURE, storage_ids=[int_cursor]
            ),
            limit=1,
        )[0].timestamp
        event_datetime = datetime.fromtimestamp(event_datetime, tz=timezone.utc) if event_datetime else None
    else:
        event_datetime = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    current_sharded_events_cursor = RunShardedEventsCursor(
        id=int_cursor,
        run_updated_after=event_datetime,
    )

    event_timestamp = event_datetime.timestamp() if event_datetime else None

    context.log.debug(f"Cursor: {context.cursor}")
    events: list[EventLogRecord] = context.instance.get_event_records(
        event_records_filter=EventRecordsFilter(
            event_type=DagsterEventType.STEP_FAILURE,
            after_cursor=current_sharded_events_cursor,
            after_timestamp=event_timestamp,
        ),
        limit=10,
    )
    # context.instance.

    if not events:
        return SensorResult(skip_reason="no new failed events")

    for event in events:
        # Check for failed asset materializations
        job_failed =context.repository_def.get_job(event.event_log_entry.job_name)
        if not event.asset_key:

            node_handle = event.event_log_entry.dagster_event.node_handle
            failed_asset_key_list = job_failed.asset_layer.asset_keys_for_node(node_handle)
            failed_asset_key = failed_asset_key_list[0] if failed_asset_key_list else None
            if not failed_asset_key:
                input_node_handles = job_failed.asset_layer.asset_keys_by_node_input_handle
                failed_asset_key_list = [assetkey for handle, assetkey in input_node_handles.items() if handle.node_handle == node_handle]
                failed_asset_key = failed_asset_key_list[0] if failed_asset_key_list else None
            if not failed_asset_key:
                output_node_handles = job_failed.asset_layer.asset_keys_by_node_output_handle
                failed_asset_key_list = [assetkey for handle, assetkey in output_node_handles.items() if handle.node_handle == node_handle]
                failed_asset_key = failed_asset_key_list[0] if failed_asset_key_list else None
        else:
            failed_asset_key = event.asset_key

        if failed_asset_key:
            asset_key = failed_asset_key

            # Fetch the owners of the failed asset
            asset_owners = get_asset_owners(asset_key, context)

            # Get downstream assets and their respective owners
            downstream_owners = get_downstream_lineage_with_owners(asset_key, job_failed, context)

            # Create the email subject and body
            email_subject = f"[analiticastetl][Error][{env_str}] Activo {asset_key.to_user_string()}, job {job_failed.name}"
            email_body = create_email_body(asset_key, downstream_owners)

            # Collect all unique owners from the failed asset and downstream assets
            all_owners = set(asset_owners)  # Use a set to avoid duplicate emails
            for owners in downstream_owners.values():
                all_owners.update(owners)

            if all_owners:
                # Send a single email to all owners in Spanish
                enviador_correo_e_analitica_farinter.send_email(
                    email_to=all_owners,  # All owners in the 'To' list
                    email_subject=email_subject,
                    email_body=email_body
                )
                # Log the notification
                context.log.info(f"Notificación enviada a los siguientes correos: {', '.join(all_owners)} con el evento id {event.storage_id}")

        return SensorResult(
            cursor=str(event.storage_id),
        )

if __name__ == "__main__":
    # Mocked test environment for running the sensor
    from unittest.mock import Mock, MagicMock
    from dagster import build_sensor_context, DagsterInstance, Field, define_asset_job, AssetExecutionContext, asset_check, AssetCheckExecutionContext, AssetCheckResult, instance_for_test

    # Mocked instance for testing
    with instance_for_test() as instance:

        # Example assets: Mock asset owners for testing
        @asset(owners=["owner1@example.com", "owner2@example.com"]
            ,config_schema={
                "fail": Field(bool, is_required=False, default_value=False),
            },
            key=AssetKey(["prefix","can_fail_asset"]),
            )
        def can_fail_asset(context: AssetExecutionContext):
            """Asset 1 represents the root asset with two owners."""
            if context.op_config.get("fail"):
                context.log.error("Asset processing failed, but no exception is raised.")
                context.add_output_metadata({"status": "failed"})
                # raise Failure("Asset processing failed.")
            int = 1 +2 +time.time_ns()
            return None
        
        @asset_check(asset=can_fail_asset, blocking=True)
        def can_fail_asset_check(context: AssetCheckExecutionContext):
            """Fail check if the asset reports a failure status in its metadata."""
            asset_key = context.check_specs[0].asset_key

            # Retrieve the latest materialization event for the asset
            materialization_records = context.instance.get_latest_materialization_events(
                [asset_key]
            )

            if not materialization_records:
                # No materializations found; fail the check
                context.log.error(f"No materialization found for asset: {asset_key}")
                return AssetCheckResult(
                    passed=True,
                    metadata={"reason": "No materialization found for the asset."},
                )

            # Get the latest materialization event
            latest_materialization = materialization_records.get(asset_key)
            materialization_event = latest_materialization.dagster_event
            materialization_metadata = (
                materialization_event.event_specific_data.materialization.metadata
            )

            # Access the 'status' from the metadata
            status_entry = materialization_metadata.get("status")
            if status_entry is None:
                # Status not found in metadata; consider it a failure
                context.log.error(f"No status found in metadata for asset: {asset_key}")
                return AssetCheckResult(
                    passed=True,
                    metadata={"reason": "No status found in asset metadata."},
                )

            # Check if the status indicates failure
            status = status_entry.value  # Extract the actual value from MetadataEntry
            if status == "failed":
                context.log.error(f"Asset {asset_key} reported failure status in metadata.")
                return AssetCheckResult(
                    passed=False,
                    metadata={"reason": "Asset reported failure status in metadata."},
                )
            else:
                # Asset is successful
                context.log.debug(f"Asset {asset_key} reported success status in metadata.")
                return AssetCheckResult(success=True)


        @asset(owners=["owner3@example.com"],deps=[can_fail_asset])
        def can_fail_asset_child_1():
            """Asset 2 depends on asset 1 and has one owner."""
            return None

        @asset(deps=[can_fail_asset_child_1])
        def can_fail_asset_child_3():
            """Asset 3 depends on asset 2 and has no owner, defaults to fallback email."""
            return None
        
        can_fail_job = define_asset_job(name="can_fail_job", selection=AssetSelection.assets(can_fail_asset).downstream())

        #Resource context
        context = build_sensor_context(instance=instance
                                    )

        #Mocked resource object to simulate email sending
        send_email_mock = MagicMock()    
        class EmailResource():
            @staticmethod
            def send_email( email_to, email_subject, email_body, context: SensorEvaluationContext = context):
                context.log.debug(f"Sent email to: {email_to}")
                context.log.debug(f"Subject: {email_subject}")
                context.log.debug(f"Body:\n{email_body}")
                send_email_mock(email_to, email_subject, email_body)        

        # Load assets into the definitions object
        defs = Definitions(
            assets=[can_fail_asset, can_fail_asset_child_1, can_fail_asset_child_3],
            asset_checks=[can_fail_asset_check],
            jobs=[can_fail_job],
            sensors=[failed_asset_notification_sensor],
            resources={"enviador_correo_e_analitica_farinter": EmailResource()},
        )

        # Mock context object
        context = build_sensor_context(instance=instance, 
                                    definitions=defs,

                                    )
        context.log.debug(f"Iniciando prueba de notificación de fallos")
        # Replace original function to use context


        # Mock instance to return fake event logs
        def return_fake_event_logs(*args, context: SensorEvaluationContext = context, **kwargs):
            event_records_filter: EventRecordsFilter = kwargs.get("event_records_filter", None)
            failed_materialization_event = MagicMock(spec=EventLogRecord)
            failed_materialization_event.asset_key = can_fail_asset.key
            failed_materialization_event.storage_id = 12345
            context.log.debug(f"After cursor: {event_records_filter.after_cursor}")
            if not event_records_filter.after_cursor \
            or int(event_records_filter.after_cursor.id) != 12345:
                return [failed_materialization_event]
            else:
                return []

        get_event_records_backup = context.instance.get_event_records
        def functional_test_events():
            send_email_mock.reset_mock()
            context.instance.get_event_records = MagicMock(side_effect=get_event_records_backup, spec=get_event_records_backup)

            context.repository_def.get_job("can_fail_job").execute_in_process(instance=instance)
            context.update_cursor(failed_asset_notification_sensor(context, enviador_correo_e_analitica_farinter=EmailResource()).cursor)

            assert context.instance.get_event_records.call_count == 1, f"Expected 1 calls accumulated to get_event_records but got {context.instance.get_event_records.call_count}"
            assert send_email_mock.call_count == 0 , f"send_email_mock.call_count {send_email_mock.call_count} Expected 0 calls to send_email"

            try:
                context.repository_def.get_job("can_fail_job").execute_in_process(instance=instance, run_config={"ops": {can_fail_asset.get_asset_spec().key.to_python_identifier(): {"config": {"fail": True}}}})
            except Exception as e:
                del e
                pass

            context.update_cursor(failed_asset_notification_sensor(context, enviador_correo_e_analitica_farinter=EmailResource()).cursor)
            assert context.instance.get_event_records.call_count == 2, f"Expected 2 calls accumulated to get_event_records but got {context.instance.get_event_records.call_count}"
            assert send_email_mock.call_count == 1 , f"send_email_mock.call_count {send_email_mock.call_count} Expected 1 calls to send_email"
            assert context.cursor is not None, "Expected cursor to be set"
            #no enviar mismo error
            context.update_cursor(failed_asset_notification_sensor(context, enviador_correo_e_analitica_farinter=EmailResource()).cursor)
            assert context.instance.get_event_records.call_count == 4, "Expected 4 calls accumulated to get_event_records"
            assert send_email_mock.call_count == 1 , f"send_email_mock.call_count {send_email_mock.call_count} Expected 1 calls to send_email"


        def unit_test_send_email():
            send_email_mock.reset_mock()
            context.instance.get_event_records = MagicMock(side_effect=return_fake_event_logs, spec=get_event_records_backup)

            # Run the sensor for testing
            context.update_cursor(failed_asset_notification_sensor(context, enviador_correo_e_analitica_farinter=EmailResource()).cursor)
            assert context.instance.get_event_records.call_count == 1 , f"Expected 1 call to get_event_records but got {context.instance.get_event_records.call_count}"
            assert send_email_mock.call_count == 1, f"Expected 1 call to send_email but got {send_email_mock.call_count}" 

            #context.update_cursor("12345")
            # Run the sensor for testing same event
            context.update_cursor(failed_asset_notification_sensor(context, enviador_correo_e_analitica_farinter=EmailResource()).cursor)
            assert context.instance.get_event_records.call_count == 3, f"Expected 3 calls accumulated to get_event_records but got {context.instance.get_event_records.call_count}"
            assert send_email_mock.call_count == 1 , f"send_email_mock.call_count {send_email_mock.call_count} Expected no more calls to send_email"
        
        
        functional_test_events()
        unit_test_send_email()
