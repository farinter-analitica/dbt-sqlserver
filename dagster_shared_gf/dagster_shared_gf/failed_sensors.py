from http.client import EXPECTATION_FAILED
import time
from dagster import AssetExecutionContext, job, sensor, AssetKey, DefaultSensorStatus, EventLogRecord, SensorEvaluationContext, AssetSelection, asset, Definitions, EventRecordsFilter, DagsterEventType
from dagster_shared_gf.resources.correo_e import EmailSenderResource
from dagster_shared_gf.shared_functions import get_all_instances_of_class, get_for_current_env
from collections import defaultdict
from datetime import datetime

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
DEFAULT_EMAIL = "brian.padilla@farinter.com"

def get_asset_owners(asset_key: AssetKey, context: SensorEvaluationContext):
    """
    Fetch asset owners from context. This simulates looking up asset metadata.
    Uses context to fetch dynamic data for owners.
    """
    asset_metadata = context.repository_def.assets_defs_by_key.get(asset_key).owners_by_key.get(asset_key)
    return asset_metadata if asset_metadata else [DEFAULT_EMAIL]

def get_downstream_lineage_with_owners(asset_key: AssetKey, context: SensorEvaluationContext):
    """
    Retrieve downstream lineage and their owners from the asset graph.
    Returns a dictionary with downstream assets as keys and their respective owners as values.
    """
    selection: AssetSelection = AssetSelection.keys(asset_key).downstream()
    downstream_assets = selection.resolve(context.repository_def.asset_graph)
    downstream_owners = defaultdict(list)
    
    for downstream_asset in downstream_assets:
        downstream_owners[str(downstream_asset)] = get_asset_owners(downstream_asset, context)

    return downstream_owners

def create_email_body(asset_key: AssetKey, downstream_owners: dict):
    """
    Create an email body in Spanish that explains the failed asset and the impact on downstream assets.
    """
    downstream_message = ""
    for downstream_asset, owners in downstream_owners.items():
        downstream_message += f"\n- {downstream_asset}: {' ,'.join(owners)}"
    
    email_body = (
        f"Se ha producido un fallo en la materialización del activo: {asset_key}.\n"
        "Debido a este fallo, los siguientes activos descendentes no se ejecutarán:\n"
        f"{downstream_message}\n"
        "Por favor, revise el error y tome las medidas necesarias."
    )
    return email_body

@sensor(
    default_status=only_prd_default_schedule_status,
    minimum_interval_seconds=get_for_current_env({"local": 1, "dev": 60, "prd": 60}),  # Adjust based on your frequency needs
)
def failed_asset_notification_sensor(context: SensorEvaluationContext, enviador_correo_e_analitica_farinter: EmailSenderResource):
    # Get the failed events since the last cursor (or from the beginning)
    current_cursor: int = int(context.cursor) if context.cursor else None
    context.log.info(f"Cursor: {context.cursor}")
    events: list[EventLogRecord] = context.instance.get_event_records(event_records_filter=EventRecordsFilter(event_type=DagsterEventType.STEP_FAILURE, after_cursor=current_cursor),limit=1000)
    context.log.info(f"Events: {events}")

    if not events:
        return

    for event in events:
        # Check for failed asset materializations
        if event.asset_key:
            asset_key = event.asset_key

            # Fetch the owners of the failed asset
            asset_owners = get_asset_owners(asset_key, context)

            # Get downstream assets and their respective owners
            downstream_owners = get_downstream_lineage_with_owners(asset_key, context)

            # Create the email subject and body
            email_subject = f"Fallo en la materialización del activo: {asset_key}"
            email_body = create_email_body(asset_key, downstream_owners)

            # Collect all unique owners from the failed asset and downstream assets
            all_owners = set(asset_owners)  # Use a set to avoid duplicate emails
            for owners in downstream_owners.values():
                all_owners.update(owners)

            if all_owners:
                # Send a single email to all owners in Spanish
                enviador_correo_e_analitica_farinter.send_email(
                    email_to=", ".join(all_owners),  # All owners in the 'To' list
                    email_subject=email_subject,
                    email_body=email_body
                )

            # Log the notification
            context.log.info(f"Notificación enviada a los siguientes correos: {', '.join(all_owners)}")

            # Update the sensor's cursor to the latest event
            context.update_cursor(str(event.storage_id))

if __name__ == "__main__":
    # Mocked test environment for running the sensor
    from unittest.mock import Mock, MagicMock
    from dagster import build_sensor_context, DagsterInstance, Field, define_asset_job, AssetExecutionContext, Failure, ExpectationResult, asset_check, AssetCheckExecutionContext, AssetCheckResult, AssetCheckSeverity

    # Mocked instance for testing
    instance: DagsterInstance = DagsterInstance.ephemeral(settings={ "loggers" : {"console": {"config": {"log_level": "DEBUG"}}}})

    # Example assets: Mock asset owners for testing
    @asset(owners=["owner1@example.com", "owner2@example.com"]
           ,config_schema={
               "fail": Field(bool, is_required=False, default_value=False),
           }
           )
    def can_fail_asset(context: AssetExecutionContext):
        """Asset 1 represents the root asset with two owners."""
        if context.op_config.get("fail"):
            context.log.error("Asset processing failed, but no exception is raised.")
            context.add_output_metadata({"status": "failed"})
            # raise Failure("Asset processing failed.")

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
            context.log.info(f"Asset {asset_key} reported success status in metadata.")
            return AssetCheckResult(success=True)


    @asset(owners=["owner3@example.com"],deps=[can_fail_asset])
    def can_fail_asset_child_1():
        """Asset 2 depends on asset 1 and has one owner."""
        return None

    @asset(deps=[can_fail_asset_child_1])
    def can_fail_asset_child_3():
        """Asset 3 depends on asset 2 and has no owner, defaults to fallback email."""
        return None
    
    can_fail_job = define_asset_job(name="can_fail_job", selection=AssetSelection.assets("can_fail_asset").downstream())

    #Resource context
    context = build_sensor_context(instance=instance
                                   )

    #Mocked resource object to simulate email sending
    send_email_mock = MagicMock()    
    class EmailResource():
        @staticmethod
        def send_email( email_to, email_subject, email_body, context: SensorEvaluationContext = context):
            context.log.info(f"Sent email to: {email_to}")
            context.log.info(f"Subject: {email_subject}")
            context.log.info(f"Body:\n{email_body}")
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
    context.log.info(f"Iniciando prueba de notificación de fallos")
    # Replace original function to use context


    # Mock instance to return fake event logs
    def return_fake_event_logs(*args, context: SensorEvaluationContext = context, **kwargs):
        event_records_filter: EventRecordsFilter = kwargs.get("event_records_filter", None)
        failed_materialization_event = MagicMock(spec=EventLogRecord)
        failed_materialization_event.asset_key = AssetKey("can_fail_asset")
        failed_materialization_event.storage_id = 12345
        context.log.info(f"After cursor: {event_records_filter.after_cursor}")
        if not event_records_filter.after_cursor \
        or int(event_records_filter.after_cursor) != 12345:
            return [failed_materialization_event]
        else:
            return []

    get_event_records_backup = context.instance.get_event_records
    def functional_test_events():
        send_email_mock.reset_mock()
        context.instance.get_event_records = MagicMock(side_effect=get_event_records_backup, spec=get_event_records_backup)

        context.repository_def.get_job("can_fail_job").execute_in_process(instance=instance)
        failed_asset_notification_sensor(context, enviador_correo_e_analitica_farinter=EmailResource())

        assert context.instance.get_event_records.call_count == 1, "Expected 1 calls accumulated to get_event_records"
        assert send_email_mock.call_count == 0 , f"send_email_mock.call_count {send_email_mock.call_count} Expected 0 calls to send_email"

        try:
            context.repository_def.get_job("can_fail_job").execute_in_process(instance=instance, run_config={"ops": {"can_fail_asset": {"config": {"fail": True}}}})
        except Exception as e:
            pass

        failed_asset_notification_sensor(context, enviador_correo_e_analitica_farinter=EmailResource())
        assert context.instance.get_event_records.call_count == 2, "Expected 1 calls accumulated to get_event_records"
        assert send_email_mock.call_count == 1 , f"send_email_mock.call_count {send_email_mock.call_count} Expected 1 calls to send_email"

    def unit_test_send_email():
        send_email_mock.reset_mock()
        context.instance.get_event_records = MagicMock(side_effect=return_fake_event_logs, spec=context.instance.get_event_records)

        # Run the sensor for testing
        failed_asset_notification_sensor(context, enviador_correo_e_analitica_farinter=EmailResource())
        assert context.instance.get_event_records.call_count == 1 , "Expected 1 call to get_event_records"
        assert send_email_mock.call_count == 1, "Expected 1 call to send_email" 

        context.update_cursor("12345")
        # Run the sensor for testing same event
        failed_asset_notification_sensor(context, enviador_correo_e_analitica_farinter=EmailResource())
        assert context.instance.get_event_records.call_count == 2, "Expected 2 calls accumulated to get_event_records"
        assert send_email_mock.call_count == 1 , f"send_email_mock.call_count {send_email_mock.call_count} Expected no more calls to send_email"
     
    
    functional_test_events()
    unit_test_send_email()

    del instance