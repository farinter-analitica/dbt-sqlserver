from typing import Sequence
import dagster as dg
from dagster_shared_gf.resources.correo_e import EmailSenderResource
from dagster_shared_gf.shared_functions import (
    get_for_current_env,
)
from dagster_shared_gf.shared_variables import env_str
from datetime import datetime, timedelta, timezone


running_default_schedule_status: dg.DefaultSensorStatus = get_for_current_env(
    {
        "local": dg.DefaultSensorStatus.STOPPED,
        "dev": dg.DefaultSensorStatus.RUNNING,
        "prd": dg.DefaultSensorStatus.RUNNING,
    }
)
stopped_default_schedule_status: dg.DefaultSensorStatus = get_for_current_env(
    {
        "local": dg.DefaultSensorStatus.STOPPED,
        "dev": dg.DefaultSensorStatus.STOPPED,
        "prd": dg.DefaultSensorStatus.STOPPED,
    }
)
only_prd_default_schedule_status: dg.DefaultSensorStatus = get_for_current_env(
    {
        "local": dg.DefaultSensorStatus.STOPPED,
        "dev": dg.DefaultSensorStatus.STOPPED,
        "prd": dg.DefaultSensorStatus.RUNNING,
    }
)
only_dev_default_schedule_status: dg.DefaultSensorStatus = get_for_current_env(
    {
        "local": dg.DefaultSensorStatus.STOPPED,
        "dev": dg.DefaultSensorStatus.RUNNING,
        "prd": dg.DefaultSensorStatus.STOPPED,
    }
)

# Default email if asset owner is not provided
# DEFAULT_EMAILS = get_for_current_env({"local": ["brian.padilla@farinter.com"], "dev": ["brian.padilla@farinter.com","edwin.martinez@farinter.com", "david.saravia@grupobrasilsv.com"], "prd": ["brian.padilla@farinter.com","edwin.martinez@farinter.com", "david.saravia@grupobrasilsv.com"]})
DEFAULT_EMAILS = ["brian.padilla@farinter.com"]


def get_asset_owners(
    asset_key: dg.AssetKey, context: dg.SensorEvaluationContext
) -> Sequence[str]:
    """
    Fetch asset owners from context. This simulates looking up asset metadata.
    Uses context to fetch dynamic data for owners.
    """
    repo_ref = context.repository_def
    if not repo_ref:
        raise ValueError("Repository definition not available")
    asset_metadata = repo_ref.assets_defs_by_key.get(asset_key)
    if not asset_metadata:
        return []
    owners = asset_metadata.owners_by_key.get(asset_key)
    return owners if owners else []


def get_downstream_lineage_with_owners(
    asset_key: dg.AssetKey, job: dg.JobDefinition, context: dg.SensorEvaluationContext
) -> dict[dg.AssetKey, Sequence[str]]:
    """
    Retrieve downstream lineage and their owners from the asset graph.
    Returns a dictionary with downstream assets as keys and their respective owners as values.
    """

    selection: dg.AssetSelection = dg.AssetSelection.assets(asset_key).downstream()
    repo_ref = context.repository_def
    if not repo_ref:
        raise ValueError("Repository definition not available")
    downstream_assets = selection.resolve(repo_ref.asset_graph)
    downstream_owners: dict[dg.AssetKey, Sequence[str]] = {}

    for downstream_asset in downstream_assets:
        downstream_owners[downstream_asset] = get_asset_owners(
            downstream_asset, context
        )

    return downstream_owners


def create_email_body(
    asset_key: dg.AssetKey, downstream_owners: dict[dg.AssetKey, Sequence[str]]
):
    """
    Create the email body to be sent when an asset fails.

    Args:
        asset_key (dg.AssetKey): The key of the asset that failed.
        downstream_owners (dict[dg.AssetKey, list[str]]): A dictionary with downstream assets as keys and their respective owners as values.

    Returns:
        str: The email body to be sent.
    """
    downstream_message = ""
    for downstream_asset, owners in downstream_owners.items():
        downstream_message += f"- {downstream_asset.to_user_string()}: {', '.join(owners if owners else 'Sin dueño definido.')}\n"

    email_body = (
        f"Se ha producido un fallo en la materialización del activo: {asset_key}.\n"
        f"Debido a este fallo, los siguientes activos descendentes no se ejecutarán:\n"
        f"{downstream_message}\n"
        f"Por favor, revise el error y tome las medidas necesarias.\n"
    )
    return email_body


@dg.sensor(
    default_status=running_default_schedule_status,
    minimum_interval_seconds=get_for_current_env(
        {"local": 60 * 5, "dev": 60 * 5, "prd": 60 * 5}
    ),  # Adjust based on your frequency needs
)
def failed_asset_notification_sensor(
    context: dg.SensorEvaluationContext,
    enviador_correo_e_analitica_farinter: EmailSenderResource,
):
    # Get the failed events since the last cursor (or from the beginning)
    int_cursor: int = 0
    event_datetime = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    if context.cursor:
        int_cursor = int(context.cursor)
        event_datetime_float = context.instance.get_event_records(
            event_records_filter=dg.EventRecordsFilter(
                event_type=dg.DagsterEventType.STEP_FAILURE, storage_ids=[int_cursor]
            ),
            limit=1,
        )[0].timestamp
        event_datetime = (
            datetime.fromtimestamp(event_datetime_float, tz=timezone.utc)
            if event_datetime_float
            else event_datetime
        )

    current_sharded_events_cursor = dg.RunShardedEventsCursor(
        id=int_cursor,
        run_updated_after=event_datetime,
    )

    event_timestamp = event_datetime.timestamp() if event_datetime else None

    context.log.debug(f"Cursor: {context.cursor}")
    events: Sequence[dg.EventLogRecord] = context.instance.get_event_records(
        event_records_filter=dg.EventRecordsFilter(
            event_type=dg.DagsterEventType.STEP_FAILURE,
            after_cursor=current_sharded_events_cursor,
            after_timestamp=event_timestamp,
        ),
        limit=10,
    )
    # context.instance.

    if not events:
        return dg.SensorResult(
            skip_reason="no new failed events", cursor=context.cursor
        )

    repo_ref = context.repository_def
    notified: set[dg.AssetKey] = set()

    if not repo_ref:
        raise ValueError("No repository definition found")
    for event in events:
        log_entry = event.event_log_entry
        job_name = log_entry.job_name
        if not job_name:
            continue
        # Check for failed asset materializations
        job_failed = repo_ref.get_job(job_name)
        if not event.asset_key:
            dagster_event = log_entry.dagster_event
            if not dagster_event:
                continue
            node_handle = dagster_event.node_handle
            if not node_handle:
                continue
            failed_asset_key_list = job_failed.asset_layer.asset_keys_for_node(
                node_handle
            )
            failed_asset_key = (
                list(failed_asset_key_list).pop() if failed_asset_key_list else None
            )
            if not failed_asset_key:
                input_node_handles = (
                    job_failed.asset_layer.asset_keys_by_node_input_handle
                )
                failed_asset_key_list = [
                    dg.AssetKey
                    for handle, dg.AssetKey in input_node_handles.items()
                    if handle.node_handle == node_handle
                ]
                failed_asset_key = (
                    failed_asset_key_list[0] if failed_asset_key_list else None
                )
            if not failed_asset_key:
                output_node_handles = (
                    job_failed.asset_layer.asset_keys_by_node_output_handle
                )
                failed_asset_key_list = [
                    dg.AssetKey
                    for handle, dg.AssetKey in output_node_handles.items()
                    if handle.node_handle == node_handle
                ]
                failed_asset_key = (
                    failed_asset_key_list[0] if failed_asset_key_list else None
                )
        else:
            failed_asset_key = event.asset_key

        if failed_asset_key:
            asset_key = failed_asset_key
            if asset_key in notified:
                continue

            # Fetch the owners of the failed asset
            asset_owners = get_asset_owners(asset_key, context)

            # Get downstream assets and their respective owners
            downstream_owners = get_downstream_lineage_with_owners(
                asset_key, job_failed, context
            )

            all_asset_keys = set(downstream_owners.keys())
            all_asset_keys.add(asset_key)

            # Create the email subject and body
            email_subject = f"[analiticastetl][Error][{env_str}] Activo {asset_key.to_user_string()}, job {job_failed.name}"
            email_body = create_email_body(asset_key, downstream_owners)

            # Collect all unique owners from the failed asset and downstream assets
            all_owners = set(asset_owners)  # Use a set to avoid duplicate emails
            all_owners.update(DEFAULT_EMAILS)
            for owners in downstream_owners.values():
                all_owners.update(owners)

            if all_owners:
                # Send a single email to all owners in Spanish
                enviador_correo_e_analitica_farinter.send_email(
                    email_to=all_owners,  # All owners in the 'To' list
                    email_subject=email_subject,
                    email_body=email_body,
                )
                notified.update(all_asset_keys)
                # Log the notification
                context.log.info(
                    f"Notificación enviada a los siguientes correos: {', '.join(all_owners)} con el evento id {event.storage_id}"
                )

        if int_cursor < event.storage_id:
            int_cursor = event.storage_id

    return dg.SensorResult(
        cursor=str(int_cursor),
    )


if __name__ == "__main__":
    # Mocked test environment for running the sensor
    from unittest.mock import MagicMock
    from typing import Any

    # Configure logging
    logger = dg.get_dagster_logger(__name__)

    def run_isolated_test(test_name: str, test_func):
        """Run each test in isolation with a fresh instance."""
        logger.info(f"🧪 {test_name}")

        with dg.instance_for_test() as test_instance:
            # Example assets: Mock asset owners for testing
            @dg.asset(
                owners=["owner1@example.com", "owner2@example.com"],
                config_schema={
                    "fail": dg.Field(bool, is_required=False, default_value=False),
                },
                key=["prefix", "can_fail_asset"],
            )
            def can_fail_asset(context: dg.AssetExecutionContext) -> None:
                """Asset 1 represents the root asset with two owners."""
                if context.op_config.get("fail"):
                    context.log.error(
                        "Asset processing failed, but no exception is raised."
                    )
                    context.add_output_metadata({"status": "failed"})
                return None

            @dg.asset_check(asset=can_fail_asset, blocking=True)
            def can_fail_asset_check(
                context: dg.AssetCheckExecutionContext,
            ) -> dg.AssetCheckResult:
                """Fail check if the asset reports a failure status in its metadata."""
                asset_key = context.check_specs[0].asset_key
                materialization_records = (
                    context.instance.get_latest_materialization_events([asset_key])
                )
                latest_materialization = (
                    materialization_records.get(asset_key)
                    if materialization_records
                    else None
                )

                if (
                    not latest_materialization
                    or not latest_materialization.dagster_event
                ):
                    return dg.AssetCheckResult(
                        passed=True,
                        metadata={"reason": "No materialization found for the asset."},
                    )

                event_data = latest_materialization.dagster_event.event_specific_data
                from dagster._core.events import StepMaterializationData

                if not isinstance(event_data, StepMaterializationData):
                    return dg.AssetCheckResult(
                        passed=True,
                        metadata={"reason": "Invalid materialization data."},
                    )

                status = event_data.materialization.metadata.get("status")
                if not status:
                    return dg.AssetCheckResult(
                        passed=True,
                        metadata={"reason": "No status found in asset metadata."},
                    )

                return dg.AssetCheckResult(
                    passed=status.value != "failed",
                    metadata={"reason": "Asset reported failure status in metadata."}
                    if status.value == "failed"
                    else {},
                )

            @dg.asset(
                owners=["owner3@example.com"],
                deps=[can_fail_asset],
                config_schema={
                    "fail": dg.Field(bool, is_required=False, default_value=False),
                },
            )
            def can_fail_asset_child_1(context: dg.AssetExecutionContext) -> None:
                """Asset 2 depends on asset 1 and has one owner. Can also fail for mid-asset testing."""
                if context.op_config.get("fail"):
                    context.log.error("Child asset processing failed.")
                    context.add_output_metadata({"status": "failed"})
                return None

            @dg.asset_check(asset=can_fail_asset_child_1, blocking=True)
            def can_fail_asset_child_1_check(
                context: dg.AssetCheckExecutionContext,
            ) -> dg.AssetCheckResult:
                """Fail check if the child asset reports a failure status in its metadata."""
                asset_key = context.check_specs[0].asset_key
                materialization_records = (
                    context.instance.get_latest_materialization_events([asset_key])
                )
                latest_materialization = (
                    materialization_records.get(asset_key)
                    if materialization_records
                    else None
                )

                if (
                    not latest_materialization
                    or not latest_materialization.dagster_event
                ):
                    return dg.AssetCheckResult(
                        passed=True, metadata={"reason": "No materialization found."}
                    )

                event_data = latest_materialization.dagster_event.event_specific_data
                from dagster._core.events import StepMaterializationData

                if not isinstance(event_data, StepMaterializationData):
                    return dg.AssetCheckResult(
                        passed=True,
                        metadata={"reason": "Invalid materialization data."},
                    )

                status = event_data.materialization.metadata.get("status")
                if not status:
                    return dg.AssetCheckResult(
                        passed=True, metadata={"reason": "No status found."}
                    )

                return dg.AssetCheckResult(
                    passed=status.value != "failed",
                    metadata={"reason": "Child asset reported failure status."}
                    if status.value == "failed"
                    else {},
                )

            @dg.asset(owners=["owner4@example.com"], deps=[can_fail_asset_child_1])
            def can_fail_asset_child_2() -> None:
                """Asset 3 depends on asset 2 and has one owner."""
                return None

            @dg.asset(deps=[can_fail_asset_child_1])
            def can_fail_asset_child_3() -> None:
                """Asset 4 depends on asset 2 and has no owner, defaults to fallback email."""
                return None

            can_fail_job = dg.define_asset_job(
                name="can_fail_job",
                selection=dg.AssetSelection.assets(can_fail_asset).downstream(),
            )

            # Test infrastructure
            send_email_mock = MagicMock()
            sent_emails: list[dict[str, Any]] = []

            class EmailResource:
                @staticmethod
                def send_email(
                    email_to: Sequence[str],
                    email_subject: str,
                    email_body: str,
                ) -> None:
                    email_data = {
                        "to": list(email_to),
                        "subject": email_subject,
                        "body": email_body,
                    }
                    sent_emails.append(email_data)
                    logger.info(f"📧 Email sent to: {', '.join(email_to)}")
                    logger.info(f"📧 Subject: {email_subject}")
                    logger.debug(f"📧 Body:\n{email_body}")
                    send_email_mock(email_to, email_subject, email_body)

            # Load assets into the definitions object
            defs = dg.Definitions(
                assets=[
                    can_fail_asset,
                    can_fail_asset_child_1,
                    can_fail_asset_child_2,
                    can_fail_asset_child_3,
                ],
                asset_checks=[can_fail_asset_check, can_fail_asset_child_1_check],
                jobs=[can_fail_job],
                sensors=[failed_asset_notification_sensor],
                resources={"enviador_correo_e_analitica_farinter": EmailResource()},
            )

            # Mock context object
            test_context = dg.build_sensor_context(
                instance=test_instance, definitions=defs
            )

            def execute_job_with_config(fail_configs: dict[dg.AssetKey, bool]) -> None:
                """Execute job with specific failure configurations."""
                run_config = {
                    "ops": {
                        asset_key.to_python_identifier(): {
                            "config": {"fail": should_fail}
                        }
                        for asset_key, should_fail in fail_configs.items()
                        if should_fail
                    }
                }
                try:
                    test_context.repository_def.get_job(
                        "can_fail_job"
                    ).execute_in_process(
                        instance=test_instance,
                        run_config=run_config if run_config["ops"] else None,
                    ) if test_context.repository_def else None
                except Exception:
                    pass  # Expected for failing assets

            def run_sensor_and_update_cursor() -> dg.SensorResult:
                """Run sensor and update cursor, returning the result."""
                sensor_result = failed_asset_notification_sensor(
                    test_context, enviador_correo_e_analitica_farinter=EmailResource()
                )
                if isinstance(sensor_result, dg.SensorResult):
                    if sensor_result.cursor:
                        test_context.update_cursor(sensor_result.cursor)
                    return sensor_result

                raise TypeError(
                    f"Sensor result is not of type SensorResult: {sensor_result}"
                )

            def assert_email_count(expected: int, message: str) -> None:
                """Assert email count with descriptive message."""
                actual = send_email_mock.call_count
                assert actual == expected, (
                    f"{message}: expected {expected}, got {actual}"
                )

            # Run the specific test function with the isolated context
            test_func(
                execute_job_with_config,
                run_sensor_and_update_cursor,
                assert_email_count,
                send_email_mock,
                sent_emails,
                can_fail_asset,
                can_fail_asset_child_1,
            )

    # Test implementations
    def test_no_failures(
        execute_job,
        run_sensor,
        assert_email_count,
        send_email_mock,
        sent_emails,
        *assets,
    ):
        execute_job({})
        run_sensor()
        assert_email_count(0, "No failures should not trigger emails")
        logger.info("✅ Test 1 passed")

    def test_root_asset_failure(
        execute_job,
        run_sensor,
        assert_email_count,
        send_email_mock,
        sent_emails,
        can_fail_asset,
        *others,
    ):
        execute_job({can_fail_asset.key: True})
        run_sensor()
        assert_email_count(1, "Root asset failure should trigger one email")

        # Verify email recipients include all affected owners
        if sent_emails:
            recipients = set(sent_emails[0]["to"])
            expected_recipients = {
                "owner1@example.com",
                "owner2@example.com",  # Root asset owners
                "owner3@example.com",
                "owner4@example.com",  # Downstream owners
                "brian.padilla@farinter.com",  # Default email
            }
            assert recipients == expected_recipients, (
                f"Expected {expected_recipients}, got {recipients}"
            )

        logger.info("✅ Test 2 passed")

    def test_mid_asset_failure_with_two_downstream(
        execute_job,
        run_sensor,
        assert_email_count,
        send_email_mock,
        sent_emails,
        can_fail_asset,
        can_fail_asset_child_1,
    ):
        # First run root asset successfully
        execute_job({})
        run_sensor()

        # Then fail the mid asset
        execute_job({can_fail_asset_child_1.key: True})
        run_sensor()
        assert_email_count(1, "Mid-asset failure should trigger one email")

        # Verify email recipients include mid-asset owner and downstream owners
        if sent_emails:
            recipients = set(sent_emails[0]["to"])
            expected_recipients = {
                "owner3@example.com",  # Mid-asset owner
                "owner4@example.com",  # Downstream child_2 owner
                "brian.padilla@farinter.com",  # Default email (for child_3 with no owner)
            }
            assert recipients == expected_recipients, (
                f"Expected {expected_recipients}, got {recipients}"
            )

            # Verify email mentions both downstream assets
            email_body = sent_emails[0]["body"]
            assert "can_fail_asset_child_2" in email_body, (
                "Email should mention downstream asset child_2"
            )
            assert "can_fail_asset_child_3" in email_body, (
                "Email should mention downstream asset child_3"
            )

        logger.info("✅ Test 3 passed")

    def test_cursor_prevents_reprocessing(
        execute_job,
        run_sensor,
        assert_email_count,
        send_email_mock,
        sent_emails,
        can_fail_asset,
        *others,
    ):
        # Create a failure event and process it
        execute_job({can_fail_asset.key: True})
        result1 = run_sensor()

        # Verify first run sent an email
        assert_email_count(1, "First run should send one email")
        assert result1.cursor is not None, "Cursor should be set after processing"

        # Run sensor again - should skip with no new events
        result2 = run_sensor()

        # Verify no additional emails were sent
        assert_email_count(1, "Second run should not send additional emails")
        assert result2.skip_reason is not None, "Second run should have a skip reason"

        logger.info("✅ Test 4 passed")

    def test_multiple_different_failures(
        execute_job,
        run_sensor,
        assert_email_count,
        send_email_mock,
        sent_emails,
        can_fail_asset,
        can_fail_asset_child_1,
    ):
        # First failure
        execute_job({can_fail_asset.key: True})
        run_sensor()
        first_count = send_email_mock.call_count

        # Second different failure (this should send another email)
        execute_job({can_fail_asset_child_1.key: True})
        run_sensor()
        second_count = send_email_mock.call_count

        assert second_count == first_count + 1, (
            f"Different failures should send separate emails: expected {first_count + 1}, got {second_count}"
        )
        logger.info("✅ Test 5 passed")

    # Run all tests in isolation
    try:
        run_isolated_test("Test 1: No failures", test_no_failures)
        run_isolated_test("Test 2: Root asset failure", test_root_asset_failure)
        run_isolated_test(
            "Test 3: Mid-asset failure with two downstream",
            test_mid_asset_failure_with_two_downstream,
        )
        run_isolated_test(
            "Test 4: Cursor prevents reprocessing", test_cursor_prevents_reprocessing
        )
        run_isolated_test(
            "Test 5: Multiple different failures", test_multiple_different_failures
        )

        logger.info("🎉 All tests passed successfully!")

    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise
