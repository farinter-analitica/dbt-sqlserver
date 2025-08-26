from unittest.mock import MagicMock
import dagster as dg

from dagster_shared_gf import shared_failed_sensors as sfs


def make_defs_and_email_resource():
    """Create assets, checks, job, and an email resource that records sends."""
    sent_emails: list[dict] = []
    send_email_mock = MagicMock()
    # Ensure tests use controlled default email lists to avoid depending on
    # repository-wide globals which may change or be overridden externally.
    sfs.DEFAULT_EMAILS = ["default.email1@farinter.com", "default.email2@farinter.com"]
    sfs.DEFAULT_RUN_FAILURE_EMAILS = [
        "default.email_rf1@farinter.com",
        "default.email_rf2@farinter.com",
        "default.email_rf3@farinter.com",
    ]

    @dg.asset(
        owners=["owner1@example.com", "owner2@example.com"],
        config_schema={
            "fail": dg.Field(bool, is_required=False, default_value=False),
        },
        key=["prefix", "can_fail_asset"],
    )
    def can_fail_asset(context: dg.AssetExecutionContext) -> None:
        if context.op_execution_context.op_config.get("fail"):
            context.log.error("Asset processing failed, but no exception is raised.")
            context.add_output_metadata({"status": "failed"})
        return None

    @dg.asset_check(asset=can_fail_asset, blocking=True)
    def can_fail_asset_check(
        context: dg.AssetCheckExecutionContext,
    ) -> dg.AssetCheckResult:
        asset_key = context.check_specs[0].asset_key
        materialization_records = context.instance.get_latest_materialization_events(
            [asset_key]
        )
        latest_materialization = (
            materialization_records.get(asset_key) if materialization_records else None
        )

        if not latest_materialization or not latest_materialization.dagster_event:
            return dg.AssetCheckResult(
                passed=True,
                metadata={"reason": "No materialization found for the asset."},
            )

        event_data = latest_materialization.dagster_event.event_specific_data
        from dagster._core.events import StepMaterializationData

        if not isinstance(event_data, StepMaterializationData):
            return dg.AssetCheckResult(
                passed=True, metadata={"reason": "Invalid materialization data."}
            )

        status = event_data.materialization.metadata.get("status")
        if not status:
            return dg.AssetCheckResult(
                passed=True, metadata={"reason": "No status found in asset metadata."}
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
        if context.op_execution_context.op_config.get("fail"):
            context.log.error("Child asset processing failed.")
            context.add_output_metadata({"status": "failed"})
        return None

    @dg.asset_check(asset=can_fail_asset_child_1, blocking=True)
    def can_fail_asset_child_1_check(
        context: dg.AssetCheckExecutionContext,
    ) -> dg.AssetCheckResult:
        asset_key = context.check_specs[0].asset_key
        materialization_records = context.instance.get_latest_materialization_events(
            [asset_key]
        )
        latest_materialization = (
            materialization_records.get(asset_key) if materialization_records else None
        )

        if not latest_materialization or not latest_materialization.dagster_event:
            return dg.AssetCheckResult(
                passed=True, metadata={"reason": "No materialization found."}
            )

        event_data = latest_materialization.dagster_event.event_specific_data
        from dagster._core.events import StepMaterializationData

        if not isinstance(event_data, StepMaterializationData):
            return dg.AssetCheckResult(
                passed=True, metadata={"reason": "Invalid materialization data."}
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
        return None

    @dg.asset(deps=[can_fail_asset_child_1])
    def can_fail_asset_child_3() -> None:
        return None

    can_fail_job = dg.define_asset_job(
        name="can_fail_job",
        selection=dg.AssetSelection.assets(can_fail_asset).downstream(),
    )

    # Job que falla antes de cualquier asset (simula fallo sin assets involucrados)
    @dg.op
    def failing_op():  # pragma: no cover - trivial
        raise RuntimeError("Early failure before assets")

    @dg.graph
    def failing_before_assets_graph():  # pragma: no cover - trivial
        failing_op()

    failing_before_assets_job = failing_before_assets_graph.to_job(
        name="failing_before_assets_job"
    )

    class EmailResource:
        @staticmethod
        def send_email(
            email_to: list[str], email_subject: str, email_body: str
        ) -> None:
            sent_emails.append(
                {"to": list(email_to), "subject": email_subject, "body": email_body}
            )
            send_email_mock(email_to, email_subject, email_body)

    defs = dg.Definitions(
        assets=[
            can_fail_asset,
            can_fail_asset_child_1,
            can_fail_asset_child_2,
            can_fail_asset_child_3,
        ],
        asset_checks=[can_fail_asset_check, can_fail_asset_child_1_check],
        jobs=[can_fail_job, failing_before_assets_job],
        sensors=[sfs.failed_asset_notification_sensor],
        resources={"enviador_correo_e_analitica_farinter": EmailResource()},
    )

    return (
        defs,
        EmailResource,
        send_email_mock,
        sent_emails,
        can_fail_asset,
        can_fail_asset_child_1,
    )


def execute_job_with_config(
    test_context, defs, test_instance, fail_configs: dict[dg.AssetKey, bool]
):
    run_config = {
        "ops": {
            asset_key.to_python_identifier(): {"config": {"fail": should_fail}}
            for asset_key, should_fail in fail_configs.items()
            if should_fail
        }
    }
    job = (
        test_context.repository_def.get_job("can_fail_job")
        if test_context.repository_def
        else None
    )
    try:
        if job:
            # return the run result so tests can construct RunStatusSensorContext
            return job.execute_in_process(
                instance=test_instance,
                run_config=run_config if run_config["ops"] else None,
                raise_on_error=False,
            )
    except Exception:
        # Should not raise because raise_on_error=False, but keep safety
        return None
    return None


def run_sensor_and_update_cursor(
    test_context: dg.SensorEvaluationContext,
    EmailResource,
    run_result: dg.ExecuteInProcessResult | None = None,
):
    # If a run_result is provided, build a RunStatusSensorContext (newer API)
    if run_result is not None:
        dagster_run = run_result.dagster_run
        # ExecuteInProcessResult provides run-level event helpers named get_run_*
        # Safely attempt to get a failure event, falling back to success event.
        dagster_event = None
        # Prefer failure event, but avoid calling getters that raise when the
        # event type is not present.
        if run_result.success is False:
            try:
                dagster_event = run_result.get_run_failure_event()
            except Exception:
                dagster_event = None
        if dagster_event is None and run_result.success is True:
            try:
                dagster_event = run_result.get_run_success_event()
            except Exception:
                dagster_event = None
        if dagster_event is None:
            return dg.SensorResult(skip_reason="No run_result provided.")
        rs_context = dg.build_run_status_sensor_context(
            sensor_name=sfs.failed_asset_notification_sensor.name,
            dagster_event=dagster_event,
            dagster_instance=test_context.instance,
            dagster_run=dagster_run,
            repository_def=test_context.repository_def,
        ).for_run_failure()

        # Call the run-failure sensor (new API). The sensor may not return a
        # SensorResult; tests should not rely on a return value here.
        returned = sfs.failed_asset_notification_sensor(
            rs_context, enviador_correo_e_analitica_farinter=EmailResource()
        )
        if isinstance(returned, dg.SensorResult) and returned.cursor is not None:
            test_context.update_cursor(returned.cursor)
        return returned
    else:
        return dg.SensorResult(skip_reason="No run_result provided.")


def test_no_failures():
    defs, EmailResource, send_email_mock, sent_emails, *_ = (
        make_defs_and_email_resource()
    )
    with dg.instance_for_test() as test_instance:
        test_context = dg.build_sensor_context(instance=test_instance, definitions=defs)
        # Execute the job (no failures) to build a run_result we can attach to the
        # RunStatusSensorContext under the new API.
        job = defs.get_job_def("can_fail_job")
        try:
            result = job.execute_in_process(
                instance=test_instance, raise_on_error=False
            )
        except Exception:
            result = None
        run_sensor_and_update_cursor(test_context, EmailResource, run_result=result)
        assert send_email_mock.call_count == 0


def test_root_asset_failure():
    defs, EmailResource, send_email_mock, sent_emails, can_fail_asset, _ = (
        make_defs_and_email_resource()
    )
    with dg.instance_for_test() as test_instance:
        test_context = dg.build_sensor_context(instance=test_instance, definitions=defs)
        # Fail root asset
        result = execute_job_with_config(
            test_context, defs, test_instance, {can_fail_asset.key: True}
        )
        run_sensor_and_update_cursor(test_context, EmailResource, run_result=result)
        assert send_email_mock.call_count == 1
        recipients = set(sent_emails[0]["to"]) if sent_emails else set()
        expected = {
            "owner1@example.com",
            "owner2@example.com",
            "owner3@example.com",
            "owner4@example.com",
            *set(sfs.DEFAULT_EMAILS),
        }
        assert recipients == expected


def test_mid_asset_failure_with_two_downstream():
    (
        defs,
        EmailResource,
        send_email_mock,
        sent_emails,
        can_fail_asset,
        can_fail_asset_child_1,
    ) = make_defs_and_email_resource()
    with dg.instance_for_test() as test_instance:
        test_context = dg.build_sensor_context(instance=test_instance, definitions=defs)
        # First run OK
        result = execute_job_with_config(test_context, defs, test_instance, {})
        run_sensor_and_update_cursor(test_context, EmailResource, run_result=result)

        # Then fail mid asset
        result = execute_job_with_config(
            test_context, defs, test_instance, {can_fail_asset_child_1.key: True}
        )
        run_sensor_and_update_cursor(test_context, EmailResource, run_result=result)
        assert send_email_mock.call_count == 1
        recipients = set(sent_emails[0]["to"]) if sent_emails else set()
        expected = {
            "owner3@example.com",
            "owner4@example.com",
            *set(sfs.DEFAULT_EMAILS),
        }
        assert recipients == expected
        body = sent_emails[0]["body"]
        assert "can_fail_asset_child_2" in body
        assert "can_fail_asset_child_3" in body


def test_cursor_prevents_reprocessing():
    defs, EmailResource, send_email_mock, sent_emails, can_fail_asset, _ = (
        make_defs_and_email_resource()
    )
    with dg.instance_for_test() as test_instance:
        test_context = dg.build_sensor_context(instance=test_instance, definitions=defs)
        result = execute_job_with_config(
            test_context, defs, test_instance, {can_fail_asset.key: True}
        )
        run_sensor_and_update_cursor(test_context, EmailResource, run_result=result)
        assert send_email_mock.call_count == 1
        result2 = run_sensor_and_update_cursor(
            test_context=test_context, EmailResource=EmailResource
        )
        # No new events -> skip reason set
        assert send_email_mock.call_count == 1
        assert isinstance(result2, dg.SensorResult)
        assert result2.skip_reason is not None


def test_multiple_different_failures():
    (
        defs,
        EmailResource,
        send_email_mock,
        sent_emails,
        can_fail_asset,
        can_fail_asset_child_1,
    ) = make_defs_and_email_resource()
    with dg.instance_for_test() as test_instance:
        test_context = dg.build_sensor_context(instance=test_instance, definitions=defs)
        result = execute_job_with_config(
            test_context, defs, test_instance, {can_fail_asset.key: True}
        )
        run_sensor_and_update_cursor(test_context, EmailResource, run_result=result)
        first_count = send_email_mock.call_count

        result = execute_job_with_config(
            test_context, defs, test_instance, {can_fail_asset_child_1.key: True}
        )
        run_sensor_and_update_cursor(test_context, EmailResource, run_result=result)
        second_count = send_email_mock.call_count
        assert second_count == first_count + 1


def test_run_failure_without_assets_triggers_run_level_email():
    (
        defs,
        EmailResource,
        send_email_mock,
        sent_emails,
        can_fail_asset,
        can_fail_asset_child_1,
    ) = make_defs_and_email_resource()
    with dg.instance_for_test() as test_instance:
        test_context = dg.build_sensor_context(instance=test_instance, definitions=defs)
        # Ejecutar job que falla antes de assets
        job = defs.get_job_def("failing_before_assets_job")
        try:
            result = job.execute_in_process(
                instance=test_instance, raise_on_error=False
            )
        except Exception:
            result = None
        run_sensor_and_update_cursor(test_context, EmailResource, run_result=result)
        # Debe haber un email a la lista DEFAULT_RUN_FAILURE_EMAILS, solo 1
        assert send_email_mock.call_count == 1
        recipients = set(sent_emails[0]["to"]) if sent_emails else set()
        assert recipients == set(sfs.DEFAULT_RUN_FAILURE_EMAILS)
        body = sent_emails[0]["body"]
        assert "Run id:" in body and "Hora inicio" in body


def test_mixed_asset_and_run_only_failures():
    (
        defs,
        EmailResource,
        send_email_mock,
        sent_emails,
        can_fail_asset,
        can_fail_asset_child_1,
    ) = make_defs_and_email_resource()
    with dg.instance_for_test() as test_instance:
        test_context = dg.build_sensor_context(instance=test_instance, definitions=defs)

        # 1) Run que falla sin assets
        job = defs.get_job_def("failing_before_assets_job")
        try:
            result = job.execute_in_process(
                instance=test_instance, raise_on_error=False
            )
        except Exception:
            result = None
        run_sensor_and_update_cursor(test_context, EmailResource, run_result=result)
        assert send_email_mock.call_count == 1
        first_recipients = set(sent_emails[-1]["to"]) if sent_emails else set()
        assert first_recipients == set(sfs.DEFAULT_RUN_FAILURE_EMAILS)

        # 2) Ahora un run con fallo de asset raíz
        result = execute_job_with_config(
            test_context, defs, test_instance, {can_fail_asset.key: True}
        )
        run_sensor_and_update_cursor(test_context, EmailResource, run_result=result)
        assert send_email_mock.call_count == 2
        second_recipients = set(sent_emails[-1]["to"]) if sent_emails else set()
        assert second_recipients != set(sfs.DEFAULT_RUN_FAILURE_EMAILS)
        # Debe incluir owners de root asset + default asset emails
        expected_asset_fail = {
            "owner1@example.com",
            "owner2@example.com",
            "owner3@example.com",
            "owner4@example.com",
            *set(sfs.DEFAULT_EMAILS),
        }
        assert second_recipients == expected_asset_fail
