import datetime

import pytest
from dagster import (
    AssetKey,
    AssetMaterialization,
    DagsterInstance,
    Definitions,
    SourceAsset,
    asset,
    evaluate_automation_conditions,
)
from dagster_shared_gf.automation.time_based import (
    my_cron_automation_condition,
)


def new_on_cron(cron_schedule: str, cron_timezone: str = "UTC"):
    """Returns an AutomationCondition which will cause a target to be executed on a given
    cron schedule, after all of its dependencies have been updated since the latest
    tick of that cron schedule or if it is a root executable asset.

    For time partitioned assets, only the latest time partition will be considered.
    """
    return my_cron_automation_condition(cron_schedule=cron_schedule)

    ## Direct testing passing:
    # return (
    #     AutomationCondition.in_latest_time_window()
    #     & AutomationCondition.cron_tick_passed(cron_schedule, cron_timezone).since_last_handled()
    #     & (
    #         AutomationCondition.all_deps_updated_since_cron(cron_schedule, cron_timezone)
    #         | IsRootExecutable()
    #         | AutomationCondition.will_be_requested()
    #     )
    # ).with_label(f"on_cron({cron_schedule}, {cron_timezone})")

    ## Should also pass, will be tested
    # return (
    #     AutomationCondition.in_latest_time_window()
    #     & AutomationCondition.cron_tick_passed(
    #         cron_schedule, cron_timezone
    #     ).since_last_handled()
    #     & (
    #         AutomationCondition.all_deps_updated_since_cron(
    #             cron_schedule, cron_timezone
    #         ).allow(AssetSelection.all().materializable().downstream()) #falla, no incluye observables
    #         | AutomationCondition.will_be_requested()
    #     )
    # ).with_label(f"on_cron({cron_schedule}, {cron_timezone})")


estandarized_on_cron = new_on_cron("@hourly")


def test_on_cron_root_asset() -> None:
    """Test that on_cron evaluates to true for root assets when cron boundary is crossed."""

    @asset(automation_condition=estandarized_on_cron)
    def root_asset() -> None: ...

    current_time = datetime.datetime(2024, 8, 16, 4, 35)
    defs = Definitions(assets=[root_asset])
    instance = DagsterInstance.ephemeral()

    # hasn't passed a cron tick
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # now passed a cron tick - root asset should fire immediately since it has no dependencies
    current_time += datetime.timedelta(minutes=30)
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 1

    # don't fire again until next cron boundary
    current_time += datetime.timedelta(minutes=1)
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # next hour, fire again
    current_time += datetime.timedelta(hours=1)
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 1


def test_on_cron_with_non_observable_source() -> None:
    """Test that on_cron do not waits for non-observable source asset updates before firing."""
    source_asset = SourceAsset("source_data")

    @asset(deps=[source_asset], automation_condition=estandarized_on_cron)
    def downstream_asset() -> None: ...

    current_time = datetime.datetime(2024, 8, 16, 4, 35)
    defs = Definitions(assets=[source_asset, downstream_asset])
    instance = DagsterInstance.ephemeral()

    # hasn't passed a cron tick
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # now passed a cron tick, source excluded so it should fire
    current_time += datetime.timedelta(minutes=30)
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 1

    # don't fire again
    current_time += datetime.timedelta(minutes=1)
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # next cron boundary passes - should fire again
    current_time += datetime.timedelta(hours=1)
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 1


def test_on_cron_with_observable_source() -> None:
    """Test that on_cron waits for observable source asset updates before firing."""
    source_asset = SourceAsset(
        "source_data", observe_fn=lambda context: context.asset_key
    )

    @asset(deps=[source_asset], automation_condition=estandarized_on_cron)
    def downstream_asset() -> None: ...

    current_time = datetime.datetime(2024, 8, 16, 4, 35)
    defs = Definitions(assets=[source_asset, downstream_asset])
    instance = DagsterInstance.ephemeral()

    # hasn't passed a cron tick
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # now passed a cron tick, but parent hasn't been updated
    current_time += datetime.timedelta(minutes=30)
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 0, "Should not fire before parent update"

    # now parent is updated, so fire
    current_time += datetime.timedelta(minutes=1)
    instance.report_runless_asset_event(AssetMaterialization("source_data"))
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 1


def test_on_cron_with_observable_source_two_runs() -> None:
    """Test that on_cron waits for observable source asset updates before firing."""

    @asset(key=AssetKey("source_data"))
    def source_asset(context) -> None: ...

    @asset(deps=[source_asset], automation_condition=estandarized_on_cron)
    def downstream_asset() -> None: ...

    current_time = datetime.datetime(2024, 8, 16, 4, 35)
    defs = Definitions(assets=[source_asset, downstream_asset])
    instance = DagsterInstance.ephemeral()

    # hasn't passed a cron tick
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # now passed a cron tick, but parent hasn't been updated
    current_time += datetime.timedelta(minutes=30)
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # now parent is updated, so fire
    current_time += datetime.timedelta(minutes=1)
    instance.report_runless_asset_event(AssetMaterialization("source_data"))
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 1, "Should fire after parent update"

    # don't keep firing
    current_time += datetime.timedelta(minutes=1)
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # ...even if the parent is updated again
    current_time += datetime.timedelta(minutes=1)
    instance.report_runless_asset_event(AssetMaterialization("source_data"))
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # new tick passes...
    current_time += datetime.timedelta(hours=1)
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # and parent is updated
    current_time += datetime.timedelta(minutes=1)
    instance.report_runless_asset_event(AssetMaterialization("source_data"))
    current_time += datetime.timedelta(minutes=1)
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 1, (
        "Should fire again after new cron tick and parent update"
    )


def test_on_cron_mixed_root_and_source_dependencies() -> None:
    """Test on_cron behavior with a mix of root assets and source dependencies."""

    @asset(key=AssetKey("external_data"))
    def source_asset(context) -> None: ...

    @asset(automation_condition=estandarized_on_cron)
    def root_asset() -> None: ...

    @asset(deps=[source_asset], automation_condition=estandarized_on_cron)
    def source_dependent_asset() -> None: ...

    @asset(deps=[root_asset, source_asset], automation_condition=estandarized_on_cron)
    def mixed_dependent_asset() -> None: ...

    current_time = datetime.datetime(2024, 8, 16, 4, 35)
    defs = Definitions(
        assets=[source_asset, root_asset, source_dependent_asset, mixed_dependent_asset]
    )
    instance = DagsterInstance.ephemeral()

    # hasn't passed a cron tick
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # cron boundary crossed - root asset should fire
    current_time += datetime.timedelta(minutes=30)
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    # root_asset should fire since it have no dependencies
    assert result.get_num_requested(AssetKey("root_asset")) == 1, (
        "Root asset should fire"
    )
    assert result.total_requested == 1, "Only root asset should fire"

    # now source_dependent_asset and mixed_dependent_asset should fire since their deps are updated
    current_time += datetime.timedelta(minutes=1)
    instance.report_runless_asset_event(AssetMaterialization("root_asset"))
    instance.report_runless_asset_event(AssetMaterialization("external_data"))
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.get_num_requested(AssetKey("source_dependent_asset")) == 1, (
        "Source dependent asset should fire"
    )
    assert result.get_num_requested(AssetKey("mixed_dependent_asset")) == 1, (
        "Mixed dependent asset should fire"
    )
    assert result.total_requested == 2, "Source and mixed dependent assets should fire"

    # no more requests
    current_time += datetime.timedelta(minutes=1)
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 0


@pytest.mark.xfail(
    reason="This test is expected to fail because the condition does not handle newly added assets before next cron"
)
def test_on_cron_newly_missing_with_updated_deps():
    """Helper para probar el comportamiento de la condición con y sin patch."""

    @asset(key=AssetKey("dep_asset"), automation_condition=estandarized_on_cron)
    def dep_asset(context) -> None: ...

    @asset(deps=[dep_asset], automation_condition=estandarized_on_cron)
    def missing_asset() -> None: ...

    current_time = datetime.datetime(2024, 8, 16, 4, 35)
    instance = DagsterInstance.ephemeral()

    # Solo existe dep_asset, lo actualizamos y evaluamos con cron
    defs = Definitions(assets=[dep_asset])
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, evaluation_time=current_time
    )
    assert result.total_requested == 0, "No debe disparar sin cron tick"
    cursor = result.cursor

    # Avanza el tiempo, pasa el cron tick, dispara dep_asset
    current_time += datetime.timedelta(minutes=61)
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=cursor, evaluation_time=current_time
    )
    assert result.total_requested == 1, "Debe disparar dep_asset por cron tick"
    cursor = result.cursor

    # Ahora introducimos missing_asset y evaluamos antes del siguiente cron tick
    current_time += datetime.timedelta(minutes=1)
    defs = Definitions(assets=[dep_asset, missing_asset])
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=cursor, evaluation_time=current_time
    )
    assert result.total_requested == 0, (
        "No Debe disparar missing_asset antes del siguiente cron tick"
    )

    current_time += datetime.timedelta(minutes=1)
    instance.report_runless_asset_event(AssetMaterialization("dep_asset"))
    defs = Definitions(assets=[dep_asset, missing_asset])
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=cursor, evaluation_time=current_time
    )
    # print(result.results.__dict__)

    # Debug statement to trace the flow
    print(f"Total requested after reporting dep_asset: {result.total_requested}")

    assert result.total_requested == 1, (
        "Debe disparar missing_asset antes del siguiente cron tick (sin patch debe fallar)"
    )
