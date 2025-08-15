import datetime
from datetime import timedelta

import pytest
from dagster import (
    AssetKey,
    AssetMaterialization,
    DagsterInstance,
    Definitions,
    AssetSelection,
    SourceAsset,
    asset,
    evaluate_automation_conditions,
)
from dagster_shared_gf.automation.time_based import (
    my_cron_automation_condition,
)
from dagster_shared_gf.shared_variables import tags_repo


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


def test_on_cron_allowed_deps_selection_only_requires_allowed_updated() -> None:
    """Verifica que solo las dependencias dentro de allowed_deps_updated_selection
    sean requeridas para disparar el asset downstream.

    Escenario:
      - asset_a (observable vía materialization manual) tiene la etiqueta AutomationHourly.
      - asset_b no tiene etiqueta.
      - downstream depende de ambos, pero la condición solo exige updates de los assets etiquetados.
    Resultado esperado: downstream se dispara tras cron tick + update de asset_a, sin requerir update de asset_b.
    """

    @asset(tags={tags_repo.AutomationHourly.key: tags_repo.AutomationHourly.value})
    def asset_a():
        pass

    @asset
    def asset_b():
        pass

    allowed_selection = AssetSelection.tag(
        key=tags_repo.AutomationHourly.key, value=tags_repo.AutomationHourly.value
    )

    @asset(
        deps=[asset_a, asset_b],
        automation_condition=my_cron_automation_condition(
            cron_schedule="@hourly",
            allowed_deps_updated_selection=allowed_selection,
            lookback_delta=timedelta(hours=2),
        ),
    )
    def downstream():
        pass

    current_time = datetime.datetime(2024, 8, 16, 4, 35)
    defs = Definitions(assets=[asset_a, asset_b, downstream])
    instance = DagsterInstance.ephemeral()

    # Evaluación inicial (antes de pasar cron tick)
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, evaluation_time=current_time
    )
    assert result.get_num_requested(AssetKey("downstream")) == 0
    cursor = result.cursor

    # Avanzamos después del primer tick (05:05)
    current_time += datetime.timedelta(minutes=30)  # 05:05
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=cursor, evaluation_time=current_time
    )
    assert result.get_num_requested(AssetKey("downstream")) == 0, (
        "No debe disparar solo por el tick sin update"
    )
    cursor = result.cursor

    # Actualizamos asset_a (05:06)
    current_time += datetime.timedelta(minutes=1)  # 05:06
    instance.report_runless_asset_event(AssetMaterialization("asset_a"))
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=cursor, evaluation_time=current_time
    )
    assert result.get_num_requested(AssetKey("downstream")) == 1, (
        "Debe disparar tras update de asset_a"
    )
    cursor = result.cursor

    # No vuelve a disparar sin nuevo cron tick (05:07)
    current_time += datetime.timedelta(minutes=1)  # 05:07
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=cursor, evaluation_time=current_time
    )
    assert result.get_num_requested(AssetKey("downstream")) == 0
    cursor = result.cursor

    # Siguiente tick pasado (06:05)
    current_time += datetime.timedelta(minutes=58)  # 06:05
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=cursor, evaluation_time=current_time
    )
    assert result.get_num_requested(AssetKey("downstream")) == 0
    cursor = result.cursor

    # Update asset_a tras nuevo tick (06:06)
    current_time += datetime.timedelta(minutes=1)  # 06:06
    instance.report_runless_asset_event(AssetMaterialization("asset_a"))
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=cursor, evaluation_time=current_time
    )
    assert result.get_num_requested(AssetKey("downstream")) == 1, (
        "Debe disparar downstream tras nuevo tick y update de asset_a"
    )


def test_deps_updated_cron_requires_update_after_each_tick() -> None:
    """Prueba mínima de deps_updated_cron.

    Se usa un solo upstream (dep_asset). El downstream sólo debe disparar si hubo update
    después de cada cron tick. Verificamos:
      1. Sin update tras primer tick => no dispara.
      2. Update tras primer tick => dispara.
      3. Sin update tras segundo tick => no dispara.
      4. Update tras segundo tick => (esperado dispara, hoy falla => xfail).
    """

    @asset
    def dep_asset():
        pass

    @asset(
        deps=[dep_asset],
        automation_condition=my_cron_automation_condition(
            cron_schedule="@daily",
            deps_updated_cron="@hourly",
            lookback_delta=timedelta(days=3),
        ),
    )
    def target():
        pass

    current_time = datetime.datetime(2024, 8, 16, 4, 35)
    defs = Definitions(assets=[dep_asset, target])
    instance = DagsterInstance.ephemeral()

    # Evaluación inicial sin tick
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, evaluation_time=current_time
    )
    assert result.get_num_requested(AssetKey("target")) == 0
    cursor = result.cursor

    # Avanzamos al primer tick pasado (05:05)
    current_time += datetime.timedelta(days=1, minutes=30)  # 05:05 next day
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=cursor, evaluation_time=current_time
    )
    assert result.get_num_requested(AssetKey("target")) == 0
    cursor = result.cursor

    # Update posterior (05:06)
    current_time += datetime.timedelta(minutes=1)  # 05:06
    instance.report_runless_asset_event(AssetMaterialization("dep_asset"))
    result = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=cursor, evaluation_time=current_time
    )
    assert result.get_num_requested(AssetKey("target")) == 1
    cursor = result.cursor
