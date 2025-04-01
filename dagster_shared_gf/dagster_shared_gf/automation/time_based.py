from dagster import AssetSelection, AutomationCondition
from dagster_shared_gf.shared_functions import get_for_current_env
from dagster_shared_gf.shared_variables import (
    default_timezone_teg,
    tags_repo,
)
from dagster_shared_gf.automation.custom_conditions import IsRootExecutable
from datetime import timedelta


# https://docs.dagster.io/concepts/automation/declarative-automation
# https://docs.dagster.io/concepts/automation/declarative-automation/customizing-automation-conditions
# ~ (tilde)	NOT; condition is not true; ex: ~A
# | (pipe)	OR; either condition must be true; ex: A | B
# & (ampersand)	AND; both conditions must be true; ex: A & B
# A.newly_true()	Condition A was false on the previous evaluation and is now true.
# A.since(B)	Condition A became true more recently than Condition B.
# AutomationCondition.any_deps_match(A)	Condition A is true for any upstream partition. Can be used with .allow() and .ignore() to target specific upstream assets. Refer to the Targeting dependencies section for an example.
# AutomationCondition.all_deps_match(A)	Condition A is true for at least one partition of each upstream asset. Can be used with .allow() and .ignore() to target specific upstream assets. Refer to the Targeting dependencies section for an example.
# AutomationCondition.any_downstream_condition()	Any AutomationCondition on a downstream asset evaluates to true


def get_cron_eager_execution_condition(cron_schedule: str) -> AutomationCondition:
    # Use eagerly base
    eager_condition = (
        AutomationCondition.in_latest_time_window()
        & (
            AutomationCondition.newly_missing() | AutomationCondition.any_deps_updated()
        ).since_last_handled()
        & ~AutomationCondition.any_deps_missing()
        & ~AutomationCondition.any_deps_in_progress()
        & ~AutomationCondition.in_progress()
    )

    cron_condition = (
        AutomationCondition.cron_tick_passed(cron_schedule).since_last_handled()
        & ~AutomationCondition.any_deps_missing()
        & ~AutomationCondition.any_deps_in_progress()
        & ~AutomationCondition.in_progress()
    ).on_cron("@hourly")

    final_condition = (eager_condition & cron_condition) | cron_condition

    return final_condition


# Example usage
hourly_condition = get_cron_eager_execution_condition("@hourly")


def my_cron_automation_condition(
    cron_schedule: str,
    ignored_deps_updated_selection: AssetSelection | None = None,
    allowed_deps_updated_selection: AssetSelection | None = None,
    lookback_delta: timedelta | None = None,
) -> AutomationCondition:
    """
    Creates an automation condition that combines cron scheduling with dependency state checks.

    This condition evaluates to True when all the following criteria are met:
    1. The current partition falls within the specified lookback window
    2. A cron tick has occurred since the last execution OR the asset is newly missing
    3. The asset is not currently running
    4. No dependencies are currently running
    5. Either:
       - The asset is a root executable (has no dependencies)
       - OR all non-ignored dependencies are either:
         * Recently updated since last execution
         * Scheduled to be updated

    Args:
        cron_schedule (str): Cron expression defining the schedule (e.g. "0 * * * *" for hourly)
        ignored_deps_updated_selection (AssetSelection | None): Asset selection to exclude from
            dependency update checks. Use this to ignore specific assets or asset groups.
        allowed_deps_updated_selection (AssetSelection | None): Asset selection to include in
            dependency update checks. Use this to target specific assets or asset groups.
        lookback_delta (timedelta | None): Maximum time window to look back for updates.
            Helps prevent stale executions by limiting how far back to check.

    Returns:
        AutomationCondition: A composite condition that enforces both timing and dependency rules.
            The condition includes descriptive labels for debugging and monitoring.

    Example:
        ```python
        condition = my_cron_automation_condition(
            cron_schedule="0 0 * * *",  # Daily at midnight
            ignored_deps_updated_selection=AssetSelection.tags({"frequency": "weekly"}),
            lookback_delta=timedelta(days=1)
        )
        ```
    """
    cron_timezone = default_timezone_teg
    cron_schedule_label = f"'{cron_schedule}' ({cron_timezone})"
    cron_tick_passed_since_last_handle = (
        AutomationCondition.cron_tick_passed(cron_schedule, cron_timezone)
        .since_last_handled()
        .with_label(f"cron_tick_passed: {cron_schedule_label}")
        | AutomationCondition.newly_missing().since_last_handled()
    )
    deps_updated_since_cron = AutomationCondition.all_deps_match(
        AutomationCondition.newly_updated().since(
            AutomationCondition.cron_tick_passed(cron_schedule, cron_timezone)
        )
        | AutomationCondition.will_be_requested()
    )
    if ignored_deps_updated_selection:
        deps_updated_since_cron = deps_updated_since_cron.ignore(
            ignored_deps_updated_selection
        )
    if allowed_deps_updated_selection:
        deps_updated_since_cron = deps_updated_since_cron.allow(
            allowed_deps_updated_selection
        )
    return (
        AutomationCondition.in_latest_time_window(lookback_delta=lookback_delta)
        & cron_tick_passed_since_last_handle
        & ~AutomationCondition.in_progress()
        & ~AutomationCondition.any_deps_in_progress()
        & (
            IsRootExecutable()
            | deps_updated_since_cron.with_label(
                f"dependencies_updated_since: {cron_schedule_label}"
            )
        )
    ).with_label(f"cron_schedule_passed_and_complied: {cron_schedule_label}")


# El cron no puede estar antes que la primera evaluacion
# de las nuevas particiones en los particionados.
daily_cron_schedule = get_for_current_env({"dev": "0 1 * * *", "prd": "8 0 * * *"})
automation_daily_delta_2_cron = my_cron_automation_condition(
    cron_schedule=daily_cron_schedule,
    allowed_deps_updated_selection=(
        AssetSelection.tag(key=tags_repo.Daily.key, value=tags_repo.Daily.value)
    ),
    lookback_delta=timedelta(days=2),
)
hourly_cron_schedule = get_for_current_env(
    {
        "dev": "01 21 * * *",
        "prd": "01 6-23 * * *",
    }
)
automation_hourly_delta_12_cron = my_cron_automation_condition(
    cron_schedule=hourly_cron_schedule,
    allowed_deps_updated_selection=(
        AssetSelection.tag(
            key=tags_repo.AutomationHourly.key, value=tags_repo.AutomationHourly.value
        )
    ),
    lookback_delta=timedelta(hours=12),
)

hourly_additional_cron_schedule = get_for_current_env(
    {
        "dev": "01 23 * * *",
        "prd": "31 6-22 * * *",
    }
)
automation_hourly_additional_delta_12_cron = my_cron_automation_condition(
    cron_schedule=hourly_additional_cron_schedule,
    allowed_deps_updated_selection=(
        AssetSelection.tag(
            key=tags_repo.HourlyAdditional.key, value=tags_repo.HourlyAdditional.value
        )
    ),
    lookback_delta=timedelta(hours=2),
)


weekly_7_cron_schedule = get_for_current_env({"dev": "0 1 * * 7", "prd": "0 16 * * 7"})
automation_weekly_7_delta_1_cron = my_cron_automation_condition(
    cron_schedule=weekly_7_cron_schedule,
    allowed_deps_updated_selection=(
        AssetSelection.tag(key=tags_repo.Weekly7.key, value=tags_repo.Weekly7.value)
    ),
    lookback_delta=timedelta(weeks=1),
)

weekly_1_cron_schedule = get_for_current_env({"dev": "0 4 * * 1", "prd": "0 3 * * 1"})
automation_weekly_1_delta_1_cron = my_cron_automation_condition(
    cron_schedule=weekly_1_cron_schedule,
    allowed_deps_updated_selection=(
        AssetSelection.tag(key=tags_repo.Weekly1.key, value=tags_repo.Weekly1.value)
    ),
    lookback_delta=timedelta(weeks=1),
)

# El cron no puede estar antes que la primera evaluacion
# de las nuevas particiones en los particionados.
monthly_start_cron_schedule = get_for_current_env(
    {"dev": "0 3 1 * *", "prd": "41 0 1 * *"}
)
automation_monthly_start_delta_1_cron = my_cron_automation_condition(
    cron_schedule=monthly_start_cron_schedule,
    allowed_deps_updated_selection=(
        AssetSelection.tag(key=tags_repo.Monthly.key, value=tags_repo.Monthly.value)
    ),
    lookback_delta=timedelta(days=32),
)

monthly_end_cron_schedule = get_for_current_env(
    {"dev": "0 3 L * *", "prd": "41 0 L * *"}
)
automation_monthly_end_delta_1_cron = my_cron_automation_condition(
    cron_schedule=monthly_end_cron_schedule,
    allowed_deps_updated_selection=(
        AssetSelection.tag(key=tags_repo.Monthly.key, value=tags_repo.Monthly.value)
    ),
    lookback_delta=timedelta(days=32),
)
