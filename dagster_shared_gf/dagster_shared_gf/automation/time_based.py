from dagster import AssetSelection, AutomationCondition, ExperimentalWarning
from dagster_shared_gf.shared_functions import get_for_current_env
from dagster_shared_gf.shared_variables import (
    default_timezone_teg,
    TagsRepositoryGF as tags_repo,
)
import warnings

warnings.filterwarnings("ignore", category=ExperimentalWarning)

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

automation_hourly_cron_prd = get_for_current_env(
    {
        "dev": None,
        "prd": (
            AutomationCondition.cron_tick_passed(
                "01 6-19 * * *", cron_timezone=default_timezone_teg
            ).since_last_handled()
            & ~AutomationCondition.in_progress()
            & ~AutomationCondition.any_deps_in_progress()
        ).with_label("Por hora (06-19)"),
    }
)

def my_daily_automation_condition() -> AutomationCondition:
        cron_schedule = get_for_current_env({"dev": "0 1 * * *", "prd": "0 0 * * *"})
        cron_timezone=default_timezone_teg
        cron_label = f"'{cron_schedule}' ({cron_timezone})"
        cron_tick_passed_since_last_handle = (AutomationCondition.cron_tick_passed(
                    cron_schedule, cron_timezone
                ).since_last_handled() | AutomationCondition.missing()).with_label(f"tick of {cron_label} passed")
        all_deps_updated_since_cron = AutomationCondition.all_deps_match(
            AutomationCondition.newly_updated().since(
                    AutomationCondition.cron_tick_passed(cron_schedule, cron_timezone)
                )
            | AutomationCondition.will_be_requested()
        ).ignore(
        selection=(
            AssetSelection.all()
            - AssetSelection.tag(key=tags_repo.Daily.key, value=tags_repo.Daily.value)
            )
        ).with_label(f"all same period parents updated since {cron_label}")
        return (
            AutomationCondition.in_latest_time_window()
            & cron_tick_passed_since_last_handle
            & all_deps_updated_since_cron
            & ~AutomationCondition.in_progress()
            & ~AutomationCondition.any_deps_in_progress()
        ).with_label(f"on cron {cron_label}")

# all_daily_deps_updated = (
#     AutomationCondition.all_deps_match(
#         (AutomationCondition.newly_updated()).with_label("newly_updated")
#         | AutomationCondition.will_be_requested()
#     )
#     .ignore(
#         selection=(
#             AssetSelection.all()
#             - AssetSelection.tag(key=tags_repo.Daily.key, value=tags_repo.Daily.value)
#         )
#     )
#     .with_label("all_daily_deps_updated")
# )

# automation_daily_cron_prd = (
#     AutomationCondition.cron_tick_passed(
#         get_for_current_env({"dev": "0 1 * * *", "prd": "10 0 * * *"}),
#         cron_timezone=default_timezone_teg,
#     ).since_last_handled().with_label(f"Cron diario {get_for_current_env({'dev':'0 1 * * *','prd':'10 0 * * *'})}")
#     & ~AutomationCondition.in_progress()
#     & ~AutomationCondition.any_deps_in_progress()
#     & all_daily_deps_updated.since_last_handled()
# ).with_label("Cron diario condicional.")

automation_daily_cron = my_daily_automation_condition() 