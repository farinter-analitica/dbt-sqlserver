from dagster import AutomationCondition
from dagster_shared_gf.shared_functions import get_for_current_env
from dagster_shared_gf.shared_variables import default_timezone_teg

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
    ).on_cron('@hourly')

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
        ),
    }
)
