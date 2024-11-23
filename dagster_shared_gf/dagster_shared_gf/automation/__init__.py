from dagster_shared_gf.automation.time_based import automation_hourly_cron_prd, automation_daily_delta_2_cron

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

__all__ = (
    "automation_hourly_cron_prd",
    "automation_daily_delta_2_cron",
)