from typing import Mapping, Optional
from dagster_shared_gf.automation.time_based import (
    automation_daily_delta_2_cron,
    automation_hourly_delta_12_cron,
    automation_weekly_7_delta_1_cron,
    automation_monthly_start_delta_1_cron,
    automation_monthly_end_delta_1_cron,
    automation_weekly_1_delta_1_cron,
)
from dagster_shared_gf.shared_variables import tags_repo, Tags
from dagster import AutomationCondition

tag_automation_mapping: dict[str, AutomationCondition] = {
    # Hourly automations
    tags_repo.AutomationHourly.key: automation_hourly_delta_12_cron,
    tags_repo.AutomationHourlyAdditional.key: automation_hourly_delta_12_cron,
    # tags_repo.HourlyAdditional.key: automation_hourly_delta_12_cron,
    # tags_repo.Hourly.key: automation_hourly_delta_12_cron,
    # Daily automation
    tags_repo.AutomationDaily.key: automation_daily_delta_2_cron,
    # tags_repo.Daily.key: automation_daily_delta_2_cron,
    # Weekly automations
    tags_repo.AutomationWeekly1.key: automation_weekly_1_delta_1_cron,
    tags_repo.AutomationWeekly7.key: automation_weekly_7_delta_1_cron,
    # tags_repo.Weekly.key: automation_weekly_7_delta_1_cron,
    # tags_repo.Weekly7.key: automation_weekly_7_delta_1_cron,
    # tags_repo.Weekly1.key: automation_weekly_1_delta_1_cron,  # Note: May need specific automation for Monday
    # Monthly automations
    tags_repo.AutomationMonthlyStart.key: automation_monthly_start_delta_1_cron,
    tags_repo.AutomationMonthlyEnd.key: automation_monthly_end_delta_1_cron,
    # tags_repo.Monthly.key: automation_monthly_start_delta_1_cron,  # Start of month
    # tags_repo.MonthlyEnd.key: automation_monthly_end_delta_1_cron,  # End of month
    # tags_repo.MonthlyStart.key: automation_monthly_start_delta_1_cron,  # Start of month
    # Eager automations
    tags_repo.AutomationEager.key: AutomationCondition.eager(),
}


def get_mapped_automation_condition(
    tags: Mapping[str, str] | Tags,
) -> Optional[AutomationCondition]:
    auto_tags_keys = tags_repo.get_automation_tags_keys()
    all_automations: AutomationCondition | None = None
    if any(item in tags.keys() for item in auto_tags_keys):
        for tag, automation in tag_automation_mapping.items():
            if tag in tags:
                all_automations = (
                    automation
                    if all_automations is None
                    else all_automations | automation
                )
    return all_automations
