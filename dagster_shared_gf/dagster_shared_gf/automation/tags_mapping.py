from dagster_shared_gf.automation.time_based import (
    automation_daily_delta_2_cron,
    automation_hourly_delta_12_cron,
    automation_weekly_7_delta_1_cron,
    automation_monthly_start_delta_1_cron,
    automation_monthly_end_delta_1_cron,
    automation_weekly_1_delta_1_cron,
)
from dagster_shared_gf.shared_variables import tags_repo
from dagster import AutomationCondition

tag_automation_mapping: dict[str, AutomationCondition] = {
    # Hourly automations
    tags_repo.Hourly.key: automation_hourly_delta_12_cron,
    tags_repo.HourlyAdditional.key: automation_hourly_delta_12_cron,
    
    # Daily automation
    tags_repo.Daily.key: automation_daily_delta_2_cron,
    
    # Weekly automations
    tags_repo.Weekly.key: automation_weekly_7_delta_1_cron,
    tags_repo.Weekly7.key: automation_weekly_7_delta_1_cron,
    tags_repo.Weekly1.key: automation_weekly_1_delta_1_cron,  # Note: May need specific automation for Monday
    
    # Monthly automations
    tags_repo.Monthly.key: automation_monthly_start_delta_1_cron,  # Start of month
    tags_repo.MonthlyEnd.key: automation_monthly_end_delta_1_cron,  # End of month
    tags_repo.MonthlyStart.key: automation_monthly_start_delta_1_cron,  # Start of month
}
