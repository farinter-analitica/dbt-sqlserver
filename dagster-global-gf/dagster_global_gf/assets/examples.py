from dagster import asset
from dagster_shared_gf.shared_variables import tags_repo


@asset(tags=tags_repo.AutomationHourly)
def shared_example_hourly_automation():
    pass


@asset(tags=tags_repo.AutomationDaily)
def shared_example_daily_automation():
    pass


@asset(tags=tags_repo.AutomationWeekly1)
def shared_example_weekly_automation():
    pass


@asset(tags=tags_repo.AutomationMonthlyStart)
def shared_example_monthly_automation():
    pass


@asset(tags=tags_repo.Daily)
def shared_example_daily_without_automation():
    pass
