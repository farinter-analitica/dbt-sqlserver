from dagster import asset
from dagster_shared_gf.shared_variables import tags_repo


@asset(tags=tags_repo.AutomationHourly)
def sap_example_hourly_automation():
    pass


@asset(tags=tags_repo.AutomationDaily)
def sap_example_daily_automation():
    pass


@asset(tags=tags_repo.AutomationWeekly1)
def sap_example_weekly_automation():
    pass


@asset(tags=tags_repo.AutomationMonthlyStart)
def sap_example_monthly_automation():
    pass


@asset(tags=tags_repo.Daily)
def sap_example_daily_without_automation():
    pass
