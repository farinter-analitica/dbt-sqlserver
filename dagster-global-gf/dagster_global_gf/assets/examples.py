from dagster import asset
from dagster_shared_gf.shared_variables import tags_repo

# Estas pruebas o ejemplos ayudan a evitar jobs predefinidos o automatizaciones vacías.


@asset(tags=tags_repo.AutomationHourly)
def global_ejemplo_prueba_test_example_hourly_automation():
    pass


@asset(tags=tags_repo.AutomationDaily)
def global_ejemplo_prueba_test_example_daily_automation():
    pass


@asset(tags=tags_repo.AutomationWeekly1)
def global_ejemplo_prueba_test_example_weekly_automation():
    pass


@asset(tags=tags_repo.AutomationMonthlyStart)
def global_ejemplo_prueba_test_example_monthly_automation():
    pass


@asset(tags=tags_repo.Daily)
def global_ejemplo_prueba_test_example_daily_without_automation():
    pass


@asset(tags=tags_repo.Hourly)
def global_ejemplo_prueba_test_example_hourly_without_automation():
    pass
