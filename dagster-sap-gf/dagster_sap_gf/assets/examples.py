from dagster import asset
from dagster_shared_gf.shared_variables import tags_repo
from dagster_shared_gf.automation import (
    automation_weekly_1_delta_1_cron,
    automation_monthly_start_delta_1_cron,
)

# Estas pruebas o ejemplos ayudan a evitar jobs predefinidos o automatizaciones vacías.


@asset(tags=tags_repo.AutomationHourly)  # El sensor ya se asigna a los que son por hora
def sap_ejemplo_prueba_test_example_hourly_automation():
    pass


@asset(
    tags=tags_repo.AutomationDaily | tags_repo.Daily
)  # Un job selecciona los assets que son por dia
def sap_ejemplo_prueba_test_example_daily_automation():
    pass


@asset(
    tags=tags_repo.AutomationWeekly1 | tags_repo.Weekly1,
    automation_condition=automation_weekly_1_delta_1_cron,
)  # Se asigna ac suponiendo que ningun sensor o job lo asigna
def sap_ejemplo_prueba_test_example_weekly_automation():
    pass


@asset(
    tags=tags_repo.AutomationMonthlyStart | tags_repo.MonthlyStart,
    automation_condition=automation_monthly_start_delta_1_cron,
)  # Se asigna ac suponiendo que ningun sensor o job lo asigna
def sap_ejemplo_prueba_test_example_monthly_automation():
    pass


@asset(tags=tags_repo.Daily)
def sap_ejemplo_prueba_test_example_daily_without_automation():
    pass


@asset(tags=tags_repo.Hourly)
def sap_ejemplo_prueba_test_example_hourly_without_automation():
    pass
