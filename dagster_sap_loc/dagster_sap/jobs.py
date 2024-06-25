from dagster import define_asset_job
from dagster import asset
from dagster_sap.assets.dbt_dwh_sap_mart_datos_maestros import dbt_dwh_sap_mart_datos_maestros_assets 
from dagster_sap.assets.dbt_dwh_sap_mart_finanzas import dbt_dwh_sap_mart_finanzas_assets
from dagster_shared_gf.shared_functions import get_variables_created_by_function

# Define the job and add to definitions on main __init__.py


dbt_dwh_sap_mart_datos_maestros_job = define_asset_job(name="dbt_dwh_sap_mart_datos_maestros_job"
                                                            , selection=[dbt_dwh_sap_mart_datos_maestros_assets])


dbt_dwh_sap_marts_job = define_asset_job(name="dbt_dwh_sap_marts_job"
                                                            , selection=[dbt_dwh_sap_mart_datos_maestros_assets, dbt_dwh_sap_mart_finanzas_assets])

__all__ = get_variables_created_by_function(define_asset_job)
