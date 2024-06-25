from dagster import define_asset_job
from dagster_kielsa.assets.dbt_dwh_kielsa_mart_datos_maestros import dbt_dwh_kielsa_mart_datos_maestros_assets 
from dagster_shared_gf.shared_functions import get_variables_created_by_function

# Define the job and add to definitions on main __init__.py



dbt_dwh_kielsa_marts_job = define_asset_job(name="dbt_dwh_sap_marts_job"
                                                            , selection=[dbt_dwh_kielsa_mart_datos_maestros_assets])


__all__ = get_variables_created_by_function(define_asset_job)