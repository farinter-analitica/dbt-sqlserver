from dagster import define_asset_job
from dagster_farinter.assets.dbt_dwh_sap_mart_datos_maestros import dbt_dwh_farinter_mart_datos_maestros_assets

# Define the job

dbt_dwh_farinter_mart_datos_maestros_job = define_asset_job(name="dbt_dwh_farinter_mart_datos_maestros_job"
                                                            , selection=[dbt_dwh_farinter_mart_datos_maestros_assets])
