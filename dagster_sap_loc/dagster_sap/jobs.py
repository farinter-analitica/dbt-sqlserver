from dagster import define_asset_job
from dagster import asset, AssetSelection
from dagster_shared_gf.shared_functions import get_variables_created_by_function

# Define the job and add to definitions on main __init__.py


dbt_dwh_sap_mart_datos_maestros_job = define_asset_job(name="dbt_dwh_sap_mart_datos_maestros_job"
                                                            , selection=AssetSelection.groups("dbt_dwh_sap_mart_datos_maestros"))


dbt_dwh_sap_marts_job = define_asset_job(name="dbt_dwh_sap_marts_job"
                                                            , selection=AssetSelection.groups("dbt_dwh_sap_mart_datos_maestros_assets"
                                                                                              , "dbt_dwh_sap_mart_finanzas_assets"))

all_jobs = get_variables_created_by_function(define_asset_job)

__all__ = list(map(lambda x: x.name, all_jobs) )