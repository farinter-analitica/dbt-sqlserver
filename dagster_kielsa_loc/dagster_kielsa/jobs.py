from dagster import define_asset_job, AssetSelection, asset, job
from dagster_shared_gf.shared_functions import get_variables_created_by_function

# Define the job and add to definitions on main __init__.py
ldcom_etl_dwh_job = define_asset_job(name="ldcom_etl_dwh_job"
                                                             , selection=AssetSelection.groups("ldcom_etl_dwh"))
dbt_dwh_kielsa_marts_job = define_asset_job(name="dbt_dwh_kielsa_marts_job"
                                                            , selection=AssetSelection.groups("dbt_dwh_kielsa_mart_datos_maestros"))
# ldcom_etl_dwh_wait_job = define_asset_job(name="ldcom_etl_dwh_wait_job"
#                                                              , selection=AssetSelection.assets("wait_op_for_ldcom_etl_dwh_job"))

all_jobs = get_variables_created_by_function(define_asset_job) 

__all__ = list(map(lambda x: x.name, all_jobs) )