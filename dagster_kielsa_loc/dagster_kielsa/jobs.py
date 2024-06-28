from dagster import define_asset_job, AssetSelection, asset, job
from dagster_shared_gf.shared_functions import get_variables_created_by_function

# Define the job and add to definitions on main __init__.py
ldcom_etl_dwh_job = define_asset_job(name="ldcom_etl_dwh_job"
                                                             , selection=AssetSelection.groups("ldcom_etl_dwh"))


dbt_dwh_kielsa_mart_datos_maestros_assets: AssetSelection = AssetSelection.groups("dbt_dwh_kielsa_mart_datos_maestros")
dbt_dwh_kielsa_marts_job = define_asset_job(name="dbt_dwh_kielsa_marts_job"
                                                            , selection=dbt_dwh_kielsa_mart_datos_maestros_assets)

ldcom_etl_dwh_all_downstream_assets: AssetSelection = AssetSelection.groups("ldcom_etl_dwh").downstream()
ldcom_etl_dwh_all_downstream_job: define_asset_job = define_asset_job(name="ldcom_etl_dwh_all_downstream_job"
                                                             , selection=ldcom_etl_dwh_all_downstream_assets)

dbt_dwh_kielsa_marts_assets_not_in_downstream: AssetSelection = dbt_dwh_kielsa_mart_datos_maestros_assets - ldcom_etl_dwh_all_downstream_assets
dbt_dwh_kielsa_marts_orphan_assets_job = define_asset_job(name="dbt_dwh_kielsa_marts_orphan_assets_job"
                                                            , selection=dbt_dwh_kielsa_marts_assets_not_in_downstream)

hourly_parent_assets: list = [ldcom_etl_dwh_job, dbt_dwh_kielsa_marts_job]

all_jobs = get_variables_created_by_function(define_asset_job) 

__all__ = list(map(lambda x: x.name, all_jobs) )

if __name__ == "__main__":
    print(dbt_dwh_kielsa_marts_assets_not_in_downstream)