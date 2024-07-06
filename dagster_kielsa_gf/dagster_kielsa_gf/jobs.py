from dagster import define_asset_job, AssetSelection, asset, job, AssetKey
from dagster_shared_gf.shared_functions import get_variables_created_by_function
from dagster_shared_gf.shared_variables import env_str
from typing import List, Any, Mapping

ExecutorConfig = Mapping[str, object]
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

knime_workflows_run_config: ExecutorConfig= {"execution": {"config": {"multiprocess": {"max_concurrent": 1}}}}
knime_workflows_start_of_month_assets: AssetSelection = AssetSelection.assets(AssetKey(["knime_wf",env_str,"DWHFP_SalidaExportarAExcel"])).downstream() ##Schedule differently
knime_workflows_start_of_month_job: define_asset_job = define_asset_job(name="knime_workflows_start_of_month_job"
                                                            , selection=knime_workflows_start_of_month_assets
                                                            , config=knime_workflows_run_config
                                                                )

knime_workflows_all_downstream_assets: AssetSelection = AssetSelection.groups("knime_workflows").downstream() - knime_workflows_start_of_month_assets
knime_workflows_all_downstream_job: define_asset_job = define_asset_job(name="knime_workflows_all_downstream_job"
                                                            , selection=knime_workflows_all_downstream_assets
                                                            , config=knime_workflows_run_config
                                                                )


hourly_parent_assets: list = []

all_jobs = get_variables_created_by_function(define_asset_job) 

__all__ = list(map(lambda x: x.name, all_jobs) )

if __name__ == "__main__":
    print(dbt_dwh_kielsa_marts_assets_not_in_downstream)