from dagster import define_asset_job, AssetSelection, asset, job, AssetKey, JobDefinition
from dagster_shared_gf.shared_functions import get_all_instances_of_class
from dagster_shared_gf.shared_variables import env_str, UnresolvedAssetJobDefinition
from typing import List, Any, Mapping

ExecutorConfig = Mapping[str, object]
# Define the job and add to definitions on main __init__.py
ldcom_etl_dwh_job = define_asset_job(name="ldcom_etl_dwh_job"
                                                             , selection=AssetSelection.groups("ldcom_etl_dwh"))

examples_assets_job = define_asset_job(name="examples_assets_job"
                                                            , selection=AssetSelection.groups("examples" #con directorio de proyecto
                                                                                              ,"dbt_examples" #con directorio de proyecto
                                                                                              ,"dbt_second_group_test" #con directorio de proyecto y tambien directamente en snapshots
                                                                                              ,"dbt_first_group" #grupo definido y usado directamente en snapshots
                                                                                              ,"dbt_first_model" #grupo no definido pero usado directamente en models
                                                                                              ))
dbt_dwh_kielsa_marts_assets: AssetSelection = AssetSelection.groups("dbt_dwh_kielsa_mart_datos_maestros"
                                                                    ,"dbt_dwh_kielsa_mart_kpp")
dbt_dwh_kielsa_marts_job = define_asset_job(name="dbt_dwh_kielsa_marts_job"
                                                            , selection=dbt_dwh_kielsa_marts_assets)

ldcom_etl_dwh_all_downstream_assets: AssetSelection = AssetSelection.groups("ldcom_etl_dwh").downstream()
ldcom_etl_dwh_all_downstream_job: define_asset_job = define_asset_job(name="ldcom_etl_dwh_all_downstream_job"
                                                             , selection=ldcom_etl_dwh_all_downstream_assets)

dbt_dwh_kielsa_marts_assets_not_in_downstream: AssetSelection = dbt_dwh_kielsa_marts_assets - ldcom_etl_dwh_all_downstream_assets
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

all_jobs = get_all_instances_of_class(class_type_list=[JobDefinition, UnresolvedAssetJobDefinition]) 

__all__ = list(map(lambda x: x.name, all_jobs) )

if __name__ == "__main__":
    print(dbt_dwh_kielsa_marts_assets_not_in_downstream)