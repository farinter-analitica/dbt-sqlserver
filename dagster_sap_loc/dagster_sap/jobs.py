from dagster import define_asset_job
from dagster import asset, AssetSelection, RunConfig, AssetKey
from dagster_shared_gf.shared_functions import get_variables_created_by_function
from .assets.dbt_sap_etl_dwh import MyDbtConfig

# Define the job and add to definitions on main __init__.py


# dbt_dwh_sap_mart_datos_maestros_job = define_asset_job(name="dbt_dwh_sap_mart_datos_maestros_job"
#                                                             , selection=AssetSelection.groups("dbt_dwh_sap_mart_datos_maestros"))


dbt_dwh_sap_marts_job = define_asset_job(name="dbt_dwh_sap_marts_job"
                                                            , selection=AssetSelection.groups("dbt_dwh_sap_mart_datos_maestros_assets"
                                                                                              , "dbt_dwh_sap_mart_finanzas_assets"))


dbt_dwh_sap_etl_dwh_job = define_asset_job(name="dbt_dwh_sap_etl_dwh_job"
                                           , selection=AssetSelection.groups("sap_etl_dwh"))

dbt_dwh_sap_etl_dwh_full_refresh_job = define_asset_job(name="dbt_dwh_sap_etl_dwh_full_refresh_job"
                                           , selection=AssetSelection.groups("sap_etl_dwh")
                                           , config=RunConfig({"dbt_sap_etl_dwh_assets": MyDbtConfig(full_refresh= True)}))


sap_etl_dwh_all_downstream_assets: AssetSelection = AssetSelection.groups("sap_etl_dwh").downstream()
sap_etl_dwh_all_downstream_assets = sap_etl_dwh_all_downstream_assets - (sap_etl_dwh_all_downstream_assets.tag(key="periodo", value="por_hora") & sap_etl_dwh_all_downstream_assets.tag(key="periodo_unico", value="true"))
sap_etl_dwh_all_downstream_job: define_asset_job = define_asset_job(name="sap_etl_dwh_all_downstream_job"
                                                            , selection=sap_etl_dwh_all_downstream_assets)


#Definir assets por hora que se extraen de dbt, agregar todos los downstream que tenga la etiqueta por_hora y agregar replicas_sap con la misma etiqueta
sap_dbt_etl_dwh_hourly_asset_keys = [AssetKey(Asset) for Asset in ["DL_SAP_MARC","DL_SAP_MARD","DL_SAP_MARA","DL_SAP_MCHB"]]
sap_etl_dwh_hourly_all_downstream_assets: AssetSelection =  AssetSelection.assets(*sap_dbt_etl_dwh_hourly_asset_keys) \
    | (AssetSelection.assets(*sap_dbt_etl_dwh_hourly_asset_keys).downstream() & AssetSelection.tag(key="periodo", value="por_hora")) \
    | (AssetSelection.tag(key="replicas_sap", value="true") & AssetSelection.tag(key="periodo", value="por_hora"))
sap_etl_dwh_hourly_all_downstream_job: define_asset_job = define_asset_job(name="sap_etl_dwh_hourly_all_downstream_job"
                                                            , selection=sap_etl_dwh_hourly_all_downstream_assets)


dbt_dwh_sap_marts_all_orphan_assets: AssetSelection = AssetSelection.groups("dbt_dwh_sap_mart_datos_maestros_assets"
                                                                                              , "dbt_dwh_sap_mart_finanzas_assets") - sap_etl_dwh_all_downstream_assets
dbt_dwh_sap_marts_all_orphan_job = define_asset_job(name="dbt_dwh_sap_marts_all_orphan_job"
                                                            , selection=dbt_dwh_sap_marts_all_orphan_assets)                                                                         

all_jobs = get_variables_created_by_function(define_asset_job)

__all__ = list(map(lambda x: x.name, all_jobs) )