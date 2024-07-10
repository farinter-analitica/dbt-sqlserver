from dagster import (asset
                     , AssetChecksDefinition 
                     , AssetExecutionContext 
                     , AssetsDefinition
                     , build_last_update_freshness_checks
                     , load_assets_from_current_module
                     , load_asset_checks_from_current_module
                     )
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_variables import env_str, TagsRepositoryGF
from dagster_shared_gf.shared_functions import filter_assets_by_tags, get_all_instances_of_class
import dagster_sap_gf.assets.dbt_dwh_sap as dbt_dwh_sap
from pathlib import Path
from typing import List, Dict, Any, Mapping, Sequence, get_args, get_origin, Union
from datetime import datetime, date, timedelta
import time, itertools
from trycast import type_repr

#vars
file_path = Path(__file__).parent.resolve()
tags_repo = TagsRepositoryGF 

@asset(
    key_prefix=["DL_FARINTER"],
    tags=tags_repo.Replicas.tag,
    compute_kind="sqlserver"
)
def DL_SAP_T001(context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource) -> None: 
    table = "DL_SAP_T001"
    database = "DL_FARINTER"
    schema = "dbo"
    sql_file_path = file_path.joinpath(f"sap_etl_dwh_sql/{table}.sql").resolve()
    
    try:
        with open(sql_file_path, encoding="utf8") as procedure:
            final_query = procedure.read()
    except IOError as e:
        context.log.error(f"Error reading SQL file: {e}")
        return

    try:
        with dwh_farinter_dl.get_connection(database) as conn:
            last_date_updated_query: str = f"""SELECT MAX(Fecha_Actualizado) FROM [{database}].[{schema}].[{table}]"""
            last_date_updated_result: List[tuple] = dwh_farinter_dl.query(query=last_date_updated_query, connection=conn)
            last_date_updated: date = date(1900, 1, 1)
            if last_date_updated_result and last_date_updated_result[0][0] is not None:
                try:
                    last_date_updated = datetime.fromisoformat(last_date_updated_result[0][0]).date()
                except ValueError as e:
                    context.log.error(f"Error converting date: {e}, defaulting to {last_date_updated}.")
            final_query = final_query.format(last_date_updated=last_date_updated.isoformat())
            dwh_farinter_dl.execute_and_commit(final_query, connection=conn)
    except Exception as e:
        context.log.error(f"Error during database operation: {e}")
        
    #return last_date_updated

#DL_paCargarSAP_Replica_BSEG
@asset(key_prefix= ["DL_FARINTER"]
        , tags=tags_repo.Replicas.tag | {"dagster/max_runtime": str(50*60) # max 50 minutes in seconds, then mark it as failed.
                }
        , compute_kind="sqlserver"
        )
def DL_SAP_BSEG(context: AssetExecutionContext
                , dwh_farinter_dl: SQLServerResource
                ) -> None: 
    table = "DL_SAP_BSEG"
    database = "DL_FARINTER"
    schema = "dbo"
    sql_file_path = file_path.joinpath(f"sap_etl_dwh_sql/{table}.sql").resolve()
    supported_envs = ["dev"] #solo trae info desde la base de datos de PRD, otro proceso en SAP la carga en PRD
    if env_str not in supported_envs:
        context.log.info(f"Saltando {table} por que no esta soportada en {env_str}.")
        return
    with open(sql_file_path, encoding="utf8") as procedure:
        final_query = str(procedure.read())
    with dwh_farinter_dl.get_connection(database) as conn:
        last_aniomes_id_query: str = f"""SELECT MAX(AnioMes_Id) FROM [{database}].[{schema}].[{table}] WITH (NOLOCK);"""
        last_aniomes_id_result: List[Any] = dwh_farinter_dl.query(query=last_aniomes_id_query, connection = conn)        
        last_date_updated_query: str = f"""
            SELECT MAX(AEDAT) FROM [{database}].[{schema}].[{table}] 
            WHERE AnioMes_Id >= {last_aniomes_id_result[0][0] if last_aniomes_id_result else (datetime.now().year*100-5)+1};
            """
        last_date_updated_result: List[Any] = dwh_farinter_dl.query(query=last_date_updated_query, connection = conn)
        last_date_updated: date = (datetime.now()-timedelta(days=5*365)).date()
        if last_date_updated_result:
            try:
                last_date_updated: date = datetime.strptime(last_date_updated_result[0][0], "%Y%m%d").date()
            except:
                context.log.error(f"Error al convertir la fecha del último registro desde la base de datos, devolviendo por defecto desde fecha {last_date_updated}.")
        final_query = final_query.format(p_FechaDesde=last_date_updated.isoformat(),p_IndicadorActualizarTodo = 0)
        #print(final_query)
        dwh_farinter_dl.execute_and_commit(final_query, connection = conn)
        


def create_store_procedure_asset(procedure_name: str, tags: Mapping[str, str]) -> AssetsDefinition:
    @asset(key_prefix= [str("DL_FARINTER")]
            , name=(procedure_name)
            , tags=tags
            , compute_kind="sqlserver"
            )
    def store_procedure_execution(context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource) -> None: 
        procedure = procedure_name
        database = "DL_FARINTER"
        schema = "dbo"
        final_query = f"EXEC [{database}].[{schema}].[{procedure}];"
        dwh_farinter_dl.execute_and_commit(final_query)

    return store_procedure_execution

def generate_hourly_store_procedure_assets() -> List[AssetsDefinition]:
    store_procedure_list:List[str] = ["DL_paCargarSAP_REPLICA_DatosMaestros"
                      ,"DL_paCargarSAP_REPLICA_VBFA"
                      ,"DL_paCargarSAP_REPLICA_SD"
                      ,"DL_paCargarSAP_REPLICA_MM"
                      ,"DL_paCargarSAP_REPLICA_WM"
                      ,"DL_paCargarSAP_REPLICA_FI"]
    
    store_procedure_assets:List[AssetsDefinition] = []
    tags= tags_repo.Replicas.tag | tags_repo.Hourly.tag 
    for procedure_name in store_procedure_list:
        store_procedure_execution = create_store_procedure_asset(procedure_name
                                                                 , tags )
        store_procedure_assets.append(store_procedure_execution)

    return store_procedure_assets

store_procedure_assets: List[AssetsDefinition] = generate_hourly_store_procedure_assets()


@asset(key_prefix= ["DL_FARINTER"]
 #       , name="sp_start_job_sap_cadahora"
        , tags= tags_repo.Replicas.tag | tags_repo.Hourly.tag | tags_repo.HourlyUnique.tag #replicas_tag | hourly_unique_tag
        , deps=store_procedure_assets+list([DL_SAP_T001])+list()
        , compute_kind="sqlserver"
        )
def sp_start_job_sap_cadahora(context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource) -> None:
    if env_str not in ("prd","local"): 
        context.log.info(f"Skipping sp_start_job_sap_cadahora for env {env_str}")
        return
    job_name = 'SAP_CadaHora'
    final_query = \
    f"""
    DECLARE @job_result int = 0;
    EXECUTE @job_result = msdb.dbo.sp_start_job @job_name = {job_name};
    SELECT @job_result as job_result;
    """
    results = dwh_farinter_dl.query(final_query, fetch_one=True)
    #check if sp returned 1 for errors
    if results is None:
        context.log.error(f"Job {job_name} not executed, fail.")
    elif results[0] == 1: 
        context.log.error(f"Job {job_name} not executed, fail.")
    elif results == 0:
        context.log.info(f"Job {job_name} executed successfully.")
    else:
        context.log.error(f"Job {job_name} not executed, fail.")

@asset(key_prefix= ["DL_FARINTER"]
        , tags=tags_repo.Replicas.tag | tags_repo.Daily.tag | tags_repo.DailyUnique.tag #replicas_tag | daily_unique_tag
        , deps=store_procedure_assets+list([DL_SAP_T001])+list([dbt_dwh_sap.dbt_sap_etl_dwh_assets])
#        , freshness_policy= FreshnessPolicy(maximum_lag_minutes=60*26, cron_schedule="0 10-16 * * *", cron_schedule_timezone="America/Tegucigalpa") #deprecated
        , compute_kind="sqlserver"
        )
def sp_start_job_sap_diario(context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource) -> None:
    job_name = 'SAP_Diario'
    final_query = \
    f"""
    DECLARE @job_result int = 0;
    EXECUTE @job_result =msdb.dbo.sp_start_job @job_name = {job_name};
    SELECT @job_result as job_result;
    """
    results = dwh_farinter_dl.query(final_query, fetch_one=True)
    #check if sp returned 1 for errors
    if results is None:
        context.log.error(f"Job {job_name} not executed, fail.")
    elif results[0] == 1: 
        context.log.error(f"Job {job_name} not executed, fail.")
    elif results == 0:
        context.log.info(f"Job {job_name} executed successfully.")
    else:
        context.log.error(f"Job {job_name} not executed, fail.")



#sp_start_job_sap_diario.with_attributes(group_names_by_key={list(sp_start_job_sap_diario.keys())[-1]: "sap_etl_dwh"})
#all_assets = load_assets_from_current_module(group_name="sap_etl_dwh") # no se puede usar ya que importamos otros assets desde assets.py
#all_assets_without_group = get_all_instances_of_class([AssetsDefinition]) + store_procedure_assets
#add group_name="sap_etl_dwh" to all_assets
#all_assets = [*map(lambda asset: asset.with_attributes(group_names_by_key={list(asset.keys)[-1]: "sap_etl_dwh"}), all_assets)]
#list comprehension equivalent
#print(all_assets_without_group)
#como agregar atributos de grupo por ejemplo:
#all_assets = [asset.with_attributes(group_names_by_key={list(asset.keys)[-1]: "sap_etl_dwh"}) for asset in all_assets_without_group]
if not __name__ == '__main__':
    all_assets = load_assets_from_current_module(group_name="sap_etl_dwh") #+ store_procedure_assets

    all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
        assets=filter_assets_by_tags(all_assets, tags=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"),
        lower_bound_delta=timedelta(hours=26),
        deadline_cron="0 9 * * 1-6",
    )
    #print(filter_assets_by_tags(all_assets, tags=hourly_tag, filter_type="any_tag_matches"), "\n")
    all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = build_last_update_freshness_checks(
        assets=filter_assets_by_tags(all_assets, tags=tags_repo.Hourly.tag, filter_type="any_tag_matches"),
        lower_bound_delta=timedelta(hours=13),
        deadline_cron="0 10-16 * * 1-6",
    )

    #all_asset_checks = load_asset_checks_from_current_module()
    #all_asset_checks: List[AssetChecksDefinition] = itertools.chain.from_iterable(get_all_instances_of_class([Sequence[AssetChecksDefinition]]))
    all_asset_checks: Sequence[AssetChecksDefinition] = load_asset_checks_from_current_module()
    all_asset_freshness_checks = all_assets_non_hourly_freshness_checks + all_assets_hourly_freshness_checks

if __name__ == '__main__':
    ##testing
    from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_dl
    from dagster import build_asset_context
    ##
    #print(generate_store_procedure_assets())
    #print((timedelta(days=-5*365) + datetime.now()).date().isoformat())
    context = build_asset_context()
    #env_str='PRD'
    def tests1():
        DL_SAP_T001(context, dwh_farinter_dl)
    def tests2():
        sp_start_job_sap_cadahora(context, dwh_farinter_dl)
    #tests1()
    tests2()
    # print("get_args " + str(get_args(all_assets_hourly_freshness_checks)))
    # print("get_origin " +str(get_origin(all_assets_hourly_freshness_checks)))
    # print("type " +  str(type(all_assets_hourly_freshness_checks)))
    # print(sp_start_job_sap_diario.tags_by_key[list(sp_start_job_sap_diario.keys)[-1]])
    # print("checks: " + str(all_asset_checks))
    #print(all_assets)
    #print("checks fres: " + str(all_asset_freshness_checks))
