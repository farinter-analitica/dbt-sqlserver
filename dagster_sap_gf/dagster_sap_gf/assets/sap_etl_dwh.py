from dagster import asset, AssetKey , AssetExecutionContext , AssetsDefinition, FreshnessPolicy
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_variables import env_str
import dagster_sap_gf.assets.dbt_dwh_sap as dbt_sap_etl
from pathlib import Path
from typing import List, Dict, Any, Mapping
from datetime import datetime, date
import time
file_path = Path(__file__).parent.resolve()

@asset(key_prefix= ["DL_FARINTER"]
        , tags={"replicas_sap": "true"}
        , freshness_policy= FreshnessPolicy(maximum_lag_minutes=60*26)
        , compute_kind="sqlserver"
        )
def DL_SAP_T001(context: AssetExecutionContext
                , dwh_farinter_dl: SQLServerResource
                ) -> None: 
    table = "DL_SAP_T001"
    database = "DL_FARINTER"
    schema = "dbo"
    sql_file_path = file_path.joinpath(f"sap_etl_dwh_sql/{table}.sql").resolve()
    with open(sql_file_path, encoding="utf8") as procedure:
        final_query = str(procedure.read())
    with dwh_farinter_dl.get_connection(database) as conn:
        last_date_updated_query: str = f"""SELECT MAX(Fecha_Actualizado) FROM [{database}].[{schema}].[{table}]"""
        last_date_updated_result: List[Any] = dwh_farinter_dl.query(query=last_date_updated_query, connection = conn)
        last_date_updated: date = "1900-01-01"
        if last_date_updated_result:
            try:
                last_date_updated: date = last_date_updated_result[0][0].date()
            except:
                context.log.error(f"Error al convertir la fecha del último registro desde la base de datos, devolviendo por defecto desde fecha {last_date_updated}.")
        final_query = final_query.format(last_date_updated=last_date_updated.isoformat())
        #print(final_query)
        dwh_farinter_dl.execute_and_commit(final_query, connection = conn)
        
    #return last_date_updated
    #result = dwh_farinter_dl.execute_and_commit("EXEC [DL_FARINTER].[dbo].[DL_paCargarSAP_REPLICA_MM]")

def create_store_procedure_asset(procedure_name) -> AssetsDefinition:
    @asset(key_prefix= ["DL_FARINTER"]
            , name=procedure_name
            , tags={"replicas_sap": "true","periodo": "por_hora"}
            , freshness_policy= FreshnessPolicy(maximum_lag_minutes=60*26)
            , compute_kind="sqlserver"
            )
    def store_procedure_execution(context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource) -> None: 
        procedure = procedure_name
        database = "DL_FARINTER"
        schema = "dbo"
        final_query = f"EXEC [{database}].[{schema}].[{procedure}];"
        dwh_farinter_dl.execute_and_commit(final_query)

    return store_procedure_execution

def generate_store_procedure_assets() -> List[AssetsDefinition]:
    store_procedure_assets = []
    for procedure_name in ["DL_paCargarSAP_REPLICA_DatosMaestros"
                      ,"DL_paCargarSAP_REPLICA_VBFA"
                      ,"DL_paCargarSAP_REPLICA_SD"
                      ,"DL_paCargarSAP_REPLICA_MM"
                      ,"DL_paCargarSAP_REPLICA_WM"
                      ,"DL_paCargarSAP_REPLICA_FI"]:
        store_procedure_execution = create_store_procedure_asset(procedure_name)
        store_procedure_assets.append(store_procedure_execution)

    return store_procedure_assets

store_procedure_assets: List[AssetsDefinition] = generate_store_procedure_assets()


@asset(key_prefix= ["DL_FARINTER"]
 #       , name="sp_start_job_sap_cadahora"
        , tags={"replicas_sap": "true","periodo": "por_hora","periodo_unico": "por_hora"}
        , deps=store_procedure_assets+list([DL_SAP_T001])+list([dbt_sap_etl.dbt_sap_etl_dwh_assets])
        , freshness_policy= FreshnessPolicy(maximum_lag_minutes=60*26, cron_schedule="0 10-16 * * *", cron_schedule_timezone="America/Tegucigalpa")
        , compute_kind="sqlserver"
        )
def sp_start_job_sap_cadahora(context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource) -> None:
    if env_str != "prd": 
        context.log.info(f"Skipping sp_start_job_sap_cadahora for env {env_str}")
        return
    job_name = 'SAP_CadaHora'
    final_query = \
    f"""
    DECLARE @job_result int = 0;
    EXECUTE @job_result =msdb.dbo.sp_start_job @job_name = {job_name};
    SELECT @job_result;
    """
    results = dwh_farinter_dl.query(final_query, fetch_one=True)
    #check if sp returned 0 for errors
    if results is None:
        context.log.info(f"Job {job_name} executed successfully.")
        return
    if results[0] == 0:
        context.log.info(f"Job {job_name} executed successfully.")
    else:
        context.log.error(f"Job {job_name} not executed, fail.")

@asset(key_prefix= ["DL_FARINTER"]
        , tags={"replicas_sap": "true","periodo": "diario","periodo_unico": "por_hora"}
        , deps=store_procedure_assets+list([DL_SAP_T001])+list([dbt_sap_etl.dbt_sap_etl_dwh_assets])
        , freshness_policy= FreshnessPolicy(maximum_lag_minutes=60*26, cron_schedule="0 10-16 * * *", cron_schedule_timezone="America/Tegucigalpa")
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
    #check if sp returned 0 for errors
    if results is None:
        context.log.info(f"Job {job_name} executed successfully.")
        return
    if results[0] == 0:
        context.log.info(f"Job {job_name} executed successfully.")
    else:
        context.log.error(f"Job {job_name} not executed, fail.")

if __name__ == '__main__':
    ##testing
    from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_dl
    from dagster import build_asset_context
    ##
    #print(generate_store_procedure_assets())
    DL_SAP_T001(context=build_asset_context(), dwh_farinter_dl=dwh_farinter_dl)