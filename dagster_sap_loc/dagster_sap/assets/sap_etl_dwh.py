from dagster import asset, AssetKey , AssetExecutionContext , AssetsDefinition
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from pathlib import Path
from typing import List, Dict, Any, Mapping
from datetime import datetime, date
import time
file_path = Path(__file__).parent.resolve()

@asset(key_prefix= ["DL_FARINTER"])
def DL_SAP_T001(context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource) -> None: 
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

def generate_store_procedure_assets() -> List[AssetsDefinition]:
    store_procedure_assets = []
    for procedure in ["DL_paCargarSAP_REPLICA_DatosMaestros"
                      ,"DL_paCargarSAP_REPLICA_VBFA"
                      ,"DL_paCargarSAP_REPLICA_SD"
                      ,"DL_paCargarSAP_REPLICA_MM"
                      ,"DL_paCargarSAP_REPLICA_WM"
                      ,"DL_paCargarSAP_REPLICA_FI"]:
        @asset(key_prefix= ["DL_FARINTER"]
               , name=procedure
               , tags={"replicas_sap"})
        def store_procedure_execution(context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource) -> None: 
            procedure = procedure.deepcopy()
            database = "DL_FARINTER"
            schema = "dbo"
            final_query = f"EXEC [{database}].[{schema}].[{procedure}];"
            dwh_farinter_dl.execute_and_commit(final_query)

        store_procedure_assets.append(store_procedure_execution)

    return store_procedure_assets

generate_store_procedure_assets()


if __name__ == '__main__':
    ##testing
    from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_dl
    from dagster import build_asset_context
    ##
    print(generate_store_procedure_assets())