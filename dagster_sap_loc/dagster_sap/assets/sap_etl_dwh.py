from dagster import asset, AssetKey , AssetExecutionContext 
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from pathlib import Path
from typing import List, Dict, Any
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


if __name__ == '__main__':
    ##testing
    from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_dl
    from dagster import build_asset_context
    ##
    print(DL_SAP_T001(context=build_asset_context(),dwh_farinter_dl=dwh_farinter_dl))