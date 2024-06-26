from dagster import asset, AssetKey , AssetExecutionContext
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
import time

@asset(key_prefix= ["DL_FARINTER"])
def DL_Kielsa_Sucursal(dwh_farinter_dl: SQLServerResource) -> None: 
    result = dwh_farinter_dl.execute_and_commit("EXEC [DL_FARINTER].[dbo].[DL_paCargarKielsa_Sucursal]")
    time.sleep(240)
    return result


