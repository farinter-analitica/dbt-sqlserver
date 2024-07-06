from dagster import asset, AssetKey , AssetExecutionContext
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
import time

dl_farinter_assets_prefix = ["DL_FARINTER"]
@asset(key_prefix= dl_farinter_assets_prefix)
def DL_Kielsa_Sucursal(dwh_farinter_dl: SQLServerResource) -> None: 
    dwh_farinter_dl.execute_and_commit("EXEC [DL_FARINTER].[dbo].[DL_paCargarKielsa_Sucursal]")
    #time.sleep(240)
    #return result

@asset(key_prefix= dl_farinter_assets_prefix)
def DL_Kielsa_Bodega(dwh_farinter_dl: SQLServerResource) -> None: 
    dwh_farinter_dl.execute_and_commit("EXEC [DL_FARINTER].[dbo].[DL_paCargarKielsa_Bodega]")



