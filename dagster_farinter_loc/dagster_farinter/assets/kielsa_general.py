from dagster import asset , AssetExecutionContext
from dagster_farinter.resources.sql_server_resources import SQLServerResource

@asset
def select_top_facturaposicion(dwh_farinter_dl: SQLServerResource):

    result = dwh_farinter_dl.query("SELECT TOP 10 * FROM DL_Kielsa_FacturaPosicionDescuento")
    return result


