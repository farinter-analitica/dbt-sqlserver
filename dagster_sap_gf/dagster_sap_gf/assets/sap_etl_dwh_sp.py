from typing import Any, Dict, List, Sequence

from dagster import (
    AssetChecksDefinition,
    AssetKey,
    AssetOut,
    AssetsDefinition,
    Output,
    asset,
    load_asset_checks_from_current_module,
    multi_asset,
)

from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_variables import tags_repo

()
dl_farinter_db = "DL_FARINTER"
dl_farinter_assets_prefix = [dl_farinter_db]
# @asset(key_prefix= dl_farinter_assets_prefix)
# def DL_Kielsa_Sucursal(dwh_farinter_dl: SQLServerResource) -> None:
#     dwh_farinter_dl.execute_and_commit("EXEC [DL_FARINTER].[dbo].[DL_paCargarKielsa_Sucursal]")
#     #time.sleep(240)
#     #return result

# @asset(key_prefix= dl_farinter_assets_prefix)
# def DL_Kielsa_Bodega(dwh_farinter_dl: SQLServerResource) -> None:
#     dwh_farinter_dl.execute_and_commit("EXEC [DL_FARINTER].[dbo].[DL_paCargarKielsa_Bodega]")

store_procedures: Dict[str, Dict[str, Any]] = {
    "BI_paCargarSAP_Hecho_Facturas_Posiciones": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": ["BI_SAP_Hecho_Facturas_Posiciones_Actual","BI_SAP_Hecho_Facturas_Posiciones_Archivo","BI_SAP_Hecho_Facturas_Posiciones_Reciente"],
        "group_name": "sap_mart_ventas",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [AssetKey(["SAPPRD", "prd", "MARA"]),
                 AssetKey(["SAPPRD", "prd", "T001"]),
                 AssetKey(["SAPPRD", "prd", "T001K"]),
                 AssetKey(["SAPPRD", "prd", "T001W"]),
                 AssetKey(["SAPPRD", "prd", "T006A"]),
                 AssetKey(["SAPPRD", "prd", "VBRK"]),
                 AssetKey(["SAPPRD", "prd", "VBRP"]),
                 AssetKey(["DL_FARINTER", "dbo", "DL_paCargarSAP_REPLICA_SD"]),
                 AssetKey(["DL_FARINTER", "dbo", "DL_paCargarSAP_REPLICA_MM"])],
    },
    "BI_paCargarSAP_Dim_Facturas": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "group_name": "sap_mart_ventas",
        "name": ["BI_SAP_Dim_Facturas_Actual","BI_SAP_Dim_Facturas_Archivo","BI_SAP_Dim_Facturas_Reciente"],
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_SAP_T001"]),
                 AssetKey(["DL_FARINTER", "dbo", "DL_SAP_VBRK"]),
                 AssetKey(["DL_FARINTER", "dbo", "DL_SAP_VBRP"]),
                 AssetKey(["DL_FARINTER", "dbo", "DL_paCargarSAP_REPLICA_SD"]),
                 AssetKey(["DL_FARINTER", "dbo", "DL_paCargarSAP_REPLICA_MM"])],
    },
    "DL_paCargarSAP_Acum_PagosCreditoVentasHist": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_SAP_Acum_PagosCreditoVentasHist",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [AssetKey(["SAPPRD", "prd", "VBRK"]),
                 AssetKey(["SAPPRD", "prd", "BSID"]),
                 AssetKey(["SAPPRD", "prd", "BSAD"])],
    },
    "DL_paCargarSAP_Acum_JerarquiaClientes": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_SAP_Acum_JerarquiaClientes",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [AssetKey(["SAPPRD", "prd", "KNB1"]),
                 AssetKey(["SAPPRD", "prd", "KNVV"]),
                 AssetKey(["SAPPRD", "prd", "TVKO"])],
    },
    "AN_paCargarSAP_Cal_ClientesRecordCredito": {
        "key_prefix": ["AN_FARINTER", "dbo"],  
        "name": "AN_SAP_Cal_ClientesRecordCredito",
        "group_name": "sap_mart_analitica",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_SAP_Acum_PagosCreditoVentasHist"])],
    },
    "AN_paCargarSAP_Cal_ClientesPendientePago": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": "AN_SAP_Cal_ClientesPendientePago",
        "group_name": "sap_mart_analitica",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_SAP_Acum_PagosCreditoVentasHist"])],
    },
    "AN_paCargarSAP_VentaCero": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": "AN_SAP_Cal_VentaCero",
        "group_name": "sap_mart_analitica",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,     
        "deps": [AssetKey(["AN_FARINTER", "dbo", "AN_SAP_Cal_ClientesPendientePago"]),
                 AssetKey(["AN_FARINTER", "dbo", "AN_SAP_Cal_ClientesRecordCredito"]),
                 AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Calendario"]),
                 AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Vendedor_SAP"]),
                 AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_CreditoHist_SAP"]),
                 AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_CondicionPago"]),
                 AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Mixto_Facturas"]),
                 AssetKey(["DL_FARINTER", "dbo", "DL_MSC_VisitasAClientes_ExcelTemp"]),
                 AssetKey(["DL_FARINTER", "dbo", "DL_SAP_Acum_JerarquiaClientes"]),
                 AssetKey(["DL_FARINTER", "dbo", "DL_SAP_Atributos_Cliente"]),
                 AssetKey(["DL_FARINTER", "dbo", "DL_SAP_Atributos_Cliente_CategoriasDistribucion"]),],
    },


}   


def create_store_procedure_asset(stored_procedure_name: str, params: Dict) -> AssetsDefinition:
    if params.get("group_name", None) is None:
        group_name = "sap_etl_dwh"
    else:
        group_name = params["group_name"]
    if not isinstance(params["name"], List):
        @asset(key_prefix= params["key_prefix"], name=params["name"],
                tags=params.get("tags", None), 
                deps=params.get("deps", None),
                group_name= group_name,
                compute_kind="sqlserver",
                description=f"EXEC [{params["key_prefix"][0]}].[{params["key_prefix"][1]}].[{stored_procedure_name}]")
        def store_procedure_execution_asset(dwh_farinter_dl: SQLServerResource) -> None: 
            dwh_farinter_dl.execute_and_commit(f"EXEC [{params["key_prefix"][0]}].[{params["key_prefix"][1]}].[{stored_procedure_name}]")

        return store_procedure_execution_asset

    else:
        @multi_asset(name=stored_procedure_name,
                        outs={name : AssetOut(key_prefix= params["key_prefix"],
                                              tags=params.get("tags", None),
                                              )
                              for name in params["name"]},
                        description=f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]",
                        deps=params.get("deps", None),
                        group_name=group_name, 
                        compute_kind="sqlserver",
                      )
        def store_procedure_execution_asset(dwh_farinter_dl: SQLServerResource): 
            dwh_farinter_dl.execute_and_commit(f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]")

            for name in params["name"]:
                yield Output(value=None, output_name=name) 

        return store_procedure_execution_asset
    

def store_procedure_asset_factory(store_procedures: Dict) -> List[AssetsDefinition]:
    return [create_store_procedure_asset(stored_procedure_name=sp
                                         , params=params) for sp, params in store_procedures.items()]


all_assets = store_procedure_asset_factory(store_procedures=store_procedures)

all_asset_checks: Sequence[AssetChecksDefinition] = tuple(load_asset_checks_from_current_module())
