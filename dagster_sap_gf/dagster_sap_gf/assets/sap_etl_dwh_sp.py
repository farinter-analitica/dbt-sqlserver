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
        "name": [
            "BI_SAP_Hecho_Facturas_Posiciones_Actual",
            "BI_SAP_Hecho_Facturas_Posiciones_Archivo",
            "BI_SAP_Hecho_Facturas_Posiciones_Reciente",
        ],
        "group_name": "sap_mart_ventas",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "MARA"]),
            AssetKey(["SAPPRD", "prd", "T001"]),
            AssetKey(["SAPPRD", "prd", "T001K"]),
            AssetKey(["SAPPRD", "prd", "T001W"]),
            AssetKey(["SAPPRD", "prd", "T006A"]),
            AssetKey(["SAPPRD", "prd", "VBRK"]),
            AssetKey(["SAPPRD", "prd", "VBRP"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_paCargarSAP_REPLICA_SD"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_paCargarSAP_REPLICA_MM"]),
        ],
    },
    "BI_paCargarSAP_Dim_Facturas": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "group_name": "sap_mart_ventas",
        "name": [
            "BI_SAP_Dim_Facturas_Actual",
            "BI_SAP_Dim_Facturas_Archivo",
            "BI_SAP_Dim_Facturas_Reciente",
        ],
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_T001"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_VBRK"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_VBRP"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_paCargarSAP_REPLICA_SD"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_paCargarSAP_REPLICA_MM"]),
        ],
    },
    "DL_paCargarSAP_Acum_PagosCreditoVentasHist": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_SAP_Acum_PagosCreditoVentasHist",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "VBRK"]),
            AssetKey(["SAPPRD", "prd", "BSID"]),
            AssetKey(["SAPPRD", "prd", "BSAD"]),
        ],
    },
    "DL_paCargarSAP_Acum_JerarquiaClientes": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_SAP_Acum_JerarquiaClientes",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "KNB1"]),
            AssetKey(["SAPPRD", "prd", "KNVV"]),
            AssetKey(["SAPPRD", "prd", "TVKO"]),
        ],
    },
    "AN_paCargarSAP_Cal_ClientesRecordCredito": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": "AN_SAP_Cal_ClientesRecordCredito",
        "group_name": "sap_mart_analitica",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_Acum_PagosCreditoVentasHist"])
        ],
    },
    "AN_paCargarSAP_Cal_ClientesPendientePago": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": "AN_SAP_Cal_ClientesPendientePago",
        "group_name": "sap_mart_analitica",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_Acum_PagosCreditoVentasHist"])
        ],
    },
    "AN_paCargarSAP_VentaCero": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": "AN_SAP_Cal_VentaCero",
        "group_name": "sap_mart_analitica",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["AN_FARINTER", "dbo", "AN_SAP_Cal_ClientesPendientePago"]),
            AssetKey(["AN_FARINTER", "dbo", "AN_SAP_Cal_ClientesRecordCredito"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Calendario"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Vendedor_SAP"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_CreditoHist_SAP"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_CondicionPago"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Mixto_Facturas"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_MSC_VisitasAClientes_ExcelTemp"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_Acum_JerarquiaClientes"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_Atributos_Cliente"]),
            AssetKey(
                [
                    "DL_FARINTER",
                    "dbo",
                    "DL_SAP_Atributos_Cliente_CategoriasDistribucion",
                ]
            ),
        ],
    },
    "DL_paCargarEdit_AlmacenFP_SAP": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Edit_AlmacenFP_SAP",
        "group_name": "sap_planning_etl",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["PLANNING_DB", "dbo", "Almacenes"]),
        ],
    },
    "DL_paCargarEdit_GrupoPlan_SAP": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Edit_GrupoPlan_SAP",
        "group_name": "sap_planning_etl",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["PLANNING_DB", "dbo", "GposObs"]),
            AssetKey(["PLANNING_DB", "dbo", "GposPlan"]),
            AssetKey(["PLANNING_DB", "dbo", "ParamSocGpo"]),
        ],
    },
    "AN_pa_SAP_ProcesarAlertasCambioPrecios": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": "AN_SAP_Alertas_CambioPrecios",
        "group_name": "sap_planning_etl",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_Precios_SAP"]),
        ],
    },
    "DL_paCargarSAPCRM_Acum_ClientesXLista": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_SAPCRM_Acum_ClientesXLista",
        "group_name": "sap_crm",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["AN_FARINTER", "dbo", "VAN_Cal_AtributosCliente_SAP"]),
            AssetKey(["CRM_FARINTER", "dbo", "CLIENTE_X_PRELISTA"]),
            AssetKey(["CRM_FARINTER", "dbo", "PRE_LISTA"]),
        ],
    },
    "BI_paCargarHecho_CreditoHist_SAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": [
            "BI_Hecho_Credito_SAP",
            "BI_Hecho_CreditoHist_SAP",
        ],
        "group_name": "sap_mart_credito",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_ClienteAreaCredito_Calc"]),
            # AssetKey(["BI_FARINTER", "dbo", "fnc_Fch_Str"]),
            AssetKey(["SAPPRD", "prd", "KNKK"]),
            AssetKey(["SAPPRD", "prd", "KNVV"]),
            AssetKey(["SAPPRD", "prd", "S067"]),
            AssetKey(["SAPPRD", "prd", "T001"]),
            AssetKey(["SAPPRD", "prd", "T052"]),
            AssetKey(["SAPPRD", "prd", "TVKO"]),
        ],
    },
    "BI_paCargarHecho_Precios_SAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_Precios_SAP",
        "group_name": "sap_mart_precios",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "A501"]),
            AssetKey(["SAPPRD", "prd", "A502"]),
            AssetKey(["SAPPRD", "prd", "A503"]),
            AssetKey(["SAPPRD", "prd", "A504"]),
            AssetKey(["SAPPRD", "prd", "A505"]),
            AssetKey(["SAPPRD", "prd", "A506"]),
            AssetKey(["SAPPRD", "prd", "A510"]),
            AssetKey(["SAPPRD", "prd", "A511"]),
            AssetKey(["SAPPRD", "prd", "A512"]),
            AssetKey(["SAPPRD", "prd", "A513"]),
            AssetKey(["SAPPRD", "prd", "A532"]),
            AssetKey(["SAPPRD", "prd", "A533"]),
            AssetKey(["SAPPRD", "prd", "A534"]),
            AssetKey(["SAPPRD", "prd", "A537"]),
            AssetKey(["SAPPRD", "prd", "A542"]),
            AssetKey(["SAPPRD", "prd", "A543"]),
            AssetKey(["SAPPRD", "prd", "A544"]),
            AssetKey(["SAPPRD", "prd", "A545"]),
            AssetKey(["SAPPRD", "prd", "A551"]),
            AssetKey(["SAPPRD", "prd", "A552"]),
            AssetKey(["SAPPRD", "prd", "KONP"]),
            AssetKey(["SAPPRD", "prd", "MARA"]),
            AssetKey(["SAPPRD", "prd", "MVKE"]),
        ],
    },
    "BI_paCargarHecho_ResultadosHist_SAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_ResultadosHist_SAP",
        "group_name": "sap_mart_resultados",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "COBK"]),
            AssetKey(["SAPPRD", "prd", "COEP"]),
            AssetKey(["SAPPRD", "prd", "CSKS"]),
            AssetKey(["SAPPRD", "prd", "FAGLFLEXA"]),
        ],
    },
    "BI_paCargarHecho_Movimiento_SAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_Movimiento_SAP",
        "group_name": "sap_mart_movimientos",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "MARA"]),
            AssetKey(["SAPPRD", "prd", "MBEW"]),
            AssetKey(["SAPPRD", "prd", "MKPF"]),
            AssetKey(["SAPPRD", "prd", "MSEG"]),
            AssetKey(["SAPPRD", "prd", "T001W"]),
        ],
    },
    "BI_paCargarHecho_Pedidos_SAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_Pedidos_SAP",
        "group_name": "sap_mart_pedidos",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Almacen_SAP"]),
            AssetKey(["SAPPRD", "prd", "KNVV"]),
            AssetKey(["SAPPRD", "prd", "T001W"]),
            AssetKey(["SAPPRD", "prd", "VBAK"]),
            AssetKey(["SAPPRD", "prd", "VBAP"]),
        ],
    },
    "DL_paCargaTemp_Recomendacion_SAP": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Temp_Recomendacion_SAP",
        "group_name": "sap_dl_recomendacion",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_Ventas_SAP"]),
        ],
    },
    "BI_paCargarHecho_PedidosCompras_SAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_PedidosCompras_SAP",
        "group_name": "sap_mart_compras",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "A"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_EBAN"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_EKBE"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_EKET"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_EKKN"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_EKKO"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_EKPO"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_LIPS"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_T001"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_T006A"]),
        ],
    },
    "BI_paCargarSAP_Hecho_FlujoFacturacion": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_SAP_Hecho_FlujoFacturacion",
        "group_name": "sap_mart_facturacion",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_LIPS"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_T001"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_T001W"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_VBAK"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_VBAP"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_VBFA"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_VBRK"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_VBRP"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_VBUK"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_ZFAR_SDT_0002"]),
            # AssetKey(["DL_FARINTER", "dbo", "sp_UtilAgregarParticionSiguienteAnioMes"]),
            # AssetKey(["DL_FARINTER", "dbo", "sp_UtilComprimirColumnstoresSiAplica"]),
            # AssetKey(["DL_FARINTER", "dbo", "sp_UtilComprimirIndicesParticionesAnteriores"]),
        ],
    },
    "BI_paCargarSAP_Dim_NotasCredito": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_SAP_Dim_NotasCredito",
        "group_name": "sap_mart_notas_credito",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Calendario"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Sociedad_SAP"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_CanalDist"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_TVSTT"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_VBFA_SiguienteFactura"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_VBRK"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_VBRP"]),
        ],
    },
    "BI_paCargarSAP_FlujoFacturacionResumen": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_SAP_FlujoFacturacionResumen",
        "group_name": "sap_mart_facturacion",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Calendario"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_CanalDist"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_Cliente"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_Facturas"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_NotasCredito"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Hecho_FlujoFacturacion"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_LIKP"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_TVSTT"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_VBAK"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_VBAP"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_VBKD"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_ZFAR_SDT_0001"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_ZFAR_SDT_0002"]),
            # AssetKey(["DL_FARINTER", "dbo", "sp_UtilAgregarParticionSiguienteAnioMes"]),
            # AssetKey(["DL_FARINTER", "dbo", "sp_UtilComprimirColumnstoresSiAplica"]),
            # AssetKey(["DL_FARINTER", "dbo", "sp_UtilComprimirIndicesParticionesAnteriores"]),
        ],
    },
    "DL_paCargarSAP_ClientesXArticulo": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Acum_ClientesXArticulo_SAP",
        "group_name": "sap_dl_clientes_articulos",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_Ventas_SAP"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_Facturas_Archivo"]),
            AssetKey(
                ["BI_FARINTER", "dbo", "BI_SAP_Hecho_Facturas_Posiciones_Archivo"]
            ),
            AssetKey(["DL_FARINTER", "dbo", "DL_paEscBitacora"]),
        ],
    },
    "DL_paCargarSAP_Replica_STXH": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_SAP_STXH",
        "group_name": "sap_dl_replica",
        "tags": tags_repo.Daily.tag,
        "deps": [
            # AssetKey(["DL_FARINTER", "dbo", "sp_UtilAgregarParticionSiguienteAnioMes"]),
        ],
    },
    "DL_paCargarSAP_Hecho_ExistenciasHist": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": [
            "DL_SAP_Hecho_ExistenciasHist_Actual",
            "DL_SAP_Hecho_ExistenciasHist_Archivo",
            "DL_SAP_Hecho_ExistenciasHist_Reciente",
            "DL_SAP_Hecho_ExistenciasLoteHist_Actual",
            "DL_SAP_Hecho_ExistenciasLoteHist_Archivo",
            "DL_SAP_Hecho_ExistenciasLoteHist_Reciente",
        ],
        "group_name": "sap_dl_existencias",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_Lote"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_CnfBitacora"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_EKBE"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_EKKO"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_EKPO"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_MARA"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_MARC"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_MARD"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_MBEW"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_MCH1"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_MCHA"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_MCHB"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_T001"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_T001K"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_T001L"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_T001W"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_TVLK"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_VBBE"]),
            AssetKey(["DL_FARINTER", "dbo", "E"]),
        ],
    },
    "BI_paCargarDim_CasaSAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_Casa_SAP",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "T023T"]),
        ],
    },
    "BI_paCargarDim_CentroCosto_SAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_CentroCosto_SAP",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "CSKT"]),
        ],
    },
    "BI_paCargarDim_CuentaContable_SAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_CuentaContable_SAP",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "CSKU"]),
        ],
    },
    "BI_paCargarDim_ExpedicionSAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_Expedicion_SAP",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "TVSTT"]),
        ],
    },
    "BI_paCargarDim_RegionSAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_Region_SAP",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "T005U"]),
        ],
    },
    "BI_paCargarDim_ZonaSAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_Zona_SAP",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "T171T"]),
        ],
    },
    "BI_paCargarDim_SectorSAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_Sector_SAP",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "T137T"]),
        ],
    },
    "BI_paCargarDim_TipoArt1SAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_TipoArt1_SAP",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "T134T"]),
        ],
    },
    "BI_paCargarDim_TipoArt2SAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_TipoArt2_SAP",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_CanalDist"]),
            AssetKey(["SAPPRD", "prd", "TVTWT"]),
        ],
    },
    "BI_paCargarDim_TipoArt3SAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_TipoArt3_SAP",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_GrupoMaterial"]),
            AssetKey(["SAPPRD", "prd", "T178T"]),
        ],
    },
    "BI_paCargarDim_TipoCliente1SAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_GrupoCliente_SAP",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "T188T"]),
        ],
    },
    "BI_paCargarDim_TipoClienteSAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_TipoCliente_SAP",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "T151T"]),
        ],
    },
    "BI_paCargarDim_TipoFacturaSAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_TipoFactura_SAP",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "TVFKT"]),
        ],
    },
    "BI_paCargarDim_VendedorSAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_Vendedor_SAP",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Edit_VendedorXCanal_SAP"]),
            AssetKey(["SAPPRD", "prd", "TVGRT"]),
        ],
    },
    "BI_paCargarSAP_Dim_GrupoCliente": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_SAP_Dim_GrupoCliente",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "G"]),
            AssetKey(["SAPPRD", "prd", "T151T"]),
        ],
    },
    "BI_paCargarSAP_Dim_GrupoPrecioCliente": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_SAP_Dim_GrupoPrecioCliente",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "G"]),
            AssetKey(["SAPPRD", "prd", "T188T"]),
        ],
    },
    "DL_paCargarSAP_Dim_TipoCambio": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": [
            "DL_SAP_Dim_TipoCambio_Actual",
            "DL_SAP_Dim_TipoCambio_Archivo",
            "DL_SAP_Dim_TipoCambio_Reciente",
        ],
        "group_name": "sap_dl_tipo_cambio",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "A"]),
            AssetKey(["DL_FARINTER", "dbo", "R"]),
            AssetKey(["SAPPRD", "prd", "TCURR"]),
        ],
    },
    "DL_spCargarSAP_Resumen_CartaCompras": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_SAP_Resumen_CartaCompras",
        "group_name": "sap_dl_carta_compras",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "ZMM_CARTA_COMPRO"]),
        ],
    },
    "BI_paCargarDim_Centro_SAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_Centro_SAP",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Region_SAP"]),
            AssetKey(["SAPPRD", "prd", "T001"]),
            AssetKey(["SAPPRD", "prd", "T001K"]),
            AssetKey(["SAPPRD", "prd", "T001W"]),
        ],
    },
    "BI_paCargarDim_MotivoSAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_Motivo_SAP",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "TVAUT"]),
        ],
    },
    "BI_paCargarSAP_Dim_ClienteAreaCredito_Calc": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": [
            "BI_SAP_Dim_ClienteAreaCredito_Calc",
            "BI_SAP_Dim_ClienteAreaCredito_Calc_Hist",
        ],
        "group_name": "sap_mart_clientes",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_ClienteAreaCredito"]),
            AssetKey(["BI_FARINTER", "dbo", "C"]),
            AssetKey(["BI_FARINTER", "dbo", "H"]),
            AssetKey(["SAPPRD", "prd", "KNKK"]),
            AssetKey(
                [
                    "BI_FARINTER",
                    "dbo",
                    "pf_BI_SAP_Dim_ClienteAreaCredito_Calc_Hist_AnioMes",
                ]
            ),
            AssetKey(["SAPPRD", "prd", "S066"]),
            AssetKey(["SAPPRD", "prd", "S067"]),
        ],
    },
    "BI_paCargarSAP_Dim_AreaVentas_OrgCanSec": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": [
            "BI_SAP_Dim_AreaVentas_OrgCanSec",
            "BI_SAP_Dim_AreaVentas_OrgCanSec_Hist",
        ],
        "group_name": "sap_mart_ventas",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "C"]),
            AssetKey(["BI_FARINTER", "dbo", "H"]),
            AssetKey(["SAPPRD", "prd", "T001"]),
            AssetKey(["SAPPRD", "prd", "TVKO"]),
            AssetKey(["SAPPRD", "prd", "TVTA"]),
        ],
    },
    "BI_paCargarDim_Articulo_SAP": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_Articulo_SAP",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "EINA"]),
            AssetKey(["SAPPRD", "prd", "MAKT"]),
            AssetKey(["SAPPRD", "prd", "MARA"]),
            AssetKey(["SAPPRD", "prd", "MVKE"]),
            AssetKey(["SAPPRD", "prd", "T023T"]),
            AssetKey(["SAPPRD", "prd", "T024X"]),
            AssetKey(["SAPPRD", "prd", "T134T"]),
            AssetKey(["SAPPRD", "prd", "T178T"]),
            AssetKey(["SAPPRD", "prd", "TSPAT"]),
            AssetKey(["SAPPRD", "prd", "TWEWT"]),
        ],
    },
    "BI_paCargarSAP_Dim_Cliente": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": ["BI_Dim_Cliente_SAP", "BI_SAP_Dim_Cliente"],
        "group_name": "sap_mart_clientes",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "ADR6"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Region_SAP"]),
            AssetKey(["BI_FARINTER", "dbo", "C"]),
            AssetKey(["SAPPRD", "prd", "CDHDR"]),
            AssetKey(["SAPPRD", "prd", "KNA1"]),
            AssetKey(["SAPPRD", "prd", "KNVV"]),
            AssetKey(["SAPPRD", "prd", "TVAST"]),
            AssetKey(["REPLICASLD", "SITEPLUS", "dbo", "ZT077X"]),
        ],
    },
    "BI_paCargarSAP_Dim_ClienteSociedad": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_SAP_Dim_ClienteSociedad",
        "group_name": "sap_mart_clientes",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "C"]),
            AssetKey(["SAPPRD", "prd", "CDHDR"]),
            AssetKey(["SAPPRD", "prd", "KNB1"]),
            AssetKey(["SAPPRD", "prd", "KNVV"]),
            AssetKey(["SAPPRD", "prd", "T001"]),
            AssetKey(["SAPPRD", "prd", "T052"]),
            AssetKey(["SAPPRD", "prd", "T052U"]),
            AssetKey(["SAPPRD", "prd", "TVKO"]),
        ],
    },
    "BI_paCargarSAP_Dim_ClienteOrganizacionCanalSector": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_SAP_Dim_ClienteOrganizacionCanalSector",
        "group_name": "sap_mart_clientes",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "C"]),
            AssetKey(["SAPPRD", "prd", "CDHDR"]),
            AssetKey(["SAPPRD", "prd", "KNVV"]),
            AssetKey(["SAPPRD", "prd", "T001"]),
            AssetKey(["SAPPRD", "prd", "T052"]),
            AssetKey(["SAPPRD", "prd", "T052U"]),
            AssetKey(["SAPPRD", "prd", "TVKO"]),
        ],
    },
    "BI_paCargarSAP_Dim_ClienteAreaCredito": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": ["BI_SAP_Dim_ClienteAreaCredito", "BI_SAP_Dim_ClienteAreaCredito_Hist"],
        "group_name": "sap_mart_clientes",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "C"]),
            AssetKey(["BI_FARINTER", "dbo", "H"]),
            AssetKey(["SAPPRD", "prd", "KNKK"]),
        ],
    },
    "DL_paCargarSAP_Dim_SociedadCliente": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_SAP_Dim_SociedadCliente",
        "group_name": "sap_dl_clientes",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [
            AssetKey(["SAPPRD", "prd", "KNB1"]),
            AssetKey(["SAPPRD", "prd", "KNVV"]),
            AssetKey(["SAPPRD", "prd", "T001"]),
            AssetKey(["SAPPRD", "prd", "T052"]),
            AssetKey(["SAPPRD", "prd", "T052U"]),
            AssetKey(["SAPPRD", "prd", "TVKO"]),
        ],
    },
    "BI_paCargarSAP_Dim_Lote": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_SAP_Dim_Lote",
        "group_name": "sap_mart_dimensiones",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_SAP_MCH1"]),
            AssetKey(["BI_FARINTER", "dbo", "L"]),
        ],
    },
}


def create_store_procedure_asset(
    stored_procedure_name: str, params: Dict
) -> AssetsDefinition:
    if params.get("group_name", None) is None:
        group_name = "sap_etl_dwh"
    else:
        group_name = params["group_name"]
    if not isinstance(params["name"], List):

        @asset(
            key_prefix=params["key_prefix"],
            name=params["name"],
            tags=params.get("tags", None),
            deps=params.get("deps", None),
            group_name=group_name,
            compute_kind="sqlserver",
            description=f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]",
        )
        def store_procedure_execution_asset(dwh_farinter_dl: SQLServerResource) -> None:
            dwh_farinter_dl.execute_and_commit(
                f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]"
            )

    else:

        @multi_asset(
            name=stored_procedure_name,
            outs={
                name: AssetOut(
                    key_prefix=params["key_prefix"],
                    tags=params.get("tags", None),
                )
                for name in params["name"]
            },
            description=f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]",
            deps=params.get("deps", None),
            group_name=group_name,
            compute_kind="sqlserver",
        )
        def store_procedure_execution_asset(dwh_farinter_dl: SQLServerResource):
            dwh_farinter_dl.execute_and_commit(
                f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]"
            )

            for name in params["name"]:
                yield Output(value=None, output_name=name)

    return store_procedure_execution_asset


def store_procedure_asset_factory(store_procedures: Dict) -> List[AssetsDefinition]:
    return [
        create_store_procedure_asset(stored_procedure_name=sp, params=params)
        for sp, params in store_procedures.items()
    ]


all_assets = store_procedure_asset_factory(store_procedures=store_procedures)

all_asset_checks: Sequence[AssetChecksDefinition] = tuple(
    load_asset_checks_from_current_module()
)
