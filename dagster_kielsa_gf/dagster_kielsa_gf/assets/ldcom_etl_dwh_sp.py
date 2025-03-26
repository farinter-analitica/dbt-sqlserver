from dagster import (
    AutomationCondition,
    asset,
    AssetKey,
    load_asset_checks_from_current_module,
    AssetChecksDefinition,
    AssetExecutionContext,
    AssetsDefinition,
    multi_asset,
    Output,
    AssetOut,
)
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_variables import tags_repo
from dagster_shared_gf.automation import get_mapped_automation_condition
from typing import Sequence, List, Dict, Any


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
    "DL_paCargarKielsa_Sucursal": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Sucursal",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Dim_MarcaComercial"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_MarcaComercial_Sucursal"]),
        ],
    },
    # reemplazado por una vista del nuevo existencias en DL
    # "BI_paCargarHecho_ExistenciasHist_Kielsa": {
    #     "key_prefix": ["BI_FARINTER", "dbo"],
    #     "name": "BI_Hecho_ExistenciasHist_Kielsa",
    #     "tags": tags_repo.Daily.tag | tags_repo.DailyUnique.tag,
    # },
    "DL_paCargarKielsa_Bodega": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Bodega",
        "tags": tags_repo.Daily.tag,
    },
    # Ahora condicional por separado
    # "DL_paCargarKielsa_Articulo_x_Bodega": {
    #     "key_prefix": ["DL_FARINTER", "dbo"],
    #     "name": "DL_Kielsa_Articulo_x_Bodega",
    #     "tags": tags_repo.Daily.tag,
    #     "owners": ["cleymer.mendoza@farinter.com"],
    # },
    "DL_paCargarKielsa_Articulo_x_Compra": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Articulo_x_Compra",
        "tags": tags_repo.Daily.tag,
    },
    "DL_paCargarKielsa_Articulo_Info": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Articulo_Info",
        "tags": tags_repo.Daily.tag,
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    "DL_paCargarKielsa_TSP_ABCCadena": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_TSP_ABCCadena",
        "tags": tags_repo.Daily.tag,
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    "DL_paCargarKielsa_Orden_Exterior_Detalle": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Orden_Exterior_Detalle",
        "tags": tags_repo.Daily.tag,
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    "DL_paCargarKielsa_Orden_Exterior_Encabezado": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Orden_Exterior_Encabezado",
        "tags": tags_repo.Daily.tag,
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    "DL_paCargarKielsa_Detalle_Pedido": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Detalle_Pedido",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    # ahora en su propio asset condicional
    # "DL_paCargarKielsa_FacturasPosiciones": {
    #     "key_prefix": ["DL_FARINTER", "dbo"],
    #     "name": "DL_Kielsa_FacturasPosiciones",
    #     "tags": tags_repo.Daily.tag,
    # },
    "DL_paCargarKielsa_Cliente": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Cliente",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
    },
    "DL_paCargarKielsa_Proveedor": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Proveedor",
        "tags": tags_repo.Daily.tag,
    },
    "DL_paCargarKielsa_Alerta": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Alerta",
        "tags": tags_repo.Daily.tag,
    },
    "DL_paCargarKielsa_Articulo_Calc": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Articulo_Calc",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo_Alerta"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_PV_Alerta"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Sucursal"]),
        ],
    },
    # Ahora condicional por separado
    # "DL_paCargarKielsa_FacturaPosicionDescuento": {
    #     "key_prefix": ["DL_FARINTER", "dbo"],
    #     "name": "DL_Kielsa_FacturaPosicionDescuento",
    #     "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
    # },
    "DL_paCargarKielsa_Seg_Rol": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Seg_Rol",
        "tags": tags_repo.Daily.tag,
    },
    # ahora en su propio asset condicional
    # "DL_paCargarKielsa_FacturaEncabezado": {
    #     "key_prefix": ["DL_FARINTER", "dbo"],
    #     "name": "DL_Kielsa_FacturaEncabezado",
    #     "tags": tags_repo.Daily.tag,
    # },
    "DL_paCargarKielsa_Bitacora_Cambio_Precio": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Bitacora_Cambio_Precio",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
    },
    "DL_paCargarKielsa_Descuento_Venta_Articulo": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Descuento_Venta_Articulo",
        "tags": tags_repo.Daily.tag,
    },
    "DL_paCargarKielsa_Descuento_Venta": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Descuento_Venta",
        "tags": tags_repo.Daily.tag,
    },
    "DL_paCargarKielsa_Boleta_CEDI_Detalle": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Boleta_CEDI_Detalle",
        "tags": tags_repo.Daily.tag,
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    "DL_paCargarKielsa_Boleta_CEDI_Encabezado": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Boleta_CEDI_Encabezado",
        "tags": tags_repo.Daily.tag,
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    "DL_paCargarKielsa_Inv_Dev_Proveedor_Detalle": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Inv_Dev_Proveedor_Detalle",
        "tags": tags_repo.Daily.tag,
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    "DL_paCargarKielsa_Inv_Dev_Proveedor_Encabezado": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Inv_Dev_Proveedor_Encabezado",
        "tags": tags_repo.Daily.tag,
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    "DL_paCargarKielsa_Precios": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Precios",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaPosicionDescuento"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Descuento_Venta"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Descuento_Venta_Articulo"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Bitacora_Cambio_Precio"]),
        ],
    },
    "DL_paCargarKielsa_Articulo_Proveedor": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Articulo_Proveedor",
        "tags": tags_repo.Daily.tag,
    },
    "DL_paCargarKielsa_KPP_SMSValidacion": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_KPP_SMSValidacion",
        "tags": tags_repo.Daily.tag,
    },
    "DL_paCargarKielsa_KPP_Suscripcion": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_KPP_Suscripcion",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
    },
    "BI_paCargarHecho_VentasHist_Kielsa_V2": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_VentasHist_Kielsa_V2",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturasPosiciones"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaEncabezado"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaPosicionDescuento"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_SubCategoria1_Articulo"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_SubCategoria2_Articulo"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_SubCategoria3_Articulo"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_SubCategoria4_Articulo"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_TC_ArticuloXMecanica_Kielsa"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Sucursal"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Exp_Factura_Express"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Exp_Orden_Encabezado"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Monedero_Tarjetas_Replica"]),
        ],
    },
    "DL_paCargarKielsa_BoletaLocal": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": ["DL_Kielsa_BoletaLocal_Encabezado", "DL_Kielsa_BoletaLocal_Detalle"],
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
    },
    "DL_paCargarKielsa_Articulo_ProveedorEstadistico_Hist": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Articulo_ProveedorEstadistico_Hist",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturasPosiciones"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaEncabezado"]),
        ],
    },
    "DL_paCargarKielsa_Articulo_Segmentado": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Articulo_Segmentado",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturasPosiciones"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaEncabezado"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo"]),
        ],
    },
    "DL_paCargarKielsa_ArticuloSucursal_Segmentado": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_ArticuloSucursal_Segmentado",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturasPosiciones"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaEncabezado"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Sucursal"]),
        ],
    },
    # Este SP se carga en DEV?
    # "AN_paCargarCal_ClientesEstadisticas_Kielsa": {
    #     "key_prefix": ["AN_FARINTER", "dbo"],
    #     "name": "AN_Cal_ClientesEstadisticas_Kielsa",
    #     "tags": tags_repo.Daily.tag,
    #     "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturasPosiciones"]),
    #              AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaEncabezado"])],
    # },
    "DL_paCargarKielsa_Empleado": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Empleado",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaEncabezado"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Seg_Rol"]),
        ],
    },
    "BI_paCargarDim_Empleado_Kielsa": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_Empleado_Kielsa",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Empleado"])],
    },
    "AN_pacargarClinicaLab_Param_Pesos": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": "AN_ClinicaLab_Param_Pesos",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["AN_FARINTER", "dbo", "AN_Param_Feriados_Kielsa"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Tiempo"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Hecho_Ventas_ClinicaLab"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Dim_Cliente"]),
        ],
    },
    "BI_paCargarClinicaLab_Hecho_ProyeccionVentas": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_ClinicaLab_Hecho_ProyeccionVentas",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [AssetKey(["AN_FARINTER", "dbo", "AN_ClinicaLab_Param_Pesos"])],
    },
    # "DL_paCargarKielsa_Mov_Inventario_Detalle": {
    #     "key_prefix": ["DL_FARINTER", "dbo"],
    #     "name": "DL_Kielsa_Mov_Inventario_Detalle",
    #     "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
    #     "deps": [AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Dim_Empresa"])],
    # },
    "DL_paCargarKielsa_Mov_Inventario_Encabezado": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Mov_Inventario_Encabezado",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Dim_Empresa"])],
    },
    "DL_paCargarKielsa_Regalia_Encabezado": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Regalia_Encabezado",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
    },
    "DL_paCargarKielsa_Regalia_Detalle": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Regalia_Detalle",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
    },
    "DL_paCargarKielsa_Identificacion_Tributaria": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Identificacion_Tributaria",
        "tags": tags_repo.Daily.tag,
    },
    "DL_paCargarKielsa_VendedorSucursal": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_VendedorSucursal",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaEncabezado"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Empleado"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Dim_Empresa"]),
        ],
    },
    "BI_paCargarHecho_VentasHist_Kielsa": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": [
            "BI_Hecho_VentasHist_Kielsa",
            "BI_Hecho_Ventas4MesesHist_Kielsa",
            "BI_Hecho_VentasResumenHist_Kielsa",
        ],
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturasPosiciones"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaEncabezado"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaPosicionDescuento"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_SubCategoria1_Articulo"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_SubCategoria2_Articulo"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_SubCategoria3_Articulo"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_SubCategoria4_Articulo"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Sucursal"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Exp_Factura_Express"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Exp_Orden_Encabezado"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Monedero_Tarjetas_Replica"]),
        ],
    },
    # Migrado a DBT y hecho columnstore
    # "DL_paCargarAcum_VentasHist_Kielsa": {
    #     "key_prefix": ["DL_FARINTER", "dbo"],
    #     "name": "DL_Acum_VentasHist_Kielsa",
    #     "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
    #     "deps": [AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_VentasHist_Kielsa"])],
    # },
    "AN_paCargarCal_ClientesMetricas_Kielsa": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": [
            "AN_Cal_ClientesMetricas_Kielsa",
            "AN_Cal_ClientesMetricasHist_Kielsa",
            "DL_Cal_MaxFechasHist_ClientesMetricas_Kielsa",
        ],
        "group_name": "kielsa_analitica_atributos",
        "tags": tags_repo.Daily.tag
        | tags_repo.UniquePeriod.tag
        | tags_repo.AutomationOnly.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Acum_VentasHist_Kielsa"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_ClientesVisitasHist"]),
        ],
    },
    "AN_paCargarCal_ClientesEstados_Kielsa": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": ["AN_Cal_ClientesEstados_Kielsa", "AN_Cal_ClientesEstadosHist_Kielsa"],
        "group_name": "kielsa_analitica_atributos",
        "tags": tags_repo.Daily.tag
        | tags_repo.UniquePeriod.tag
        | tags_repo.AutomationOnly.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Acum_VentasHist_Kielsa"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_ClientesVisitasHist"]),
        ],
    },
    "AN_paCargarCal_ClientesCategorias_Kielsa": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": [
            "AN_Cal_ClientesCategorias_Kielsa",
            "AN_Cal_ClientesCategoriasDetalle_Kielsa",
            "AN_Cal_ClientesCategoriasHist_Kielsa",
        ],
        "group_name": "kielsa_analitica_atributos",
        "tags": tags_repo.Daily.tag
        | tags_repo.UniquePeriod.tag
        | tags_repo.AutomationOnly.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Acum_VentasHist_Kielsa"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_ExpressVentasHist_Kielsa"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Dim_OrdenExp_Kielsa"]),
        ],
    },
    "AN_paCargarCal_ClientesEstadisticas_Kielsa": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": [
            "AN_Cal_ClientesEstadisticas_Kielsa",
            "AN_Cal_ClientesEstadisticasHist_Kielsa",
        ],
        "group_name": "kielsa_analitica_atributos",
        "tags": tags_repo.Daily.tag
        | tags_repo.UniquePeriod.tag
        | tags_repo.AutomationOnly.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturasPosiciones"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaEncabezado"]),
        ],
    },
    "DL_paCargarAcum_ClientesXLista_CRM": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Acum_ClientesXLista_CRM",
        "group_name": "kielsa_analitica_atributos",
        "tags": tags_repo.Daily.tag
        | tags_repo.UniquePeriod.tag
        | tags_repo.AutomationOnly.tag,
        "deps": [
            AssetKey(["AN_FARINTER", "dbo", "AN_Cal_AtributosCliente_Kielsa"]),
            AssetKey(["CRM_FARINTER", "dbo", "CLIENTE_X_PRELISTA"]),
            AssetKey(["CRM_FARINTER", "dbo", "PRE_LISTA"]),
        ],
    },
    "DL_paCargarKN_ArticulosRecomendados_Kielsa": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "group_name": "kielsa_analitica_atributos",
        "name": "DL_KN_ArticulosRecomendadosSMS_Kielsa",
        "tags": tags_repo.Daily.tag
        | tags_repo.UniquePeriod.tag
        | tags_repo.AutomationOnly.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Acum_VentasHist_Kielsa"]),
            AssetKey(["AN_FARINTER", "dbo", "AN_Cal_AtributosCliente_Kielsa"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_KN_RegistroEncabezadoSMS_Kielsa"]),
        ],
    },
    "DL_paCargarKN_ClientesValidacion_Kielsa": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "group_name": "kielsa_analitica_atributos",
        "name": "DL_KN_ClientesValidacion_Kielsa",
        "tags": tags_repo.Daily.tag
        | tags_repo.UniquePeriod.tag
        | tags_repo.AutomationOnly.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Acum_VentasHist_Kielsa"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_ClientesVisitasHist"]),
        ],
    },
    "DL_paCargarKN_RegistroPeriodicoSMS_Kielsa": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_KN_RegistroPeriodicoSMS_Kielsa",
        "group_name": "kielsa_analitica_atributos",
        "tags": tags_repo.Daily.tag
        | tags_repo.UniquePeriod.tag
        | tags_repo.AutomationOnly.tag,
        "deps": [
            AssetKey(["AN_FARINTER", "dbo", "AN_Cal_AtributosCliente_Kielsa"]),
        ],
    },
    "AN_pacargarParam_Pesos_Kielsa": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": "AN_Param_Pesos_Kielsa",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_Ventas4MesesHist_Kielsa"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_VentasResumenHist_Kielsa"]),
        ],
    },
    "BI_paCargarCal_PesosProy_Kielsa": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_PesosSemana_Kielsa",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["AN_FARINTER", "dbo", "AN_Param_Pesos_Kielsa"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_VentasHist_Kielsa"]),
        ],
    },
    "DL_paCargarKielsa_ClientesVisitasHist": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_ClientesVisitasHist",
        "group_name": "kielsa_analitica_atributos",
        "tags": tags_repo.Daily.tag
        | tags_repo.UniquePeriod.tag
        | tags_repo.AutomationOnly.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Acum_VentasHist_Kielsa"]),
        ],
    },
    "DL_paCargarKielsa_Tipo_Mov_Inventario": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Tipo_Mov_Inventario",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["multi_server_ldcom", "dbo", "multiples_tablas_prd"])],
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    "DL_paCargarKielsa_INV_Demanda": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_INV_Demanda",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["multi_server_ldcom", "dbo", "multiples_tablas_prd"])],
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    "DL_paCargarKielsa_INV_Demanda_Histo": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_INV_Demanda_Histo",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["multi_server_ldcom", "dbo", "multiples_tablas_prd"])],
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    # Ahora condicional por separado
    # "DL_paCargarKielsa_Articulo_x_Sucursal": {
    #     "key_prefix": ["DL_FARINTER", "dbo"],
    #     "name": "DL_Kielsa_Articulo_x_Sucursal",
    #     "tags": tags_repo.Daily.tag ,
    #     "deps": [AssetKey(["multi_server_ldcom", "dbo", "multiples_tablas_prd"])],
    #     "owners": ["cleymer.mendoza@farinter.com"],
    # },
    "DL_paCargarKielsa_Boleta_Exterior_Hist": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Boleta_Exterior_Hist",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [AssetKey(["multi_server_ldcom", "dbo", "multiples_tablas_prd"])],
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    "DL_paCargarKielsa_Orden_Local_Detalle": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Orden_Local_Detalle",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [AssetKey(["multi_server_ldcom", "dbo", "multiples_tablas_prd"])],
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    "DL_paCargarKielsa_Orden_Local_Encabezado": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Orden_Local_Encabezado",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["multi_server_ldcom", "dbo", "multiples_tablas_prd"])],
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    "DL_paCargarKielsa_Inv_Despacho_Detalle": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Inv_Despacho_Detalle",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["multi_server_ldcom", "dbo", "multiples_tablas_prd"])],
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    "DL_paCargarKielsa_Inv_Despacho_Encabezado": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Inv_Despacho_Encabezado",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["multi_server_ldcom", "dbo", "multiples_tablas_prd"])],
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    # Ahora en nuevo proceso con sp existencias
    # "BI_paCargarHecho_Inventarios_Kielsa": {
    #     "key_prefix": ["BI_FARINTER", "dbo"],
    #     "name": "BI_Hecho_Inventarios_Kielsa",
    #     "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
    #     "deps": [AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_VentasHist_Kielsa"]),
    #              AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Bodega_Kielsa"]),],
    # },
    "DL_paCargarKielsa_ClientesXArticulo": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Acum_ClientesXArticulo_Kielsa",
        "tags": tags_repo.Daily.tag
        | tags_repo.UniquePeriod.tag
        | tags_repo.DetenerCarga.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturasPosiciones"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaEncabezado"]),
        ],
        "owners": ["cleymer.mendoza@farinter.com"],
    },
    "BI_paCargarDim_Tiempo": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_Tiempo",
        "tags": tags_repo.Daily.tag,
    },
    ##eCommerce
    "AN_pacargarParam_PesoseCommerce_Kielsa": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": ["AN_Param_PesoseCommerce_Kielsa"],
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(
                ["DL_FARINTER", "dbo", "DL_Hecho_VentaseCommerceResumenHist_Kielsa"]
            ),
            AssetKey(["AN_FARINTER", "dbo", "AN_Param_Feriados_Kielsa"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Tiempo"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Hecho_VentaseCommerceHist_Kielsa"]),
        ],
    },
    "BI_paCargarHecho_ProyeccionVentaseCommerce_Kielsa": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_ProyeccionVentaseCommerce_Kielsa",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [AssetKey(["AN_FARINTER", "dbo", "AN_Param_PesoseCommerce_Kielsa"])],
    },
    "DL_paCargarKielsa_MecanicaCanje": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_MecanicaCanje",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_PV_Alerta"])],
    },
    # Ahora en dbt y dagster
    # "BI_paCargarDim_MecanicaCanje_Kielsa": {
    #     "key_prefix": ["BI_FARINTER", "dbo"],
    #     "keys_out": [AssetKey(["BI_FARINTER", "dbo","BI_Dim_MecanicaCanje_Kielsa"]),
    #              AssetKey(["DL_FARINTER", "dbo","DL_TC_ArticuloXMecanica_Kielsa"])],
    #     "tags": tags_repo.Daily.tag | tags_repo.DailyUnique.tag,
    #     "deps": [AssetKey(["multi_server_ldcom", "dbo", "multiples_tablas_prd"]), ### TODO : Cambiar origen externo por interno del DWH
    #              AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_PV_Alerta"]),
    #              AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo_Alerta"]),],
    # },
    "BI_paCargarHecho_IngresosHist_Kielsa": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_IngresosHist_Kielsa",
        "tags": tags_repo.Daily.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo_Calc"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_BoletaLocal_Detalle"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_BoletaLocal_Encabezado"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Sucursal"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_TC_ArticuloXMecanica_Kielsa"]),
            AssetKey(["AN_FARINTER", "dbo", "AN_Cal_ArticulosEstado_Kielsa"]),
        ],
    },
    "BI_paCargarHecho_ComprasHist_Kielsa": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_ComprasHist_Kielsa",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_IngresosHist_Kielsa"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_PV_Alerta"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo_Alerta"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Sucursal"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_TC_ArticuloXMecanica_Kielsa"]),
            AssetKey(
                ["multi_server_ldcom", "dbo", "multiples_tablas_prd"]
            ),  ### TODO : Cambiar origen externo por interno del DWH
            AssetKey(["replicasld_siteplus", "dbo", "TSP_ABCCadena"]),
        ],
    },
    "DL_paCargarTC_CasaXProveedor_Kielsa": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_TC_CasaXProveedor_Kielsa",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Sucursal"]),
            AssetKey(["DL_FARINTER", "dbo", "VDWH_TC_CasaXProveedor_Kielsa"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_ComprasHist_Kielsa"]),
        ],
    },
    "AN_paCargarCal_CasaXProveedor_Kielsa": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": "AN_Cal_CasaXProveedor_Kielsa",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["AN_FARINTER", "dbo", "VDWH_CasaXProveedor_Kielsa"]),
            AssetKey(["AN_FARINTER", "dbo", "VDWH_TC_CasaXProveedor1_Kielsa"]),
            AssetKey(["AN_FARINTER", "dbo", "VDWH_TC_CasaXProveedor2_Kielsa"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_TC_CasaXProveedor_Kielsa"]),
        ],
    },
    "DL_paCargarTE_CDR_Kielsa": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_TE_CDR_Kielsa",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
    },
    "BI_paCargarHecho_RegaliasHist_Kielsa": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_RegaliasHist_Kielsa",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_TC_ArticuloXMecanica_Kielsa"]),
            AssetKey(["AN_FARINTER", "dbo", "AN_Cal_CasaXProveedor_Kielsa"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo_Calc"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo_Info"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Identificacion_Tributaria"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Regalia_Detalle"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Regalia_Encabezado"]),
            AssetKey(["LDCOMHN_LDCOM_KIELSA", "dbo", "FE_Identificacion_Tributaria"]),
            AssetKey(["LDCOMHN_LDCOM_KIELSA", "dbo", "Regalia_Detalle"]),
            AssetKey(["LDCOMHN_LDCOM_KIELSA", "dbo", "Regalia_Encabezado"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Sucursal"]),
            AssetKey(["multi_server_ldcom", "dbo", "multiples_tablas_prd"]),
        ],  ### TODO : Cambiar origen externo por interno del DWH
    },
    "BI_paCargarHecho_ProyeccionVentas_Kielsa": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_ProyeccionVentas_Kielsa",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [AssetKey(["AN_FARINTER", "dbo", "AN_Param_Pesos_Kielsa"])],
    },
    "DL_paCargarKielsa_MetaHist": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_MetaHist",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [AssetKey(["DL_FARINTER", "excel", "DL_Kielsa_MetaHist_Temp"])],
    },
    "DL_paCargarKielsa_Articulo_Alerta": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Articulo_Alerta",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo"])],
    },
    "BI_paCargarHecho_ProyeccionDescuentoCupon_Kielsa": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_ProyeccionDescuentoCupon_Kielsa",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["AN_FARINTER", "dbo", "AN_Param_PesosDesc_Kielsa"]),
        ],
    },
    "DL_paCargarKielsa_Monedero": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Monedero",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Monedero_Tarjetas_Replica"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Monedero_Plan"]),
        ],
    },
    "BI_paCargarHecho_DescuentoCuponHist_Kielsa": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_DescuentoCuponHist_Kielsa",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Monedero_Tarjetas_Replica"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Cliente"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo"]),
            AssetKey(["AN_FARINTER", "dbo", "AN_Cal_CasaXProveedor_Kielsa"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Sucursal"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo_Alerta"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_TC_ArticuloXMecanica_Kielsa"]),
            AssetKey(["LDCOM_KIELSA", "dbo", "Exp_Orden_Encabezado"]),
            AssetKey(["LDCOM_KIELSA", "dbo", "Factura_Detalle_Cupon"]),
            AssetKey(["LDCOM_KIELSA", "dbo", "Factura_Forma_Pago"]),
            AssetKey(["LDCOM_KIELSA", "dbo", "Exp_Factura_Express"]),
            AssetKey(["LDCOM_KIELSA", "dbo", "Ticket_Forma_Pago"]),
            AssetKey(["multi_server_ldcom", "dbo", "multiples_tablas_prd"]),
        ],  ### TODO : Cambiar origen externo por interno del DWH
    },
    "AN_pacargarParam_PesosDesc_Kielsa": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": "AN_Param_PesosDesc_Kielsa",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_DescuentoCuponHist_Kielsa"]),
            AssetKey(["AN_FARINTER", "dbo", "AN_Param_Feriados_Kielsa"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Tiempo"]),
        ],
    },
    "DL_paCargarTC_Sugeridos_Kielsa": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_TC_Sugeridos_Kielsa",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
    },
    "BI_paCargarHecho_Sugeridos_Kielsa": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": ["BI_Hecho_Sugeridos_Kielsa", "BI_Hecho_SugeridosResumen_Kielsa"],
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [
            AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Dim_Articulo"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Dim_Sucursal"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_TC_Sugeridos_Kielsa"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Boleta_Exterior_Hist"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_BoletaLocal_Encabezado"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_BoletaLocal_Detalle"]),
            AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Calendario"]),
        ],
    },
    "AN_paCargarCal_ArticulosEstado_Kielsa": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": "AN_Cal_ArticulosEstado_Kielsa",
        "tags": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag,
        "deps": [AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_Sugeridos_Kielsa"])],
    },
    "DL_paCargarKielsa_ExistenciaHist": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_ExistenciaHist",
        "tags": tags_repo.Daily.tag | tags_repo.Hourly.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo_x_Bodega"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo_x_Sucursal"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Sucursal"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_TC_ArticuloXMecanica_Kielsa"]),
            AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturasPosiciones"]),
        ],
    },
}


def create_store_procedure_asset(
    stored_procedure_name: str, group_name: str, params: Dict
) -> AssetsDefinition:
    tags = params.get("tags", None)
    automation_condition: AutomationCondition | None = None
    if tags:
        automation_condition = get_mapped_automation_condition(tags)

    if (
        not isinstance(params.get("name", []), List)
        and params.get("keys_out", None) is None
    ):

        @asset(
            key_prefix=params["key_prefix"],
            name=params["name"],
            tags=tags,
            deps=params.get("deps", None),
            owners=params.get("owners", None),
            group_name=group_name,
            compute_kind="sqlserver",
            description=f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]",
            automation_condition=automation_condition,
        )
        def store_procedure_execution_asset(
            context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
        ) -> None:
            dwh_farinter_dl.execute_and_commit(
                f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]"
            )

        return store_procedure_execution_asset

    elif (
        isinstance(params.get("name", None), List)
        or params.get("keys_out", None) is not None
    ):
        final_outs = {}
        if isinstance(params.get("name", None), List):
            final_outs = {
                name: AssetOut(
                    key_prefix=params["key_prefix"],
                    tags=tags,
                    owners=params.get("owners", None),
                    automation_condition=automation_condition,
                )
                for name in params["name"]
            }
        elif params.get("keys_out", None) is not None:
            final_outs = {
                current_key.path[-1]: AssetOut(
                    key=current_key,
                    tags=tags,
                    owners=params.get("owners", None),
                    automation_condition=automation_condition,
                )
                for current_key in params["keys_out"]
            }

        @multi_asset(
            name=stored_procedure_name,
            outs=final_outs,
            description=f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]",
            deps=params.get("deps", None),
            group_name=group_name,
            compute_kind="sqlserver",
        )
        def store_procedure_execution_asset(
            context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
        ):
            dwh_farinter_dl.execute_and_commit(
                f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]"
            )

            for name in final_outs:
                yield Output(value=None, output_name=name)

        return store_procedure_execution_asset

    else:
        raise ValueError(f"Invalid params: {params}")


def store_procedure_asset_factory(store_procedures: Dict) -> List[AssetsDefinition]:
    return [
        create_store_procedure_asset(
            stored_procedure_name=sp,
            group_name=params.get("group_name", "ldcom_etl_dwh"),
            params=params,
        )
        for sp, params in store_procedures.items()
    ]


all_assets = store_procedure_asset_factory(store_procedures=store_procedures)

all_asset_checks: Sequence[AssetChecksDefinition] = (
    load_asset_checks_from_current_module()
)
