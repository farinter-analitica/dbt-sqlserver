from dagster import (asset, AssetKey , load_assets_from_current_module, load_asset_checks_from_current_module, build_last_update_freshness_checks, AssetChecksDefinition, AssetsDefinition, multi_asset, AssetSpec, Output)
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_functions import filter_assets_by_tags, get_all_instances_of_class
from dagster_shared_gf.shared_variables import TagsRepositoryGF, env_str
from datetime import timedelta
from typing import Sequence, List, Mapping, Dict, Any

tags_repo = TagsRepositoryGF()
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
        "tags": tags_repo.Daily.tag,
    },
    "BI_paCargarHecho_ExistenciasHist_Kielsa": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_ExistenciasHist_Kielsa",
        "tags": tags_repo.Daily.tag,
    },
    "DL_paCargarKielsa_Bodega": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Bodega",
        "tags": tags_repo.Daily.tag,
    },
    "DL_paCargarKielsa_Articulo_x_Bodega": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Articulo_x_Bodega",
        "tags": tags_repo.Daily.tag,
    },
    "DL_paCargarKielsa_Articulo_x_Compra": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Articulo_x_Compra",
        "tags": tags_repo.Daily.tag,
    },
    "DL_paCargarKielsa_FacturasPosiciones": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_FacturasPosiciones",
        "tags": tags_repo.Daily.tag,
    },
    "DL_paCargarKielsa_FacturaPosicionDescuento": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_FacturaPosicionDescuento",
        "tags": tags_repo.Daily.tag,    
    },
    "DL_paCargarKielsa_FacturaEncabezado": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_FacturaEncabezado",
        "tags": tags_repo.Daily.tag,
    },
    "DL_paCargarKielsa_Bitacora_Cambio_Precio": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Bitacora_Cambio_Precio",
        "tags": tags_repo.Daily.tag,
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
    "DL_paCargarKielsa_Precios": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Precios",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaPosicionDescuento"]),
                 AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Descuento_Venta"]),
                 AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Descuento_Venta_Articulo"]),
                 AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Bitacora_Cambio_Precio"])],
    },
    "DL_paCargarKielsa_Articulo_Proveedor": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Articulo_Proveedor",
        "tags": tags_repo.Daily.tag,
    },
    "DL_paCargarKielsa_Precios": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Precios",
        "tags": tags_repo.Daily.tag,
    },
    "BI_paCargarHecho_VentasHist_Kielsa_V2": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_VentasHist_Kielsa_V2",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturasPosiciones"]), 
                 AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaEncabezado"]),
                 AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaPosicionDescuento"])],
    },
    "DL_paCargarKielsa_BoletaLocal": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": ["DL_Kielsa_BoletaLocal_Encabezado","DL_Kielsa_BoletaLocal_Detalle"],
        "tags": tags_repo.Daily.tag,
    },
    "DL_paCargarKielsa_Articulo_ProveedorEstadistico_Hist": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Articulo_ProveedorEstadistico_Hist",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturasPosiciones"])],
    },
    "DL_paCargarKielsa_Articulo_Segmentado": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Articulo_Segmentado",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturasPosiciones"]),
                 AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo"])],
    },
    "DL_paCargarKielsa_ArticuloSucursal_Segmentado": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_ArticuloSucursal_Segmentado",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturasPosiciones"]),
                 AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Articulo"]),
                 AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Sucursal"])],
    },
    "AN_paCargarCal_ClientesEstadisticas_Kielsa": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": "AN_Cal_ClientesEstadisticas_Kielsa",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturasPosiciones"]), 
                 AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaEncabezado"])],
    },
    "DL_paCargarKielsa_Empleado": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_Empleado",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaEncabezado"])],
    },
    "BI_paCargarDim_Empleado_Kielsa": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_Empleado_Kielsa",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Empleado"])],
    },
    "DL_paCargarKielsa_VendedorSucursal": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_VendedorSucursal",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaEncabezado"]),
                AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Empleado"]),
                AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Dim_Empresa"])],  
    },
        "BI_paCargarHecho_VentasHist_Kielsa": {
            "key_prefix": ["BI_FARINTER", "dbo"],
            "name": ["BI_Hecho_VentasHist_Kielsa","BI_Hecho_Ventas4MesesHist_Kielsa","BI_Hecho_VentasResumenHist_Kielsa"],
            "tags": tags_repo.Daily.tag,
            "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturasPosiciones"]), 
                     AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaEncabezado"]),
                     AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaPosicionDescuento"])],
    },
    "DL_paCargarAcum_VentasHist_Kielsa": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Acum_VentasHist_Kielsa",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_VentasHist_Kielsa"])],
    },
    "AN_pacargarParam_Pesos_Kielsa": {
        "key_prefix": ["AN_FARINTER", "dbo"],
        "name": "AN_Param_Pesos_Kielsa",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_Ventas4MesesHist_Kielsa"]),
                 AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_VentasResumenHist_Kielsa"])],
    },
    "BI_paCargarCal_PesosProy_Kielsa": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_PesosSemana_Kielsa",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["AN_FARINTER", "dbo", "AN_Param_Pesos_Kielsa"]), AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_VentasHist_Kielsa"])],
    },
    "DL_paCargarKielsa_ClientesVisitasHist": {
        "key_prefix": ["DL_FARINTER", "dbo"],
        "name": "DL_Kielsa_ClientesVisitasHist",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_Acum_VentasHist_Kielsa"])],
    },
    "BI_paCargarHecho_Inventarios_Kielsa": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_Inventarios_Kielsa",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_VentasHist_Kielsa"]),
                 AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Bodega_Kielsa"]),],
    },
}   


def create_store_procedure_asset(stored_procedure_name: str, group_name: str, params: Dict) -> AssetsDefinition:
    if not isinstance(params["name"], List):
        @asset(key_prefix= params["key_prefix"], name=params["name"],
                tags=params.get("tags", None), 
                deps=params.get("deps", None),
                group_name=group_name,
                compute_kind="sqlserver",
                description=f"EXEC [{params["key_prefix"][0]}].[{params["key_prefix"][1]}].[{stored_procedure_name}]")
        def store_procedure_execution_asset(dwh_farinter_dl: SQLServerResource) -> None: 
            dwh_farinter_dl.execute_and_commit(f"EXEC [{params["key_prefix"][0]}].[{params["key_prefix"][1]}].[{stored_procedure_name}]")

        return store_procedure_execution_asset

    else:
        @multi_asset(name=stored_procedure_name,
                    specs=[AssetSpec(key=AssetKey([params["key_prefix"][0], params["key_prefix"][1], name]), 
                                      tags=params.get("tags", None),
                                      deps=params.get("deps", None),
                                      description=f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]") 
                                      for name in params["name"]],
                      group_name=group_name, 
                      compute_kind="sqlserver",)
        def store_procedure_execution_asset(dwh_farinter_dl: SQLServerResource): 
            dwh_farinter_dl.execute_and_commit(f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]")

            for name in params["name"]:
                yield Output(None)

        return store_procedure_execution_asset
    

def store_procedure_asset_factory(store_procedures: Dict) -> List[AssetsDefinition]:
    return [create_store_procedure_asset(stored_procedure_name=sp
                                         , group_name="ldcom_etl_dwh"
                                         , params=params) for sp, params in store_procedures.items()]


all_assets = store_procedure_asset_factory(store_procedures=store_procedures)

all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(all_assets, tags=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"),
    lower_bound_delta=timedelta(hours=26),
    deadline_cron="0 9 * * 1-6",
)
# print(filter_assets_by_tags(all_assets, tags=hourly_tag, filter_type="any_tag_matches"), "\n")
all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(all_assets, tags=tags_repo.Hourly.tag, filter_type="any_tag_matches"),
    lower_bound_delta=timedelta(hours=13),
    deadline_cron="0 10-16 * * 1-6",
)

all_asset_checks: Sequence[AssetChecksDefinition] = load_asset_checks_from_current_module()
all_asset_freshness_checks = all_assets_non_hourly_freshness_checks + all_assets_hourly_freshness_checks
