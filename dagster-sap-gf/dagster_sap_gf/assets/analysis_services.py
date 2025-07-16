from typing import Sequence

from dagster import (
    AssetChecksDefinition,
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    asset,
    load_asset_checks_from_current_module,
    load_assets_from_current_module,
)

from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_variables import env_str, tags_repo


@asset(
    key_prefix=["DWH_204", "SSAS"],
    tags=tags_repo.Daily.tag | tags_repo.AutomationHourly.tag,
    deps=[
        AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_Ventas_SAP"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_InventariosHist_SAP"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_TipoCliente_SAP"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Factura_SAP"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Almacen_SAP"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Centro_SAP"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Sector_SAP"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Articulo_SAP"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Cliente_SAP"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Lote_SAP"]),
    ],
    description="EXEC msdb.dbo.sp_start_job @job_name = 'SAP_OLAP_PLANNING_CadaHora';",
)
def olap_planning_sap_ejecucion(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    if env_str == "prd":
        dwh_farinter_dl.execute_and_commit(
            "EXEC msdb.dbo.sp_start_job @job_name = 'SAP_OLAP_PLANNING_CadaHora';"
        )
    else:
        context.log.info("No se ejecuta el job de OLAP VENTAS KIELSA en dev")


if __name__ == "__main__":
    all_assets = tuple(load_assets_from_current_module(group_name="analysis_services"))
    print(
        [asset.keys for asset in all_assets if isinstance(asset, AssetChecksDefinition)]
    )
    print([asset.key for asset in all_assets if isinstance(asset, AssetSpec)])


else:
    all_assets = tuple(load_assets_from_current_module(group_name="analysis_services"))

    all_asset_checks: Sequence[AssetChecksDefinition] = (
        load_asset_checks_from_current_module()
    )
