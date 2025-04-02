from typing import Sequence

from dagster import (
    AssetChecksDefinition,
    AssetExecutionContext,
    AssetKey,
    Field,
    AssetSpec,
    asset,
    load_asset_checks_from_current_module,
    load_assets_from_current_module,
)

from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_variables import env_str, tags_repo
from dagster_shared_gf.automation import automation_hourly_delta_12_cron


@asset(
    key_prefix=["DWH_204", "SSAS"],
    tags=tags_repo.Daily.tag,
    deps=[
        AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_RegaliasHist_Kielsa"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_VentasHist_Kielsa"]),
        AssetKey(["AN_FARINTER", "dbo", "AN_Param_Pesos_Kielsa"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_PesosSemana_Kielsa"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_ProyeccionVentas_Kielsa"]),
        AssetKey(["AN_FARINTER", "dbo", "AN_Param_PesosDesc_Kielsa"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_ProyeccionDescuentoCupon_Kielsa"]),
    ],
    description="EXEC msdb.dbo.sp_start_job @job_name = 'OLAP VENTAS KIELSA';",
)
def olap_ventas_kielsa_ejecucion(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    if env_str == "prd":
        dwh_farinter_dl.execute_and_commit(
            "EXEC msdb.dbo.sp_start_job @job_name = 'OLAP VENTAS KIELSA';"
        )
    else:
        context.log.info("No se ejecuta el job de OLAP VENTAS KIELSA en dev")


@asset(
    key_prefix=["DWH_TABULAR", "SSAS"],
    tags=tags_repo.Daily.tag | tags_repo.AutomationHourly.tag,
    deps=[
        AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Hecho_FacturaPosicion"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Hecho_FacturaEncabezado"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Dim_Monedero"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Dim_Articulo"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Agr_Sucursal_PartDiaSemana"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Hecho_ProyeccionVenta_SucCanArt"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Dim_Empleado"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Calendario_Dinamico"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Dim_Sucursal"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_ExistenciasHist_Kielsa"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_KPP_Hecho_Suscripcion_Hist"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_KPP_Hecho_Suscripcion_Actual"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Hecho_Regalia_Detalle"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Hecho_Receta"]),
    ],
    description="EXEC msdb.dbo.sp_start_job @job_name = 'Kielsa_Tabular_General' ó EXEC msdb.dbo.sp_start_job @job_name = 'Kielsa_Tabular_General_CadaHora';",
    config_schema={
        "hourly": Field(bool, is_required=False, default_value=False),
        "daily": Field(bool, is_required=False, default_value=False),
    },
    automation_condition=automation_hourly_delta_12_cron,
)
def olap_tabular_kielsa_general_ejecucion(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    if env_str == "prd":
        run_tags_keys = context.run_tags.keys()
        if (
            tags_repo.Hourly.key in run_tags_keys
            or tags_repo.AutomationHourly.key in run_tags_keys
            or context.op_execution_context.op_config.get("hourly")
        ):
            dwh_farinter_dl.execute_and_commit(
                "EXEC msdb.dbo.sp_start_job @job_name = 'Kielsa_Tabular_General_CadaHora';"
            )
        elif (
            tags_repo.Daily.key in run_tags_keys
            or tags_repo.AutomationDaily.key in run_tags_keys
            or context.op_execution_context.op_config.get("daily")
        ):
            dwh_farinter_dl.execute_and_commit(
                "EXEC msdb.dbo.sp_start_job @job_name = 'Kielsa_Tabular_General';"
            )
        else:
            context.log.error(
                "No se ejecuta el job de OLAP Tabular sin especificar el tipo de ejecución."
            )
    else:
        context.log.info(f"No se ejecuta el job de OLAP VENTAS KIELSA en {env_str}")


@asset(
    key_prefix=["DWH_204", "SSAS"],
    tags=tags_repo.Daily.tag | tags_repo.AutomationHourly.tag,
    deps=[
        AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_Sugeridos_Kielsa"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_SugeridosResumen_Kielsa"]),
    ],
    description="EXEC msdb.dbo.sp_start_job @job_name = 'OLAP SUGERIDOS KIELSA';",
    automation_condition=automation_hourly_delta_12_cron,
)
def olap_sugeridos_kielsa_ejecucion(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    if env_str == "prd":
        dwh_farinter_dl.execute_and_commit(
            "EXEC msdb.dbo.sp_start_job @job_name = 'OLAP SUGERIDOS KIELSA';"
        )
    else:
        context.log.info("No se ejecuta el job de OLAP SUGERIDOS KIELSA en dev")


@asset(
    key_prefix=["DWH_204", "SSAS"],
    tags=tags_repo.Daily.tag
    | tags_repo.AutomationHourly.tag
    | tags_repo.HourlyAdditional.tag,
    deps=[
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Articulo_Kielsa"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_ArticuloPadre_Kielsa"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Ciudad_Kielsa"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Pais"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Sucursal_Kielsa"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_SucursalDest_Kielsa"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_Existencias_Kielsa"]),
    ],
    description="EXEC msdb.dbo.sp_start_job @job_name = 'OLAP EXISTENCIAS KIELSA';",
    automation_condition=automation_hourly_delta_12_cron,
)
def olap_existencias_kielsa_ejecucion(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    if env_str == "prd":
        dwh_farinter_dl.execute_and_commit(
            "EXEC msdb.dbo.sp_start_job @job_name = 'OLAP EXISTENCIAS KIELSA';"
        )
    else:
        context.log.info("No se ejecuta el job de OLAP EXISTENCIAS KIELSA en dev")


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
