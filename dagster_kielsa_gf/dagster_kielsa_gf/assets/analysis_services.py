from dagster import (
    asset,
    AssetKey,
    load_assets_from_current_module,
    load_asset_checks_from_current_module,
    build_last_update_freshness_checks,
    AssetChecksDefinition,
    AssetExecutionContext,
    AssetsDefinition,
    multi_asset,
    AssetSpec,
    Output,
    AssetOut,
    Field,
)
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_functions import (
    filter_assets_by_tags,
    get_all_instances_of_class,
)
from dagster_shared_gf.shared_variables import TagsRepositoryGF as tags_repo, env_str
from datetime import timedelta
from typing import Sequence, List, Mapping, Dict, Any


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
    description="EXEC msdb.dbo.sp_start_job @job_name = 'OLAP VENTAS KIELSA';"
)
def olap_ventas_kielsa_ejecucion(context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource) -> None:
    if env_str == "prd": 
        dwh_farinter_dl.execute_and_commit(f"EXEC msdb.dbo.sp_start_job @job_name = 'OLAP VENTAS KIELSA';")
    else:
        context.log.info("No se ejecuta el job de OLAP VENTAS KIELSA en dev")

@asset(
    key_prefix=["DWH_TABULAR", "SSAS"],
    tags=tags_repo.Daily.tag | tags_repo.Hourly.tag,
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
    ],
    description="EXEC msdb.dbo.sp_start_job @job_name = 'Kielsa_Tabular_General' ó EXEC msdb.dbo.sp_start_job @job_name = 'Kielsa_Tabular_General_CadaHora';",
    config_schema={"hourly": Field(bool, is_required=False, default_value=False),
                   "daily": Field(bool, is_required=False, default_value=False)},

)
def olap_tabular_kielsa_general_ejecucion(context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource) -> None:
    if env_str == "dev" or env_str == "local": 
        if context.job_def.tags.get(tags_repo.Hourly.key) is not None or context.op_execution_context.op_config.get("hourly"):
            dwh_farinter_dl.execute_and_commit(f"EXEC msdb.dbo.sp_start_job @job_name = 'Kielsa_Tabular_General_CadaHora';")
        elif context.job_def.tags.get(tags_repo.Daily.key) is not None or context.op_execution_context.op_config.get("daily"):
            dwh_farinter_dl.execute_and_commit(f"EXEC msdb.dbo.sp_start_job @job_name = 'Kielsa_Tabular_General';")
        else:
            context.log.error("No se ejecuta el job de OLAP Tabular sin especificar el tipo de ejecución.")
    else:
        context.log.info(f"No se ejecuta el job de OLAP VENTAS KIELSA en {env_str}")


all_assets = load_assets_from_current_module(group_name="analysis_services")

all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"),
    lower_bound_delta=timedelta(hours=26),
    deadline_cron="0 9 * * 1-6",
)
# print(filter_assets_by_tags(all_assets, tags=hourly_tag, filter_type="any_tag_matches"), "\n")
all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="any_tag_matches"),
    lower_bound_delta=timedelta(hours=13),
    deadline_cron="0 10-16 * * 1-6",
)

all_asset_checks: Sequence[AssetChecksDefinition] = load_asset_checks_from_current_module()
all_asset_freshness_checks = all_assets_non_hourly_freshness_checks + all_assets_hourly_freshness_checks
