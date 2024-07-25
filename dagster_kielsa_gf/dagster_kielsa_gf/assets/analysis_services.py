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
)
def olap_ventas_kielsa_ejecucion(context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource) -> None:
    if env_str == "prd": 
        dwh_farinter_dl.execute_and_commit(f"EXEC msdb.dbo.sp_start_job @job_name = 'OLAP VENTAS KIELSA';")
    else:
        context.log.info("No se ejecuta el job de OLAP VENTAS KIELSA en dev")


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
