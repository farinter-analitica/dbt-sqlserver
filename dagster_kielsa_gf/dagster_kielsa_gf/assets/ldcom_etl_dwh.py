from dagster import asset, AssetKey , load_assets_from_current_module, load_asset_checks_from_current_module, build_last_update_freshness_checks, AssetChecksDefinition, AssetsDefinition
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_functions import filter_assets_by_tags, get_all_instances_of_class
from dagster_shared_gf.shared_variables import TagsRepositoryGF
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
    "BI_paCargarHecho_VentasHist_Kielsa_V2": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Hecho_VentasHist_Kielsa_V2",
        "tags": tags_repo.Daily.tag,
        "deps": [AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturasPosiciones"])],
    },
}
# print(store_procedures)
# raise Exception

def create_store_procedure_asset(stored_procedure_name: str, group_name: str, params: Dict) -> AssetsDefinition:
    @asset(key_prefix= params["key_prefix"], name=params["name"], tags=params.get("tags", None), deps=params.get("deps", None),
            group_name=group_name,
            description=f"EXEC [{params["key_prefix"][0]}].[{params["key_prefix"][1]}].[{stored_procedure_name}]")
    def store_procedure_execution_asset(dwh_farinter_dl: SQLServerResource) -> None: 
        dwh_farinter_dl.execute_and_commit(f"EXEC [{params["key_prefix"][0]}].[{params["key_prefix"][1]}].[{stored_procedure_name}]")

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
