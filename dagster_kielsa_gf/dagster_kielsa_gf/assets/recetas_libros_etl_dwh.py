from dagster import asset, multi_asset, AssetSpec , AssetKey, load_assets_from_current_module, load_asset_checks_from_current_module, build_last_update_freshness_checks, AssetChecksDefinition
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_functions import filter_assets_by_tags, get_all_instances_of_class
from dagster_shared_gf.shared_variables import TagsRepositoryGF
from datetime import timedelta
from typing import Sequence

tags_repo = TagsRepositoryGF
dl_farinter_assets_prefix = ["DL_FARINTER","dbo"]
#DL_Kielsa_RecetasCabecera, DL_Kielsa_RecetasDetalle, DL_Kielsa_RecetasMedicos
@multi_asset(specs= [AssetSpec(key=AssetKey(dl_farinter_assets_prefix + ["DL_Kielsa_RecetasCabecera"]))
                     ,AssetSpec(key=AssetKey(dl_farinter_assets_prefix + ["DL_Kielsa_RecetasDetalle"]))
                     ,AssetSpec(key=AssetKey(dl_farinter_assets_prefix + ["DL_Kielsa_RecetasMedicos"]))
                    ]
       )
def DL_paCargarKielsa_Recetas(dwh_farinter_dl: SQLServerResource) -> tuple[None, None, None]: 
    dwh_farinter_dl.execute_and_commit("EXEC [DL_FARINTER].[dbo].[DL_paCargarKielsa_Recetas]")

    return None, None, None

@multi_asset(specs= [AssetSpec(key=AssetKey(dl_farinter_assets_prefix + ["DL_Kielsa_Libros_Cliente"]))
                     ,AssetSpec(key=AssetKey(dl_farinter_assets_prefix + ["DL_Kielsa_Libros_Historico"]))
                     ,AssetSpec(key=AssetKey(dl_farinter_assets_prefix + ["DL_Kielsa_Libros_Tipo"]))
                    ]
       )
def DL_paCargarKielsa_Libros(dwh_farinter_dl: SQLServerResource) -> tuple[None, None, None]: 
    dwh_farinter_dl.execute_and_commit("EXEC [DL_FARINTER].[dbo].[DL_paCargarKielsa_Libros]")
    
    return None, None, None

all_assets = load_assets_from_current_module(group_name="recetas_libros_etl_dwh")

all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"),
    lower_bound_delta=timedelta(hours=26),
    deadline_cron="0 9 * * 1-6",
)
#print(filter_assets_by_tags(all_assets, tags=hourly_tag, filter_type="any_tag_matches"), "\n")
all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="any_tag_matches"),
    lower_bound_delta=timedelta(hours=13),
    deadline_cron="0 10-16 * * 1-6",
)

all_asset_checks: Sequence[AssetChecksDefinition] = load_asset_checks_from_current_module()
all_asset_freshness_checks = all_assets_non_hourly_freshness_checks + all_assets_hourly_freshness_checks
