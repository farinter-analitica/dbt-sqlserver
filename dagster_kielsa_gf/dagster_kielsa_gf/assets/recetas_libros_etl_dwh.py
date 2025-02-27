from dagster import (
    multi_asset,
    AssetSpec,
    AssetKey,
    load_assets_from_current_module,
    load_asset_checks_from_current_module,
    AssetChecksDefinition,
)
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.automation import automation_hourly_delta_12_cron
from dagster_shared_gf.shared_variables import tags_repo
from typing import Sequence


dl_farinter_assets_prefix = ["DL_FARINTER", "dbo"]


# DL_Kielsa_RecetasCabecera, DL_Kielsa_RecetasDetalle, DL_Kielsa_RecetasMedicos
@multi_asset(
    specs=[
        AssetSpec(
            key=AssetKey(dl_farinter_assets_prefix + ["DL_Kielsa_RecetasCabecera"]),
            tags=tags_repo.Hourly
            | tags_repo.AutomationOnly,  # check automation condition on load_assets_from_current_module
        ),
        AssetSpec(
            key=AssetKey(dl_farinter_assets_prefix + ["DL_Kielsa_RecetasDetalle"]),
            tags=tags_repo.Hourly
            | tags_repo.AutomationOnly,  # check automation condition on load_assets_from_current_module
        ),
        AssetSpec(
            key=AssetKey(dl_farinter_assets_prefix + ["DL_Kielsa_RecetasMedicos"]),
            tags=tags_repo.Hourly
            | tags_repo.AutomationOnly,  # check automation condition on load_assets_from_current_module
        ),
    ],
    op_tags=tags_repo.Hourly,  # check automation condition on load_assets_from_current_module
)
def DL_paCargarKielsa_Recetas(
    dwh_farinter_dl: SQLServerResource,
) -> tuple[None, None, None]:
    dwh_farinter_dl.execute_and_commit(
        "EXEC [DL_FARINTER].[dbo].[DL_paCargarKielsa_Recetas]"
    )

    return None, None, None


@multi_asset(
    specs=[
        AssetSpec(
            key=AssetKey(dl_farinter_assets_prefix + ["DL_Kielsa_Libros_Cliente"]),
            tags=tags_repo.Hourly
            | tags_repo.AutomationOnly,  # check automation condition on load_assets_from_current_module
        ),
        AssetSpec(
            key=AssetKey(dl_farinter_assets_prefix + ["DL_Kielsa_Libros_Historico"]),
            tags=tags_repo.Hourly
            | tags_repo.AutomationOnly,  # check automation condition on load_assets_from_current_module
        ),
        AssetSpec(
            key=AssetKey(dl_farinter_assets_prefix + ["DL_Kielsa_Libros_Tipo"]),
            tags=tags_repo.Hourly
            | tags_repo.AutomationOnly,  # check automation condition on load_assets_from_current_module
        ),
    ],
    op_tags=tags_repo.Hourly,  # check automation condition on load_assets_from_current_module
)
def DL_paCargarKielsa_Libros(
    dwh_farinter_dl: SQLServerResource,
) -> tuple[None, None, None]:
    dwh_farinter_dl.execute_and_commit(
        "EXEC [DL_FARINTER].[dbo].[DL_paCargarKielsa_Libros]"
    )

    return None, None, None


all_assets = tuple(
    load_assets_from_current_module(
        group_name="recetas_libros_etl_dwh",
        automation_condition=automation_hourly_delta_12_cron,
    )
)

all_asset_checks: Sequence[AssetChecksDefinition] = (
    load_asset_checks_from_current_module()
)
