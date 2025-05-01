from typing import Any, Dict, List

from dagster import (
    AssetKey,
    AssetOut,
    AssetsDefinition,
    AutomationCondition,
    Output,
    asset,
    multi_asset,
)

from dagster_shared_gf.automation.tags_mapping import get_mapped_automation_condition
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_variables import tags_repo

store_procedures: Dict[str, Dict[str, Any]] = {
    "BI_paCargarDim_Calendario": {
        "key_prefix": ["BI_FARINTER", "dbo"],
        "name": "BI_Dim_Calendario",
        "group_name": "tiempo",
        "tags": tags_repo.DetenerCarga.tag,
        "deps": [
            AssetKey(["DL_FARINTER", "dbo", "DL_CalendarioBase"]),
        ],
    },
}


def create_store_procedure_asset(
    stored_procedure_name: str, params: Dict
) -> AssetsDefinition:
    tags = params.get("tags", None)
    automation_condition: AutomationCondition | None = None
    if tags:
        automation_condition = get_mapped_automation_condition(tags)

    if params.get("group_name", None) is None:
        group_name = "etl_dwh"
    else:
        group_name = params["group_name"]
    if not isinstance(params["name"], List):

        @asset(
            key_prefix=params["key_prefix"],
            name=params["name"],
            tags=tags,
            deps=params.get("deps", None),
            group_name=group_name,
            compute_kind="sqlserver",
            description=f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]",
            automation_condition=automation_condition,
        )
        def store_procedure_execution_asset(dwh_farinter_dl: SQLServerResource) -> None:
            dwh_farinter_dl.execute_and_commit(
                f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]"
            )

    else:
        final_outs = {}
        if isinstance(params.get("name", None), List):
            final_outs = {
                name: AssetOut(
                    key_prefix=params["key_prefix"],
                    tags=tags,
                    description=f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]",
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
                    description=f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]",
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
