import datetime
import dagster as dg
from dagster_kielsa_gf.assets.ldcom_etl_dwh_sp_config import store_procedures
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.automation import get_mapped_automation_condition
from typing import Sequence, List, Dict


def create_store_procedure_asset(
    stored_procedure_name: str, group_name: str, params: Dict
) -> dg.AssetsDefinition:
    tags = params.get("tags", None)
    automation_condition: dg.AutomationCondition | None = None
    if tags:
        automation_condition = get_mapped_automation_condition(tags)

    if (
        not isinstance(params.get("name", []), List)
        and params.get("keys_out", None) is None
    ):

        @dg.asset(
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
            context: dg.AssetExecutionContext, dwh_farinter_dl: SQLServerResource
        ) -> dg.Output:
            momento_inicio = datetime.datetime.now()
            dwh_farinter_dl.execute_and_commit(
                f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]"
            )

            duracion_total = datetime.datetime.now() - momento_inicio

            return dg.Output(
                value=None,
                metadata={
                    "duration_total": duracion_total.total_seconds(),
                },
            )

        return store_procedure_execution_asset

    elif (
        isinstance(params.get("name", None), List)
        or params.get("keys_out", None) is not None
    ):
        final_outs = {}
        if isinstance(params.get("name", None), List):
            final_outs = {
                name: dg.AssetOut(
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
                current_key.path[-1]: dg.AssetOut(
                    key=current_key,
                    tags=tags,
                    description=f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]",
                    owners=params.get("owners", None),
                    automation_condition=automation_condition,
                )
                for current_key in params["keys_out"]
            }

        @dg.multi_asset(
            name=stored_procedure_name,
            outs=final_outs,
            description=f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]",
            deps=params.get("deps", None),
            group_name=group_name,
            compute_kind="sqlserver",
        )
        def store_procedure_execution_asset(
            context: dg.AssetExecutionContext, dwh_farinter_dl: SQLServerResource
        ):
            momento_inicio = datetime.datetime.now()

            dwh_farinter_dl.execute_and_commit(
                f"EXEC [{params['key_prefix'][0]}].[{params['key_prefix'][1]}].[{stored_procedure_name}]"
            )

            duracion_total = datetime.datetime.now() - momento_inicio

            for name in final_outs:
                yield dg.Output(
                    value=None,
                    output_name=name,
                    metadata={"duracion_total": duracion_total.total_seconds()},
                )

        return store_procedure_execution_asset

    else:
        raise ValueError(f"Invalid params: {params}")


def store_procedure_asset_factory(store_procedures: dict) -> list[dg.AssetsDefinition]:
    return [
        create_store_procedure_asset(
            stored_procedure_name=sp,
            group_name=params.get("group_name", "ldcom_etl_dwh"),
            params=params,
        )
        for sp, params in store_procedures.items()
    ]


all_assets = store_procedure_asset_factory(store_procedures=store_procedures)

all_asset_checks: Sequence[dg.AssetChecksDefinition] = (
    dg.load_asset_checks_from_current_module()
)

if __name__ == "__main__":
    from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_dl

    definitions = dg.Definitions(
        assets=all_assets,
        asset_checks=all_asset_checks,
        resources={
            "dwh_farinter_dl": dwh_farinter_dl,
        },
    )
    print(definitions)
