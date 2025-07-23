from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, List, NamedTuple, Sequence

from dagster import (
    AssetChecksDefinition,
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    AssetsDefinition,
    Field,
    asset,
    load_asset_checks_from_current_module,
    load_assets_from_current_module,
    multi_asset,
)

from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_variables import env_str, tags_repo
from dagster_shared_gf import automation

# vars
file_path = Path(__file__).parent.resolve()
tags_rep_dai_hour = tags_repo.Replicas | tags_repo.Daily | tags_repo.AutomationHourly
tags_rep_dai_unique = tags_repo.Replicas | tags_repo.Daily | tags_repo.UniquePeriod


# Helper function to execute stored procedures with parameters
def execute_sp_with_params(
    context: AssetExecutionContext,
    dwh_farinter_dl: SQLServerResource,
    procedure_name: str,
    database: str = "DL_FARINTER",
    schema: str = "dbo",
    params: dict[str, Any] | None = None,
) -> None:
    # Build parameter string if params are provided
    param_str = ""
    if params:
        param_parts = []
        for key, value in params.items():
            if isinstance(value, str):
                param_parts.append(f"@{key}='{value}'")
            else:
                param_parts.append(f"@{key}={value}")
        param_str = ", ".join(param_parts)

    final_query = f"EXEC [{database}].[{schema}].[{procedure_name}] {param_str};"
    context.log.info(f"Executing: {final_query}")

    try:
        dwh_farinter_dl.execute_and_commit(final_query)
        context.log.info(f"Successfully executed {procedure_name}")
    except Exception as e:
        context.log.error(f"Error executing {procedure_name}: {str(e)}")
        raise


@asset(
    key_prefix=["DL_FARINTER", "dbo"],
    tags=tags_repo.Replicas | tags_repo.Daily,
    compute_kind="sqlserver",
)
def DL_SAP_T001(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    table = "DL_SAP_T001"
    database = "DL_FARINTER"
    schema = "dbo"
    sql_file_path = file_path.joinpath(f"sap_etl_dwh_sql/{table}.sql").resolve()

    try:
        with open(sql_file_path, encoding="utf8") as procedure:
            final_query = procedure.read()
    except IOError as e:
        context.log.error(f"Error reading SQL file: {e}")
        return

    try:
        with dwh_farinter_dl.get_connection(database) as conn:
            last_date_updated_query: str = f"""SELECT MAX(Fecha_Actualizado) FROM [{database}].[{schema}].[{table}]"""
            last_date_updated_result: List[NamedTuple] = dwh_farinter_dl.query(
                query=last_date_updated_query,
                connection=conn,  # type: ignore
            )
            last_date_updated: date = date(1900, 1, 1)
            if last_date_updated_result and last_date_updated_result[0][0] is not None:
                try:
                    last_date_updated = datetime.fromisoformat(
                        str(last_date_updated_result[0][0])
                    ).date()
                except ValueError as e:
                    context.log.error(
                        f"Error converting date: {e}, defaulting to {last_date_updated}."
                    )
            final_query = final_query.format(
                last_date_updated=last_date_updated.isoformat()
            )
            dwh_farinter_dl.execute_and_commit(final_query, connection=conn)
    except Exception as e:
        context.log.error(f"Error during database operation: {e}")

    # return last_date_updated


# DL_paCargarSAP_Replica_BSEG
@asset(
    key_prefix=["DL_FARINTER", "dbo"],
    tags=tags_repo.Replicas
    | tags_repo.Daily
    | {
        "dagster/max_runtime": str(
            50 * 60
        )  # max 50 minutes in seconds, then mark it as failed.
    },
    compute_kind="sqlserver",
    config_schema={
        "p_fecha_desde": Field(str, is_required=False, default_value=""),
        "p_actualizar_todo": Field(bool, is_required=False, default_value=False),
    },
)
def DL_SAP_BSEG(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    table = "DL_SAP_BSEG"
    database = "DL_FARINTER"
    schema = "dbo"
    sql_file_path = file_path.joinpath(f"sap_etl_dwh_sql/{table}.sql").resolve()
    supported_envs = [
        "dev"
    ]  # solo trae info desde la base de datos de PRD, otro proceso en SAP la carga en PRD
    if env_str not in supported_envs:
        context.log.info(f"Saltando {table} por que no esta soportada en {env_str}.")
        return
    with open(sql_file_path, encoding="utf8") as procedure:
        final_query = str(procedure.read())
    with dwh_farinter_dl.get_connection(database, autocommit=True) as conn:
        if context.op_execution_context.op_config.get(
            "p_fecha_desde"
        ) != "" and context.op_execution_context.op_config.get("p_fecha_desde"):
            last_date_updated = datetime.fromisoformat(
                context.op_execution_context.op_config.get("p_fecha_desde")
            ).date()
        else:
            last_aniomes_id_query: str = f"""SELECT MAX(AnioMes_Id) FROM [{database}].[{schema}].[{table}] WITH (NOLOCK);"""
            last_aniomes_id_result: List[Any] = dwh_farinter_dl.query(
                query=last_aniomes_id_query,
                connection=conn,  # type: ignore
            )
            last_date_updated_query: str = f"""
                SELECT MAX(AEDAT) FROM [{database}].[{schema}].[{table}] 
                WHERE AnioMes_Id >= {last_aniomes_id_result[0][0] if last_aniomes_id_result else (datetime.now().year * 100 - 5) + 1};
                """
            last_date_updated_result: List[Any] = dwh_farinter_dl.query(
                query=last_date_updated_query,
                connection=conn,  # type: ignore
            )
            last_date_updated: date = (datetime.now() - timedelta(days=5 * 365)).date()
            if last_date_updated_result:
                try:
                    last_date_updated: date = datetime.strptime(
                        last_date_updated_result[0][0], "%Y%m%d"
                    ).date()
                except ValueError:
                    context.log.error(
                        f"Error al convertir la fecha del último registro desde la base de datos, devolviendo por defecto desde fecha {last_date_updated}."
                    )
        final_query = final_query.format(
            p_FechaDesde=last_date_updated.isoformat(),
            p_IndicadorActualizarTodo=int(
                context.op_execution_context.op_config.get("p_actualizar_todo")
            ),
        )
        # print(final_query)
        dwh_farinter_dl.execute_and_commit(final_query, connection=conn)


# Multi-asset for DL_paCargarSAP_REPLICA_DatosMaestros
@multi_asset(
    name="DL_paCargarSAP_REPLICA_DatosMaestros",
    outs={
        "DL_SAP_CSKA": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_CSKS": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_CSKU": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_FAGL_011PC": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_FAGL_011QT": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_FAGL_011SC": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_FAGL_011ZC": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_KNVV": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_SKA1": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_SKAT": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_SKB1": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_T004": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_T011": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_T024B": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_T156": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_TVAG": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_TVST": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_TVSTT": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
    },
    compute_kind="sqlserver",
    op_tags=tags_rep_dai_hour,
    deps=[
        AssetKey(["SAPPRD", "dbo", "CSKA"]),
        AssetKey(["SAPPRD", "dbo", "CSKS"]),
        AssetKey(["SAPPRD", "dbo", "CSKU"]),
        AssetKey(["SAPPRD", "dbo", "FAGL_011PC"]),
        AssetKey(["SAPPRD", "dbo", "FAGL_011QT"]),
        AssetKey(["SAPPRD", "dbo", "FAGL_011SC"]),
        AssetKey(["SAPPRD", "dbo", "FAGL_011ZC"]),
        AssetKey(["SAPPRD", "dbo", "KNVV"]),
        AssetKey(["SAPPRD", "dbo", "MARA"]),
        AssetKey(["SAPPRD", "dbo", "MARC"]),
        AssetKey(["SAPPRD", "dbo", "MARD"]),
        AssetKey(["SAPPRD", "dbo", "MBEW"]),
        AssetKey(["SAPPRD", "dbo", "MCH1"]),
        AssetKey(["SAPPRD", "dbo", "MCHA"]),
        AssetKey(["SAPPRD", "dbo", "MCHB"]),
        AssetKey(["SAPPRD", "dbo", "SKA1"]),
        AssetKey(["SAPPRD", "dbo", "SKAT"]),
        AssetKey(["SAPPRD", "dbo", "SKB1"]),
        AssetKey(["SAPPRD", "dbo", "T004"]),
        AssetKey(["SAPPRD", "dbo", "T011"]),
        AssetKey(["SAPPRD", "dbo", "T024B"]),
        AssetKey(["SAPPRD", "dbo", "T156"]),
        AssetKey(["SAPPRD", "dbo", "TVAG"]),
        AssetKey(["SAPPRD", "dbo", "TVST"]),
        AssetKey(["SAPPRD", "dbo", "TVSTT"]),
    ],
)
def cargar_sap_replica_datos_maestros(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
):
    """
    Loads master data from SAP into the data warehouse.
    This includes data from tables like CSKA, CSKS, CSKU, FAGL_011PC, etc.
    """
    execute_sp_with_params(
        context, dwh_farinter_dl, "DL_paCargarSAP_REPLICA_DatosMaestros"
    )

    # Return None for each output
    return tuple(
        None for _ in cargar_sap_replica_datos_maestros.keys_by_output_name.keys()
    )


# Multi-asset for DL_paCargarSAP_REPLICA_SD
@multi_asset(
    name="DL_paCargarSAP_REPLICA_SD",
    outs={
        "DL_SAP_LIKP": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour | tags_repo.Weekly7,
            automation_condition=automation.automation_weekly_7_delta_1_cron,
        ),
        "DL_SAP_LIPS": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour | tags_repo.Weekly7,
            automation_condition=automation.automation_weekly_7_delta_1_cron,
        ),
        "DL_SAP_VBAK": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour | tags_repo.Weekly7,
            automation_condition=automation.automation_weekly_7_delta_1_cron,
        ),
        "DL_SAP_VBAP": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour | tags_repo.Weekly7,
            automation_condition=automation.automation_weekly_7_delta_1_cron,
        ),
        "DL_SAP_VBBE": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour | tags_repo.Weekly7,
            automation_condition=automation.automation_weekly_7_delta_1_cron,
        ),
        "DL_SAP_VBKD": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour | tags_repo.Weekly7,
            automation_condition=automation.automation_weekly_7_delta_1_cron,
        ),
        "DL_SAP_VBRK": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour | tags_repo.Weekly7,
            automation_condition=automation.automation_weekly_7_delta_1_cron,
        ),
        "DL_SAP_VBRP": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour | tags_repo.Weekly7,
            automation_condition=automation.automation_weekly_7_delta_1_cron,
        ),
        "DL_SAP_VBUK": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour | tags_repo.Weekly7,
            automation_condition=automation.automation_weekly_7_delta_1_cron,
        ),
        "DL_SAP_ZDT_ESTADO_T_NC": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour | tags_repo.Weekly7,
            automation_condition=automation.automation_weekly_7_delta_1_cron,
        ),
        "DL_SAP_ZDT_ZCPV_LOG": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour | tags_repo.Weekly7,
            automation_condition=automation.automation_weekly_7_delta_1_cron,
        ),
        "DL_SAP_ZFAR_SDT_0001": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour | tags_repo.Weekly7,
            automation_condition=automation.automation_weekly_7_delta_1_cron,
        ),
        "DL_SAP_ZFAR_SDT_0002": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour | tags_repo.Weekly7,
            automation_condition=automation.automation_weekly_7_delta_1_cron,
        ),
    },
    compute_kind="sqlserver",
    op_tags=tags_rep_dai_hour | tags_repo.Weekly7,
    deps=[
        AssetKey(["SAPPRD", "dbo", "LIKP"]),
        AssetKey(["SAPPRD", "dbo", "LIPS"]),
        AssetKey(["SAPPRD", "dbo", "VBAK"]),
        AssetKey(["SAPPRD", "dbo", "VBAP"]),
        AssetKey(["SAPPRD", "dbo", "VBBE"]),
        AssetKey(["SAPPRD", "dbo", "VBKD"]),
        AssetKey(["SAPPRD", "dbo", "VBRK"]),
        AssetKey(["SAPPRD", "dbo", "VBRP"]),
        AssetKey(["SAPPRD", "dbo", "VBUK"]),
        AssetKey(["SAPPRD", "dbo", "ZDT_ESTADO_T_NC"]),
        AssetKey(["SAPPRD", "dbo", "ZDT_ZCPV_LOG"]),
        AssetKey(["SAPPRD", "dbo", "ZFAR_SDT_0001"]),
        AssetKey(["SAPPRD", "dbo", "ZFAR_SDT_0002"]),
    ],
)
def cargar_sap_replica_sd(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
):
    """
    Loads Sales and Distribution (SD) data from SAP into the data warehouse.
    This includes data from tables like LIKP, LIPS, VBAK, VBAP, etc.
    """
    # Check if this is a daily run by examining the tags
    is_daily_run = tags_repo.Daily.key in context.run_tags
    is_weekly_run = tags_repo.Weekly.key in context.run_tags

    if is_daily_run and env_str == "prd" or is_weekly_run and env_str == "dev":
        # For daily runs, use specific parameters
        context.log.info(
            "Running daily SD replication with specific parameters for VBAP"
        )
        execute_sp_with_params(
            context,
            dwh_farinter_dl,
            "DL_paCargarSAP_REPLICA_SD",
            params={"ListaActualizar": "VBAP", "ForzarMeses": 3},
        )
    # For regular runs and after daily special, use the standard procedure
    execute_sp_with_params(context, dwh_farinter_dl, "DL_paCargarSAP_REPLICA_SD")

    # Return None for each output
    return tuple(None for _ in cargar_sap_replica_sd.keys_by_output_name.keys())


# Multi-asset for DL_paCargarSAP_REPLICA_MM
@multi_asset(
    name="DL_paCargarSAP_REPLICA_MM",
    outs={
        "DL_SAP_EBAN": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_EKBE": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_EKBE_Entregas": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_EKET": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_EKKN": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_EKKO": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_EKPO": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_MKPF": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
        "DL_SAP_MSEG": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_hour,
            automation_condition=automation.automation_hourly_delta_12_cron,
        ),
    },
    compute_kind="sqlserver",
    op_tags=tags_rep_dai_hour,
    deps=[
        AssetKey(["SAPPRD", "dbo", "EBAN"]),
        AssetKey(["SAPPRD", "dbo", "EKBE"]),
        AssetKey(["SAPPRD", "dbo", "EKET"]),
        AssetKey(["SAPPRD", "dbo", "EKKN"]),
        AssetKey(["SAPPRD", "dbo", "EKKO"]),
        AssetKey(["SAPPRD", "dbo", "EKPO"]),
        AssetKey(["SAPPRD", "dbo", "MKPF"]),
        AssetKey(["SAPPRD", "dbo", "MSEG"]),
    ],
)
def cargar_sap_replica_mm(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
):
    """
    Loads Materials Management (MM) data from SAP into the data warehouse.
    This includes data from tables like EBAN, EKBE, EKET, EKKN, etc.
    """
    execute_sp_with_params(context, dwh_farinter_dl, "DL_paCargarSAP_REPLICA_MM")

    # Return None for each output
    return tuple(None for _ in cargar_sap_replica_mm.keys_by_output_name.keys())


# Multi-asset for DL_paCargarSAP_REPLICA_WM
@multi_asset(
    name="DL_paCargarSAP_REPLICA_WM",
    outs={
        "DL_SAP_LTAK": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"], tags=tags_rep_dai_hour
        ),
        "DL_SAP_LTAP": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"], tags=tags_rep_dai_hour
        ),
    },
    compute_kind="sqlserver",
    op_tags=tags_rep_dai_hour,
    deps=[
        AssetKey(["SAPPRD", "dbo", "DL_SAP_LTAK"]),
        AssetKey(["SAPPRD", "dbo", "DL_SAP_LTAP"]),
    ],
)
def cargar_sap_replica_wm(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
):
    """
    Loads Warehouse Management (WM) data from SAP into the data warehouse.
    This includes data from tables like LTAK, LTAP.
    """
    execute_sp_with_params(context, dwh_farinter_dl, "DL_paCargarSAP_REPLICA_WM")

    # Return None for each output
    return tuple(None for _ in cargar_sap_replica_wm.keys_by_output_name.keys())


# Multi-asset for DL_paCargarSAP_REPLICA_VBFA
@multi_asset(
    name="DL_paCargarSAP_REPLICA_VBFA",
    outs={
        "DL_SAP_VBFA": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_unique | tags_repo.AutomationWeekly7,
            automation_condition=automation.automation_weekly_7_delta_1_cron,
        ),
        "DL_SAP_VBFA_OrigenFactura": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_unique | tags_repo.AutomationWeekly7,
            automation_condition=automation.automation_weekly_7_delta_1_cron,
        ),
        "DL_SAP_VBFA_OrigenPedido": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_unique | tags_repo.AutomationWeekly7,
            automation_condition=automation.automation_weekly_7_delta_1_cron,
        ),
        "DL_SAP_VBFA_SiguienteFactura": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_unique | tags_repo.AutomationWeekly7,
            automation_condition=automation.automation_weekly_7_delta_1_cron,
        ),
    },
    compute_kind="sqlserver",
    op_tags=tags_rep_dai_unique | tags_repo.Weekly7,
    deps=[
        AssetKey(["SAPPRD", "dbo", "DL_SAP_VBRK"]),
        AssetKey(["SAPPRD", "dbo", "DL_SAP_VBFA"]),
        AssetKey(["SAPPRD", "dbo", "DL_SAP_VBAK"]),
        AssetKey(["SAPPRD", "dbo", "DL_SAP_VBRP"]),
        AssetKey(["SAPPRD", "dbo", "DL_SAP_VBAP"]),
    ],
)
def cargar_sap_replica_vbfa(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
):
    """
    Loads document flow data (VBFA) from SAP into the data warehouse.
    This includes relationships between sales documents, deliveries, and invoices.
    """
    # Check if this is a daily run by examining the tags
    is_daily_run = tags_repo.Daily.key in context.run_tags
    is_weekly_run = tags_repo.Weekly.key in context.run_tags

    if is_daily_run and env_str == "prd" or is_weekly_run and env_str == "dev":
        # For daily runs in prd environment, use specific parameters
        context.log.info("Running daily VBFA replication with specific parameters")
        execute_sp_with_params(
            context,
            dwh_farinter_dl,
            "DL_paCargarSAP_REPLICA_VBFA",
            params={"ListaActualizar": "VBFA", "ForzarMeses": 3},
        )
    # For regular runs and after daily special, use the standard procedure
    execute_sp_with_params(context, dwh_farinter_dl, "DL_paCargarSAP_REPLICA_VBFA")

    # Return None for each output
    return tuple(None for _ in cargar_sap_replica_vbfa.keys_by_output_name.keys())


# Multi-asset for DL_paCargarSAP_REPLICA_FI (continued)
@multi_asset(
    name="DL_paCargarSAP_REPLICA_FI",
    outs={
        "DL_SAP_BKPF": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_unique
            | tags_repo.AutomationWeekly7
            | tags_repo.AutomationMonthlyStart,
            automation_condition=automation.automation_weekly_7_delta_1_cron
            | automation.automation_monthly_start_delta_1_cron,
        ),
        "DL_SAP_BSAD": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_unique
            | tags_repo.AutomationWeekly7
            | tags_repo.AutomationMonthlyStart,
            automation_condition=automation.automation_weekly_7_delta_1_cron
            | automation.automation_monthly_start_delta_1_cron,
        ),
        "DL_SAP_BSID": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_unique
            | tags_repo.AutomationWeekly7
            | tags_repo.AutomationMonthlyStart,
            automation_condition=automation.automation_weekly_7_delta_1_cron
            | automation.automation_monthly_start_delta_1_cron,
        ),
        "DL_SAP_COBK": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_unique
            | tags_repo.AutomationWeekly7
            | tags_repo.AutomationMonthlyStart,
            automation_condition=automation.automation_weekly_7_delta_1_cron
            | automation.automation_monthly_start_delta_1_cron,
        ),
        "DL_SAP_COEP": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_unique
            | tags_repo.AutomationWeekly7
            | tags_repo.AutomationMonthlyStart,
            automation_condition=automation.automation_weekly_7_delta_1_cron
            | automation.automation_monthly_start_delta_1_cron,
        ),
        "DL_SAP_FAGLFLEXA": AssetOut(
            key_prefix=["DL_FARINTER", "dbo"],
            tags=tags_rep_dai_unique
            | tags_repo.AutomationWeekly7
            | tags_repo.AutomationMonthlyStart,
            automation_condition=automation.automation_weekly_7_delta_1_cron
            | automation.automation_monthly_start_delta_1_cron,
        ),
    },
    compute_kind="sqlserver",
    op_tags=tags_rep_dai_unique
    | tags_repo.AutomationWeekly7
    | tags_repo.AutomationMonthlyStart,
    deps=[
        AssetKey(["SAPPRD", "dbo", "DL_SAP_BKPF"]),
        AssetKey(["SAPPRD", "dbo", "DL_SAP_BSAD"]),
        AssetKey(["SAPPRD", "dbo", "DL_SAP_BSID"]),
        AssetKey(["SAPPRD", "dbo", "DL_SAP_COBK"]),
        AssetKey(["SAPPRD", "dbo", "DL_SAP_COEP"]),
        AssetKey(["SAPPRD", "dbo", "DL_SAP_FAGLFLEXA"]),
        # AssetKey(["DL_FARINTER", "dbo", "sp_UtilAgregarParticionSiguienteAnioMes"]),
        # AssetKey(["DL_FARINTER", "dbo", "sp_UtilComprimirColumnstoresSiAplica"]),
        # AssetKey(["DL_FARINTER", "dbo", "sp_UtilComprimirIndicesParticionesAnteriores"]),
    ],
)
def cargar_sap_replica_fi(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
):
    """
    Loads Financial Accounting (FI) data from SAP into the data warehouse.
    This includes data from tables like BKPF, BSAD, BSID, COBK, COEP, etc.
    """
    # Lógica semanal y diaria
    is_weekly_run = tags_repo.Weekly.key in context.run_tags
    is_monthly_run = tags_repo.Monthly.key in context.run_tags

    if is_weekly_run and env_str == "prd" or is_monthly_run and env_str == "dev":
        context.log.info(
            "Running FI replication with specific parameters (weekly/daily)"
        )
        execute_sp_with_params(
            context,
            dwh_farinter_dl,
            "DL_paCargarSAP_REPLICA_FI",
            params={"ListaActualizar": "FI", "ForzarMeses": 3},
        )
    # Ejecución estándar
    execute_sp_with_params(context, dwh_farinter_dl, "DL_paCargarSAP_REPLICA_FI")

    # Return None for each output
    return tuple(None for _ in cargar_sap_replica_fi.keys_by_output_name.keys())


@asset(
    key_prefix=["DL_FARINTER", "dbo"],
    tags=tags_rep_dai_unique,
    deps=(
        DL_SAP_T001,
        AssetKey(
            [
                "DL_FARINTER",
                "dbo",
                "SP_Ejecutado_DL_paSecuenciaSAP_HechosDimensiones",
            ]
        ),
    ),
    #        , freshness_policy= FreshnessPolicy(maximum_lag_minutes=60*26, cron_schedule="0 10-16 * * *", cron_schedule_timezone="America/Tegucigalpa") #deprecated
    compute_kind="sqlserver",
)
def sp_start_job_sap_diario(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    job_name = "SAP_Diario"
    final_query = f"""
    DECLARE @job_result int = 0;
    EXECUTE @job_result = msdb.dbo.sp_start_job @job_name = '{job_name}';
    SELECT @job_result as job_result;
    """
    results = dwh_farinter_dl.query(final_query, fetch_val=True)
    # check if sp returned 1 for errors
    if results is None:
        context.log.error(f"Job {job_name} not executed, fail.")
    elif results == 1:
        context.log.error(f"Job {job_name} not executed, fail.")
    elif results == 0:
        context.log.info(f"Job {job_name} executed successfully.")
    else:
        context.log.error(f"Job {job_name} not executed, fail.")


# Helper function to get date parameters
def get_date_params():
    current_date = datetime.now().date()
    prev_month_date = current_date - timedelta(days=30)

    return {
        "anio": current_date.year,
        "mes": current_date.month,
        "anioinicio": prev_month_date.year,
        "mesinicio": prev_month_date.month,
        "usarwhiles": 1,
    }


# Asset for DL_paActualizarClientesTiemposFechaFacturas_SAP
@asset(
    key_prefix=["DL_FARINTER", "dbo"],
    name="DL_ClientesTiemposFechaFacturas_SAP",
    tags=tags_repo.Daily | tags_repo.UniquePeriod.tag,
    compute_kind="sqlserver",
    deps=[
        AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_Ventas_SAP"]),
    ],
)
def actualizar_clientes_tiempos_fecha_facturas(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    """Updates client times and date information from invoices."""
    execute_sp_with_params(
        context, dwh_farinter_dl, "DL_paActualizarClientesTiemposFechaFacturas_SAP"
    )


# Asset for DL_paCargarSAP_Atributos_Cliente
@asset(
    key_prefix=["DL_FARINTER", "dbo"],
    name="DL_SAP_Atributos_Cliente",
    tags=tags_repo.Daily | tags_repo.UniquePeriod.tag,
    compute_kind="sqlserver",
    deps=[
        AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_Facturas"]),
        AssetKey(["DL_FARINTER", "dbo", "DL_ClientesTiemposFechaFacturas_SAP"]),
    ],
)
def cargar_sap_atributos_cliente(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    """Loads client attributes from SAP data."""
    execute_sp_with_params(
        context,
        dwh_farinter_dl,
        "DL_paCargarSAP_Atributos_Cliente",
        params=get_date_params(),
    )


# Asset for DL_paCargarSAP_Atributos_Cliente_Categorias
@asset(
    key_prefix=["DL_FARINTER", "dbo"],
    name="DL_SAP_Atributos_Cliente_CategoriasDistribucion",
    tags=tags_repo.Daily | tags_repo.UniquePeriod.tag,
    compute_kind="sqlserver",
    deps=[
        AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Mixto_Facturas"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Articulo_SAP"]),
        AssetKey(["DL_FARINTER", "dbo", "DL_SAP_Atributos_Cliente"]),
    ],
)
def cargar_sap_atributos_cliente_categorias(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    """Loads client category attributes from SAP data."""
    execute_sp_with_params(
        context,
        dwh_farinter_dl,
        "DL_paCargarSAP_Atributos_Cliente_Categorias",
        params=get_date_params(),
    )


# Asset for DL_paActualizarSAP_Atributos_Cliente
@asset(
    key_prefix=["DL_FARINTER", "dbo"],
    name="DL_SAP_Atributos_Cliente_Actualizado",
    tags=tags_repo.Daily | tags_repo.UniquePeriod.tag,
    compute_kind="sqlserver",
    deps=[
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Cliente_SAP"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Hecho_CreditoHist_SAP"]),
        AssetKey(["DL_FARINTER", "dbo", "DL_CalendarioBase"]),
        AssetKey(["DL_FARINTER", "dbo", "DL_ClientesTiemposFechaFacturas_SAP"]),
        AssetKey(["DL_FARINTER", "dbo", "DL_SAP_Atributos_Cliente"]),
        AssetKey(
            ["DL_FARINTER", "dbo", "DL_SAP_Atributos_Cliente_CategoriasDistribucion"]
        ),
    ],
)
def actualizar_sap_atributos_cliente(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    """Updates client attributes with additional information."""
    execute_sp_with_params(
        context,
        dwh_farinter_dl,
        "DL_paActualizarSAP_Atributos_Cliente",
        params=get_date_params(),
    )


# Asset for DL_paActualizarSAP_Atributos_Cliente_Estadisticas
@asset(
    key_prefix=["DL_FARINTER", "dbo"],
    name="DL_SAP_Atributos_Cliente_Estadisticas",
    tags=tags_repo.Daily | tags_repo.UniquePeriod.tag,
    compute_kind="sqlserver",
    deps=[
        AssetKey(["DL_FARINTER", "dbo", "DL_SAP_Atributos_Cliente"]),
        AssetKey(["DL_FARINTER", "dbo", "DL_SAP_Atributos_Cliente_Actualizado"]),
    ],
)
def actualizar_sap_atributos_cliente_estadisticas(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    """Updates client statistics and trend information."""
    execute_sp_with_params(
        context,
        dwh_farinter_dl,
        "DL_paActualizarSAP_Atributos_Cliente_Estadisticas",
        params=get_date_params(),
    )


# Asset for DL_paCargarSAP_UltimosDatos_Cliente
@asset(
    key_prefix=["DL_FARINTER", "dbo"],
    name="DL_SAP_UltimosDatos_Cliente",
    tags=tags_repo.Daily | tags_repo.UniquePeriod.tag,
    compute_kind="sqlserver",
    deps=[
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Factura_SAP"]),
        AssetKey(["DL_FARINTER", "dbo", "DL_SAP_Atributos_Cliente_Estadisticas"]),
    ],
)
def cargar_sap_ultimos_datos_cliente(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    """Loads the latest client data from SAP."""
    execute_sp_with_params(
        context, dwh_farinter_dl, "DL_paCargarSAP_UltimosDatos_Cliente"
    )


@asset(
    key_prefix=["AN_FARINTER", "dbo"],
    name="AN_Cal_AtributosCliente_SAP",
    tags=tags_repo.Daily | tags_repo.UniquePeriod.tag,
    compute_kind="sqlserver",
    deps=[
        AssetKey(["DL_FARINTER", "dbo", "DL_SAP_UltimosDatos_Cliente"]),
        AssetKey(["DL_FARINTER", "dbo", "DL_SAP_Atributos_Cliente"]),
        AssetKey(["DL_FARINTER", "dbo", "DL_TC_ClienteTicketProm_SAP"]),
        AssetKey(["DL_FARINTER", "dbo", "VDL_SAP_Atributos_Cliente_CategoriasUltPref"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_GrupoMaterial"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Zona_SAP"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Dias"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Vendedor_SAP"]),
        AssetKey(["DL_FARINTER", "dbo", "VDL_SAP_Atributos_Cliente_EstadoUltimaTrx"]),
        AssetKey(["DL_FARINTER", "dbo", "VDL_SAP_Atributos_Cliente_EstadoAnterior"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_Cliente_SAP"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_SociedadCliente"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_GrupoCliente"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_Dim_TipoArt2_SAP"]),
        AssetKey(["BI_FARINTER", "dbo", "BI_SAP_Dim_Cliente_Estado"]),
        AssetKey(["DL_FARINTER", "dbo", "DL_ClientesTiemposFechaFacturas_SAP"]),
        AssetKey(["AN_FARINTER", "dbo", "AN_SAP_Cal_ClientesRecordCredito"]),
    ],
)
def cargar_cal_atributos_cliente_sap(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    """Loads client attributes into the analytics database for reporting."""
    execute_sp_with_params(
        context,
        dwh_farinter_dl,
        "AN_paCargarCal_AtributosCliente_SAP",
        database="AN_FARINTER",
        schema="dbo",
    )


all_assets = tuple(
    load_assets_from_current_module(group_name="sap_etl_dwh")
)  # + store_procedure_assets

all_asset_checks: Sequence[AssetChecksDefinition] = tuple(
    (load_asset_checks_from_current_module())
)


if __name__ == "__main__":
    ##testing
    print(
        str([asset.keys for asset in all_assets if isinstance(asset, AssetsDefinition)])
    )
