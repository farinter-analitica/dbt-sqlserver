import dagster as dg
from dagster import AssetKey
import datetime
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_variables import tags_repo, env_str
from dagster_shared_gf.shared_functions import get_for_current_env
from textwrap import dedent

# dwh_farinter_sms_kielsa
from dagster_shared_gf.automation.time_based import my_cron_automation_condition

# Automatiza la ejecucion a estas horas y con estas condiciones en dependencias
automation_sms_kielsa = (
    my_cron_automation_condition(
        cron_schedule=get_for_current_env(
            {"prd": "30 6,10,14,17 * * *", "dev": "30 8 * * *"}
        ),
        allowed_deps_updated_selection=dg.AssetSelection.tag(
            key=tags_repo.Hourly.key, value=tags_repo.Hourly.value
        )
        | dg.AssetSelection.tag(
            key=tags_repo.AutomationHourly.key, value=tags_repo.AutomationHourly.value
        ),  # Permite solo estos assets dependientes en la verificacion de si ya actualizó o no
        deps_updated_cron=get_for_current_env({"prd": "@Hourly", "dev": "@Daily"}),
    )
    if env_str == "prd"
    else dg.AutomationCondition.code_version_changed()
)


def execute_sql(
    context: dg.AssetExecutionContext | dg.OpExecutionContext,
    sql: str,
    sql_server_resource: SQLServerResource,
    database: str = "SMS_KIELSA",
) -> None:
    query = f"USE {database};"
    final_query = f"{query}\n{sql};"
    context.log.info(f"Executing: {final_query}")

    try:
        sql_server_resource.execute_and_commit(final_query)
        context.log.info("Successfully executed SQL")
    except Exception as e:
        context.log.error(f"Error executing SQL: {str(e)}")
        raise


@dg.asset(
    key=AssetKey(["SMS_KIELSA", "dbo", "SMS_paCargaInicial_ClientesPorPerderse"]),
    deps=[
        AssetKey(["AN_FARINTER", "dbo", "AN_Kielsa_EstadosCliente"]),
        AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Monedero"]),
        AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Monedero_Tarjetas_Replica"]),
        AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Monedero_Plan"]),
    ],
    automation_condition=automation_sms_kielsa,
    compute_kind="sqlserver",
    tags=tags_repo.AutomationOnly,  # Sin esto los tests exigen que sea parte de un job
    group_name="sms_kielsa",
    description="EXEC [SMS_KIELSA].dbo.SMS_paCargaInicial_ClientesPorPerderse",
    owners=["brian.padilla@farinter.com", "edwin.martinez@farinter.com"],
)
def sms_kielsa_carga_inicial_clientes_por_perderse(
    context: dg.OpExecutionContext,
    dwh_farinter_sms_kielsa: SQLServerResource,
):
    sql = dedent("""
        declare @FechaFin datetime = convert(date,getdate())

        exec [SMS_KIELSA].dbo.SMS_paCargaInicial_ClientesPorPerderse @fechaFin
    """)
    momento_inicio = datetime.datetime.now()
    execute_sql(context, sql, dwh_farinter_sms_kielsa)
    duracion_total = datetime.datetime.now() - momento_inicio

    return dg.Output(
        value=None,
        metadata={"duration_total": duracion_total.total_seconds()},
    )


@dg.multi_asset(
    outs={
        "SMS_paMonitoreo_ClientesPorPerderse": dg.AssetOut(
            key_prefix=["SMS_KIELSA", "dbo"],
            description="EXEC [SMS_KIELSA].dbo.SMS_paMonitoreo_ClientesPorPerderse",
            owners=["brian.padilla@farinter.com", "edwin.martinez@farinter.com"],
            automation_condition=automation_sms_kielsa,
            kinds={"sqlserver"},
            tags=tags_repo.AutomationOnly,
            group_name="sms_kielsa",
        ),
        "SMS_paCargaMensajes_ClientesPorPerderse": dg.AssetOut(
            key_prefix=["SMS_KIELSA", "dbo"],
            description="EXEC [SMS_KIELSA].dbo.SMS_paCargaMensajes_ClientesPorPerderse",
            owners=["brian.padilla@farinter.com", "edwin.martinez@farinter.com"],
            automation_condition=automation_sms_kielsa,
            kinds={"sqlserver"},
            tags=tags_repo.AutomationOnly,
            group_name="sms_kielsa",
        ),
    },
    deps=[
        AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturaEncabezado"]),
        AssetKey(["SMS_KIELSA", "dbo", "SMS_paCargaInicial_ClientesPorPerderse"]),
    ],
)
def sms_kielsa_monitoreo_clientes_por_perderse(
    context: dg.AssetExecutionContext, dwh_farinter_sms_kielsa: SQLServerResource
):
    sql = dedent(
        f"""
        declare @FechaFin datetime = convert(date,getdate())
        declare @FechaIndex datetime = dateadd(day,{-16 if env_str == "prd" else -2},@fechaFin)

        while @FechaIndex<=@FechaFin begin

            exec [SMS_KIELSA].[dbo].[SMS_paMonitoreo_ClientesPorPerderse] @fechaIndex

            exec [SMS_KIELSA].dbo.SMS_paCargaMensajes_ClientesPorPerderse @fechaIndex

        set @FechaIndex = dateadd(day,1, @FechaIndex)

        end
    """
    )

    momento_inicio = datetime.datetime.now()
    execute_sql(context, sql, dwh_farinter_sms_kielsa)
    duracion_total = datetime.datetime.now() - momento_inicio

    # Emit two outputs (they represent the two SPs executed)
    yield dg.Output(
        value=None,
        output_name="SMS_paMonitoreo_ClientesPorPerderse",
        metadata={"duration_total": duracion_total.total_seconds() / 2},
    )
    yield dg.Output(
        value=None,
        output_name="SMS_paCargaMensajes_ClientesPorPerderse",
        metadata={"duration_total": duracion_total.total_seconds() / 2},
    )


@dg.asset(
    key=AssetKey(["SMS_KIELSA", "dbo", "SMS_paCargaInicial_ClientesNuevos"]),
    description="EXEC [SMS_KIELSA].dbo.SMS_paCargaInicial_ClientesNuevos",
    owners=["brian.padilla@farinter.com", "edwin.martinez@farinter.com"],
    deps=[
        AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Monedero"]),
        AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Monedero_Tarjetas_Replica"]),
        AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Monedero_Plan"]),
        AssetKey(["SMS_KIELSA", "dbo", "SMS_paMonitoreo_ClientesPorPerderse"]),
    ],
    automation_condition=automation_sms_kielsa,
    tags=tags_repo.AutomationOnly,
    group_name="sms_kielsa",
    compute_kind="sqlserver",
)
def sms_kielsa_clientes_nuevos_cargar_inicial(
    context: dg.OpExecutionContext,
    dwh_farinter_sms_kielsa: SQLServerResource,
):
    sql = dedent("""
        exec [SMS_KIELSA].[dbo].[SMS_paCargaInicial_ClientesNuevos]
    """)
    momento_inicio = datetime.datetime.now()
    execute_sql(context, sql, dwh_farinter_sms_kielsa)
    duracion_total = datetime.datetime.now() - momento_inicio

    return dg.Output(
        value=None,
        metadata={"duration_total": duracion_total.total_seconds()},
    )


@dg.asset(
    key=AssetKey(["SMS_KIELSA", "dbo", "SMS_paMonitoreo_ClientesNuevos"]),
    description="EXEC [SMS_KIELSA].dbo.SMS_paMonitoreo_ClientesNuevos",
    owners=["brian.padilla@farinter.com", "edwin.martinez@farinter.com"],
    deps=[
        AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Hecho_FacturaEncabezado"]),
        AssetKey(["SMS_KIELSA", "dbo", "SMS_paCargaInicial_ClientesNuevos"]),
    ],
    automation_condition=automation_sms_kielsa,
    tags=tags_repo.AutomationOnly,
    group_name="sms_kielsa",
    compute_kind="sqlserver",
)
def sms_kielsa_clientes_nuevos_monitoreo(
    context: dg.OpExecutionContext,
    dwh_farinter_sms_kielsa: SQLServerResource,
):
    sql = dedent("""
        declare @fecha datetime = dateadd(day,-2, getdate())

        exec [SMS_KIELSA].[dbo].[SMS_paMonitoreo_ClientesNuevos] @fecha    
    """)
    momento_inicio = datetime.datetime.now()
    execute_sql(context, sql, dwh_farinter_sms_kielsa)
    duracion_total = datetime.datetime.now() - momento_inicio

    return dg.Output(
        value=None,
        metadata={"duration_total": duracion_total.total_seconds()},
    )


if __name__ == "__main__":
    # Quick local test: build Definitions for these assets and print them.
    try:
        from dagster_shared_gf.resources.sql_server_resources import (
            dwh_farinter_sms_kielsa,
        )

        definitions = dg.Definitions(
            assets=[
                sms_kielsa_carga_inicial_clientes_por_perderse,
                sms_kielsa_monitoreo_clientes_por_perderse,
                sms_kielsa_clientes_nuevos_cargar_inicial,
                sms_kielsa_clientes_nuevos_monitoreo,
            ],
            resources={"dwh_farinter_sms_kielsa": dwh_farinter_sms_kielsa},
        )

        print(definitions)
    except Exception as e:
        print(f"Quick test failed: {e}")
