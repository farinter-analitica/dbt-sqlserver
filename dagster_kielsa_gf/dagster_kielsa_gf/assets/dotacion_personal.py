import datetime as dt

import dagster as dg
import polars as pl
from pydantic import Field
from sqlalchemy.engine import Engine as SQLAlchemyEngine

import dagster_shared_gf.resources.postgresql_resources as sqlpg
import dagster_shared_gf.resources.sql_server_resources as sqlsr
from dagster_shared_gf import automation as auto_def
from dagster_shared_gf.shared_helpers import DataframeSQLTableManager
from dagster_shared_gf.shared_variables import env_str, tags_repo


class DotacionPersonalConfig(dg.Config):
    fecha_inicio: str = Field(
        default_factory=lambda: (dt.date.today() - dt.timedelta(days=31)).strftime(
            "%Y-%m-%d"
        ),
        description="Fecha de inicio para el procesamiento inclusive (por defecto: 31 días atrás)",
    )
    fecha_fin: str = Field(
        default_factory=lambda: (dt.date.today() - dt.timedelta(days=1)).strftime(
            "%Y-%m-%d"
        ),
        description="Fecha de fin para el procesamiento inclusive (por defecto: ayer)",
    )
    drop_target: bool = Field(
        default=False,
        description="Si es verdadero, la tabla destino se elimina antes de la carga",
    )
    drop_temp: bool = Field(
        default=True,
        description="Si es verdadero, la tabla temporal se elimina después de la carga",
    )


def obtener_emps_hora_rm(
    engine: SQLAlchemyEngine, fecha_inicio_dt: dt.date, fecha_fin_dt: dt.date
) -> pl.DataFrame:
    fecha_inicio_str = fecha_inicio_dt.strftime("%Y-%m-%d")
    fecha_fin_str = fecha_fin_dt.strftime("%Y-%m-%d")
    query = f"""
    WITH HourNumbers AS (
        SELECT TOP(24)
            ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) - 1 AS HourIndex
        FROM master..spt_values a WITH (NOLOCK)
        CROSS JOIN master..spt_values b WITH (NOLOCK)
    ),
    ShiftsExpanded AS (
        SELECT
            t.Pais_Id AS Emp_Id, 
            t.Sucursal_Id,
            t.Empleado_Marcador_Id,
            DATEADD(
            HOUR,
            DATEDIFF(HOUR, 0, t.FH_Entrada_Corregida),
            0
            ) AS StartHour,
            t.FH_Entrada_Corregida,
            t.FH_Salida_Corregida,
            n.HourIndex
        FROM [BI_FARINTER].[dbo].[BI_Kielsa_Hecho_RelojMarcador] AS t
        CROSS JOIN HourNumbers AS n
        WHERE
        TRY_CONVERT(DATE, t.FH_Entrada_Corregida, 126)
               >= '{fecha_inicio_str}' 
        and TRY_CONVERT(DATE, t.FH_Entrada_Corregida, 126)
        < '{fecha_fin_str}'
        and n.HourIndex 
        <= DATEDIFF(
            HOUR,
            DATEADD(HOUR, DATEDIFF(HOUR, 0, t.FH_Entrada_Corregida), 0),
            t.FH_Salida_Corregida
            )
        and t.Es_Valido = 1 
    ),
    HourlyPresence AS (
        SELECT
            Emp_Id,
            Sucursal_Id,
            Empleado_Marcador_Id,
            DATEADD(HOUR, HourIndex, StartHour) AS HourStamp
        FROM ShiftsExpanded
    ),
    FilteredBySchedule AS (
        SELECT
            hp.Emp_Id,
            hp.Sucursal_Id,
            CAST(hp.HourStamp AS date)       AS Fecha,
            DATEPART(HOUR, hp.HourStamp)     AS Hora,
            hp.Empleado_Marcador_Id
        FROM HourlyPresence AS hp
        INNER JOIN [BI_FARINTER].[dbo].[BI_Kielsa_Dim_Sucursal_Horario_DiaSemana] AS h
        ON h.suc_id = hp.Sucursal_Id
        AND DATEPART(weekday, hp.HourStamp) = h.Dia_Semana_Iso_Id
        WHERE
        (
            h.h_apertura < h.h_cierre
            AND CAST(hp.HourStamp AS time) >= h.h_apertura
            AND CAST(hp.HourStamp AS time) <  h.h_cierre
        )
        OR
        (
            h.h_apertura >= h.h_cierre
            AND (
                CAST(hp.HourStamp AS time) >= h.h_apertura
            OR CAST(hp.HourStamp AS time) <  h.h_cierre
            )
        )
    )
    SELECT
        Emp_Id,
        Sucursal_Id AS Suc_Id,
        Fecha AS Fecha_Id,
        Hora AS Hora_Id,
        COUNT(DISTINCT Empleado_Marcador_Id) AS Conteo_Emps
    FROM FilteredBySchedule
    GROUP BY
        Emp_Id,
        Sucursal_Id,
        Fecha,
        Hora
    ORDER BY
        Emp_Id,
        Sucursal_Id,
        Fecha,
        Hora;
    """
    return pl.read_database(query=query, connection=engine)


def obtener_emps_hora_asig(
    engine: SQLAlchemyEngine, fecha_inicio_dt: dt.date, fecha_fin_dt: dt.date
) -> pl.DataFrame:
    fecha_inicio_str = fecha_inicio_dt.strftime("%Y-%m-%d")
    fecha_fin_str = fecha_fin_dt.strftime("%Y-%m-%d")
    query = f"""
    WITH planned_base AS (
        SELECT h.sucursal_id AS sucursal_id,
            dh.id AS plan_id,
            (dh.fecha + dh.hora_inicio) AS start_ts,
            (dh.fecha + dh.hora_salida) AS end_ts,
            dh.creado AS creado,
            date_trunc('week', (dh.fecha + dh.hora_inicio))::date AS week_start
        FROM public.app_detalle_horario dh
            JOIN public.app_horario h ON h.id = dh.horario_id
        WHERE es_final = TRUE
            AND dh.fecha::date >= TIMESTAMP '{fecha_inicio_str}'
            AND dh.fecha::date < TIMESTAMP '{fecha_fin_str}'
    ),
    planned AS (
        SELECT *
        FROM planned_base p
        WHERE p.creado::date <= p.week_start + INTERVAL '3 days'
            AND p.creado::date >= p.week_start - INTERVAL '7 days'
    ),
    expanded AS (
        SELECT p.sucursal_id,
            generate_series(
                date_trunc('hour', p.start_ts),
                p.end_ts,
                '1 hour'
            ) AS hour_stamp
        FROM planned AS p
    ),
    per_hour AS (
        SELECT sucursal_id,
            hour_stamp::date AS fecha,
            EXTRACT(
                HOUR
                FROM hour_stamp
            )::int AS hora
        FROM expanded
    )
    SELECT sucursal_id AS suc_id,
        fecha AS fecha_id,
        hora AS hora_id,
        COUNT(*) AS empleados_asignados
    FROM per_hour
    GROUP BY sucursal_id,
        fecha,
        hora
    ORDER BY sucursal_id,
        fecha,
        hora;
    """
    return pl.read_database(query=query, connection=engine)


def obtener_efectividad_dotacion_planificada(
    sqlserver_engine: SQLAlchemyEngine,
    postgres_engine: SQLAlchemyEngine,
    fecha_inicio: str,
    fecha_fin: str,
) -> pl.DataFrame:
    # Convertir las fechas de entrada a datetime.date
    fecha_inicio_dt = dt.datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
    fecha_fin_dt = dt.datetime.strptime(fecha_fin, "%Y-%m-%d").date()

    # Ajustar fecha_inicio al lunes de la semana correspondiente
    dias_hasta_lunes = (fecha_inicio_dt.weekday()) % 7
    lunes_inicio = fecha_inicio_dt - dt.timedelta(days=dias_hasta_lunes)

    # Ajustar fecha_fin al lunes de la semana siguiente (filtro exclusivo)
    dias_hasta_lunes_fin = (7 - fecha_fin_dt.weekday()) % 7
    lunes_fin = fecha_fin_dt + dt.timedelta(days=dias_hasta_lunes_fin)

    # No pasar de hoy: ajustar lunes_fin al lunes anterior o igual a hoy si es necesario
    hoy = dt.datetime.today().date()
    dias_hasta_lunes_hoy = (7 - hoy.weekday()) % 7
    lunes_anterior_o_hoy = hoy + dt.timedelta(days=dias_hasta_lunes_hoy)
    if lunes_fin > lunes_anterior_o_hoy:
        lunes_fin = lunes_anterior_o_hoy

    # Usar el lunes de inicio como fecha_inicio_dt
    fecha_inicio_dt = lunes_inicio

    # Usar el lunes de fin como fecha_fin_dt (exclusivo)
    fecha_fin_dt = lunes_fin
    df_asignados = obtener_emps_hora_asig(
        postgres_engine, fecha_inicio_dt, fecha_fin_dt
    )
    df_marcaciones = obtener_emps_hora_rm(
        sqlserver_engine, fecha_inicio_dt, fecha_fin_dt
    )

    df_asignados = df_asignados.rename(
        {
            "suc_id": "Suc_Id",
            "fecha_id": "Fecha_Id",
            "hora_id": "Hora_Id",
            "empleados_asignados": "Empleados_Asignados",
        }
    )
    df_asignados = df_asignados.with_columns(pl.lit(1).alias("Emp_Id"))
    df_marcaciones = df_marcaciones.rename(
        {
            "Conteo_Emps": "Emps_Reloj_Marcador",
        }
    )
    df_combinado = df_asignados.join(
        df_marcaciones, on=["Emp_Id", "Suc_Id", "Fecha_Id", "Hora_Id"], how="inner"
    )
    df_combinado = df_combinado.with_columns(pl.col("Emps_Reloj_Marcador").fill_null(0))
    df_con_semana = df_combinado.with_columns(
        [
            pl.col("Fecha_Id").dt.year().alias("Anio_Id"),
            pl.col("Fecha_Id").dt.month().alias("Mes_Id"),
            pl.col("Fecha_Id").dt.week().alias("Semana_Id"),
        ]
    )
    df_con_semana = df_con_semana.with_columns(
        [
            (
                (pl.col("Empleados_Asignados") - pl.col("Emps_Reloj_Marcador")).abs()
                / pl.when(pl.col("Empleados_Asignados") > 0)
                .then(pl.col("Empleados_Asignados"))
                .otherwise(1)
            ).alias("Diferencia_Porcentual"),
            (
                (pl.col("Empleados_Asignados") - pl.col("Emps_Reloj_Marcador")).abs()
            ).alias("Diferencia_ABS"),
        ]
    )
    return df_con_semana


@dg.asset(
    key_prefix=("IA_FARINTER", "dbo"),
    description="Datos combinados de dotación de personal: empleados asignados vs marcaciones reales por hora y sucursal.",
    group_name="dotacion_personal",
    tags=tags_repo.AutomationWeekly1,
    deps=[
        dg.AssetKey(("BI_FARINTER", "dbo", "BI_Kielsa_Hecho_RelojMarcador")),
        dg.AssetKey(("BI_FARINTER", "dbo", "BI_Kielsa_Dim_Sucursal_Horario_DiaSemana")),
        dg.AssetKey(
            ("bd_ia_dotacion", "public", "app_detalle_horario")
        ),  # Asumiendo que está en la DB por defecto de db_ia_dotacion_gf
        dg.AssetKey(
            ("bd_ia_dotacion", "public", "app_horario")
        ),  # Asumiendo que está en la DB por defecto de db_ia_dotacion_gf
    ],
    automation_condition=auto_def.automation_weekly_1_delta_1_cron,
)
def IA_Kielsa_Dotacion_Efectividad_Planificado(
    context: dg.AssetExecutionContext,
    dwh_farinter_bi: sqlsr.SQLServerResource,
    db_ia_dotacion_gf: sqlpg.PostgreSQLResource,
    dwh_farinter_ia: sqlsr.SQLServerResource,
    config: DotacionPersonalConfig,
) -> dg.Output[None]:
    context.log.info(
        f"Procesando para las semanas completas entre {config.fecha_inicio} y {config.fecha_fin}."
    )

    t0 = dt.datetime.now()
    sql_server_engine = dwh_farinter_bi.get_sqlalchemy_engine()
    postgres_engine = db_ia_dotacion_gf.get_engine()

    df_resultado = obtener_efectividad_dotacion_planificada(
        sqlserver_engine=sql_server_engine,
        postgres_engine=postgres_engine,
        fecha_inicio=config.fecha_inicio,
        fecha_fin=config.fecha_fin,
    )

    # Usar el helper para upsert
    manager = DataframeSQLTableManager(
        df=df_resultado,
        db_schema="dbo",
        table_name="IA_Kielsa_Dotacion_Efectividad_Planificado",
        sqla_engine=dwh_farinter_ia.get_sqlalchemy_engine(),
        primary_keys=("Suc_Id", "Fecha_Id", "Hora_Id"),
        load_date_col="Fecha_Carga",
        update_date_col="Fecha_Actualizado",
    )
    manager.upsert_dataframe(drop_temp=config.drop_temp, drop_target=config.drop_target)
    num_registros = df_resultado.height
    num_registros_total_tabla = manager.filas_total_tabla
    sucursales_unicas = df_resultado["Suc_Id"].n_unique() if num_registros > 0 else 0
    duracion = (dt.datetime.now() - t0).total_seconds()

    metadata = {
        "num_registros": num_registros,
        "num_registros_total_tabla": num_registros_total_tabla,
        "sucursales_analizadas": sucursales_unicas,
        "fecha_inicio": str(config.fecha_inicio),
        "fecha_fin": str(config.fecha_fin),
        "duracion_segundos": duracion,
    }
    context.add_output_metadata(metadata=metadata)
    return dg.Output(value=None, metadata=metadata)


if __name__ == "__main__":
    import warnings

    # Set up environment string if needed
    # env_str = "local"  # or fetch from env

    start_time = dt.datetime.now()

    with dg.instance_for_test() as instance:
        # Optionally warn or adjust for local/test mode
        if env_str == "local":
            warnings.warn(
                "Running in local mode, using test data and/or mock resources"
            )

        # Define a mock asset if your asset has dependencies
        @dg.asset(name="between_asset")
        def mock_between_asset() -> int:
            return 1

        # Optionally mock resources if needed
        # from dagster import ResourceDefinition
        # mock_resource_a = ResourceDefinition.mock_resource()
        # mock_resource_b = ResourceDefinition.mock_resource()

        dict_config = dg.RunConfig(
            ops={
                IA_Kielsa_Dotacion_Efectividad_Planificado.node_def.name: DotacionPersonalConfig(
                    # drop_temp=True,
                    # drop_target=True,
                )
            }
        )

        result = dg.materialize(
            assets=[
                mock_between_asset,
                IA_Kielsa_Dotacion_Efectividad_Planificado,
                # Add other assets as needed
            ],
            instance=instance,
            resources={
                "dwh_farinter_bi": sqlsr.dwh_farinter_bi,
                "dwh_farinter_dl": sqlsr.dwh_farinter_dl,
                "dwh_farinter_ia": sqlsr.dwh_farinter_ia,
                "db_ia_dotacion_gf": sqlpg.db_ia_dotacion_gf,
                # Add other resources as needed
            },
            run_config=dict_config,
        )
        # Print output for each asset/multi-asset node
        for assetmat in result.asset_materializations_for_node(
            IA_Kielsa_Dotacion_Efectividad_Planificado.node_def.name
        ):
            print(assetmat.metadata)
        # print(result.output_for_node(YOUR_MULTI_ASSET.node_def.name))

    end_time = dt.datetime.now()
    print(
        f"Tiempo de ejecución: {end_time - start_time}, desde {start_time}, hasta {end_time}"
    )
