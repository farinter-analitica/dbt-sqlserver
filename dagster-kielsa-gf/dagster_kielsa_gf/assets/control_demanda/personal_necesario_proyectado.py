import datetime as dt
import math
from textwrap import dedent

import polars as pl
import polars.selectors as cs
from dagster import (
    AssetExecutionContext,
    AssetKey,
    asset,
)

from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_helpers import DataframeSQLScriptGenerator
from dagster_shared_gf.shared_variables import env_str, tags_repo
from dagster_shared_gf.automation import automation_weekly_1_delta_1_cron

cfg = pl.Config()


def debug_print(msg):
    print(msg)


def get_demanda_forecast_data(dwh_farinter_bi: SQLServerResource) -> pl.DataFrame:
    query_trx = dedent(
        """
        SELECT
            P.Fecha_Id,
            P.Emp_Id,
            P.Suc_Id,
            P.Hora_Id,
            C.Dia_de_la_Semana,
            P.Conteo_Transacciones,
            P.Cantidad_Articulos,
            P.Cantidad_Padre,
            P.Segundos_Transaccion_Estimado,
            P.Segundos_Actividad_Estimado,
            ISNULL(CASE WHEN CAJ.Emp_Id IS NOT NULL
                THEN 1 ELSE 0 END,0) AS Es_Suc_Autoservicio
        FROM "BI_FARINTER"."dbo".BI_Kielsa_Hecho_ProyeccionVenta_BaseMes_SucHora P
        INNER JOIN "BI_FARINTER"."dbo"."BI_Dim_Calendario_Dinamico" C
            ON P.Fecha_Id = C.Fecha_Calendario
        LEFT JOIN (SELECT Emp_Id, Suc_Id 
                FROM "DL_FARINTER"."dbo"."DL_Kielsa_Caja"
                WHERE Caja_Nombre LIKE '%auto%'
                GROUP BY Emp_Id, Suc_Id
                ) CAJ
            ON P.Emp_Id = CAJ.Emp_Id
            AND P.Suc_Id = CAJ.Suc_Id
        """
    )

    df = (
        pl.read_database(query_trx, dwh_farinter_bi.get_arrow_odbc_conn_string())
        .lazy()
        .collect(engine="streaming")
    )
    df = df.with_columns((cs.numeric() - cs.ends_with("_id")).cast(pl.Float64))

    return df


def get_demanda_forecast_params_data(
    dwh_farinter_bi: SQLServerResource,
) -> pl.DataFrame:
    query_trx = dedent(
        f"""
        SELECT MAX(
            CASE
                WHEN variable = 'desviacion_estandar' THEN {"coeficiente_ajustado" if env_str == "prd" else "coeficiente"}
                ELSE NULL
            END
            ) AS Desviacion_Estandar,
        MAX(
            CASE
                WHEN variable = 'cola_promedio_maxima' THEN {"coeficiente_ajustado" if env_str == "prd" else "coeficiente"}
                ELSE NULL
            END
        ) AS Cola_Promedio_Maxima,
        MAX(
            CASE
                WHEN variable = 'tiempo_espera_prom_maximo' THEN {"coeficiente_ajustado" if env_str == "prd" else "coeficiente"}
                ELSE NULL
            END
        ) AS Tiempo_Espera_Promedio_Maximo
        FROM "DL_FARINTER"."nocodb_data_gf"."kielsa_tiempo_transaccion_coeficiente"
        WHERE variable IN ('desviacion_estandar', 'cola_promedio_maxima', 'tiempo_espera_prom_maximo')
        """
    )

    df = (
        pl.read_database(query_trx, dwh_farinter_bi.get_arrow_odbc_conn_string())
        .lazy()
        .collect(engine="streaming")
    )
    df = df.with_columns((cs.numeric()).cast(pl.Float64))

    return df


def calcular_personal_necesario(
    df: pl.DataFrame, df_params: pl.DataFrame
) -> pl.DataFrame:
    """
    Calcula el personal necesario utilizando teoría de colas con corrección
    para sistemas multi-servidor.
    """
    # Convertir columnas numéricas a float64
    df = df.with_columns(
        (cs.numeric() - cs.ends_with("_id")).cast(pl.Float64),
        pl.lit(df_params.get_column("Desviacion_Estandar").item()).alias(
            "Desviacion_Estandar"
        ),
    )
    df_params = df_params.with_columns(
        (cs.numeric() - cs.ends_with("_id")).cast(pl.Float64)
    )

    # Parámetros de calidad de servicio
    TIEMPO_ESPERA_MAX = (
        df_params.get_column("Tiempo_Espera_Promedio_Maximo").item() or 60 * 3
    )  # segundos
    UTILIZACION_MAX = 0.70  # tasa máxima de utilización
    BETA_FACTOR = 1.0  # factor de seguridad para square-root staffing
    COLA_MAX = (
        df_params.get_column("Cola_Promedio_Maxima").item() or 2
    )  # máximo número de clientes en cola permitido

    # Calcular tasas y métricas base
    df = df.with_columns(
        # Tasa de llegadas (λ) - transacciones por hora
        pl.col("Conteo_Transacciones").alias("lambda_hora"),
        # Tiempo medio de servicio por transacción en segundos
        (pl.col("Segundos_Actividad_Estimado") / pl.col("Conteo_Transacciones")).alias(
            "tiempo_servicio_por_transaccion"
        ),
    ).with_columns(
        # Tasa de servicio por servidor (μ) - transacciones por hora que puede atender un solo empleado
        (3600 / (pl.col("tiempo_servicio_por_transaccion"))).alias("mu_servidor_hora"),
        # Tiempo medio de servicio en horas
        ((pl.col("tiempo_servicio_por_transaccion")) / 3600).alias(
            "tiempo_servicio_hora"
        ),
        # Coeficiente de variación al cuadrado (CV²)
        (
            (
                pl.col("Desviacion_Estandar")
                / (pl.col("tiempo_servicio_por_transaccion"))
            )
            ** 2
        ).alias("cv_tiempo_cuadrado"),
    )

    # Calcular la carga ofrecida (a = λ/μ) - representa el número mínimo de servidores necesarios
    # # para que el sistema sea estable
    # df = df.with_columns(
    #     (pl.col("lambda_hora") / pl.col("mu_servidor_hora")).alias(
    #         "carga_ofrecida_total"
    #     )
    # )

    # Alternativamente, podemos calcular la carga ofrecida directamente desde los segundos totales
    df = df.with_columns(
        (pl.col("Segundos_Actividad_Estimado") / 3600).alias("carga_ofrecida_total"),
        ((pl.col("cv_tiempo_cuadrado") + 1) / 2).sqrt().alias("cv_tiempo"),
    )

    # Calcular personal mínimo (carga total redondeada hacia arriba)
    df = df.with_columns(
        pl.col("carga_ofrecida_total").ceil().alias("personal_minimo_absoluto")
    )

    # Personal según utilización máxima permitida
    df = df.with_columns(
        (pl.col("carga_ofrecida_total") / UTILIZACION_MAX)
        .ceil()
        .alias("personal_utilizacion_n_p")
    )

    # Personal según square-root staffing rule (como método independiente, no dentro de teoría de colas)
    df = df.with_columns(
        (
            pl.col("carga_ofrecida_total")
            + BETA_FACTOR
            * pl.col("cv_tiempo").clip(0, 1.5)
            * pl.col("carga_ofrecida_total").sqrt()
        )
        .ceil()
        .alias("personal_square_root")
    )

    def calcular_metricas_cola(row: dict) -> dict:
        """
        Calcula personal necesario y métricas usando fórmulas correctas de teoría de colas
        para sistemas M/G/c, implementando también restricción de COLA_MAX.
        """
        # Valores por defecto para casos extremos
        default = {
            "personal_espera_n_seg": 1,
            "personal_cola_max": 1,
            "personal_recomendado": 1,
            "utilizacion": 0.0,
            "tiempo_espera_promedio": 0.0,
            "longitud_cola_promedio": 0.0,
        }

        if any(value is None for value in row.values()):
            return default

        carga = float(row["carga_ofrecida_total"])
        lambda_h = float(row["lambda_hora"])
        mu_h = float(row["mu_servidor_hora"])
        cv_sq = float(row["cv_tiempo_cuadrado"])
        tiempo_s = float(row["tiempo_servicio_hora"])
        es_suc_autoservicio = int(row["Es_Suc_Autoservicio"])

        # Manejo de casos extremos
        if carga < 0.01 or lambda_h < 0.01 or mu_h < 0.01:
            default["utilizacion"] = 0.0 if carga <= 0 else carga
            return default

        # Personal mínimo necesario para estabilidad
        c_min = math.ceil(carga)

        # Factor de corrección para distribución general de servicio (Allen-Cunneen)
        factor_g = (cv_sq + 1) / 2

        # Tiempo de espera máximo en horas
        # max_wq_hours = TIEMPO_ESPERA_MAX / 3600

        def calcular_erlang_c(c, a):
            """Calcula la probabilidad de espera usando la fórmula de Erlang-C."""
            if c <= a:  # Sistema inestable
                return 1.0

            # Cálculo de la suma para el denominador
            suma = 0
            for i in range(c):
                suma += (a**i) / math.factorial(i)

            # Término adicional para el denominador
            term = (a**c) / (math.factorial(c) * (1 - a / c))

            # Fórmula de Erlang-C
            p0 = 1 / (suma + term)
            pc = (a**c) / (math.factorial(c) * (1 - a / c)) * p0

            return pc

        def calcular_tiempo_espera(c):
            """Calcula el tiempo de espera promedio para un sistema M/G/c."""
            if c <= carga:  # Sistema inestable
                return float("inf")

            rho = carga / c  # Utilización por servidor

            # Probabilidad de espera (Erlang-C)
            pc = calcular_erlang_c(c, carga)

            # Tiempo de espera promedio según Allen-Cunneen
            wq = pc * factor_g * tiempo_s / (c * (1 - rho))

            return wq * 3600  # Convertir a segundos

        def calcular_longitud_cola(c):
            """Calcula la longitud promedio de la cola para un sistema M/G/c."""
            if c <= carga:  # Sistema inestable
                return float("inf")

            # Tiempo de espera en horas
            wq_hours = calcular_tiempo_espera(c) / 3600

            # Longitud de cola por Ley de Little: Lq = λ * Wq
            lq = lambda_h * wq_hours

            return lq

        # Calcular personal para cumplir con tiempo de espera máximo
        c_tiempo = c_min
        while (
            calcular_tiempo_espera(c_tiempo) > TIEMPO_ESPERA_MAX
            and c_tiempo < c_min + 20
        ):
            c_tiempo += 1

        # Calcular personal para cumplir con longitud de cola máxima
        c_cola = c_min
        while calcular_longitud_cola(c_cola) > COLA_MAX and c_cola < c_min + 20:
            c_cola += 1

        if (
            es_suc_autoservicio == 1
            and c_cola <= 1
            and calcular_longitud_cola(c_cola) > 1
        ):
            c_cola += 1

        # El personal recomendado es el máximo que satisface ambos criterios
        c_recomendado = max(c_tiempo, c_cola)

        # Utilización por servidor con el personal recomendado
        utilizacion = carga / c_recomendado if c_recomendado > 0 else 1.0

        # Calcular métricas finales con el personal recomendado
        tiempo_espera = calcular_tiempo_espera(c_recomendado)
        longitud_cola = calcular_longitud_cola(c_recomendado)

        return {
            "personal_espera_n_seg": c_tiempo,
            "personal_cola_max": c_cola,
            "personal_recomendado": c_recomendado,
            "utilizacion": utilizacion,
            "tiempo_espera_promedio": tiempo_espera,
            "longitud_cola_promedio": longitud_cola,
        }

    # Aplicar cálculos de teoría de colas
    df = df.with_columns(
        pl.struct(
            [
                "carga_ofrecida_total",
                "lambda_hora",
                "mu_servidor_hora",
                "cv_tiempo_cuadrado",
                "tiempo_servicio_hora",
                "Es_Suc_Autoservicio",
            ]
        )
        .map_elements(calcular_metricas_cola)
        .alias("metricas_cola")
    )

    # Extraer resultados
    df = df.with_columns(pl.col("metricas_cola").struct.unnest())

    # Limpiar y asegurar valores mínimos
    df = df.drop("metricas_cola").with_columns(
        [
            pl.col(col).clip(1)
            for col in [
                "personal_minimo_absoluto",
                "personal_espera_n_seg",
                "personal_cola_max",
                "personal_utilizacion_n_p",
                "personal_square_root",
                "personal_recomendado",
            ]
        ]
    )

    return df


@asset(
    key_prefix=("IA_FARINTER", "dbo"),
    description="Proyección de personal necesario para atender la demanda.",
    deps=(
        AssetKey(
            ("BI_FARINTER", "dbo", "BI_Kielsa_Hecho_ProyeccionVenta_BaseMes_SucHora")
        ),
        AssetKey(
            ("DL_FARINTER", "nocodb_data_gf", "kielsa_tiempo_transaccion_coeficiente")
        ),
        AssetKey(("BI_FARINTER", "dbo", "BI_Dim_Calendario_Dinamico")),
        AssetKey(("DL_FARINTER", "dbo", "DL_Kielsa_Caja")),
    ),
    tags=tags_repo.AutomationWeekly1 | tags_repo.AutomationOnly,
    automation_condition=automation_weekly_1_delta_1_cron,
)
def IA_Kielsa_Proyeccion_Personal_Necesario(
    context: AssetExecutionContext,
    dwh_farinter_ia: SQLServerResource,
    dwh_farinter_bi: SQLServerResource,
) -> None:
    df_demanda_forecast = get_demanda_forecast_data(
        dwh_farinter_bi=dwh_farinter_bi,
    )
    df_demanda_forecast_params = get_demanda_forecast_params_data(dwh_farinter_bi)
    # if (
    #     df_demanda_forecast_params.is_empty()
    #     or (df_demanda_forecast_params.null_count().sum_horizontal().item() or 1) > 0
    # ):
    #     raise ValueError(
    #         f"No se econtraron los parametros para calcular el personal necesario {df_demanda_forecast_params.head(10)}"
    #     )
    df_personal_necesario = calcular_personal_necesario(
        df_demanda_forecast, df_demanda_forecast_params
    )

    # df_personal_necesario = df_personal_necesario.filter(
    #                 pl.col("Suc_Id").is_in((115,))
    #                 & (pl.col("Emp_Id") == 1)
    #             )
    print(f"Por guardar {len(df_personal_necesario)} filas")
    with dwh_farinter_ia.get_sqlalchemy_conn() as conn:
        sg = DataframeSQLScriptGenerator(
            primary_keys=("Fecha_Id", "Suc_Id", "Hora_Id", "Emp_Id"),
            db_schema="dbo",
            table_name="IA_Kielsa_Proyeccion_Personal_Necesario",
            df=df_personal_necesario,
            temp_table_name="IA_Kielsa_Proyeccion_Personal_Necesario_NEW",
        )

        if env_str == "local":
            with pl.Config() as c:
                c.set_tbl_rows(-1)
                c.set_tbl_cols(-1)
                print(df_personal_necesario.head(10))
                print(df_personal_necesario.describe())
                df_personal_necesario.write_csv(".cache/df_personal_necesario.csv")
            return

        dwh_farinter_ia.execute_and_commit(
            sg.drop_table_sql_script(temp=True), connection=conn
        )
        dwh_farinter_ia.execute_and_commit(
            sg.create_table_sql_script(temp=True), connection=conn
        )
        dwh_farinter_ia.execute_and_commit(
            sg.columnstore_table_sql_script(temp=True), connection=conn
        )

        # First write as regular table
        sg.df.write_database(
            table_name=sg.temp_table_name,
            connection=conn,
            if_table_exists="append",
        )

        dwh_farinter_ia.execute_and_commit(
            sg.primary_key_table_sql_script(temp=True), connection=conn
        )

        dwh_farinter_ia.execute_and_commit(sg.swap_table_with_temp(), connection=conn)


# ---------------------------------------------------------------------------------------
if __name__ == "__main__" and 1 == 2:
    from polars import Config

    from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_bi

    cfg = Config()
    cfg.set_tbl_cols(20)
    cfg.set_tbl_rows(100)

    df_demanda_forecast = get_demanda_forecast_data(
        dwh_farinter_bi=dwh_farinter_bi,
        # config=ConfigGetData(
        #     fecha_desde=pdt.today().subtract(days=365),
        #     use_cache=True,
        #     cache_max_age_seconds=3600 * 3600,
        # ),
    )
    df_demanda_forecast_params = get_demanda_forecast_params_data(dwh_farinter_bi)
    df_personal_necesario = calcular_personal_necesario(
        df_demanda_forecast, df_demanda_forecast_params
    )

    # Filtrar según los criterios: Suc_Id = 115 y Fecha_Id entre '20250324' y '20250331'
    df_filtered = df_personal_necesario.filter(
        # (pl.col("Suc_Id") == 115)
        # &
        (pl.col("Fecha_Id") >= pl.date(2025, 3, 24))
        & (pl.col("Fecha_Id") < pl.date(2025, 3, 31))
    )

    df_filtered.sort("Suc_Id", "Fecha_Id", "Hora_Id").write_excel(
        ".cache/personal_necesario_proyectado.xlsx",
        worksheet="personal_necesario_proyectado",
    )

if __name__ == "__main__" and 1 == 1:
    from dagster import instance_for_test, materialize
    from dagster_polars import PolarsParquetIOManager
    from dagster_shared_gf.resources.sql_server_resources import (
        dwh_farinter_ia,
        dwh_farinter_bi,
    )
    import datetime as dt

    start_time = dt.datetime.now()
    with instance_for_test() as instance:

        @asset(name="between_asset")
        def mock_between_asset() -> int:
            return 1

        result = materialize(
            assets=[mock_between_asset, IA_Kielsa_Proyeccion_Personal_Necesario],
            instance=instance,
            resources={
                "dwh_farinter_ia": dwh_farinter_ia,
                "dwh_farinter_bi": dwh_farinter_bi,
                "polars_parquet_io_manager": PolarsParquetIOManager(),
            },
        )
        print(
            result.output_for_node(
                IA_Kielsa_Proyeccion_Personal_Necesario.node_def.name
            )
        )

    end_time = dt.datetime.now()
    print(
        f"Tiempo de ejecución: {end_time - start_time}, desde {start_time}, hasta {end_time}"
    )
