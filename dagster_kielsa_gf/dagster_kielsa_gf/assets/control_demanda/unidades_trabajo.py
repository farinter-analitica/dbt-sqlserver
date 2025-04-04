import datetime as dt
import math
import os
import pandas as pd
import pendulum as pdt
import polars as pl
import polars.selectors as cs
import dagster as dg
from polars import col
from dagster_shared_gf.resources.sql_server_resources import (
    SQLServerResource,
)
from dagster_shared_gf.shared_helpers import ParquetCacheHandler
from textwrap import dedent
from pydantic import Field

from sklearn.linear_model import LinearRegression
import numpy as np
from xgboost import XGBRegressor
from sklearn.model_selection import cross_validate
from sklearn.ensemble import RandomForestRegressor

cfg = pl.Config()


def debug_print(str):
    print(str)


class ConfigGetTrxData(dg.Config):
    fecha_desde: dt.date = Field(
        description="Fecha desde la cual se quieren obtener las transacciones",
        default=pdt.today().subtract(months=3).replace(day=1),
    )
    use_cache: bool = Field(
        description="Whether to use cached data if available",
        default=True,
    )
    cache_max_age_seconds: int = Field(
        description="Maximum age of cache in hours",
        default=3600 * 24,
    )


def calculate_min_sample_size(
    confidence_level: float = 0.95,
    margin_of_error: float = 0.1,
    proportion: float = 0.5,
) -> int:
    """
    Calcula el tamaño mínimo de muestra necesario para una estimación de proporción.

    Args:
        confidence_level: Nivel de confianza (por defecto 0.95 para 95%)
        margin_of_error: Margen de error tolerable (por defecto 0.1 para 10%)
        proportion: Proporción estimada (0.5 es el valor más conservador)

    Returns:
        Tamaño mínimo de muestra requerido
    """
    # Calcular el valor crítico Z basado en el nivel de confianza
    if confidence_level == 0.90:
        z = 1.645
    elif confidence_level == 0.95:
        z = 1.96
    elif confidence_level == 0.99:
        z = 2.576
    else:
        raise ValueError("Nivel de confianza no soportado. Use 0.90, 0.95 o 0.99")

    # Calcular tamaño mínimo de muestra
    n_min = math.ceil((z**2 * proportion * (1 - proportion)) / (margin_of_error**2))

    return n_min


def get_transactions_data(
    dwh_farinter_bi: SQLServerResource, config: ConfigGetTrxData
) -> pl.DataFrame:
    if config.use_cache:
        cache_handler = ParquetCacheHandler(filename="ut_trx_data")
        df = cache_handler.get_cached_data(max_age_seconds=config.cache_max_age_seconds)
        if df is not None:
            return df

    fecha_desde_str = config.fecha_desde.strftime("%Y%m%d")
    query_trx = dedent(f""" 
    SELECT
        FE.EmpSucDocCajFac_Id,
        FE.Emp_Id,
        FE.Suc_Id,
        FE.TipoDoc_id,
        FE.Caja_Id,
        FE.Factura_Id,
        FE.Factura_Fecha,
        FE.Factura_FechaHora,
        FE.Factura_Origen,
        FP.cantidad_unidades,
        FP.cantidad_productos,
        FP.valor_neto,
        FP.contiene_servicios,
        FP.contiene_tengo,
        FP.contiene_consumo,
        FP.contiene_farma,
        FP.precio_unitario_prom,                    
        FP.canal_venta_id,
        TB.TipoBodega_Nombre,
        S.Departamento_Id,
        S.EmpDepMunCiu_Id,
        S.Zona_Nombre,
        S.TipoSucursal_Id,
        FP.acumula_monedero,
        TC.TipoCliente_Nombre,
        CASE WHEN TC.TipoCliente_Nombre LIKE '%TER%EDAD%' 
                OR TC.TipoCliente_Nombre LIKE '%CUART%EDAD%'
                OR M.Tipo_Plan LIKE '%TER%EDAD%' 
                OR M.Tipo_Plan LIKE '%CUART%EDAD%'
            THEN 1 ELSE 0 END AS es_tercera_edad,
        CASE WHEN TC.TipoCliente_Nombre LIKE '%CLINIC%' 
                OR M.Tipo_Plan LIKE '%CLINIC%' 
            THEN 1 ELSE 0 END AS es_clinica,
        CASE WHEN TC.TipoCliente_Nombre LIKE '%ASEGURADO%' 
                OR M.Tipo_Plan LIKE '%CLINIC%' 
            THEN 1 ELSE 0 END AS es_asegurado,
        M.Tipo_Plan
    FROM BI_FARINTER.dbo.BI_Kielsa_Hecho_FacturaEncabezado FE
    INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Bodega BD
    ON FE.Emp_Id = BD.Emp_Id
    AND FE.Suc_Id = BD.Sucursal_Id
    AND FE.Bodega_Id = BD.Bodega_Id
    INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_TipoBodega TB
    ON FE.Emp_Id = TB.Emp_Id
    AND BD.TipoBodega_Id = TB.TipoBodega_Id
    INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal S
    ON FE.Emp_Id = S.Emp_Id
    AND FE.Suc_Id = S.Sucursal_Id
    INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Monedero M
    ON FE.Emp_Id = M.Emp_Id
    AND FE.Monedero_Id = M.Monedero_Id
    INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Cliente C
    ON FE.Emp_Id = C.Emp_Id
    AND FE.Cliente_Id = C.Cliente_Id
    INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_TipoCliente TC
    ON FE.Emp_Id = TC.Emp_Id
    AND C.Tipo_Cliente_Id = TC.TipoCliente_Id
    INNER JOIN 
        (SELECT 
            FP.Emp_Id, 
            FP.Suc_Id, 
            FP.TipoDoc_id, 
            FP.Caja_Id, 
            FP.Factura_Id,
            SUM(FP.Cantidad_Padre) AS cantidad_unidades,
            COUNT(DISTINCT FP.Articulo_Id) AS cantidad_productos,
            SUM(FP.Valor_Neto) AS valor_neto,
            AVG(FP.Detalle_Precio_Unitario) AS precio_unitario_prom,
            MAX(FP.CanalVenta_Id) AS canal_venta_id,
            CASE WHEN SUM(FP.Valor_Acum_Monedero)>0 THEN 1 ELSE 0 END AS acumula_monedero,
            CASE WHEN MAX(CASE WHEN A.DeptoArt_Nombre LIKE 'SERVICIOS'
                       OR A.DeptoArt_Nombre LIKE 'SERVICIOS M%V%L%'
                        THEN 1 ELSE 0 END) =1
                THEN 1
                ELSE 0
            END AS contiene_servicios,
            CASE WHEN MAX(CASE WHEN A.DeptoArt_Nombre LIKE '%CONSUMO%'
                        THEN 1 ELSE 0 END) =1
                THEN 1
                ELSE 0
            END AS contiene_consumo,
            CASE WHEN MAX(CASE WHEN A.DeptoArt_Nombre LIKE '%FARMA%'
                        THEN 1 ELSE 0 END) =1
                THEN 1
                ELSE 0
            END AS contiene_farma,
            CASE WHEN MAX(CASE WHEN A.Casa_Nombre LIKE '%TENGO%' THEN 1 ELSE 0 END) =1
                THEN 1
                ELSE 0
            END AS contiene_tengo
        FROM BI_FARINTER.dbo.BI_Kielsa_Hecho_FacturaPosicion FP
        INNER JOIN [BI_FARINTER].[dbo].[BI_Kielsa_Dim_Articulo] A
        ON FP.Articulo_Id = A.Articulo_Id
        AND FP.Emp_Id = A.Emp_Id
        WHERE FP.Factura_Fecha>= '{fecha_desde_str}' --Filtra por la particion
        GROUP BY FP.Emp_Id, FP.Suc_Id, FP.TipoDoc_id, FP.Caja_Id, FP.Factura_Id
        ) FP
        ON FE.Emp_Id = FP.Emp_Id
        AND FE.Suc_Id = FP.Suc_Id
        AND FE.TipoDoc_id = FP.TipoDoc_id
        AND FE.Caja_Id = FP.Caja_Id
        AND FE.Factura_Id = FP.Factura_Id

    WHERE FE.Factura_Fecha>= '{fecha_desde_str}' AND FE.Emp_Id = 1
    """)

    df = (
        pl.read_database(query_trx, dwh_farinter_bi.get_arrow_odbc_conn_string())
        .lazy()
        .collect(engine="streaming")
    )

    if config.use_cache:
        cache_handler = ParquetCacheHandler(filename="ut_trx_data")
        cache_handler.save_to_cache(df)

    return df


def tranform_transactions_data(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    # Calcular hora_id y clave caja
    cs_columnas_categoricas = (
        cs.by_name("acumula_monedero")
        | cs.by_name("contiene_tengo")
        | cs.starts_with("es_")
        | cs.starts_with("contiene_")
        | cs.ends_with("_id")
        | cs.by_dtype(pl.String)
        | cs.by_dtype(pl.Categorical)
    )

    df = (
        df.with_columns(
            col("Factura_FechaHora").dt.hour().alias("hora_id"),
            pl.concat_str(col("Emp_Id"), col("Suc_Id"), col("Caja_Id")).alias(
                "EmpSucCaj_Id"
            ),
            # Convertir IDs a variables categoricas
            (cs_columnas_categoricas & cs.numeric()).cast(pl.String),
            # .cast(pl.Categorical),
            (cs_columnas_categoricas & cs.string()),  # .cast(pl.Categorical),
        )
        # Convertir numericas restantes as float64
        .with_columns(cs.numeric().cast(pl.Float64))
        .lazy()
    )

    # Identificar metricas de transacciones
    debug_print("Calculando metricas de transacciones...")
    df_horas = (
        df.group_by("EmpSucCaj_Id", "Factura_Fecha", "hora_id")
        .agg(
            col("EmpSucCaj_Id").count().alias("total_transacciones_hora"),
        )
        .with_columns(
            # Identificar horas pico para cada sucursal y fecha
            pl.max("total_transacciones_hora")
            .over(["EmpSucCaj_Id", "Factura_Fecha"])
            .alias("max_transacciones_dia"),
            pl.count()
            .over(["EmpSucCaj_Id", "Factura_Fecha"])
            .alias("total_transacciones_dia"),
        )
        .with_columns(
            (col("total_transacciones_hora") == col("max_transacciones_dia")).alias(
                "es_hora_pico"
            ),
        )
    ).collect(engine="streaming")

    # Filtrar top 20% horas, y algunos filtros logicos
    debug_print("Filtrando top 20% horas...")
    df_horas = df_horas.filter(
        (col("total_transacciones_hora") > 2) & (col("total_transacciones_dia") > 12)
    )

    debug_print(f"Horas totales: {df_horas.height}")
    df_horas = df_horas.top_k(
        int(0.20 * df_horas.height), by="total_transacciones_hora"
    )

    # Muestra final
    debug_print("Join de filtro")
    df = (
        df.join(
            df_horas.lazy(),
            on=["EmpSucCaj_Id", "Factura_Fecha", "hora_id"],
            how="inner",
            suffix="_right",
        )
        .drop(pl.selectors.ends_with("_right"))
        .collect(engine="streaming")
    )

    del df_horas
    # Añadir tiempo entre transacciones y filtrar por muestras validas
    debug_print("Calculando tiempo entre transacciones...")
    df = df.with_columns(
        col("Factura_FechaHora")
        .diff()
        .over(
            "EmpSucCaj_Id",
            "Factura_Fecha",
            "hora_id",
            order_by=("EmpSucCaj_Id", "Factura_Fecha", "hora_id", "Factura_FechaHora"),
        )
        .dt.total_seconds()
        .alias("tiempo_transaccion_segs")
    ).filter(col("tiempo_transaccion_segs").is_not_null())

    # Filtros basicos
    df = df.filter(
        (col("tiempo_transaccion_segs") > 0) & (col("tiempo_transaccion_segs") < 3500)
    ).lazy()

    q1 = df.select(col("tiempo_transaccion_segs")).quantile(0.02).collect().item()
    q3 = df.select(col("tiempo_transaccion_segs")).quantile(0.98).collect().item()

    debug_print(f"q1: {q1}, q3: {q3}")

    # Coefiicientes de correlacion
    # coef_productos = 0.161  # resultado del proceso
    # coef_unidades = 0.124  # resultado del proceso
    # suma_coef = coef_productos + coef_unidades
    # # peso_productos = coef_productos / suma_coef
    # # peso_unidades = coef_unidades / suma_coef
    # peso_productos = 0.1  # Impactan por instancia
    # peso_unidades = 0.07  # Impactan por instancia
    # peso_tercera_edad = 0.02  # Impacta solo una vez
    # peso_contiene_servicios = -0.3  # Impacta solo una vez

    df = (
        df.filter(
            (col("tiempo_transaccion_segs") >= q1)
            & (col("tiempo_transaccion_segs") <= q3)
        )
        .with_columns(
            col("cantidad_productos").abs(),
            col("cantidad_unidades").abs(),
            col("precio_unitario_prom").abs(),
            col("valor_neto").qcut(4).alias("valor_neto_qcut"),
            col("precio_unitario_prom").qcut(3).alias("precio_unitario_prom_qcut"),
            (col("cantidad_unidades") - col("cantidad_productos"))
            .clip(0)
            .alias("cantidad_unidades_relativa"),
            pl.lit(1).cast(pl.Float64).alias("ctd_transacciones"),
            (
                (col("cantidad_productos"))
                .log(base=3)
                .fill_nan(None)
                .fill_null(strategy="mean")
                .cast(pl.Float64)
            ).alias("log_cantidad_productos"),
            (
                col("precio_unitario_prom")
                .log1p()
                .fill_nan(None)
                .fill_null(strategy="mean")
                .cast(pl.Float64)
            ).alias("log_precio_unitario_prom"),
        )
        .with_columns(
            col("cantidad_unidades_relativa")
            .log1p()
            .fill_nan(None)
            .fill_null(strategy="mean")
            .cast(pl.Float64)
            .alias("log_cantidad_unidades_relativa"),
        )
        # Este enfoque genera correlacion 0.162
        # .with_columns(
        #     (
        #         peso_productos * col("cantidad_productos") *
        #         (1 + peso_unidades * (col("cantidad_unidades") / col("cantidad_productos")).clip(1, 10).log1p() * 0.5)
        #     ).alias("cantidad_trabajo")
        # )
        # .with_columns(
        #     (
        #         # Trabajo base por transacción
        #         4.0
        #         +
        #         # Trabajo por cada producto diferente (buscar código, etc.)
        #         (peso_productos) * (col("cantidad_productos") - 1)
        #         +
        #         # Trabajo por cada unidad adicional (manejo y escaneo)
        #         (peso_unidades)
        #         * (col("cantidad_unidades") - col("cantidad_productos"))
        #         .clip(0)
        #         .log1p()  # Penalizar valores grandes
        #         # +
        #         # Trabajo por tercera edad
        #         # (peso_tercera_edad)
        #         # * col("es_tercera_edad").cast(pl.Float64)
        #         # +
        #         # # Trabajo por servicios
        #         # (peso_contiene_servicios)
        #         # * col("contiene_servicios").cast(pl.Float64)
        #     ).alias("cantidad_trabajo")
        .with_columns(
            (
                # Base transaction time
                216.4261
                +
                # Impact of insurance status
                43.1136 * col("es_asegurado").cast(pl.Int64)
                +
                # Impact of sales channel
                -36.6906 * (col("canal_venta_id") == "2").cast(pl.Int64)
                + 21.8788 * (col("canal_venta_id") == "1").cast(pl.Int64)
                + -5.2901 * (col("canal_venta_id") == "3").cast(pl.Int64)
                + 26.9593 * (col("canal_venta_id") == "4").cast(pl.Int64)
                + -6.8573 * (col("canal_venta_id") == "5").cast(pl.Int64)
                +
                # Impact of number of products (nonlinear transformation)
                70.0767 * col("log_cantidad_productos")
                +
                # Impact of invoice origin
                26.9593 * (col("Factura_Origen") == "PR").cast(pl.Int64)
                + -22.4259 * (col("Factura_Origen") == "EX").cast(pl.Int64)
                + -26.4123 * (col("Factura_Origen") == "PU").cast(pl.Int64)
                + 21.8788 * (col("Factura_Origen") == "FA").cast(pl.Int64)
                +
                # Impact of pharmaceutical products
                20.8107 * col("contiene_farma").cast(pl.Int64)
                +
                # Impact of senior citizen status
                17.4914 * col("es_tercera_edad").cast(pl.Int64)
                +
                # Impact of relative units (nonlinear transformation)
                10.2871 * col("log_cantidad_unidades_relativa")
                +
                # Impact of wallet accumulation
                5.5998 * col("acumula_monedero").cast(pl.Int64)
                +
                # Impact of clinic status
                4.8859 * col("es_clinica").cast(pl.Int64)
                +
                # Impact of consumer products
                0.1741 * col("contiene_consumo").cast(pl.Int64)
                +
                # Impact of unit price (nonlinear transformation)
                12.0249 * col("log_precio_unitario_prom")
            )
            .fill_null(216.4261)
            .alias("tiempo_transaccion_estimado")
        )
    ).collect(engine="streaming")

    # --- Agrupar categorías con muestras insuficientes en "otros" ---
    n_min = calculate_min_sample_size(
        confidence_level=0.95, margin_of_error=0.1, proportion=0.5
    )
    # Nota: para estos parámetros, n_min ≈ 97

    # Procesar
    categorical_columns = cs.expand_selector(df, cs_columnas_categoricas)
    df_lazy = df.lazy()

    # Process each column one by one, but maintain lazy state throughout
    for column_name in categorical_columns:
        debug_print(f"Procesando columna: {column_name}")
        # Calculate value counts in eager mode (small operation)
        counts_df = df.select(pl.col(column_name)).to_series().value_counts()

        # Find categories with insufficient samples
        categorias_insuficientes = counts_df.filter(pl.col("count") < n_min)

        if categorias_insuficientes.height > 1:
            # Create a list of values to replace (more efficient than using is_in with a dataframe)
            valores_a_reemplazar = categorias_insuficientes[column_name]

            # Update the lazy dataframe with the replacement
            df_lazy = df_lazy.with_columns(
                pl.when(pl.col(column_name).is_in(valores_a_reemplazar))
                .then(pl.lit("otros"))
                .otherwise(pl.col(column_name))
                .alias(column_name)
            )

    # Collect only once at the end with streaming engine
    df = df_lazy.collect(engine="streaming")

    return df


def calcular_correlacion_variables(
    df: pl.DataFrame,
    columna_objectivo: str,
    columnas_a_excluir: list = [],
    variables_a_correlacionar: list = [],
    num_rangos: int = 30,  # Número de rangos para variables numéricas
) -> dict[str, pl.DataFrame]:
    """
    Calcula la correlación entre variables seleccionadas y la columna objetivo.

    Args:
        df: DataFrame con los datos procesados
        columna_objectivo: Nombre de la columna objetivo para la correlación.
        columnas_a_excluir: Lista de columnas a excluir de la correlación.
        variables_a_correlacionar: Lista de variables para correlacionar.
                                  Si está vacía, se seleccionan automáticamente.
        num_rangos: Número de rangos para discretizar variables numéricas en el análisis detallado.

    Returns:
        Diccionario con dos DataFrames:
        - resumen_correlaciones: DataFrame con correlaciones de todas las variables
        - detalle_variables: DataFrame con métricas detalladas por rangos/categorías
    """
    # Verificar que la columna objetivo existe en el DataFrame
    if columna_objectivo not in df.columns:
        raise ValueError(f"El DataFrame debe contener la columna '{columna_objectivo}'")

    columna_promedio = f"{columna_objectivo}_promedio"
    columna_mediana = f"{columna_objectivo}_mediana"
    columna_desviacion = f"{columna_objectivo}_desviacion"
    columna_minimo = f"{columna_objectivo}_minimo"
    columna_maximo = f"{columna_objectivo}_maximo"

    n_min = calculate_min_sample_size(
        confidence_level=0.95, margin_of_error=0.1, proportion=0.5
    )
    debug_print(f"Tamaño mínimo de muestra: {n_min}")
    # Nota: Para estos parámetros, el tamaño mínimo de muestra es 385

    # Identificar variables numéricas y categóricas
    variables_numericas = [
        col
        for col in df.columns
        if col not in columnas_a_excluir
        and df[col].dtype.is_numeric()
        and (col in variables_a_correlacionar or len(variables_a_correlacionar) == 0)
        and col != columna_objectivo
    ]

    variables_categoricas = [
        col
        for col in df.columns
        if col not in columnas_a_excluir
        and not df[col].dtype.is_numeric()
        and (col in variables_a_correlacionar or len(variables_a_correlacionar) == 0)
        and col != columna_objectivo
    ]

    # Asegurarse objetivo es float
    df = df.with_columns(
        [
            pl.col(columna_objectivo).cast(pl.Float64),
        ]
    )
    # 1. DATAFRAME DE RESUMEN DE CORRELACIONES
    resultados_resumen = []

    # Procesar variables numéricas para el resumen
    for variable in variables_numericas:
        try:
            # Calcular correlaciones
            corr_pearson = df.select(
                pl.corr(columna_objectivo, variable).alias("correlacion_pearson")
            ).item()

            corr_spearman = df.select(
                pl.corr(columna_objectivo, variable, method="spearman").alias(
                    "correlacion_spearman"
                )
            ).item()

            # Estadísticas básicas
            stats = df.select(
                [
                    pl.mean(variable).alias("media_variable"),
                    pl.std(variable).alias("desviacion_e_variable"),
                    pl.min(variable).alias("minimo_variable"),
                    pl.max(variable).alias("maximo_variable"),
                    pl.count(variable).alias("n_observaciones"),
                ]
            ).to_dicts()[0]

            resultados_resumen.append(
                {
                    "variable": variable,
                    "tipo": "numérica",
                    "correlacion_pearson": corr_pearson,
                    "correlacion_spearman": corr_spearman,
                    "correlacion_ratio": None,  # No aplica para numéricas
                    **stats,
                }
            )
        except Exception as e:
            resultados_resumen.append(
                {"variable": variable, "tipo": "numérica", "error": str(e)}
            )

    # Procesar variables categóricas para el resumen
    for variable in variables_categoricas:
        try:
            # Calcular estadísticas básicas para la variable categórica
            categorias_count = df[variable].value_counts()
            n_categorias = categorias_count.height
            n_muestras = categorias_count["count"].median()
            n_observaciones_totales = (
                categorias_count.filter(pl.col(variable).is_not_null())
                .select("count")
                .to_series()
                .sum()
            )
            n_validos = categorias_count.filter(pl.col("count") > n_min).height

            n_muestras = (
                max(int(n_muestras), n_min)
                if isinstance(n_muestras, (int, float))
                else 0
            )
            n_observaciones_totales = (
                int(n_observaciones_totales)
                if isinstance(n_observaciones_totales, (int, float))
                else 0
            )

            correlacion_ratio = None
            if n_validos > 1:
                # Hacer remuestreo de la variable categórica
                df_resample = pl.concat(
                    [
                        resample.sample(n=n_muestras, seed=42, with_replacement=True)
                        for resample in df.partition_by(variable)
                    ]
                )

                n_observaciones = df_resample.filter(
                    pl.col(variable).is_not_null()
                ).height

                # Calcular correlación ratio (eta cuadrado) de manera segura
                try:
                    # Agrupar por la variable categórica
                    stats_por_categoria = df_resample.group_by(variable).agg(
                        [
                            pl.mean(columna_objectivo).alias(columna_promedio),
                            pl.count(columna_objectivo).alias("n_observaciones"),
                        ]
                    )

                    # Varianza total
                    varianza_total = df_resample.get_column(columna_objectivo).var()
                    varianza_total = (
                        varianza_total
                        if isinstance(varianza_total, (int, float))
                        else None
                    )

                    if varianza_total is not None and varianza_total > 0:
                        # Varianza entre grupos
                        media_global = df_resample.get_column(columna_objectivo).mean()
                        media_global = (
                            media_global
                            if isinstance(media_global, (int, float))
                            else None
                        )

                        # Calcular suma de cuadrados entre grupos
                        suma_cuadrados_entre = 0
                        for row in stats_por_categoria.to_dicts():
                            if (
                                row[columna_promedio] is not None
                                and row["n_observaciones"] > 0
                            ):
                                suma_cuadrados_entre += (
                                    row["n_observaciones"]
                                    * (row[columna_promedio] - media_global) ** 2
                                )

                        # Correlación ratio (eta cuadrado)
                        correlacion_ratio = suma_cuadrados_entre / (
                            varianza_total * n_observaciones
                        )
                except Exception as e:
                    # Si hay error en el cálculo de correlación, continuar con valor nulo
                    correlacion_ratio = None
                    del e
            else:
                print(
                    f"No hay suficientes categorías válidas {n_validos}/{n_categorias} para {variable}"
                )

            # Añadir al resultado de resumen
            resultados_resumen.append(
                {
                    "variable": variable,
                    "tipo": "categórica",
                    "correlacion_pearson": None,  # No aplica para categóricas
                    "correlacion_spearman": None,  # No aplica para categóricas
                    "correlacion_ratio": correlacion_ratio,
                    "media_variable": None,  # No aplica directamente para categóricas
                    "desviacion_e_variable": None,
                    "minimo_variable": None,
                    "maximo_variable": None,
                    "n_observaciones": n_observaciones_totales,
                    "n_categorias": n_categorias,
                }
            )
        except Exception as e:
            resultados_resumen.append(
                {"variable": variable, "tipo": "categórica", "error": str(e)}
            )

    # Crear DataFrame de resumen
    df_resumen = pl.DataFrame(
        resultados_resumen,
        schema=pl.Schema(
            {
                "variable": pl.Utf8,
                "tipo": pl.Utf8,
                "correlacion_pearson": pl.Float64,
                "correlacion_spearman": pl.Float64,
                "correlacion_ratio": pl.Float64,
                "media_variable": pl.Float64,
                "desviacion_e_variable": pl.Float64,
                "minimo_variable": pl.Float64,
                "maximo_variable": pl.Float64,
                "n_observaciones": pl.Int64,
                "n_categorias": pl.Int64,
            }
        ),
    )

    # Ordenar por correlación (absoluta) de mayor a menor
    if df_resumen.height > 0:
        df_resumen = df_resumen.with_columns(
            [
                (
                    pl.when(col("correlacion_pearson").is_not_null())
                    .then(col("correlacion_pearson").abs())
                    .otherwise(
                        pl.when(col("correlacion_ratio").is_not_null())
                        .then(col("correlacion_ratio"))
                        .otherwise(0)
                    )
                    * pl.lit(100.0)
                )
                .round(6)
                .alias("valor_correlacion_0_100")
            ]
        ).sort("valor_correlacion_0_100", descending=True)

    # 2. DATAFRAME DE DETALLE POR RANGOS/CATEGORÍAS
    resultados_detalle = []

    # Procesar variables numéricas para el detalle (por rangos)
    for variable in variables_numericas:
        try:
            # Crear rangos para la variable numérica
            min_val = df[variable].min()
            max_val = df[variable].max()
            min_val = float(min_val) if isinstance(min_val, (int, float)) else 0.0
            max_val = float(max_val) if isinstance(max_val, (int, float)) else 99.0
            n_min_rangos = calculate_min_sample_size(
                confidence_level=0.95, margin_of_error=0.05
            )

            if min_val is not None and max_val is not None and min_val != max_val:
                # Ordenar los valores para determinar puntos de corte
                valores_ordenados = (
                    df.select(pl.col(variable))
                    .sort(variable)
                    .filter(pl.col(variable).is_not_null())
                )
                total_muestras = valores_ordenados.height

                if (
                    total_muestras >= n_min_rangos * 2
                ):  # Verificar que hay suficientes muestras para al menos 2 rangos
                    # Primera iteración: crear rangos iniciales
                    max_rangos_posibles = min(
                        num_rangos, total_muestras // n_min_rangos
                    )

                    if max_rangos_posibles <= 1:
                        # Si solo podemos tener un rango, crear uno que abarque todo
                        df_con_rangos = df.with_columns(
                            [
                                pl.lit(0).alias("rango_idx"),
                                pl.lit(min_val).alias("rango_min"),
                                pl.lit(max_val).alias("rango_max"),
                            ]
                        )
                    else:
                        # Crear rangos iniciales con aproximadamente igual número de muestras
                        puntos_corte = []
                        muestras_por_rango = total_muestras // max_rangos_posibles

                        for i in range(1, max_rangos_posibles):
                            idx = i * muestras_por_rango - 1
                            if idx < total_muestras:
                                puntos_corte.append(valores_ordenados[idx, variable])

                        # Añadir columna con el rango inicial
                        def asignar_rango_inicial(x):
                            if x is None:
                                return None
                            for i, corte in enumerate(puntos_corte):
                                if x <= corte:
                                    return i
                            return len(puntos_corte)

                        df_con_rangos_inicial = df.with_columns(
                            pl.col(variable)
                            .map_elements(asignar_rango_inicial)
                            .alias("rango_idx_inicial")
                        )

                        # Contar muestras en cada rango inicial
                        conteo_rangos = df_con_rangos_inicial.group_by(
                            "rango_idx_inicial"
                        ).agg(
                            pl.count().alias("n_muestras"),
                            pl.min(variable).alias("rango_min"),
                            pl.max(variable).alias("rango_max"),
                        )

                        # Segunda iteración: fusionar rangos con muestras insuficientes
                        rangos_finales = []
                        rango_actual = None
                        muestras_acumuladas = 0

                        for row in conteo_rangos.sort("rango_idx_inicial").to_dicts():
                            if rango_actual is None:
                                rango_actual = {
                                    "rango_idx_inicial": row["rango_idx_inicial"],
                                    "rango_min": row["rango_min"],
                                    "rango_max": row["rango_max"],
                                    "n_muestras": row["n_muestras"],
                                }
                                muestras_acumuladas = row["n_muestras"]
                            elif (
                                muestras_acumuladas < n_min_rangos
                                or row["n_muestras"] < n_min_rangos
                            ):
                                # Fusionar con el rango actual si alguno no tiene suficientes muestras
                                rango_actual["rango_max"] = row["rango_max"]
                                muestras_acumuladas += row["n_muestras"]
                            else:
                                # Guardar el rango actual y comenzar uno nuevo
                                rangos_finales.append(rango_actual)
                                rango_actual = {
                                    "rango_idx_inicial": row["rango_idx_inicial"],
                                    "rango_min": row["rango_min"],
                                    "rango_max": row["rango_max"],
                                    "n_muestras": row["n_muestras"],
                                }
                                muestras_acumuladas = row["n_muestras"]

                        # Añadir el último rango
                        if rango_actual is not None:
                            rangos_finales.append(rango_actual)

                        # Crear mapeo de rangos iniciales a rangos finales
                        mapeo_rangos = {}
                        for idx, rango in enumerate(rangos_finales):
                            rango_inicial_min = rango["rango_idx_inicial"]
                            if idx < len(rangos_finales) - 1:
                                rango_inicial_max = (
                                    rangos_finales[idx + 1]["rango_idx_inicial"] - 1
                                )
                            else:
                                rango_inicial_max = max_val

                            for i in range(
                                rango_inicial_min, int(rango_inicial_max) + 1
                            ):
                                mapeo_rangos[i] = idx

                        # Crear DataFrame con los límites finales
                        df_limites = pl.DataFrame(
                            [
                                {
                                    "rango_idx": idx,
                                    "rango_min": rango["rango_min"],
                                    "rango_max": rango["rango_max"],
                                }
                                for idx, rango in enumerate(rangos_finales)
                            ]
                        )

                        # Asignar rangos finales
                        df_con_rangos = df_con_rangos_inicial.with_columns(
                            pl.col("rango_idx_inicial")
                            .map_elements(
                                lambda x: mapeo_rangos.get(x) if x is not None else None
                            )
                            .alias("rango_idx")
                        )

                        # Unir con el DataFrame de límites
                        df_con_rangos = df_con_rangos.join(
                            df_limites, on="rango_idx", how="left"
                        ).drop("rango_idx_inicial")
                else:
                    # Si no hay suficientes muestras, crear un solo rango
                    df_con_rangos = df.with_columns(
                        [
                            pl.lit(0).alias("rango_idx"),
                            pl.lit(min_val).alias("rango_min"),
                            pl.lit(max_val).alias("rango_max"),
                        ]
                    )

                # Agrupar por rango y calcular estadísticas
                stats_por_rango = (
                    df_con_rangos.group_by("rango_idx", "rango_min", "rango_max")
                    .agg(
                        [
                            pl.mean(columna_objectivo).alias(columna_promedio),
                            pl.median(columna_objectivo).alias(columna_mediana),
                            pl.std(columna_objectivo).alias(columna_desviacion),
                            pl.min(columna_objectivo).alias(columna_minimo),
                            pl.max(columna_objectivo).alias(columna_maximo),
                            pl.count(columna_objectivo).alias("n_observaciones"),
                            pl.mean(variable).alias("media_variable"),
                            pl.std(variable).alias("desviacion_e_variable"),
                            pl.min(variable).alias("minimo_variable"),
                            pl.max(variable).alias("maximo_variable"),
                        ]
                    )
                    .sort("rango_idx")
                )

                # Añadir al resultado
                for rango_row in stats_por_rango.to_dicts():
                    etiqueta_rango = (
                        f"{rango_row['rango_min']:.2f} - {rango_row['rango_max']:.2f}"
                    )

                    resultados_detalle.append(
                        {
                            "variable": variable,
                            "tipo": "numérica",
                            "categoria_o_rango": etiqueta_rango,
                            "rango_min": rango_row["rango_min"],
                            "rango_max": rango_row["rango_max"],
                            columna_promedio: rango_row[columna_promedio],
                            columna_mediana: rango_row[columna_mediana],
                            columna_desviacion: rango_row[columna_desviacion],
                            columna_minimo: rango_row[columna_minimo],
                            columna_maximo: rango_row[columna_maximo],
                            "n_observaciones": rango_row["n_observaciones"],
                            "media_variable": rango_row["media_variable"],
                            "desviacion_e_variable": rango_row["desviacion_e_variable"],
                            "minimo_variable": rango_row["minimo_variable"],
                            "maximo_variable": rango_row["maximo_variable"],
                            "error": None,
                        }
                    )
            else:
                # Si todos los valores son iguales, no se pueden crear rangos
                stats = df.select(
                    [
                        pl.mean(columna_objectivo).alias(columna_promedio),
                        pl.median(columna_objectivo).alias(columna_mediana),
                        pl.std(columna_objectivo).alias(columna_desviacion),
                        pl.min(columna_objectivo).alias(columna_minimo),
                        pl.max(columna_objectivo).alias(columna_maximo),
                        pl.count(columna_objectivo).alias("n_observaciones"),
                        pl.mean(variable).alias("media_variable"),
                        pl.std(variable).alias("desviacion_e_variable"),
                        pl.min(variable).alias("minimo_variable"),
                        pl.max(variable).alias("maximo_variable"),
                    ]
                ).to_dicts()[0]

                resultados_detalle.append(
                    {
                        "variable": variable,
                        "tipo": "numérica",
                        "categoria_o_rango": f"Valor único: {min_val}",
                        "rango_min": min_val,
                        "rango_max": max_val,
                        **stats,
                    }
                )

        except Exception as e:
            resultados_detalle.append(
                {
                    "variable": variable,
                    "tipo": "numérica",
                    "categoria_o_rango": "ERROR",
                    "error": str(e),
                }
            )

    # Procesar variables categóricas para el detalle
    for variable in variables_categoricas:
        try:
            # Calcular estadísticas por categoría
            stats_por_categoria = (
                df.group_by(variable)
                .agg(
                    [
                        pl.mean(columna_objectivo).alias(columna_promedio),
                        pl.median(columna_objectivo).alias(columna_mediana),
                        pl.std(columna_objectivo).alias(columna_desviacion),
                        pl.min(columna_objectivo).alias(columna_minimo),
                        pl.max(columna_objectivo).alias(columna_maximo),
                        pl.count(columna_objectivo).alias("n_observaciones"),
                    ]
                )
                .sort(columna_promedio, descending=True)
            )

            # Añadir al resultado
            for cat_row in stats_por_categoria.to_dicts():
                categoria = cat_row[variable]
                categoria_str = "NULL" if categoria is None else str(categoria)

                resultados_detalle.append(
                    {
                        "variable": variable,
                        "tipo": "categórica",
                        "categoria_o_rango": categoria_str,
                        "rango_min": None,
                        "rango_max": None,
                        columna_promedio: cat_row[columna_promedio],
                        columna_mediana: cat_row[columna_mediana],
                        columna_desviacion: cat_row[columna_desviacion],
                        columna_minimo: cat_row[columna_minimo],
                        columna_maximo: cat_row[columna_maximo],
                        "n_observaciones": cat_row["n_observaciones"],
                        "media_variable": None,
                        "desviacion_e_variable": None,
                        "minimo_variable": None,
                        "maximo_variable": None,
                        "error": None,
                    }
                )
        except Exception as e:
            resultados_detalle.append(
                {
                    "variable": variable,
                    "tipo": "categórica",
                    "categoria_o_rango": "ERROR",
                    "error": str(e),
                }
            )

    # Crear DataFrame de detalle
    df_detalle = pl.DataFrame(
        resultados_detalle,
        schema=pl.Schema(
            {
                "variable": pl.Utf8,
                "tipo": pl.Utf8,
                "categoria_o_rango": pl.Utf8,
                "rango_min": pl.Float64,
                "rango_max": pl.Float64,
                columna_promedio: pl.Float64,
                columna_mediana: pl.Float64,
                columna_desviacion: pl.Float64,
                columna_minimo: pl.Float64,
                columna_maximo: pl.Float64,
                "n_observaciones": pl.Int64,
                "media_variable": pl.Float64,
                "desviacion_e_variable": pl.Float64,
                "minimo_variable": pl.Float64,
                "maximo_variable": pl.Float64,
                "error": pl.Utf8,
            }
        ),
    )

    # Ordenar por variable y luego por número de observaciones
    if df_detalle.height > 0:
        df_detalle = df_detalle.sort(
            ["variable", "n_observaciones"], descending=[False, True]
        )

    # Devolver ambos resultados
    return {
        "resumen_correlaciones": df_resumen,
        "detalle_variables": df_detalle,
    }


def evaluate_multimodels(
    df_in, return_model: bool = False, target: str = "tiempo_transaccion_segs"
):
    """
    Evaluates multiple regression models on the given Polars DataFrame using 5-fold cross-validation.

    Assumes the target column is 'tiempo_transaccion_segs'. This function sanitizes column names
    to ensure compatibility with XGBoost.

    Args:
        df (pl.DataFrame): Input Polars DataFrame with features and target.
        return_model (bool): If True, stores the fitted model in the results.

    Returns:
        dict: A dictionary where each key is a model name and the value is a dictionary
              with evaluation metrics and optionally the trained model.
    """
    # Check if target column exists
    if target not in df_in.columns:
        raise ValueError(
            f"Target column '{target}' not found in DataFrame. Available columns: {df_in.columns}"
        )
    df: pd.DataFrame = df_in.to_pandas()

    # Separate features and target.
    y = df[target]
    X = df.drop(target, axis=1)

    # One-hot encode categorical features using Polars' to_dummies().
    X_dummies = pd.get_dummies(X)

    # Ensure valid column names.
    X_dummies.columns = [
        str(col)
        .replace("[", "_")
        .replace("]", "_")
        .replace("<", "_")
        .replace(">", "_")
        .replace(" ", "_")
        for col in X_dummies.columns
    ]

    # Convert to Pandas (scikit-learn requires a Pandas DataFrame or NumPy array).
    X_pd = X_dummies
    y_pd = y

    # Define candidate models with parallel processing enabled (n_jobs=-1).
    models = {
        "LinearRegression": LinearRegression(),
        "XGBRegressor": XGBRegressor(
            n_estimators=15, learning_rate=0.1, max_depth=6, random_state=42, n_jobs=4
        ),
        "RandomForest": RandomForestRegressor(
            n_estimators=15, random_state=42, n_jobs=4
        ),
    }

    results = {}
    scoring = {"r2": "r2", "neg_mse": "neg_mean_squared_error"}

    for name, model in models.items():
        cv_results = cross_validate(
            model, X_pd, y_pd, cv=5, scoring=scoring, return_train_score=True
        )
        # Compute RMSE from negative MSE.
        train_rmse = np.mean(np.sqrt(-cv_results["train_neg_mse"]))
        test_rmse = np.mean(np.sqrt(-cv_results["test_neg_mse"]))
        results[name] = {
            "train_r2": np.mean(cv_results["train_r2"]),
            "test_r2": np.mean(cv_results["test_r2"]),
            "train_rmse": train_rmse,
            "test_rmse": test_rmse,
            "model": model.fit(X_pd, y_pd) if return_model else None,
        }

    return results


def save_model(model, filename):
    """
    Saves the given model to the .cache/ directory using joblib.

    Args:
        model: The trained model to be saved.
        filename (str): The filename for the saved model (e.g., "model.pkl").
    """
    os.makedirs(".cache", exist_ok=True)
    import joblib

    filepath = os.path.join(".cache", filename)
    joblib.dump(model, filepath)
    print(f"Model saved to {filepath}")
    # To load the model later, use:
    # loaded_model = joblib.load(filepath)


def check_feature_importance(
    model, X, y: pl.Series, sample_size=None, n_repeats=5, scoring="r2"
):
    """
    Computes permutation importance for the given model on the provided Polars dataset,
    then displays the features ranked by their effect on R².

    Args:
        model: The trained model (assumed to be already fitted).
        X (pl.DataFrame): Feature Polars DataFrame.
        y (pl.Series): Target variable (Polars Series).
        sample_size (int, optional): If provided and X is larger, a random sample of this size is used.
        n_repeats (int): Number of times to permute each feature.
        scoring (str): Scoring metric, default is 'r2'.

    Returns:
        pd.DataFrame: A DataFrame with features and their average importance scores.
    """
    from sklearn.inspection import permutation_importance

    # Sanitize and work with full Polars DataFrame.
    X = X.to_pandas()
    # Convert to Pandas.
    X_pd = pd.get_dummies(X)
    y_pd = y.to_pandas()

    # Ensure valid column names.
    X_pd.columns = [
        str(col)
        .replace("[", "_")
        .replace("]", "_")
        .replace("<", "_")
        .replace(">", "_")
        .replace(" ", "_")
        for col in X_pd.columns
    ]

    # Use a subset if sample_size is provided.
    if sample_size is not None and len(X_pd) > sample_size:
        X_sample = X_pd.sample(n=sample_size, random_state=42)
        y_sample = y_pd.loc[X_sample.index]
    else:
        X_sample = X_pd
        y_sample = y_pd

    imp_results = permutation_importance(
        model, X_sample, y_sample, n_repeats=n_repeats, scoring=scoring, random_state=42
    )
    importance_df = pd.DataFrame(
        {"feature": X_pd.columns, "importance": imp_results.importances_mean}  # type: ignore
    ).sort_values(by="importance", ascending=False)

    print("Permutation Importance (effect on R²):")
    print(importance_df)
    return importance_df


# ----------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------
if __name__ == "__main__":
    # ----------------------------------------------------------------------------------
    from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_bi

    cfg.set_tbl_cols(20)
    df = get_transactions_data(
        dwh_farinter_bi,
        config=ConfigGetTrxData(
            fecha_desde=pdt.today().subtract(days=365),
            use_cache=True,
            cache_max_age_seconds=3600 * 3600,
        ),
    )
    print(
        df.select(
            "EmpSucDocCajFac_Id",
            pl.selectors.matches(".*Fecha.*"),
            pl.selectors.matches(".*cantidad.*"),
            pl.selectors.matches(".*valor.*"),
            pl.selectors.matches(".*contiene.*"),
            pl.selectors.matches(".*es_.*"),
        ).describe()
    )
    df = tranform_transactions_data(df)

    exclude_columns = [
        "EmpSucDocCajFac_Id",
        "Emp_Id",
        "Suc_Id",
        "Caja_Id",
        "Factura_Id",
        "Factura_Fecha",
        "Factura_FechaHora",
        "EmpSucCaj_Id",
        "es_hora_pico",
        "max_transacciones_dia",
        "valor_neto",
        "hora_id",
        "total_transacciones_hora",
    ]

    print(
        df.select(
            "EmpSucDocCajFac_Id", "Factura_FechaHora", "tiempo_transaccion_segs"
        ).describe()
    )

    df_sensibles = (
        df.select(
            (cs.numeric() | cs.categorical() | cs.string() | cs.boolean()).exclude(
                exclude_columns
            )
        )
        .sample(300000, seed=42)
        .select(
            "es_tercera_edad",
            # "cantidad_productos",
            "tiempo_transaccion_segs",
            # "valor_neto_qcut",
            # "cantidad_unidades_relativa",
            # "es_clinica",
            "es_asegurado",
            # "cantidad_unidades",
            # "contiene_servicios",
            # "contiene_consumo",
            "contiene_farma",
            # "Factura_Origen",
            # "canal_venta_id",
            # "precio_unitario_prom",
            # "precio_unitario_prom_qcut",
            # "TipoDoc_id",
            # "Tipo_Plan",
            # "TipoCliente_Nombre",
            # "Suc_Id",
            "acumula_monedero",
            "log_cantidad_productos",
            "log_precio_unitario_prom",
            "log_cantidad_unidades_relativa",
            # "ctd_transacciones",
        )
    )

    # --------------------------------
    # -------------------------------------------------------------------------
    # -------------------------------------------------------------------------
    # Multimodel evaluation:
    print(df_sensibles.columns)
    eval_results = evaluate_multimodels(
        df_sensibles, return_model=True, target="tiempo_transaccion_segs"
    )
    print("Multimodel Evaluation Results:")
    for model_name, metrics in eval_results.items():
        print(f"\nModel: {model_name}")
        print(
            f"  Train RMSE: {metrics['train_rmse']:.4f}, Train R²: {metrics['train_r2']:.4f}"
        )
        print(
            f"  Test  RMSE: {metrics['test_rmse']:.4f}, Test  R²: {metrics['test_r2']:.4f}"
        )
        if "model" in metrics:
            save_model(metrics["model"], f"model_{model_name}.pkl")
            X_full = df_sensibles.drop("tiempo_transaccion_segs")
            y_full = df_sensibles["tiempo_transaccion_segs"]
            rf_model = metrics["model"]
            importance_df = check_feature_importance(
                rf_model, X_full, y_full, sample_size=200, n_repeats=10
            )

    # -------------------------------------------------------------------------
    # Save one of the trained models, if needed.
    # For example, to save the XGBRegressor model:
    # best_model = eval_results["XGBRegressor"]["model"]
    # save_model(best_model, "best_model_xgb.pkl")

    # -------------------------------------------------------------------------
    # Check feature importance (using permutation importance) for a chosen model.
    # Here we use RandomForest as an example.
    # X_full = df_sensibles.drop("tiempo_transaccion_segs")
    # y_full = df_sensibles["tiempo_transaccion_segs"]
    # rf_model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    # rf_model.fit(X_full.to_dummies()), y_full))
    # importance_df = check_feature_importance(rf_model, X_full.to_dummies(), y_full, sample_size=200, n_repeats=10)

    # --------------------------------

    resultados = calcular_correlacion_variables(
        df,
        columna_objectivo="tiempo_transaccion_segs",
        # columna_objectivo="cantidad_productos",
        columnas_a_excluir=exclude_columns,
    )
    print(resultados["detalle_variables"].head(100))
    print(resultados["resumen_correlaciones"].head(100))
    resultados["detalle_variables"].write_excel(".cache/detalle_variables.xlsx")
    resultados["resumen_correlaciones"].write_excel(".cache/resumen_correlaciones.xlsx")
    if 1 == 1:
        exit()

    # -----------------------------------
    # Bootstrap test - 20 iteraciones
    print("\n--- Iniciando prueba de bootstrap (20 iteraciones) ---")

    # Número de iteraciones de bootstrap
    n_bootstrap = 20

    # Listas para almacenar resultados de bootstrap
    bootstrap_resumen = []
    bootstrap_detalle = []

    sample_size = max(int(df.height * 0.25), calculate_min_sample_size())

    # Realizar bootstrap
    for i in range(n_bootstrap):
        print(f"Iteración bootstrap {i + 1}/{n_bootstrap}")

        # Tomar una muestra bootstrap
        df_bootstrap = df.sample(n=sample_size, with_replacement=True, seed=i + 1)

        # Calcular correlaciones en la muestra bootstrap
        resultados_bootstrap = calcular_correlacion_variables(
            df_bootstrap,
            columna_objectivo="tiempo_transaccion_segs",
            # columna_objectivo="cantidad_productos",
            columnas_a_excluir=[
                "EmpSucDocCajFac_Id",
                "Emp_Id",
                "Suc_Id",
                "Caja_Id",
                "Factura_Id",
                "Factura_Fecha",
                "Factura_FechaHora",
                "EmpSucCaj_Id",
                "es_hora_pico",
                "max_transacciones_dia",
                "valor_neto",
                "hora_id",
                "total_transacciones_hora",
            ],
        )

        # Añadir número de iteración a los resultados
        df_resumen_iter = resultados_bootstrap["resumen_correlaciones"].with_columns(
            pl.lit(i + 1).alias("bootstrap_iteracion")
        )

        df_detalle_iter = resultados_bootstrap["detalle_variables"].with_columns(
            pl.lit(i + 1).alias("bootstrap_iteracion")
        )

        # Almacenar resultados
        bootstrap_resumen.append(df_resumen_iter)
        bootstrap_detalle.append(df_detalle_iter)

    # Combinar todos los resultados de bootstrap
    df_bootstrap_resumen_all = pl.concat(bootstrap_resumen)
    df_bootstrap_detalle_all = pl.concat(bootstrap_detalle)

    # Identificar columnas numéricas en ambos dataframes
    columnas_numericas_resumen = [
        col
        for col in df_bootstrap_resumen_all.columns
        if df_bootstrap_resumen_all[col].dtype.is_numeric()
        and col not in ["bootstrap_iteracion"]
    ]

    columnas_numericas_detalle = [
        col
        for col in df_bootstrap_detalle_all.columns
        if df_bootstrap_detalle_all[col].dtype.is_numeric()
        and col not in ["bootstrap_iteracion", "rango_min", "rango_max"]
    ]

    # Calcular estadísticas de bootstrap para el resumen
    # Usar expresiones de Polars para crear todas las métricas de una vez
    aggs_resumen = []
    for col_name in columnas_numericas_resumen:
        aggs_resumen.extend(
            [
                pl.mean(col_name).alias(f"{col_name}_media"),
                pl.median(col_name).alias(f"{col_name}_mediana"),
                pl.std(col_name).alias(f"{col_name}_desviacion"),
                pl.quantile(col_name, 0.05).alias(f"{col_name}_p05"),
                pl.quantile(col_name, 0.95).alias(f"{col_name}_p95"),
                pl.min(col_name).alias(f"{col_name}_min"),
                pl.max(col_name).alias(f"{col_name}_max"),
            ]
        )

    df_bootstrap_resumen_stats = df_bootstrap_resumen_all.group_by(
        "variable", "tipo"
    ).agg([*aggs_resumen, pl.count().alias("n_bootstrap")])

    # Calcular estadísticas de bootstrap para el detalle
    # Agrupar por variable, tipo y categoría/rango
    aggs_detalle = []
    for col_name in columnas_numericas_detalle:
        aggs_detalle.extend(
            [
                pl.mean(col_name).alias(f"{col_name}_media"),
                pl.median(col_name).alias(f"{col_name}_mediana"),
                pl.std(col_name).alias(f"{col_name}_desviacion"),
                pl.quantile(col_name, 0.05).alias(f"{col_name}_p05"),
                pl.quantile(col_name, 0.95).alias(f"{col_name}_p95"),
                pl.min(col_name).alias(f"{col_name}_min"),
                pl.max(col_name).alias(f"{col_name}_max"),
            ]
        )

    df_bootstrap_detalle_stats = df_bootstrap_detalle_all.group_by(
        "variable", "tipo", "categoria_o_rango"
    ).agg([*aggs_detalle, pl.count().alias("n_bootstrap")])

    # Calcular coeficientes de variación para todas las métricas numéricas
    # Resumen
    cv_exprs_resumen = [
        (pl.col(f"{col_name}_desviacion") / pl.col(f"{col_name}_media").abs()).alias(
            f"cv_{col_name}"
        )
        for col_name in columnas_numericas_resumen
        if col_name not in ["n_observaciones", "n_categorias"]  # Excluir conteos
    ]

    df_bootstrap_resumen_stats = df_bootstrap_resumen_stats.with_columns(
        cv_exprs_resumen
    )

    # Detalle
    cv_exprs_detalle = [
        (pl.col(f"{col_name}_desviacion") / pl.col(f"{col_name}_media").abs()).alias(
            f"cv_{col_name}"
        )
        for col_name in columnas_numericas_detalle
        if col_name not in ["n_observaciones"]  # Excluir conteos
    ]

    df_bootstrap_detalle_stats = df_bootstrap_detalle_stats.with_columns(
        cv_exprs_detalle
    )

    # Unir con los resultados originales para comparación
    df_bootstrap_resumen_comparacion = df_bootstrap_resumen_stats.join(
        resultados["resumen_correlaciones"],
        on=["variable", "tipo"],
        how="left",
        suffix="_original",
    )

    df_bootstrap_detalle_comparacion = df_bootstrap_detalle_stats.join(
        resultados["detalle_variables"],
        on=["variable", "tipo", "categoria_o_rango"],
        how="left",
        suffix="_original",
    )

    # Crear métricas de estabilidad general
    df_bootstrap_resumen_comparacion = df_bootstrap_resumen_comparacion.with_columns(
        [
            pl.when(pl.col("tipo") == "numérica")
            .then(pl.col("cv_correlacion_pearson"))
            .otherwise(pl.col("cv_correlacion_ratio"))
            .alias("cv_principal")
        ]
    ).sort(["tipo", "cv_principal"])

    # Guardar resultados
    df_bootstrap_resumen_all.write_excel(".cache/bootstrap_resumen_all_iterations.xlsx")
    df_bootstrap_detalle_all.write_excel(".cache/bootstrap_detalle_all_iterations.xlsx")
    df_bootstrap_resumen_stats.write_excel(".cache/bootstrap_resumen_stats.xlsx")
    df_bootstrap_detalle_stats.write_excel(".cache/bootstrap_detalle_stats.xlsx")
    df_bootstrap_resumen_comparacion.write_excel(
        ".cache/bootstrap_resumen_comparacion.xlsx"
    )
    df_bootstrap_detalle_comparacion.write_excel(
        ".cache/bootstrap_detalle_comparacion.xlsx"
    )

    print("\n--- Resultados del bootstrap guardados en .cache/bootstrap_*.xlsx ---")
    print("Resumen de estadísticas bootstrap (top 10 variables por estabilidad):")
    print(df_bootstrap_resumen_comparacion.head(10))
