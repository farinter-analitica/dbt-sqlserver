import datetime as dt
import pendulum as pdt
import polars as pl
import dagster as dg
from polars import col
from dagster_shared_gf.resources.sql_server_resources import (
    SQLServerResource,
)
from dagster_shared_gf.shared_helpers import ParquetCacheHandler
from textwrap import dedent
from pydantic import Field


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
        FP.cantidad_unidades,
        FP.cantidad_productos,
        FP.valor_neto,
        FP.contiene_servicios,
        FP.contiene_tengo,
        TB.TipoBodega_Nombre,
        S.Departamento_Id,
        S.EmpDepMunCiu_Id,
        S.Zona_Nombre,
        S.TipoSucursal_Id,
        FP.acumula_monedero,
        TC.TipoCliente_Nombre,
        CASE WHEN TC.TipoCliente_Nombre LIKE '%TER%EDAD%' 
                OR M.Tipo_Plan LIKE '%TER%EDAD%' 
            THEN 1 ELSE 0 END AS es_tercera_edad,
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
            CASE WHEN SUM(FP.Valor_Acum_Monedero)>0 THEN 1 ELSE 0 END AS acumula_monedero,
            CASE WHEN MAX(CASE WHEN A.DeptoArt_Nombre LIKE '%SERVICIO%' THEN 1 ELSE 0 END) =1
                THEN 1
                ELSE 0
            END AS contiene_servicios,
            CASE WHEN MAX(CASE WHEN A.Articulo_Nombre LIKE '%TENGO%' THEN 1 ELSE 0 END) =1
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
        .collect()
    )

    if config.use_cache:
        cache_handler = ParquetCacheHandler(filename="ut_trx_data")
        cache_handler.save_to_cache(df)

    return df


def tranform_transactions_data(df: pl.DataFrame) -> pl.DataFrame:
    # Calcular hora_id y clave caja
    df = df.with_columns(
        col("Factura_FechaHora").dt.hour().alias("hora_id"),
        pl.concat_str(col("Emp_Id"), col("Suc_Id"), col("Caja_Id")).alias(
            "EmpSucCaj_Id"
        ),
        # Convertir IDs a variables categoricas
        col("TipoSucursal_Id").cast(pl.Utf8).cast(pl.Categorical),
        col("TipoDoc_id").cast(pl.Utf8).cast(pl.Categorical),
        col("acumula_monedero").cast(pl.Utf8).cast(pl.Categorical),
        col("es_.*").cast(pl.Utf8).cast(pl.Categorical),
        col("contiene_.*").cast(pl.Utf8).cast(pl.Categorical),
    )

    # Identificar metricas de transacciones
    df_horas = (
        df.group_by("EmpSucCaj_Id", "Factura_Fecha", "hora_id")
        .agg(
            col("EmpSucCaj_Id").count().alias("total_transacciones_hora"),
        )
        .with_columns(
            # Identificar horas pico para cada sucursal y fecha
            pl.max("total_transacciones_hora")
            .over(["EmpSucCaj_Id", "Factura_Fecha"])
            .alias("max_transacciones_dia")
        )
        .with_columns(
            (col("total_transacciones_hora") == col("max_transacciones_dia")).alias(
                "es_hora_pico"
            ),
        )
    )

    # Filtrar solo horas pico y de esas top 25% horas, y algunos filtros logicos
    df_horas = df_horas.filter(
        col("es_hora_pico") & (col("total_transacciones_hora") > 2)
    ).top_k(int(0.25 * df_horas.height), by="total_transacciones_hora")

    # Muestra final
    df = df.join(
        df_horas, on=["EmpSucCaj_Id", "hora_id"], how="inner", suffix="_right"
    ).drop(pl.selectors.ends_with("_right"))

    # Añadir tiempo entre transacciones y filtrar por muestras validas
    df = (
        df.sort("EmpSucCaj_Id", "Factura_Fecha", "hora_id", "Factura_FechaHora")
        .with_columns(
            col("Factura_FechaHora")
            .diff()
            .over("EmpSucCaj_Id", "Factura_Fecha", "hora_id")
            .dt.total_seconds()
            .alias("tiempo_transaccion_segs")
        )
        .filter(col("tiempo_transaccion_segs").is_not_null())
    )

    # Filtros basicos
    df = df.filter(
        (col("tiempo_transaccion_segs") > 0) & (col("tiempo_transaccion_segs") < 3500)
    )

    q1 = df["tiempo_transaccion_segs"].quantile(0.01)
    q3 = df["tiempo_transaccion_segs"].quantile(0.99)

    df = df.filter(
        (col("tiempo_transaccion_segs") >= q1) & (col("tiempo_transaccion_segs") <= q3)
    ).with_columns(
        pl.max_horizontal("cantidad_productos", "cantidad_unidades").alias(
            "cantidad_trabajo"
        )
    )

    return df


def calcular_correlacion_variables(
    df: pl.DataFrame,
    columna_objectivo: str,
    columnas_a_excluir: list = [],
    variables_a_correlacionar: list = [],
    num_rangos: int = 5,  # Número de rangos para variables numéricas
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
            n_categorias = df[variable].n_unique()
            n_observaciones = df.filter(pl.col(variable).is_not_null()).height

            # Calcular correlación ratio (eta cuadrado) de manera segura
            correlacion_ratio = None

            try:
                # Agrupar por la variable categórica
                stats_por_categoria = df.group_by(variable).agg(
                    [
                        pl.mean(columna_objectivo).alias(columna_promedio),
                        pl.count(columna_objectivo).alias("n_observaciones"),
                    ]
                )

                # Solo calcular si hay más de una categoría y suficientes datos
                if n_categorias > 1 and n_observaciones > 0:
                    # Varianza total
                    varianza_total = df.select(pl.var(columna_objectivo)).item()

                    if varianza_total is not None and varianza_total > 0:
                        # Varianza entre grupos
                        media_global = df.select(pl.mean(columna_objectivo)).item()

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
                    "n_observaciones": n_observaciones,
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

            if min_val is not None and max_val is not None and min_val != max_val:
                # Crear rangos
                rango_size = (max_val - min_val) / float(num_rangos)

                # Añadir columna con el rango
                df_con_rangos = df.with_columns(
                    [
                        pl.when(col(variable).is_not_null())
                        .then(
                            ((col(variable) - min_val) / rango_size)
                            .floor()
                            .cast(pl.Int32)
                        )
                        .otherwise(None)
                        .alias("rango_idx")
                    ]
                )

                # Calcular límites de cada rango
                df_con_rangos = df_con_rangos.with_columns(
                    [
                        pl.lit(min_val).alias("min_global"),
                        pl.lit(rango_size).alias("rango_size"),
                    ]
                )

                df_con_rangos = df_con_rangos.with_columns(
                    [
                        (
                            col("min_global") + col("rango_idx") * col("rango_size")
                        ).alias("rango_min"),
                        (
                            col("min_global")
                            + (col("rango_idx") + 1) * col("rango_size")
                        ).alias("rango_max"),
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


if __name__ == "__main__":
    from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_bi

    with pl.Config(set_tbl_cols=20) as cfg:
        df = get_transactions_data(
            dwh_farinter_bi,
            config=ConfigGetTrxData(
                fecha_desde=pdt.today().subtract(days=15),
                use_cache=True,
                cache_max_age_seconds=3600 * 24,
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
        print(
            df.select(
                "EmpSucDocCajFac_Id", "Factura_FechaHora", "tiempo_transaccion_segs"
            ).describe()
        )

        resultados = calcular_correlacion_variables(
            df,
            columna_objectivo="tiempo_transaccion_segs",
            columnas_a_excluir=[
                "EmpSucDocCajFac_Id",
                "Emp_Id",
                "Suc_Id",
                "Caja_Id",
                "Factura_Id",
                "Factura_Fecha",
                "Factura_FechaHora",
                "EmpSucCaj_Id",
                "fue_hora_pico",
                "max_transacciones_dia",
                "valor_neto",
            ],
        )
        print(resultados["detalle_variables"].head(100))
        print(resultados["resumen_correlaciones"].head(100))
        resultados["detalle_variables"].write_parquet(
            ".cache/detalle_variables.parquet"
        )
        resultados["resumen_correlaciones"].write_parquet(
            ".cache/resumen_correlaciones.parquet"
        )
