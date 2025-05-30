import datetime as dt
import math
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
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import numpy as np
from xgboost import XGBRegressor
from sklearn.model_selection import cross_val_score
import statsmodels.api as sm

cfg = pl.Config()


def debug_print(str):
    print(str)


class ConfigGetData(dg.Config):
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
    dwh_farinter_bi: SQLServerResource, config: ConfigGetData
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
        --CASE WHEN ABS(FE.Valor_Costo_Bonificacion) >0 
        --                THEN 1 ELSE 0 END AS es_bonificado,
        CASE WHEN TC.TipoCliente_Nombre LIKE '%TER%EDAD%' 
                OR TC.TipoCliente_Nombre LIKE '%CUART%EDAD%'
                OR M.Tipo_Plan LIKE '%TER%EDAD%' 
                OR M.Tipo_Plan LIKE '%CUART%EDAD%'
            THEN 1 ELSE 0 END AS es_tercera_edad,
        CASE WHEN TC.TipoCliente_Nombre LIKE '%CLINIC%' 
                OR M.Tipo_Plan LIKE '%CLINIC%' 
            THEN 1 ELSE 0 END AS es_clinica,
        CASE WHEN TC.TipoCliente_Nombre LIKE '%ASEGURADO%' 
                OR M.Tipo_Plan LIKE '%ASEGURAD%' 
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

    WHERE FE.Factura_Fecha>= '{fecha_desde_str}' AND FE.Emp_Id = 1 AND FE.Suc_Id IN (107)
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


def get_marcador_data(
    dwh_farinter_bi: SQLServerResource, config: ConfigGetData
) -> pl.DataFrame:
    if config.use_cache:
        cache_handler = ParquetCacheHandler(filename="ut_marcador_data")
        df = cache_handler.get_cached_data(max_age_seconds=config.cache_max_age_seconds)
        if df is not None:
            return df

    fecha_desde_str = config.fecha_desde.strftime("%Y%m%d")
    query_trx = dedent(f""" 
        SELECT Pais_Id as Emp_Id, 
            Sucursal_Id as Suc_Id, 
            Fecha_Calendario as Factura_Fecha, 
            Hora_Id as hora_id, 
            EH.Cantidad_Empleados
        FROM BI_FARINTER.dbo.BI_Kielsa_Agr_Sucursal_Marcador_EmpleadosHora EH
        WHERE EH.Fecha_Calendario >= '{fecha_desde_str}' AND Pais_Id=1
    """)

    df = (
        pl.read_database(query_trx, dwh_farinter_bi.get_arrow_odbc_conn_string())
        .lazy()
        .collect(engine="streaming")
    )

    if config.use_cache:
        cache_handler = ParquetCacheHandler(filename="ut_marcador_data")
        cache_handler.save_to_cache(df)

    return df


def tranform_transactions_data(
    df: pl.DataFrame | pl.LazyFrame, df_marcador: pl.DataFrame
) -> pl.DataFrame:
    llaves_primarias_id = (
        "Emp_Id",
        "Suc_Id",
        "EmpSucDocCajFac_Id",
        "EmpSucCaj_Id",
        "hora_id",
        "Caja_Id",
    )

    # Calcular hora_id y clave caja
    cs_columnas_categoricas = (
        cs.by_name("acumula_monedero")
        | cs.by_name("contiene_tengo")
        | cs.starts_with("es_")
        | cs.starts_with("contiene_")
        | cs.ends_with("_id")
        | cs.by_dtype(pl.String)
        | cs.by_dtype(pl.Categorical)
    ) - cs.by_name(llaves_primarias_id)

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

    df = (
        df.filter(
            (col("tiempo_transaccion_segs") >= q1)
            & (col("tiempo_transaccion_segs") <= q3)
        )
        .with_columns(
            col("cantidad_productos").abs(),
            col("cantidad_unidades").abs(),
            col("precio_unitario_prom").abs(),
            col("valor_neto").qcut(3).alias("valor_neto_qcut"),
            col("precio_unitario_prom").qcut(3).alias("precio_unitario_prom_qcut"),
            (col("cantidad_unidades") - col("cantidad_productos"))
            .clip(0)
            .alias("cantidad_unidades_relativa"),
            pl.lit(1).cast(pl.Float64).alias("ctd_transacciones"),
            (
                (col("cantidad_productos"))
                .log1p()
                .fill_nan(None)
                .fill_null(1)
                .cast(pl.Float64)
            ).alias("log_cantidad_productos"),
            (
                col("precio_unitario_prom")
                .log1p()
                .fill_nan(None)
                .fill_null(1)
                .cast(pl.Float64)
            ).alias("log_precio_unitario_prom"),
            col("EmpSucCaj_Id")
            .n_unique()
            .over(partition_by=("Emp_Id", "Suc_Id"))
            .alias("Cajas_Activas"),
        )
        .with_columns(
            col("cantidad_unidades_relativa")
            .log1p()
            .fill_nan(None)
            .fill_null(0)
            .cast(pl.Float64)
            .alias("log_cantidad_unidades_relativa"),
        )
        .with_columns(
            (
                # Base transaction time
                143.407
                +
                # Impact of number of products (nonlinear transformation)
                98.564 * col("log_cantidad_productos")
                +
                # Impact of pharmaceutical products
                20.968 * col("contiene_farma").cast(pl.Int64)
                +
                # Impact of senior citizen status
                13.807 * col("es_tercera_edad").cast(pl.Int64)
                +
                # Impact of relative units (nonlinear transformation)
                11.722 * col("log_cantidad_unidades_relativa")
                +
                # Impact of unit price (nonlinear transformation)
                11.335 * col("log_precio_unitario_prom")
                +
                # Impact of wallet accumulation
                6.027 * col("acumula_monedero").cast(pl.Int64)
            )
            .fill_null(216.4261)
            .alias("tiempo_transaccion_estimado")
        )
    ).collect(engine="streaming")

    # Filtrar solo horas con cajas suficientes para los empleados
    print("Filtrando horas con cajas suficientes para los empleados...")
    df = (
        df.join(
            df_marcador.with_columns(cs.numeric().cast(pl.Float64)),
            on=("Emp_Id", "Suc_Id", "hora_id", "Factura_Fecha"),
        )
        .filter(col("Cajas_Activas") + 1 >= col("Cantidad_Empleados"))
        .drop(cs.ends_with("_right") | cs.by_name("Cantidad_Empleados"))
    )

    # --- Agrupar categorías con muestras insuficientes en "otros" ---
    n_min = calculate_min_sample_size(
        confidence_level=0.95, margin_of_error=0.1, proportion=0.5
    )
    # Nota: para estos parámetros, n_min ≈ 97

    # Procesar
    categorical_columns = cs.expand_selector(df, cs_columnas_categoricas)

    # print(df.sort( by=( "Factura_Fecha", "total_transacciones_hora"), descending = True).head(20))
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


def visualize_data_distribution(
    df: pl.DataFrame,
    target_column: str = "tiempo_transaccion_segs",
    feature_columns: list | None = None,
    max_features: int = 6,
    sample_size: int = 10000,
    output_file: str | None = None,
):
    """
    Creates interactive visualizations for analyzing dispersion and distribution of data
    and saves them as an HTML file that can be opened in any browser.

    Args:
        df (pl.DataFrame): The DataFrame containing the data to visualize
        target_column (str): The target variable to analyze (default: "tiempo_transaccion_segs")
        feature_columns (list, optional): List of feature columns to visualize. If None,
                                         selects top correlated features automatically
        max_features (int): Maximum number of features to display (default: 6)
        sample_size (int): Number of samples to use for visualization (default: 10000)
        output_file (str, optional): Path to save the HTML file. If None, saves to '.cache/data_visualization.html'

    Returns:
        str: Path to the generated HTML file
    """
    try:
        import plotly.express as px
        import numpy as np
        import os
        import webbrowser
    except ImportError:
        print("This function requires plotly to be installed.")
        print("Install with: pip install plotly")
        return None

    # Set default output file
    if output_file is None:
        os.makedirs(".cache", exist_ok=True)
        output_file = ".cache/data_visualization.html"

    # Sample data if needed
    if len(df) > sample_size:
        df_sample = df.sample(n=sample_size, seed=42)
    else:
        df_sample = df

    # Convert to pandas for plotly compatibility
    df_pd = df_sample.to_pandas()

    # If feature columns not provided, select top correlated features
    if feature_columns is None:
        # Calculate correlations with target
        correlations = {}
        numeric_cols = [
            col
            for col in df.columns
            if df[col].dtype.is_numeric() and col != target_column
        ]

        for col in numeric_cols:
            try:
                corr = abs(df.select(pl.corr(target_column, col)).item())
                if not np.isnan(corr):
                    correlations[col] = corr
            except Exception as e:
                del e
                pass

        # Select top correlated features
        feature_columns = sorted(correlations.items(), key=lambda x: x[1], reverse=True)
        feature_columns = [col for col, _ in feature_columns[:max_features]]
        print(f"Selected top correlated features: {feature_columns}")

    # Limit to max_features
    if len(feature_columns) > max_features:
        feature_columns = feature_columns[:max_features]

    # Create HTML content
    html_parts = []
    html_parts.append(f"""
    <html>
    <head>
        <title>Data Analysis for {target_column}</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }}
            .container {{
                background-color: white;
                padding: 20px;
                border-radius: 5px;
                box-shadow: 0 0 10px rgba(0,0,0,0.1);
                margin-bottom: 20px;
            }}
            h1 {{
                color: #2c3e50;
                text-align: center;
            }}
            h2 {{
                color: #3498db;
                border-bottom: 1px solid #eee;
                padding-bottom: 10px;
            }}
        </style>
    </head>
    <body>
        <h1>Data Analysis for {target_column}</h1>
    """)

    # 1. Target Distribution
    print("Creating target distribution plot...")
    fig_target = px.histogram(
        df_pd, x=target_column, title=f"Distribution of {target_column}", marginal="box"
    )
    html_parts.append('<div class="container">')
    html_parts.append("<h2>Target Distribution</h2>")
    html_parts.append(fig_target.to_html(full_html=False, include_plotlyjs="cdn"))
    html_parts.append("</div>")

    # 2. Feature Distributions
    print("Creating feature distribution plots...")
    html_parts.append('<div class="container">')
    html_parts.append("<h2>Feature Distributions</h2>")

    for col in feature_columns:
        if df[col].dtype.is_numeric():
            fig = px.histogram(df_pd, x=col, title=f"Distribution of {col}")
            html_parts.append(fig.to_html(full_html=False, include_plotlyjs="cdn"))
        else:
            # For categorical features
            value_counts = df[col].value_counts().to_pandas()
            fig = px.bar(value_counts, x=col, y="count", title=f"Distribution of {col}")
            html_parts.append(fig.to_html(full_html=False, include_plotlyjs="cdn"))

    html_parts.append("</div>")

    # 3. Scatter plots for dispersion analysis
    print("Creating dispersion plots...")
    html_parts.append('<div class="container">')
    html_parts.append("<h2>Dispersion Analysis</h2>")

    for col in feature_columns:
        if df[col].dtype.is_numeric():
            fig = px.scatter(
                df_pd,
                x=col,
                y=target_column,
                title=f"{col} vs {target_column}",
                trendline="ols",
            )
            html_parts.append(fig.to_html(full_html=False, include_plotlyjs="cdn"))
        else:
            # For categorical features, use box plots
            fig = px.box(
                df_pd, x=col, y=target_column, title=f"{col} vs {target_column}"
            )
            html_parts.append(fig.to_html(full_html=False, include_plotlyjs="cdn"))

    html_parts.append("</div>")

    # 4. Correlation Heatmap
    print("Creating correlation heatmap...")
    numeric_df = df_sample.select(
        [col for col in df_sample.columns if df_sample[col].dtype.is_numeric()]
    )
    corr_matrix = numeric_df.to_pandas().corr()

    fig_heatmap = px.imshow(
        corr_matrix,
        text_auto=".2f",  # type: ignore
        aspect="auto",
        title="Correlation Heatmap",  # type: ignore
    )

    html_parts.append('<div class="container">')
    html_parts.append("<h2>Correlation Heatmap</h2>")
    html_parts.append(fig_heatmap.to_html(full_html=False, include_plotlyjs="cdn"))
    html_parts.append("</div>")

    # 5. Pair plot for selected features (limited to 4 for readability)
    print("Creating pair plot...")
    pair_features = feature_columns[: min(4, len(feature_columns))] + [target_column]
    fig_pair = px.scatter_matrix(
        df_pd[pair_features],
        dimensions=pair_features,
        title="Pair Plot of Selected Features",
        opacity=0.5,
    )

    html_parts.append('<div class="container">')
    html_parts.append("<h2>Pair Plot</h2>")
    html_parts.append(fig_pair.to_html(full_html=False, include_plotlyjs="cdn"))
    html_parts.append("</div>")

    # Close HTML
    html_parts.append("</body></html>")

    # Write HTML file
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(html_parts))

    print(f"Visualization saved to {output_file}")
    print("Opening visualization in browser...")

    # Open in browser
    webbrowser.open("file://" + os.path.abspath(output_file))

    return output_file


# Example usage in the script:
# if __name__ == "__main__":
#     visualize_data_distribution(df_sensibles, target_column="tiempo_transaccion_segs")


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


def extract_nonlinear_function(model, X_sample, feature_names):
    """
    Extract a non-linear function representation from an XGBoost model.

    Args:
        model: Trained XGBoost model
        X_sample: Sample data to use for function generation
        feature_names: Names of features

    Returns:
        Dictionary with non-linear function components
    """
    # Extract tree structure
    tree_dump = model.get_booster().get_dump(dump_format="json")

    # Convert to JSON for analysis
    import json

    trees = [json.loads(tree) for tree in tree_dump]

    # Extract interaction terms
    interactions = []

    # Extract non-linear transformations from trees
    transformations = []

    # Analyze thresholds for each feature across trees
    feature_thresholds = {}

    for tree_idx, tree in enumerate(trees[:20]):  # Limit to first 20 trees

        def extract_thresholds(node, path=None):
            if path is None:
                path = []

            if "split" in node:
                # Handle both integer indices and feature names in 'split'
                feature_name = None
                try:
                    # Try to parse as integer index
                    feature_idx = int(node["split"])
                    feature_name = feature_names[feature_idx]
                except ValueError:
                    # If not an integer, it's already the feature name
                    feature_name = node["split"]

                threshold = node["split_condition"]

                if feature_name not in feature_thresholds:
                    feature_thresholds[feature_name] = []

                # Ensure threshold is a number, not a list
                if isinstance(threshold, (int, float)):
                    feature_thresholds[feature_name].append(threshold)

                # Process children
                if "children" in node and len(node["children"]) >= 2:
                    extract_thresholds(
                        node["children"][0], path + [(feature_name, "<=", threshold)]
                    )
                    extract_thresholds(
                        node["children"][1], path + [(feature_name, ">", threshold)]
                    )

        try:
            extract_thresholds(tree)
        except Exception as e:
            print(f"Error extracting thresholds from tree {tree_idx}: {e}")

    # Analyze threshold distributions to identify non-linear transformations
    for feature, thresholds in feature_thresholds.items():
        if len(thresholds) > 5:  # Only consider features with multiple thresholds
            # Ensure all thresholds are numeric
            numeric_thresholds = [t for t in thresholds if isinstance(t, (int, float))]

            if len(numeric_thresholds) > 5:  # Still need enough numeric thresholds
                numeric_thresholds.sort()

                # Check for log-like distribution
                try:
                    ratio = max(numeric_thresholds) / (min(numeric_thresholds) + 1e-10)
                    if ratio > 10:
                        transformations.append(
                            {
                                "feature": feature,
                                "transformation": "log",
                                "thresholds": numeric_thresholds[
                                    :5
                                ],  # First 5 thresholds
                            }
                        )
                    # Check for threshold clusters (potential step functions)
                    elif (
                        len(set([round(t, 2) for t in numeric_thresholds]))
                        < len(numeric_thresholds) / 2
                    ):
                        transformations.append(
                            {
                                "feature": feature,
                                "transformation": "step",
                                "thresholds": sorted(
                                    list(set([round(t, 2) for t in numeric_thresholds]))
                                ),
                            }
                        )
                except Exception as e:
                    print(f"Error analyzing thresholds for {feature}: {e}")

    # Create partial dependence plots for top features
    pdp_data = []

    # Get top features by importance from model
    importances = model.feature_importances_
    top_features_idx = importances.argsort()[-5:][::-1]  # Top 5 features
    top_features = [feature_names[i] for i in top_features_idx]

    # Create simple interaction representation based on top features
    for i, feature1 in enumerate(top_features[:3]):  # Use top 3 features
        for feature2 in top_features[i + 1 : i + 4]:  # Pair with next 3 features
            importance1 = importances[list(feature_names).index(feature1)]
            importance2 = importances[list(feature_names).index(feature2)]

            interactions.append(
                {
                    "feature1": feature1,
                    "feature2": feature2,
                    "strength": float(importance1 * importance2),
                    "relative_strength": float(
                        (importance1 * importance2) / (importances.sum() ** 2) * 100
                    ),
                }
            )

    # Sort by strength
    interactions.sort(key=lambda x: x["strength"], reverse=True)
    interactions = interactions[:10]  # Keep top 10

    return {
        "interactions": interactions,
        "transformations": transformations,
        "partial_dependence": pdp_data,
    }


def print_nonlinear_formula(resultado):
    """
    Prints a human-readable representation of the nonlinear function extracted from the model.

    Args:
        resultado: Dictionary containing the model results including nonlinear_function
    """
    if "nonlinear_function" not in resultado:
        print("No nonlinear function information available")
        return

    nonlinear_function = resultado["nonlinear_function"]

    print("\n=== NONLINEAR FUNCTION REPRESENTATION ===\n")

    # 1. Print base formula (from linear approximation as starting point)
    print("Base Formula (Linear Approximation):")
    print(f"tiempo_transaccion_segs ≈ {resultado['formula']}")

    # 2. Print feature transformations
    if (
        "transformations" in nonlinear_function
        and nonlinear_function["transformations"]
    ):
        print("\nNonlinear Feature Transformations:")
        for transform in nonlinear_function["transformations"]:
            feature = transform["feature"]
            transformation = transform["transformation"]
            thresholds = transform["thresholds"]

            if transformation == "log":
                print(f"  • {feature}: Apply log transformation (log({feature} + 1))")
            elif transformation == "step":
                threshold_str = ", ".join([f"{t:.2f}" for t in thresholds[:3]])
                if len(thresholds) > 3:
                    threshold_str += f", ... ({len(thresholds) - 3} more)"
                print(
                    f"  • {feature}: Step function with thresholds at [{threshold_str}]"
                )

    # 3. Print feature interactions
    if "interactions" in nonlinear_function and nonlinear_function["interactions"]:
        print("\nFeature Interactions:")
        for i, interaction in enumerate(
            nonlinear_function["interactions"][:5]
        ):  # Top 5 interactions
            f1 = interaction["feature1"]
            f2 = interaction["feature2"]
            strength = interaction["relative_strength"]
            print(f"  • Interaction {i + 1}: {f1} × {f2} (strength: {strength:.2f}%)")

    # 4. Print partial dependence information
    if (
        "partial_dependence" in nonlinear_function
        and nonlinear_function["partial_dependence"]
    ):
        print("\nNonlinear Effects (Partial Dependence):")
        for pdp in nonlinear_function["partial_dependence"]:
            feature = pdp["feature"]
            values = pdp["values"]
            predictions = pdp["predictions"]

            # Determine the shape of the relationship
            relationship = "complex"
            diffs = [
                predictions[i + 1] - predictions[i] for i in range(len(predictions) - 1)
            ]

            if all(d > 0 for d in diffs):
                relationship = "monotonically increasing"
            elif all(d < 0 for d in diffs):
                relationship = "monotonically decreasing"
            elif len(diffs) > 2 and diffs[0] * diffs[-1] < 0:
                relationship = "U-shaped or inverted U-shaped"

            print(f"  • {feature}: {relationship} relationship")

            # Print a few key points
            if len(values) > 2:
                print(
                    f"    - When {feature} = {values[0]:.2f}: prediction ≈ {predictions[0]:.2f}"
                )
                mid_idx = len(values) // 2
                print(
                    f"    - When {feature} = {values[mid_idx]:.2f}: prediction ≈ {predictions[mid_idx]:.2f}"
                )
                print(
                    f"    - When {feature} = {values[-1]:.2f}: prediction ≈ {predictions[-1]:.2f}"
                )

    # 5. Print tree rules (simplified)
    if "tree_rules" in resultado and not resultado["tree_rules"].is_empty():
        print("\nDecision Rules (from first tree):")
        for i, rule in enumerate(resultado["tree_rules"].to_dicts()[:3]):  # Top 3 rules
            print(
                f"  • Rule {rule['regla_id']}: IF {rule['condiciones']} THEN prediction ≈ {rule['prediccion']:.4f}"
            )

    print("\n=== END OF NONLINEAR FUNCTION REPRESENTATION ===")


def build_nonlinear_transaction_time_model(df):
    """
    Construye un modelo XGBoost para predecir el tiempo de transacción,
    capturando relaciones no lineales entre las variables.
    """
    print("Construyendo modelo XGBoost para tiempo de transacción...")

    # Columnas a excluir
    columnas_a_excluir = [
        "EmpSucDocCajFac_Id",
        "Emp_Id",
        "Suc_Id",
        "Caja_Id",
        "Factura_Id",
        "Factura_Fecha",
        "Factura_FechaHora",
        "EmpSucCaj_Id",
        "tiempo_transaccion_segs",
        "es_hora_pico",
        "max_transacciones_dia",
        "total_transacciones_hora",
    ]

    # Preparar datos para el modelo
    X = df.drop(
        cs.by_name(columnas_a_excluir + ["tiempo_transaccion_segs"], require_all=False)
    ).fill_null(0)
    y = df["tiempo_transaccion_segs"]

    # Convertir a pandas
    X_pd = X.to_pandas()
    y_pd = y.to_pandas()

    # Convertir columnas de texto a categóricas
    for col_name in X_pd.select_dtypes(include=["object"]).columns:
        X_pd[col_name] = X_pd[col_name].astype("category")

    # Dividir en conjuntos de entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(
        X_pd, y_pd, test_size=0.2, random_state=42
    )

    # Configurar y entrenar modelo XGBoost con soporte para variables categóricas
    model = XGBRegressor(
        n_estimators=100,
        learning_rate=0.1,
        max_depth=6,
        min_child_weight=1,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="reg:squarederror",
        random_state=42,
        enable_categorical=True,  # Habilitar soporte para variables categóricas
    )

    # Entrenar el modelo con versión simplificada
    model.fit(X_train, y_train)

    # Evaluar el modelo
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    mse_train = mean_squared_error(y_train, y_pred_train)
    mse_test = mean_squared_error(y_test, y_pred_test)
    rmse_train = np.sqrt(mse_train)
    rmse_test = np.sqrt(mse_test)
    r2_train = r2_score(y_train, y_pred_train)
    r2_test = r2_score(y_test, y_pred_test)

    # Validación cruzada simplificada
    try:
        cv_scores = cross_val_score(
            model, X_pd, y_pd, cv=5, scoring="neg_mean_squared_error"
        )
        rmse_cv = np.sqrt(-cv_scores)
        rmse_cv_mean = rmse_cv.mean()
        rmse_cv_std = rmse_cv.std()
    except Exception as e:
        print(f"Error en validación cruzada: {e}")
        rmse_cv_mean = None
        rmse_cv_std = None

    # Obtener importancia de características
    feature_importance = model.feature_importances_
    feature_names = X_pd.columns

    # Crear DataFrame de importancia de características
    importance_df = pl.DataFrame(
        {
            "variable": feature_names,
            "importancia": feature_importance,
        }
    ).sort("importancia", descending=True)

    # Calcular importancia relativa (porcentaje)
    importance_df = importance_df.with_columns(
        (pl.col("importancia") / pl.col("importancia").sum() * 100).alias(
            "importancia_porcentaje"
        )
    )

    # Imprimir resultados
    print("\nModelo XGBoost para Tiempo de Transacción:")
    print(f"RMSE (train): {rmse_train:.4f}, R² (train): {r2_train:.4f}")
    print(f"RMSE (test): {rmse_test:.4f}, R² (test): {r2_test:.4f}")
    if rmse_cv_mean is not None:
        print(f"RMSE (CV 5-fold): {rmse_cv_mean:.4f} ± {rmse_cv_std:.4f}")

    print("\nVariables más importantes (top 10):")
    print(importance_df.head(10))

    # Guardar resultados en Excel
    importance_df.write_excel(
        ".cache/importancia_modelo_xgboost_tiempo_transaccion.xlsx"
    )

    # 1. Aproximación lineal a las predicciones de XGBoost
    print(
        "\nGenerando aproximación lineal estadísticamente significativa al modelo XGBoost..."
    )

    # Get XGBoost predictions for the entire dataset
    y_pred_xgb = model.predict(X_pd)

    # Build statistically significant linear model
    significant_model_results = build_statistically_significant_linear_model(
        df, X_pd, y_pred_xgb, feature_names
    )

    # Use the results
    linear_approx_df = significant_model_results["coefficients"]
    formula = significant_model_results["formula"]

    # Get the linear model from the results
    linear_model = significant_model_results.get("model")

    # Calculate R² for the linear approximation if not already included
    if "r2_linear_approx" in significant_model_results:
        r2_approx = significant_model_results["r2_linear_approx"]
    else:
        try:
            # If using statsmodels
            if hasattr(linear_model, "rsquared"):
                r2_approx = linear_model.rsquared
            # If using sklearn
            elif hasattr(linear_model, "score"):
                r2_approx = linear_model.score(X_pd, y_pred_xgb)
            else:
                # Manual calculation
                if hasattr(linear_model, "predict"):
                    y_pred_linear = predict_with_model(linear_model, X_pd)
                else:
                    # If we have a statsmodels results object
                    y_pred_linear = linear_model.fittedvalues
                r2_approx = r2_score(y_pred_xgb, y_pred_linear)
            print(f"R² de la aproximación lineal: {r2_approx:.4f}")
        except Exception as e:
            print(f"Error al calcular R² de la aproximación lineal: {e}")
            r2_approx = None

    print(f"tiempo_transaccion_segs ≈ {formula} + ...")

    # Guardar resultados
    linear_approx_df.write_excel(".cache/aproximacion_lineal_xgboost.xlsx")

    # 2. Extraer reglas de árboles individuales
    print("\nExtrayendo reglas de árboles individuales...")
    try:
        tree_dump = model.get_booster().get_dump(dump_format="json")

        # Convertir el primer árbol a formato JSON para análisis
        import json

        first_tree = json.loads(tree_dump[0])

        # Función recursiva para extraer reglas de un árbol
        def extract_rules(node, feature_names, path=None):
            if path is None:
                path = []

            # Si es un nodo hoja, devolver la predicción y la ruta
            if "leaf" in node:
                return [{"path": path, "prediction": node["leaf"]}]

            # Si es un nodo interno, continuar por ambas ramas
            feature_name = None
            try:
                # Try to parse as integer index
                feature_idx = int(node["split"])
                feature_name = feature_names[feature_idx]
            except ValueError:
                # If not an integer, it's already the feature name
                feature_name = node["split"]

            threshold = node["split_condition"]

            # Format threshold properly based on its type
            if isinstance(threshold, (int, float)):
                threshold_str = f"{threshold:.4f}"
            else:
                # For non-numeric thresholds (like lists or other objects)
                threshold_str = str(threshold)

            # Rama izquierda (menor o igual que el umbral)
            left_path = path + [f"{feature_name} <= {threshold_str}"]
            left_rules = extract_rules(node["children"][0], feature_names, left_path)

            # Rama derecha (mayor que el umbral)
            right_path = path + [f"{feature_name} > {threshold_str}"]
            right_rules = extract_rules(node["children"][1], feature_names, right_path)

            return left_rules + right_rules

        # Extraer reglas del primer árbol
        first_tree_rules = extract_rules(first_tree, list(X_pd.columns))

        # Ordenar reglas por valor de predicción
        first_tree_rules.sort(key=lambda x: x["prediction"], reverse=True)

        # Crear DataFrame con las reglas
        rules_data = []
        for i, rule in enumerate(
            first_tree_rules[:5]
        ):  # Tomar las 5 reglas principales
            rules_data.append(
                {
                    "regla_id": i + 1,
                    "condiciones": " AND ".join(rule["path"]),
                    "prediccion": rule["prediction"],
                }
            )

        rules_df = pl.DataFrame(rules_data)

        # Guardar resultados
        rules_df.write_excel(".cache/reglas_arbol_xgboost.xlsx")

        # Mostrar las reglas
        print("\nReglas extraídas del primer árbol (top 5):")
        print(rules_df)

    except Exception as e:
        print(f"Error al extraer reglas del árbol: {e}")
        rules_df = pl.DataFrame(
            {
                "regla_id": [0],
                "condiciones": ["Error al extraer reglas"],
                "prediccion": [0],
            }
        )

    # Add non-linear function extraction
    nonlinear_function = extract_nonlinear_function(
        model, X_pd.sample(min(1000, X_pd.shape[0])), feature_names
    )

    # Añadir a los resultados
    resultado = {
        "model": model,
        "feature_names": feature_names,
        "metrics": {
            "rmse_train": rmse_train,
            "rmse_test": rmse_test,
            "rmse_cv_mean": rmse_cv_mean,
            "rmse_cv_std": rmse_cv_std,
            "r2_train": r2_train,
            "r2_test": r2_test,
            "r2_linear_approx": r2_approx,
        },
        "feature_importance": importance_df,
        "linear_approximation": linear_approx_df,
        "formula": formula,
        "tree_rules": rules_df,  # Añadir las reglas del árbol
        "nonlinear_function": nonlinear_function,  # Añadir la función no lineal
    }

    return resultado


def build_statistically_significant_linear_model(
    df, X_pd, y_pred_xgb, feature_names, alpha=0.05
):
    """
    Builds a linear model that only includes statistically significant relationships.

    Args:
        df: Original DataFrame
        X_pd: Features DataFrame
        y_pred_xgb: Target predictions from XGBoost
        feature_names: Names of features
        alpha: Significance level (default 0.05)

    Returns:
        Dictionary with model and statistics
    """
    import statsmodels.api as sm
    import pandas as pd
    import numpy as np

    print("\nBuilding statistically significant linear model...")

    # Identify categorical columns
    categorical_columns = []
    for col_name in X_pd.columns:
        if X_pd[col_name].dtype == "object" or pd.api.types.is_categorical_dtype(  # type: ignore
            X_pd[col_name]
        ):  # type: ignore
            if (
                len(X_pd[col_name].unique()) <= 10
            ):  # Only consider columns with reasonable number of categories
                categorical_columns.append(col_name)

    # Create a copy of the data for processing
    X_processed = X_pd.copy()

    # Store mappings for formula representation
    categorical_mappings = {}

    # Handle categorical columns properly - convert to string first to avoid category issues
    for col_name in categorical_columns:
        X_processed[col_name] = X_processed[col_name].astype(str)

    # Fill any NaN values - now safe because categorical columns are strings
    # X_processed = X_processed.fillna("missing")

    # Process non-categorical columns (ensure numeric)
    for col_name in X_processed.columns:
        if col_name not in categorical_columns:
            X_processed[col_name] = pd.to_numeric(
                X_processed[col_name], errors="coerce"
            )

    # One-hot encode categorical variables
    X_dummies = pd.get_dummies(
        X_processed, columns=categorical_columns, drop_first=False
    )

    # Store the mapping of dummy columns to original categories
    for col_name in categorical_columns:
        categories = X_processed[col_name].unique()
        mapping = {}
        for cat in categories:
            if cat != "missing":  # Skip missing values in the mapping
                dummy_col = f"{col_name}_{cat}"
                if dummy_col in X_dummies.columns:
                    mapping[dummy_col] = cat
        categorical_mappings[col_name] = mapping

    # Fill any remaining NaN values with 0 (should be safe now for numeric columns)
    X_dummies = X_dummies.fillna(0)

    # Add constant for intercept
    X_with_const = sm.add_constant(X_dummies)

    # Check for perfect multicollinearity
    cols_to_drop = []
    for col_name in X_with_const.columns:  # type: ignore
        if col_name == "const":
            continue

        # Check if column has zero variance (all values are the same)
        try:
            if X_with_const[col_name].var() == 0:  # type: ignore
                cols_to_drop.append(col_name)
                print(f"Dropping column with zero variance: {col_name}")
        except (TypeError, ValueError) as e:
            # If we can't calculate variance (e.g., due to non-numeric values), drop the column
            cols_to_drop.append(col_name)
            print(
                f"Dropping column that can't be used in variance calculation: {col_name}, Error: {e}"
            )

    # Drop problematic columns
    if cols_to_drop:
        X_with_const = X_with_const.drop(columns=cols_to_drop)  # type: ignore

    # Initial model with all features
    try:
        model = sm.OLS(y_pred_xgb, X_with_const)
        results = model.fit()
    except Exception as e:
        print(f"Error fitting initial model: {e}")
        print("Trying with a simpler approach...")

        # If OLS fails, try a simpler approach with sklearn
        from sklearn.linear_model import LinearRegression

        # Save the column names for later prediction
        feature_columns = X_dummies.columns.tolist()

        lr = LinearRegression()
        lr.fit(X_dummies, y_pred_xgb)

        # Create a simple results object
        coef_df = pl.DataFrame(
            {
                "variable": feature_columns,
                "coeficiente": lr.coef_,
                "p_valor": [None] * len(lr.coef_),
                "error_estandar": [None] * len(lr.coef_),
                "coef_abs": np.abs(lr.coef_),
            }
        )

        # Add intercept
        coef_df = pl.concat(
            [
                pl.DataFrame(
                    {
                        "variable": ["const"],
                        "coeficiente": [lr.intercept_],
                        "p_valor": [None],
                        "error_estandar": [None],
                        "coef_abs": [abs(lr.intercept_)],
                    }
                ),
                coef_df,
            ]
        )

        # Sort by absolute coefficient value
        coef_df = coef_df.sort("coef_abs", descending=True)

        # Calculate relative importance
        total_abs = coef_df.filter(pl.col("variable") != "const")["coef_abs"].sum()
        coef_df = coef_df.with_columns(
            (
                pl.when(pl.col("variable") != "const")
                .then(pl.col("coef_abs") / total_abs * 100)
                .otherwise(0)
                .alias("importancia_porcentaje")
            )
        )

        # Add original category information for dummy variables
        coef_df = coef_df.with_columns(
            pl.col("variable")
            .map_elements(lambda var: get_original_category(var, categorical_mappings))
            .alias("variable_with_category")
        )

        # Build formula representation
        intercept = lr.intercept_
        formula_parts = [f"{intercept:.4f}"]

        for i, var in enumerate(feature_columns):
            coef = lr.coef_[i]
            if abs(coef) < 0.0001:  # Skip coefficients that are effectively zero
                continue

            sign = "+" if coef >= 0 else ""

            # Check if this is a dummy variable
            original_category = get_original_category(
                var, categorical_mappings, return_parts=True
            )
            if original_category != var:
                base_var, category = original_category
                formula_parts.append(f"{sign}{coef:.4f} * {base_var}={category}")
            else:
                formula_parts.append(f"{sign}{coef:.4f} * {var}")

        formula = " ".join(formula_parts)

        return {
            "model": lr,
            "coefficients": coef_df,
            "formula": formula,
            "significant_features": feature_columns,
            "vif_data": None,
            "categorical_mappings": categorical_mappings,
            "feature_columns": feature_columns,  # Save feature columns for prediction
        }

    # Check for multicollinearity
    print("Checking for multicollinearity...")
    try:
        from statsmodels.stats.outliers_influence import variance_inflation_factor

        # Calculate VIF for each feature
        vif_data = pd.DataFrame()
        vif_data["feature"] = X_with_const.columns  # type: ignore
        vif_data["VIF"] = [
            variance_inflation_factor(X_with_const.values, i)  # type: ignore
            for i in range(X_with_const.shape[1])
        ]

        # Print features with high VIF
        high_vif = vif_data[vif_data["VIF"] > 10].sort_values("VIF", ascending=False)  # type: ignore
        if not high_vif.empty:
            print("Features with high multicollinearity (VIF > 10):")
            print(high_vif)
    except Exception as e:
        print(f"Error calculating VIF: {e}")
        vif_data = None

    # Iteratively remove non-significant features
    significant_features = ["const"]
    pvalues = results.pvalues

    print("\nRemoving non-significant features (p > 0.05)...")
    for feature, pvalue in zip(X_with_const.columns, pvalues):  # type: ignore
        if pvalue <= alpha:
            significant_features.append(feature)
        else:
            print(f"  Removing {feature} (p-value: {pvalue:.4f})")

    # If we removed too many features, keep at least the top 5 most important
    if len(significant_features) < 6:
        print("Too few significant features, keeping top features by importance...")

        top_features = (
            results.params.abs().sort_values(ascending=False).index[:5].tolist()
        )

        for feature in top_features:
            if feature not in significant_features and feature != "const":
                significant_features.append(feature)
                print(f"  Adding back {feature} based on coefficient magnitude")

    # Build final model with only significant features
    X_significant = X_with_const[significant_features]
    final_model = sm.OLS(y_pred_xgb, X_significant)
    final_results = final_model.fit()

    print("\nFinal model summary:")
    print(f"R-squared: {final_results.rsquared:.4f}")
    print(f"Adjusted R-squared: {final_results.rsquared_adj:.4f}")

    # Create DataFrame with coefficients
    coef_df = pl.DataFrame(
        {
            "variable": final_results.params.index.tolist(),
            "coeficiente": final_results.params.values,
            "p_valor": final_results.pvalues.values,
            "error_estandar": final_results.bse.values,
            "coef_abs": np.abs(final_results.params.values),
        }
    ).sort("coef_abs", descending=True)

    # Calculate relative importance
    total_abs = coef_df.filter(pl.col("variable") != "const")["coef_abs"].sum()
    coef_df = coef_df.with_columns(
        (
            pl.when(pl.col("variable") != "const")
            .then(pl.col("coef_abs") / total_abs * 100)
            .otherwise(0)
            .alias("importancia_porcentaje")
        )
    )

    # Add original category information for dummy variables
    coef_df = coef_df.with_columns(
        pl.col("variable")
        .map_elements(lambda var: get_original_category(var, categorical_mappings))
        .alias("variable_with_category")
    )

    # Build formula representation
    intercept = coef_df.filter(pl.col("variable") == "const")["coeficiente"][0]
    formula_parts = [f"{intercept:.4f}"]

    for row in coef_df.filter(pl.col("variable") != "const").to_dicts():
        coef = row["coeficiente"]
        var = row["variable"]
        if abs(coef) < 0.0001:  # Skip coefficients that are effectively zero
            continue

        sign = "+" if coef >= 0 else ""

        # Check if this is a dummy variable
        original_category = get_original_category(
            var, categorical_mappings, return_parts=True
        )
        if original_category != var:
            base_var, category = original_category
            formula_parts.append(f"{sign}{coef:.4f} * {base_var}={category}")
        else:
            formula_parts.append(f"{sign}{coef:.4f} * {var}")

    formula = " ".join(formula_parts)

    return {
        "model": final_results,
        "coefficients": coef_df,
        "formula": formula,
        "significant_features": significant_features,
        "vif_data": vif_data,
        "categorical_mappings": categorical_mappings,
        "feature_columns": X_significant.columns.tolist(),  # type: ignore # Save feature columns for prediction
    }


def predict_with_model(model_info, new_data):
    """
    Make predictions with the trained model, ensuring feature consistency

    Args:
        model_info: Dictionary returned by build_statistically_significant_linear_model
        new_data: New data for prediction

    Returns:
        Predictions
    """
    # Get the model and feature columns
    model = model_info["model"]
    feature_columns = model_info["feature_columns"]
    categorical_mappings = model_info["categorical_mappings"]

    # Prepare categorical columns
    categorical_columns = list(categorical_mappings.keys())

    # Process data similar to training
    X_processed = new_data.copy()

    # Handle categorical columns
    for col_name in categorical_columns:
        if col_name in X_processed.columns:
            X_processed[col_name] = X_processed[col_name].astype(str)

    # One-hot encode
    X_dummies = pd.get_dummies(
        X_processed, columns=categorical_columns, drop_first=False
    )

    # Ensure all columns from training are present
    for col_name in feature_columns:
        if col_name not in X_dummies.columns and col_name != "const":
            X_dummies[col_name] = 0

    # Select only the columns used in training
    X_pred = X_dummies[feature_columns]

    # Add constant if using statsmodels
    if hasattr(model, "predict") and callable(getattr(model, "predict")):
        # sklearn model
        return model.predict(X_pred)
    else:
        # statsmodels model
        X_pred = sm.add_constant(X_pred)
        return model.predict(X_pred)


def get_original_category(var, categorical_mappings, return_parts=False):
    """Helper function to get original category from dummy variable name"""
    for base_var, mapping in categorical_mappings.items():
        for dummy_var, category in mapping.items():
            if var == dummy_var:
                if return_parts:
                    return (base_var, category)
                return f"{base_var}={category}"
    return var


def identify_high_transaction_time_factors(
    modelo_resultado, df, threshold_percentile=90
):
    """
    Automatically identifies factors that explain higher transaction times.

    Args:
        modelo_resultado: Dictionary with model results
        df: DataFrame with transaction data
        threshold_percentile: Percentile to define "high" transaction times

    Returns:
        Dictionary with factors that explain high transaction times
    """
    print("\n=== FACTORS EXPLAINING HIGH TRANSACTION TIMES ===\n")

    # 1. Define what constitutes a "high" transaction time
    high_time_threshold = df["tiempo_transaccion_segs"].quantile(
        threshold_percentile / 100
    )
    print(
        f"High transaction time threshold (P{threshold_percentile}): {high_time_threshold:.2f} seconds\n"
    )

    # 2. Get top features from XGBoost importance
    top_features = modelo_resultado["feature_importance"].head(10)
    print("Top 10 features by importance:")
    for row in top_features.to_dicts():
        print(f"  • {row['variable']}: {row['importancia_porcentaje']:.2f}%")

    # 3. Get top coefficients from linear approximation
    top_coefs = (
        modelo_resultado["linear_approximation"]
        .filter(pl.col("variable") != "(intercepto)")
        .sort("coef_abs", descending=True)
        .head(10)
    )

    print("\nTop 10 factors by linear coefficient magnitude:")
    for row in top_coefs.to_dicts():
        sign = "+" if row["coeficiente"] > 0 else "-"
        print(
            f"  • {row['variable']}: {sign}{abs(row['coeficiente']):.4f} ({row['importancia_porcentaje']:.2f}%)"
        )

    # 4. Identify rules that lead to high transaction times
    high_time_rules = (
        modelo_resultado["tree_rules"]
        .filter(pl.col("prediccion") > high_time_threshold)
        .sort("prediccion", descending=True)
    )

    if not high_time_rules.is_empty():
        print("\nRules that predict high transaction times:")
        for row in high_time_rules.head(3).to_dicts():
            print(
                f"  • IF {row['condiciones']} THEN tiempo ≈ {row['prediccion']:.2f} seconds"
            )

    # 5. Analyze high transaction time samples directly
    high_time_samples = df.filter(
        pl.col("tiempo_transaccion_segs") > high_time_threshold
    )
    low_time_samples = df.filter(
        pl.col("tiempo_transaccion_segs") <= high_time_threshold
    )

    # Compare averages for numeric features
    numeric_cols = [
        col
        for col in df.columns
        if df[col].dtype.is_numeric()
        and col != "tiempo_transaccion_segs"
        and col in top_features["variable"].to_list()
    ]

    print("\nNumeric feature comparison (high vs. normal transaction times):")
    for col_name in numeric_cols:
        high_avg = high_time_samples[col_name].mean()
        low_avg = low_time_samples[col_name].mean()
        diff_pct = (
            ((high_avg - low_avg) / low_avg * 100) if low_avg != 0 else float("inf")
        )

        print(f"  • {col_name}: {high_avg:.2f} vs {low_avg:.2f} ({diff_pct:+.1f}%)")

    # Compare distributions for categorical features
    categorical_cols = [
        col_name
        for col_name in df.columns
        if not df[col_name].dtype.is_numeric()
        and col_name in top_features["variable"].to_list()
    ]

    print("\nCategorical feature comparison (high vs. normal transaction times):")
    for col_name in categorical_cols[:5]:  # Limit to top 5 categorical features
        # Get value counts for high transaction times
        high_counts = (
            high_time_samples.group_by(col_name)
            .agg(
                pl.count().alias("count"),
                pl.mean("tiempo_transaccion_segs").alias("avg_time"),
            )
            .sort("avg_time", descending=True)
        )

        # Calculate percentages
        total_high = high_time_samples.height
        high_counts = high_counts.with_columns(
            (pl.col("count") / total_high * 100).alias("percentage")
        )

        print(f"  • {col_name} values most associated with high times:")
        for row in high_counts.head(3).to_dicts():
            category = row[col_name] if row[col_name] is not None else "NULL"
            print(
                f"    - {category}: {row['percentage']:.1f}% of high-time transactions, avg: {row['avg_time']:.2f}s"
            )

    # 6. Identify key combinations using decision tree rules
    print("\nKey combinations of factors leading to high transaction times:")
    if not high_time_rules.is_empty():
        for i, rule in enumerate(high_time_rules.head(3).to_dicts()):
            print(
                f"  • Combination {i + 1}: {rule['condiciones']} → {rule['prediccion']:.2f}s"
            )
    else:
        # If no rules available, create a simple decision tree on high vs. low times
        try:
            from sklearn.tree import DecisionTreeClassifier, export_text

            # Prepare data
            X_cols = [
                col for col in top_features["variable"].to_list() if col in df.columns
            ]
            X = df.select(X_cols).to_pandas()
            y = (df["tiempo_transaccion_segs"] > high_time_threshold).to_pandas()

            # Train a simple decision tree
            dt = DecisionTreeClassifier(max_depth=3, random_state=42)
            dt.fit(X, y)

            # Extract rules
            tree_rules = export_text(dt, feature_names=X_cols)
            print(f"  Decision tree for high transaction times:\n{tree_rules}")
        except Exception as e:
            print(f"  Could not create decision tree: {e}")

    print("\n=== END OF HIGH TRANSACTION TIME FACTORS ANALYSIS ===")

    return {
        "high_time_threshold": high_time_threshold,
        "top_features": top_features,
        "top_coefficients": top_coefs,
        "high_time_rules": high_time_rules,
        "numeric_comparison": {
            col: {
                "high": high_time_samples[col].mean(),
                "normal": low_time_samples[col].mean(),
            }
            for col in numeric_cols
        },
        "categorical_insights": {
            col: high_counts.to_dicts()  # type: ignore
            for col in categorical_cols[:5]  # type: ignore
        },
    }


def build_transaction_time_model(df):
    """
    Construye un modelo de regresión lineal para predecir el tiempo de transacción.

    Args:
        df: DataFrame con los datos procesados de transacciones

    Returns:
        dict: Diccionario con el modelo, métricas y coeficientes
    """
    print("Construyendo modelo de regresión para tiempo de transacción...")

    # Seleccionar variables predictoras potenciales (numéricas y categóricas)
    # Excluir identificadores y la variable objetivo
    columnas_a_excluir = [
        "EmpSucDocCajFac_Id",
        "Emp_Id",
        "Suc_Id",
        "Caja_Id",
        "Factura_Id",
        "Factura_Fecha",
        "Factura_FechaHora",
        "EmpSucCaj_Id",
        "tiempo_transaccion_segs",
        "es_hora_pico",
        "max_transacciones_dia",
        "total_transacciones_hora",
    ]

    # Preparar variables para el modelo
    variables_numericas = [
        col
        for col in df.columns
        if col not in columnas_a_excluir and df[col].dtype.is_numeric()
    ]

    variables_categoricas = [
        col
        for col in df.columns
        if col not in columnas_a_excluir and not df[col].dtype.is_numeric()
    ]

    # Convertir variables categóricas a dummies (one-hot encoding)
    X_numeric = df.select(variables_numericas).to_pandas()

    # Crear dummies para variables categóricas
    dummies = []
    dummy_names = []

    for col_name in variables_categoricas:
        # Obtener categorías únicas
        categorias = df[col_name].unique().to_list()

        for categoria in categorias:
            if categoria is not None:
                # Crear variable dummy (1 si coincide, 0 si no)
                dummy_name = f"{col_name}_{categoria}"
                dummy_names.append(dummy_name)
                dummy_values = (df[col_name] == categoria).cast(pl.Int64).to_pandas()
                dummies.append(dummy_values)

    # Combinar variables numéricas y dummies
    if dummies:
        X_dummies = np.column_stack(dummies)
        X = np.hstack((X_numeric, X_dummies))
        feature_names = variables_numericas + dummy_names
    else:
        X = X_numeric
        feature_names = variables_numericas

    # Variable objetivo
    y = df["tiempo_transaccion_segs"].to_pandas()

    # Dividir en conjuntos de entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Entrenar modelo de regresión lineal
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Evaluar el modelo
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    mse_train = mean_squared_error(y_train, y_pred_train)
    mse_test = mean_squared_error(y_test, y_pred_test)
    r2_train = r2_score(y_train, y_pred_train)
    r2_test = r2_score(y_test, y_pred_test)

    # Obtener coeficientes y ordenarlos por importancia
    coefs = list(zip(feature_names, model.coef_))
    coefs_sorted = sorted(coefs, key=lambda x: abs(x[1]), reverse=True)

    # Crear DataFrame de coeficientes
    coef_df = pl.DataFrame(
        {
            "variable": [c[0] for c in coefs_sorted],
            "coeficiente": [c[1] for c in coefs_sorted],
            "coef_abs": [abs(c[1]) for c in coefs_sorted],
        }
    ).sort("coef_abs", descending=True)

    # Calcular importancia relativa (porcentaje)
    total_importance = coef_df["coef_abs"].sum()
    coef_df = coef_df.with_columns(
        (pl.col("coef_abs") / total_importance * 100).alias("importancia_porcentaje")
    )

    # Imprimir resultados
    print("\nModelo de Regresión Lineal para Tiempo de Transacción:")
    print(f"MSE (train): {mse_train:.4f}, R² (train): {r2_train:.4f}")
    print(f"MSE (test): {mse_test:.4f}, R² (test): {r2_test:.4f}")
    print(f"\nIntercept (tiempo base): {model.intercept_:.4f} segundos")
    print("\nVariables más importantes (top 10):")
    print(coef_df.head(10))

    # Guardar resultados en Excel
    coef_df.write_excel(".cache/coeficientes_modelo_tiempo_transaccion.xlsx")

    return {
        "model": model,
        "feature_names": feature_names,
        "metrics": {
            "mse_train": mse_train,
            "mse_test": mse_test,
            "r2_train": r2_train,
            "r2_test": r2_test,
            "intercept": model.intercept_,
        },
        "coefficients": coef_df,
    }


# ----------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------
if __name__ == "__main__":
    # ----------------------------------------------------------------------------------
    from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_bi

    import joblib

    with pl.Config(set_tbl_cols=20) as cfg:
        df = get_transactions_data(
            dwh_farinter_bi,
            config=ConfigGetData(
                fecha_desde=pdt.today().subtract(days=31),
                use_cache=True,
                cache_max_age_seconds=3600,
            ),
        )
        df_marcador = get_marcador_data(
            dwh_farinter_bi,
            config=ConfigGetData(
                fecha_desde=pdt.today().subtract(days=31),
                use_cache=True,
                cache_max_age_seconds=3600,
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
        df = tranform_transactions_data(df, df_marcador)

        visualize_data_distribution(
            df.select(
                "es_tercera_edad",
                "cantidad_productos",
                "tiempo_transaccion_segs",
                "valor_neto_qcut",
                "cantidad_unidades_relativa",
                "es_clinica",
                "es_asegurado",
                "cantidad_unidades",
                "contiene_servicios",
                "contiene_consumo",
                "contiene_farma",
                "Factura_Origen",
                "canal_venta_id",
                "precio_unitario_prom",
                "TipoDoc_id",
                "Tipo_Plan",
                "TipoCliente_Nombre",
                "Suc_Id",
                "acumula_monedero",
                "log_cantidad_productos",
                "log_precio_unitario_prom",
                "log_cantidad_unidades_relativa",
                # "ctd_transacciones",
            ),
            target_column="tiempo_transaccion_segs",
        )

        print(
            df.select(
                "EmpSucDocCajFac_Id", "Factura_FechaHora", "tiempo_transaccion_segs"
            )
            .top_k(20, by="Factura_FechaHora")
            .head(20)
        )

        df_sensibles = df.select(
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

        # --------------------------------
        # Construir y evaluar el modelo de regresión lineal para tiempo de tranfsacción
        modelo_resultado = build_transaction_time_model(df_sensibles)

        # Obtener los pesos óptimos
        coeficientes = modelo_resultado["coefficients"]
        print("\nPesos óptimos del modelo de regresión lineal:")
        print(coeficientes)

        # Guardar modelo y resultados
        joblib.dump(modelo_resultado["model"], ".cache/modelo_tiempo_transaccion.pkl")
        print("Modelo guardado en .cache/modelo_tiempo_transaccion.pkl")

        # --------------------------------

        # Construir y evaluar el modelo XGBoost para tiempo de transacción
        print("\n--- Construyendo modelo no lineal para tiempo de transacción ---")
        modelo_resultado = build_nonlinear_transaction_time_model(df_sensibles)

        # Obtener la importancia de las características
        importancia = modelo_resultado["feature_importance"]
        print("\nImportancia de variables en el modelo XGBoost:")
        print(importancia.head(15))  # Mostrar las 15 variables más importantes

        # Guardar modelo y resultados
        import joblib

        joblib.dump(
            modelo_resultado["model"], ".cache/modelo_xgboost_tiempo_transaccion.pkl"
        )
        print("Modelo guardado en .cache/modelo_xgboost_tiempo_transaccion.pkl")

        # Visualizar importancia de características (opcional)
        try:
            import matplotlib.pyplot as plt

            # Tomar las 15 características más importantes
            top_features = importancia.head(15)

            plt.figure(figsize=(10, 8))
            plt.barh(
                top_features["variable"].to_list()[::-1],
                top_features["importancia_porcentaje"].to_list()[::-1],
            )
            plt.xlabel("Importancia (%)")
            plt.title("Importancia de Variables en Tiempo de Transacción")
            plt.tight_layout()
            plt.savefig(".cache/importancia_variables_tiempo_transaccion.png")
            print(
                "Gráfico guardado en .cache/importancia_variables_tiempo_transaccion.png"
            )
        except Exception as e:
            print(f"No se pudo generar el gráfico: {e}")

        print("\n--- Aproximación lineal al modelo XGBoost ---")
        print("Coeficientes de la aproximación lineal (top 10):")
        print(modelo_resultado["linear_approximation"].head(10))

        print("\n--- Reglas extraídas del primer árbol ---")
        print(modelo_resultado["tree_rules"].head(10))

        # Construir una representación de la fórmula completa
        linear_approx_df = modelo_resultado["linear_approximation"]
        intercept = linear_approx_df.filter(
            (pl.col("variable") == "(intercepto)") | (pl.col("variable") == "const")
        )["coeficiente"][0]

        formula_parts = [f"{intercept:.4f}"]
        for row in linear_approx_df.filter(
            pl.col("variable") != "(intercepto)"
        ).to_dicts():
            coef = row["coeficiente"]
            var = row["variable"]
            sign = "+" if coef >= 0 else ""
            formula_parts.append(f"{sign}{coef:.4f} * {var}")

        formula = " ".join(formula_parts)
        print("\n--- Fórmula completa de la aproximación lineal ---")
        print(f"tiempo_transaccion_segs ≈ {formula}")

        # Guardar la fórmula en un archivo de texto
        with open(".cache/formula_aproximada_tiempo_transaccion.txt", "w") as f:
            f.write(f"Fórmula aproximada para tiempo_transaccion_segs:\n\n{formula}")

        print("\nFórmula guardada en .cache/formula_aproximada_tiempo_transaccion.txt")

        print_nonlinear_formula(modelo_resultado)

        high_time_factors = identify_high_transaction_time_factors(
            modelo_resultado, df, threshold_percentile=90
        )
        print("\nFactores con tiempo de transacción alto:")
        print(high_time_factors)

        # --------------------------------

        resultados = calcular_correlacion_variables(
            df,
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
        print(resultados["detalle_variables"].head(100))
        print(resultados["resumen_correlaciones"].head(100))
        resultados["detalle_variables"].write_excel(".cache/detalle_variables.xlsx")
        resultados["resumen_correlaciones"].write_excel(
            ".cache/resumen_correlaciones.xlsx"
        )

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
            df_resumen_iter = resultados_bootstrap[
                "resumen_correlaciones"
            ].with_columns(pl.lit(i + 1).alias("bootstrap_iteracion"))

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
            (
                pl.col(f"{col_name}_desviacion") / pl.col(f"{col_name}_media").abs()
            ).alias(f"cv_{col_name}")
            for col_name in columnas_numericas_resumen
            if col_name not in ["n_observaciones", "n_categorias"]  # Excluir conteos
        ]

        df_bootstrap_resumen_stats = df_bootstrap_resumen_stats.with_columns(
            cv_exprs_resumen
        )

        # Detalle
        cv_exprs_detalle = [
            (
                pl.col(f"{col_name}_desviacion") / pl.col(f"{col_name}_media").abs()
            ).alias(f"cv_{col_name}")
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
        df_bootstrap_resumen_comparacion = (
            df_bootstrap_resumen_comparacion.with_columns(
                [
                    pl.when(pl.col("tipo") == "numérica")
                    .then(pl.col("cv_correlacion_pearson"))
                    .otherwise(pl.col("cv_correlacion_ratio"))
                    .alias("cv_principal")
                ]
            ).sort(["tipo", "cv_principal"])
        )

        # Guardar resultados
        df_bootstrap_resumen_all.write_excel(
            ".cache/bootstrap_resumen_all_iterations.xlsx"
        )
        df_bootstrap_detalle_all.write_excel(
            ".cache/bootstrap_detalle_all_iterations.xlsx"
        )
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
