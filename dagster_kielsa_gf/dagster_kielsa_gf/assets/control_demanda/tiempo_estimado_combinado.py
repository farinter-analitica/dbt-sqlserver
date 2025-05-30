import datetime as dt
import math
import pandas as pd
import pendulum as pdt
import polars as pl
import polars.selectors as cs
import dagster as dg
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
from sklearn.dummy import DummyRegressor
from sklearn.linear_model import RANSACRegressor
from dataclasses import dataclass


cfg = pl.Config()
pl.enable_string_cache()


@dataclass
class DataFrameWithMeta:
    df: pl.DataFrame | pl.LazyFrame
    primary_keys: tuple[str, ...]
    categorical_columns: tuple[str, ...]

    def __post_init__(self):
        """Validate primary keys and categorical"""
        missing_keys = set(self.primary_keys) - set(self.df.columns)
        if missing_keys:
            raise ValueError(f"Missing primary keys: {missing_keys}")

        missing_cat = set(self.categorical_columns) - set(self.df.columns)
        if missing_cat:
            raise ValueError(f"Missing categorical columns: {missing_cat}")

    @property
    def dataframe(self) -> pl.DataFrame:
        if isinstance(self.df, pl.LazyFrame):
            self.df = self.df.collect(engine="streaming")
        return self.df

    @property
    def lazyframe(self) -> pl.LazyFrame:
        if isinstance(self.df, pl.DataFrame):
            self.df = self.df.lazy()
        return self.df


def debug_print(str):
    print(str)


class ConfigGetData(dg.Config):
    fecha_desde: dt.date = Field(
        description="Fecha desde la cual se quieren obtener los eventos inclusive",
        default=pdt.today().subtract(months=3).replace(day=1),
    )

    fecha_hasta: dt.date = Field(
        description="Fecha hasta la cual se quieren obtener los eventos inclusive",
        default=pdt.today().subtract(days=1),
    )
    use_cache: bool = Field(
        description="Whether to use cached data if available",
        default=True,
    )
    cache_max_age_seconds: int = Field(
        description="Maximum age of cache in hours",
        default=3600 * 24,
    )

    cache_name: str | None = Field(
        description="Nombre del archivo cache",
        default=None,
    )

    sucursales: list | None = Field(
        description="Lista de sucursales a procesar",
        default=None,
    )


def filtro_ransac_median(
    df: pl.DataFrame,
    col_gap: str = "tiempo_actividad_segs",
    time_col: str = "Timestamp_Aplicado",
    residual_factor: float = 3.0,
    min_samples: int | None = None,
) -> pl.DataFrame:
    """
    1) Ordena por time_col.
    2) RANSAC con DummyRegressor(median) para identificar el ‘nivel’ típico.
    3) Quita todos los gaps que RANSAC marca como outliers.
    """

    # 1) Orden y posición
    df_original = df.sort(time_col).with_row_count("pos_org")
    df_ord = df_original.sort(time_col).with_row_count("pos")

    # 2) Preparar X,y
    X = df_ord["pos"].to_numpy().reshape(-1, 1)
    y = df_ord[col_gap].to_numpy()

    # 3) Umbral por MAD global
    med = np.median(y)
    mad = np.median(np.abs(y - med))
    thresh = residual_factor * mad

    # 4) RANSAC sobre modelo constante (mediana)
    base = DummyRegressor(strategy="median")
    if min_samples is None:
        min_samples = max(1, len(y) // 100)  # al menos 1% de los datos
    ransac = RANSACRegressor(
        estimator=base,
        residual_threshold=thresh,
        min_samples=min_samples,
        random_state=42,
    )
    ransac.fit(X, y)

    # 5) Máscara de inliers
    inlier_mask = ransac.inlier_mask_

    # 6) Filtrar en Polars
    return (
        df_ord.with_columns(pl.Series("mask", inlier_mask.tolist()))
        .filter(pl.col("mask"))
        .sort("pos_org")
        .drop(["pos", "pos_org", "mask"])
    )


def filter_inactivity_samples(df: pl.DataFrame) -> pl.DataFrame:
    """
    Elimina muestras que representan períodos de inactividad.

    Criterios de inactividad:
      1. Gaps extremadamente largos (> 30 min).
      2. Tiempo largo (> percentil 75) en sucursal inactiva (≤ 1 usuario en ventana de 15 min).
      3. Muestra aislada (gaps prev/next > 10 min) + tiempo largo + muy baja actividad (≤ 2 actos en 15 min).
      4. Usuario único en ventana 15 min + tiempo largo + sucursal inactiva.
      5. **(Nuevo)** Baja demanda: ≥ 2 usuarios en ventana de 1 hora pero tiempo medio > tiempo largo.

    Parámetros:
      df debe tener al menos:
        - "Timestamp_Aplicado" (Datetime)
        - "tiempo_actividad_segs" (Numérico)

    Retorna:
      DataFrame filtrado sin las filas de inactividad.
    """
    if df.is_empty():
        return df

    # Parámetros
    GAP_MIN = 600  # 10 min
    EXTREME_GAP = 1800  # 30 min
    # percentil 75 de los tiempos de actividad
    long_gap = (
        df.select(
            pl.col("tiempo_actividad_segs").quantile(0.75, interpolation="linear")
        ).item()
        or 600
    )

    return (
        df.lazy()
        # — Ventanas y gaps —
        .with_columns(
            [
                # ventana 15 min y ventana 1 hora
                pl.col("Timestamp_Aplicado").dt.truncate("15m").alias("vent_15m"),
                pl.col("Timestamp_Aplicado").dt.truncate("1h").alias("vent_1h"),
                # gaps prev/next por usuario
                pl.col("tiempo_actividad_segs")
                .shift(1)
                .over("EmpSucUsr_Id", order_by="Timestamp_Aplicado")
                .alias("gap_prev"),
                pl.col("tiempo_actividad_segs")
                .shift(-1)
                .over("EmpSucUsr_Id", order_by="Timestamp_Aplicado")
                .alias("gap_next"),
            ]
        )
        # — Estadísticas por ventana 15m —
        .with_columns(
            [
                pl.count().over(["Emp_Id", "Suc_Id", "vent_15m"]).alias("acts_15m"),
                pl.n_unique("EmpSucUsr_Id")
                .over(["Emp_Id", "Suc_Id", "vent_15m"])
                .alias("users_15m"),
                pl.count().over(["EmpSucUsr_Id", "vent_15m"]).alias("acts_usr_15m"),
            ]
        )
        # — Estadísticas por ventana 1h (nuevo criterio) —
        .with_columns(
            [
                pl.n_unique("EmpSucUsr_Id")
                .over(["Emp_Id", "Suc_Id", "vent_1h"])
                .alias("users_1h"),
                pl.col("tiempo_actividad_segs")
                .mean()
                .over(["Emp_Id", "Suc_Id", "vent_1h"])
                .alias("avg_time_1h"),
            ]
        )
        # — Marcar inactividad según todos los criterios —
        .with_columns(
            [
                (
                    # 1. gap extremo
                    (pl.col("tiempo_actividad_segs") > EXTREME_GAP)
                    |
                    # 2. tiempo largo + sucursal inactiva
                    (
                        (pl.col("tiempo_actividad_segs") > long_gap)
                        & (pl.col("users_15m") <= 1)
                    )
                    |
                    # 3. aislada + tiempo largo + muy baja actividad
                    (
                        (pl.col("gap_prev").fill_null(0) > GAP_MIN)
                        & (pl.col("gap_next").fill_null(0) > GAP_MIN)
                        & (pl.col("tiempo_actividad_segs") > long_gap)
                        & (pl.col("acts_15m") <= 2)
                    )
                    |
                    # 4. usuario único + tiempo largo + sucursal inactiva
                    (
                        (pl.col("acts_usr_15m") <= 1)
                        & (pl.col("tiempo_actividad_segs") > long_gap)
                        & (pl.col("users_15m") <= 1)
                    )
                    |
                    # 5. baja demanda 1h: ≥2 usuarios pero avg_time > long_gap
                    ((pl.col("users_1h") >= 2) & (pl.col("avg_time_1h") > long_gap))
                ).alias("is_inactive")
            ]
        )
        # — Filtrar y limpiar las columnas de apoyo —
        .filter(~pl.col("is_inactive"))
        .drop(
            [
                "vent_15m",
                "vent_1h",
                "gap_prev",
                "gap_next",
                "acts_15m",
                "users_15m",
                "acts_usr_15m",
                "users_1h",
                "avg_time_1h",
                "is_inactive",
            ]
        )
        .collect()
    )


def marcar_segmentos_baja_actividad(
    df: pl.DataFrame,
    col_gap: str = "tiempo_actividad_segs",
    usuario_col: str = "EmpSucUsr_Id",
    timestamp_col: str = "Timestamp_Aplicado",  # Necesario para ordenar correctamente
    window_size_rolling: int = 5,  # La 'window' original, será 2*window+1 para rolling_median
    threshold_factor: float = 2.0,
    min_segment_len: int = 3,
) -> pl.DataFrame:
    """
    Marca registros que pertenecen a segmentos de baja actividad usando solo Polars.
    Agrega columna booleana 'eliminar_por_segmento'.
    """
    # Asegurar que el DataFrame esté ordenado por usuario y luego por tiempo
    # Es crucial para las operaciones de ventana y rle_id
    df_sorted = df.sort(usuario_col, timestamp_col)

    # Calcular la mediana global de gaps por usuario
    df_with_medians = df_sorted.with_columns(
        pl.median(col_gap).over(usuario_col).alias("global_median_user")
    )

    # Calcular la mediana móvil de gaps por usuario
    # La window_size en rolling_median es el número total de observaciones en la ventana.
    # Si la 'window' original era para +/- N puntos, la window_size total es 2*N + 1.
    actual_rolling_window = (2 * window_size_rolling) + 1
    df_with_medians = df_with_medians.with_columns(
        pl.col(col_gap)
        .rolling_median(
            window_size=actual_rolling_window,
            center=True,  # Centra la ventana
            min_samples=1,  # Opcional: ajusta si quieres un mínimo de obs para calcular
        )
        .over(usuario_col)
        .alias("rolling_median_user")
    )

    # Identificar periodos donde la mediana móvil es significativamente alta
    df_with_flags = df_with_medians.with_columns(
        (
            pl.col("rolling_median_user")
            > (pl.col("global_median_user") * threshold_factor)
        ).alias("is_potential_low_activity")
    )

    # Identificar segmentos consecutivos de 'is_potential_low_activity'
    # Usamos rle_id() para dar un ID único a cada "corrida" de valores consecutivos
    df_with_segments = df_with_flags.with_columns(
        pl.col("is_potential_low_activity")
        .rle_id()
        .over(usuario_col)
        .alias("segment_run_id")
    )

    # Calcular la longitud de cada segmento
    df_with_segment_len = df_with_segments.with_columns(
        pl.count().over([usuario_col, "segment_run_id"]).alias("current_segment_length")
    )

    # Marcar para eliminación si el segmento es de 'baja actividad' Y su longitud es suficiente
    df_final_marked = df_with_segment_len.with_columns(
        (
            pl.col("is_potential_low_activity")
            & (pl.col("current_segment_length") >= min_segment_len)
        ).alias("eliminar_por_segmento")
    )

    # Opcional: eliminar columnas intermedias
    return df_final_marked.drop(
        "global_median_user",
        "rolling_median_user",
        "is_potential_low_activity",
        "segment_run_id",
        "current_segment_length",
    )


def symmetrize_distribution(
    df: pl.DataFrame, col: str = "tiempo_actividad_segs"
) -> pl.DataFrame:
    # 1) Extraemos array y calculamos percentiles
    x = df[col].to_numpy().astype(float)
    # q25 = np.percentile(x, 25)
    q50 = np.percentile(x, 50)
    percentiles = np.percentile(x, np.arange(51, 91))  # 51 al 90

    # 2) Si el tramo de arriba no existe o Q90=Q50, devolvemos sin cambios
    q90 = percentiles[-1]
    if q90 <= q50:
        return df

    # 3) Reescalado elástico por tramos entre percentiles usando escala de percentil espejo
    mapped = x.copy()
    percentiles_above = percentiles
    percentiles_below = np.percentile(x, 100 - np.arange(51, 91))  # 49 al 10
    prev_p = q50
    rng = np.random.default_rng()
    for i, (p_above, p_below) in enumerate(zip(percentiles_above, percentiles_below)):
        next_p = p_above
        mirror_p = p_below
        # Para cada tramo entre percentiles
        mask = (x > prev_p) & (x <= next_p)
        # Selecciona aleatoriamente el 50% de los valores en el tramo para escalar
        idx = np.where(mask)[0]
        n_select = int(np.ceil(len(idx) * 0.5))
        if n_select > 0:
            selected_idx = rng.choice(idx, size=n_select, replace=False)
        else:
            selected_idx = np.array([], dtype=int)
        # Elasticidad: escala depende de la distancia relativa a q50 dentro del tramo
        if next_p - q50 == 0:
            scale = 1.0
        else:
            rel = (x[selected_idx] - q50) / (next_p - q50)
            scale = (q50 - mirror_p) / (next_p - q50)
            elastic_scale = 1 - rel + rel * scale
            mapped[selected_idx] = q50 + (x[selected_idx] - q50) * elastic_scale
        prev_p = next_p

    # Para valores mayores al percentil 90, usar el último factor de escala con espejo en p10
    mask = x > q90
    idx = np.where(mask)[0]
    n_select = int(np.ceil(len(idx) * 0.5))
    rng = np.random.default_rng()
    if n_select > 0:
        selected_idx = rng.choice(idx, size=n_select, replace=False)
    else:
        selected_idx = np.array([], dtype=int)
    p10 = np.percentile(x, 10)
    if q90 - q50 == 0:
        scale = 1.0
    else:
        rel = (x[selected_idx] - q50) / (q90 - q50)
        scale = (q50 - p10) / (q90 - q50)
        elastic_scale = 1 - rel + rel * scale
        mapped[selected_idx] = q50 + (x[selected_idx] - q50) * elastic_scale

    # 5) Reconstruimos el DataFrame con la columna transformada
    return df.with_columns(pl.Series(col, mapped))


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


def get_movements_data(
    dwh_farinter_bi: SQLServerResource, config: ConfigGetData
) -> pl.DataFrame:
    cache_name = config.cache_name or "ut_mov_data"
    if config.use_cache:
        cache_handler = ParquetCacheHandler(filename=cache_name)
        df = cache_handler.get_cached_data(max_age_seconds=config.cache_max_age_seconds)
        if df is not None:
            return df

    fecha_desde_str = config.fecha_desde.strftime("%Y%m%d")
    fecha_hasta_str = config.fecha_hasta.strftime("%Y%m%d")
    query_mov = dedent(f""" 
    SELECT
        ME.Emp_Id,
        ME.Suc_Id,
        ME.Bodega_Id,
        ME.Mov_Id,
        ME.Tipo_Id,
        ME.Usuario_Id,
        ME.Mov_Fecha,
        ME.Mov_Fecha_Aplicado,
        ME.Mov_Fecha_Aplicado AS Mov_Fecha_Final,
        ME.Mov_Fecha_Recibido,
        ME.Mov_Estado,
        ME.Mov_Interbodega,
        ME.Mov_Origen,
        MP.cantidad_items,
        MP.cantidad_total,
        MP.valor_total,
        MP.costo_unitario_prom,
        MP.precio_unitario_prom,
        MP.contiene_merma,
        MP.es_interbodega AS es_interbodega_detalle, -- Renamed to avoid clash
        MP.es_ajuste,
        TM.Tipo_Nombre,
        TM.Tipo_Estadistica,
        TM.Tipo_Suma_Inventario,
        TM.Tipo_Mov_Merma,
        S.Departamento_Id,
        S.EmpDepMunCiu_Id,
        S.Zona_Nombre,
        S.TipoSucursal_Id,
        U.Usuario_Nombre,
        CASE WHEN TM.Tipo_Mov_Merma = 1 THEN 1 ELSE 0 END AS es_merma,
        CASE WHEN TM.Tipo_Suma_Inventario = 1 THEN 1 ELSE 0 END AS suma_inventario,
        CASE WHEN TM.Tipo_InterBodega = 1 THEN 1 ELSE 0 END AS es_tipo_interbodega,
        CASE WHEN ME.Mov_Interbodega = 1 THEN 1 ELSE 0 END AS es_mov_interbodega
    FROM DL_FARINTER.dbo.DL_Kielsa_Mov_Inventario_Encabezado ME
    INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal S
    ON ME.Emp_Id = S.Emp_Id
    AND ME.Suc_Id = S.Sucursal_Id
    INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Tipo_Mov_Inventario TM
    ON ME.Emp_Id = TM.Emp_Id
    AND ME.Tipo_Id = TM.Tipo_Id
    LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Usuario U
    ON ME.Emp_Id = U.Emp_Id
    AND ME.Usuario_Id = U.Usuario_Id
    INNER JOIN 
        (SELECT 
            MH.Emp_Id, 
            MH.Suc_Id, 
            MH.Bodega_Id, 
            MH.Mov_Id,
            COUNT(DISTINCT MH.Articulo_Id) AS cantidad_items,
            SUM(MH.Detalle_Cantidad) AS cantidad_total,
            SUM(MH.Detalle_Cantidad * MH.Detalle_Precio_Unitario) AS valor_total,
            AVG(MH.Detalle_Costo_Unitario) AS costo_unitario_prom,
            AVG(MH.Detalle_Precio_Unitario) AS precio_unitario_prom,
            CASE WHEN MAX(CASE WHEN TM.Tipo_Mov_Merma = 1 THEN 1 ELSE 0 END) = 1
                THEN 1 ELSE 0 END AS contiene_merma,
            CASE WHEN MAX(CASE WHEN MH.Suc_Destino != MH.Suc_Id THEN 1 ELSE 0 END) = 1
                THEN 1 ELSE 0 END AS es_interbodega,
            CASE WHEN MAX(CASE WHEN TM.Tipo_Nombre LIKE '%AJUSTE%' THEN 1 ELSE 0 END) = 1
                THEN 1 ELSE 0 END AS es_ajuste
        FROM DL_FARINTER.dbo.DL_Kielsa_MovimientosHist MH
        INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Tipo_Mov_Inventario TM
        ON MH.Emp_Id = TM.Emp_Id
        AND MH.Tipo_Id = TM.Tipo_Id
        WHERE MH.AnioMes_Id >= '{fecha_desde_str[:6]}' --Filtra por la particion
        AND MH.AnioMes_Id <= '{fecha_hasta_str[:6]}'
        GROUP BY MH.Emp_Id, MH.Suc_Id, MH.Bodega_Id, MH.Mov_Id
        ) MP
        ON ME.Emp_Id = MP.Emp_Id
        AND ME.Suc_Id = MP.Suc_Id
        AND ME.Bodega_Id = MP.Bodega_Id
        AND ME.Mov_Id = MP.Mov_Id

    WHERE ME.Mov_Fecha >= '{fecha_desde_str}' AND ME.Mov_Fecha <= '{fecha_hasta_str}'
    AND ME.Emp_Id = 1
    {"AND ME.Suc_Id IN (" + ",".join(str(s) for s in config.sucursales) + ")" if config.sucursales is not None else ""}
    UNION ALL
    SELECT
        ME.Emp_Id,
        ME.Suc_Destino AS Suc_Id,
        ME.Bodega_Destino AS Bodega_Id,
        ME.Mov_Id,
        ME.Tipo_Id,
        ME.Usuario_Id,
        ME.Mov_Fecha,
        ME.Mov_Fecha_Aplicado,
        ME.Mov_Fecha_Recibido AS Mov_Fecha_Final,
        ME.Mov_Fecha_Recibido,
        ME.Mov_Estado,
        ME.Mov_Interbodega,
        ME.Mov_Origen,
        MP.cantidad_items,
        MP.cantidad_total,
        MP.valor_total,
        MP.costo_unitario_prom,
        MP.precio_unitario_prom,
        MP.contiene_merma,
        MP.es_interbodega AS es_interbodega_detalle, -- Renamed to avoid clash
        MP.es_ajuste,
        TM.Tipo_Nombre,
        TM.Tipo_Estadistica,
        TM.Tipo_Suma_Inventario,
        TM.Tipo_Mov_Merma,
        S.Departamento_Id,
        S.EmpDepMunCiu_Id,
        S.Zona_Nombre,
        S.TipoSucursal_Id,
        U.Usuario_Nombre,
        CASE WHEN TM.Tipo_Mov_Merma = 1 THEN 1 ELSE 0 END AS es_merma,
        CASE WHEN TM.Tipo_Suma_Inventario = 1 THEN 1 ELSE 0 END AS suma_inventario,
        CASE WHEN TM.Tipo_InterBodega = 1 THEN 1 ELSE 0 END AS es_tipo_interbodega,
        CASE WHEN ME.Mov_Interbodega = 1 THEN 1 ELSE 0 END AS es_mov_interbodega
    FROM DL_FARINTER.dbo.DL_Kielsa_Mov_Inventario_Encabezado ME
    INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal S
    ON ME.Emp_Id = S.Emp_Id
    AND ME.Suc_Id = S.Sucursal_Id
    INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Tipo_Mov_Inventario TM
    ON ME.Emp_Id = TM.Emp_Id
    AND ME.Tipo_Id = TM.Tipo_Id
    LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Usuario U
    ON ME.Emp_Id = U.Emp_Id
    AND ME.Usuario_Id = U.Usuario_Id
    INNER JOIN 
        (SELECT 
            MH.Emp_Id, 
            MH.Suc_Id, 
            MH.Bodega_Id, 
            MH.Mov_Id,
            COUNT(DISTINCT MH.Articulo_Id) AS cantidad_items,
            SUM(MH.Detalle_Cantidad) AS cantidad_total,
            SUM(MH.Detalle_Cantidad * MH.Detalle_Precio_Unitario) AS valor_total,
            AVG(MH.Detalle_Costo_Unitario) AS costo_unitario_prom,
            AVG(MH.Detalle_Precio_Unitario) AS precio_unitario_prom,
            CASE WHEN MAX(CASE WHEN TM.Tipo_Mov_Merma = 1 THEN 1 ELSE 0 END) = 1
                THEN 1 ELSE 0 END AS contiene_merma,
            CASE WHEN MAX(CASE WHEN MH.Suc_Destino != MH.Suc_Id THEN 1 ELSE 0 END) = 1
                THEN 1 ELSE 0 END AS es_interbodega,
            CASE WHEN MAX(CASE WHEN TM.Tipo_Nombre LIKE '%AJUSTE%' THEN 1 ELSE 0 END) = 1
                THEN 1 ELSE 0 END AS es_ajuste
        FROM DL_FARINTER.dbo.DL_Kielsa_MovimientosHist MH
        INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Tipo_Mov_Inventario TM
        ON MH.Emp_Id = TM.Emp_Id
        AND MH.Tipo_Id = TM.Tipo_Id
        WHERE MH.AnioMes_Id >= '{fecha_desde_str[:6]}' --Filtra por la particion
        AND MH.AnioMes_Id <= '{fecha_hasta_str[:6]}' --Filtra por la particion
        GROUP BY MH.Emp_Id, MH.Suc_Id, MH.Bodega_Id, MH.Mov_Id
        ) MP
        ON ME.Emp_Id = MP.Emp_Id
        AND ME.Suc_Id = MP.Suc_Id
        AND ME.Bodega_Id = MP.Bodega_Id
        AND ME.Mov_Id = MP.Mov_Id
    WHERE ME.Mov_Fecha >= '{fecha_desde_str}' AND ME.Mov_Fecha <= '{fecha_hasta_str}'
    AND ME.Emp_Id = 1    
    {"AND ME.Suc_Id IN (" + ",".join(str(s) for s in config.sucursales) + ")" if config.sucursales is not None else ""}
    """)

    df = (
        pl.read_database(query_mov, dwh_farinter_bi.get_arrow_odbc_conn_string())
        .lazy()
        .with_columns(
            pl.col("es_interbodega_detalle").alias("es_interbodega")
        )  # Use the one from MP
        .drop("es_interbodega_detalle")
        .collect(engine="streaming")
    )

    if config.use_cache:
        cache_handler = ParquetCacheHandler(filename=cache_name)
        cache_handler.save_to_cache(df)

    return df


def get_transactions_data(
    dwh_farinter_bi: SQLServerResource, config: ConfigGetData
) -> pl.DataFrame:
    cache_name = config.cache_name or "ut_trx_data"

    if config.use_cache:
        cache_handler = ParquetCacheHandler(filename=cache_name)
        df = cache_handler.get_cached_data(max_age_seconds=config.cache_max_age_seconds)
        if df is not None:
            return df

    fecha_desde_str = config.fecha_desde.strftime("%Y%m%d")
    fecha_hasta_str = config.fecha_hasta.strftime("%Y%m%d")
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
        FE.Usuario_Id, 
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
                OR M.Tipo_Plan LIKE '%ASEGURAD%' 
            THEN 1 ELSE 0 END AS es_asegurado,
        M.Tipo_Plan,
        CASE WHEN CAJ.Emp_Id IS NOT NULL
            THEN 1 ELSE 0 END AS es_suc_autoservicio
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
    LEFT JOIN (SELECT Emp_Id, Suc_Id 
                FROM DL_FARINTER.dbo.DL_Kielsa_Caja 
                WHERE Caja_Nombre LIKE '%auto%'
                GROUP BY Emp_Id, Suc_Id
                       ) CAJ
    ON FE.Emp_Id = CAJ.Emp_Id
    AND FE.Suc_Id = CAJ.Suc_Id
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
        AND FP.Factura_Fecha<= '{fecha_hasta_str}'
        GROUP BY FP.Emp_Id, FP.Suc_Id, FP.TipoDoc_id, FP.Caja_Id, FP.Factura_Id
        ) FP
        ON FE.Emp_Id = FP.Emp_Id
        AND FE.Suc_Id = FP.Suc_Id
        AND FE.TipoDoc_id = FP.TipoDoc_id
        AND FE.Caja_Id = FP.Caja_Id
        AND FE.Factura_Id = FP.Factura_Id

    WHERE FE.Factura_Fecha>= '{fecha_desde_str}' AND FE.Factura_Fecha<= '{fecha_hasta_str}'
    AND FE.Emp_Id = 1
    {"AND FE.Suc_Id IN (" + ",".join(str(s) for s in config.sucursales) + ")" if config.sucursales is not None else ""}
    """)

    df = (
        pl.read_database(query_trx, dwh_farinter_bi.get_arrow_odbc_conn_string())
        .lazy()
        .collect(engine="streaming")
    )

    if config.use_cache:
        cache_handler = ParquetCacheHandler(filename=cache_name)
        cache_handler.save_to_cache(df)

    return df


def get_marcador_data(
    dwh_farinter_bi: SQLServerResource, config: ConfigGetData
) -> pl.DataFrame:
    cache_name = config.cache_name or "ut_marcador_mov_data"

    if config.use_cache:
        cache_handler = ParquetCacheHandler(filename=cache_name)
        df = cache_handler.get_cached_data(max_age_seconds=config.cache_max_age_seconds)
        if df is not None:
            return df

    fecha_desde_str = config.fecha_desde.strftime("%Y%m%d")
    fecha_hasta_str = config.fecha_hasta.strftime("%Y%m%d")
    query_marcador = dedent(f""" 
        SELECT Pais_Id as Emp_Id, 
            Sucursal_Id as Suc_Id, 
            Fecha_Calendario,  
            Hora_Id as hora_id, 
            EH.Cantidad_Empleados
        FROM BI_FARINTER.dbo.BI_Kielsa_Agr_Sucursal_Marcador_EmpleadosHora EH
        WHERE EH.Fecha_Calendario >= '{fecha_desde_str}' 
        AND EH.Fecha_Calendario <= '{fecha_hasta_str}'
        AND Pais_Id=1
        {"AND Sucursal_Id IN (" + ",".join(str(s) for s in config.sucursales) + ")" if config.sucursales is not None else ""}
    """)  # Renamed query variable

    df = (
        pl.read_database(
            query_marcador, dwh_farinter_bi.get_arrow_odbc_conn_string()
        )  # Used renamed variable
        .lazy()
        .collect(engine="streaming")
    )

    if config.use_cache:
        cache_handler = ParquetCacheHandler(filename=cache_name)
        cache_handler.save_to_cache(df)

    return df


def tranform_movements_data(df: pl.DataFrame | pl.LazyFrame) -> DataFrameWithMeta:
    """
    Transforms raw movement data by creating new features, casting types,
    and calculating estimated movement times.
    """
    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    llaves_primarias_id = (
        "Emp_Id",
        "Suc_Id",
        "Usuario_Id",
        "hora_id",  # hora_id will be added
    )

    cs_columnas_categoricas = (
        cs.by_name("es_interbodega")
        | cs.by_name("contiene_merma")
        | cs.starts_with("es_")
        | cs.starts_with("suma_")
        | cs.ends_with("_Id")  # Note: Changed from _id to _Id to match schema
        | cs.by_dtype(pl.String)
        | cs.by_dtype(pl.Categorical)
    ) - cs.by_name(llaves_primarias_id)

    df = df.with_columns(
        pl.col("Mov_Fecha_Final").dt.hour().alias("hora_id"),
        pl.concat_str(
            pl.col("Emp_Id").cast(pl.String),
            pl.col("Suc_Id").cast(pl.String),
            pl.col("Usuario_Id").cast(pl.String),
            separator="-",
        ).alias("EmpSucUsr_Id"),
    ).with_columns(
        # Ensure main IDs are correct type for joins later
        (cs.by_name(llaves_primarias_id) & cs.numeric()).cast(pl.Int64)
    )

    # Perform casting for categorical and numerical columns
    df = df.with_columns(
        (cs_columnas_categoricas & cs.numeric()).cast(pl.String),
        (
            cs_columnas_categoricas & cs.string()
        ),  # Already strings, ensure they are processed if needed
    ).with_columns(
        # Cast all other numeric columns (not part of cs_columnas_categoricas and not llaves_primarias_id) to Float64
        (
            cs.numeric()
            - cs.by_name(llaves_primarias_id)
            - (cs_columnas_categoricas & cs.numeric())
        ).cast(pl.Float64)
    )

    df = (
        df.with_columns(
            pl.col("cantidad_items").abs().fill_null(0),
            pl.col("cantidad_total").abs().fill_null(0),
            pl.col("costo_unitario_prom").abs().fill_null(0),
            pl.col("valor_total")
            .qcut(3, allow_duplicates=True)
            .alias("valor_total_qcut"),
            pl.col("costo_unitario_prom")
            .qcut(3, allow_duplicates=True)
            .alias("costo_unitario_prom_qcut"),
            (pl.col("cantidad_total") - pl.col("cantidad_items"))
            .clip(0)
            .alias("cantidad_total_relativa"),
            pl.lit(1).cast(pl.Float64).alias("ctd_movimientos"),
            (
                (pl.col("cantidad_items"))
                .log1p()
                .fill_nan(0)  # log1p of 0 is 0. fill_nan for safety.
                .fill_null(0)
                .cast(pl.Float64)
            ).alias("log_cantidad_items"),
            (
                pl.col("costo_unitario_prom")
                .log1p()
                .fill_nan(0)
                .fill_null(0)
                .cast(pl.Float64)
            ).alias("log_costo_unitario_prom"),
            pl.col("EmpSucUsr_Id")
            .n_unique()
            .over(["Emp_Id", "Suc_Id"])
            .alias("Usuarios_Activos"),
        )
        .with_columns(
            pl.col("cantidad_total_relativa")
            .log1p()
            .fill_nan(0)
            .fill_null(0)
            .cast(pl.Float64)
            .alias("log_cantidad_total_relativa"),
        )
        .with_columns(
            (
                180.0  # Base movement time
                + 120.0 * pl.col("log_cantidad_items")
                + 45.0 * pl.col("contiene_merma").cast(pl.Int64).fill_null(0)
                + 30.0
                * pl.col("es_interbodega")
                .cast(pl.Int64)
                .fill_null(0)  # es_interbodega from MP
                + 25.0 * pl.col("log_cantidad_total_relativa")
                + 20.0 * pl.col("log_costo_unitario_prom")
                + 15.0 * pl.col("es_ajuste").cast(pl.Int64).fill_null(0)
            )
            .fill_null(300.0)  # Default estimated time
            .alias("tiempo_movimiento_estimado")
        )
    )

    # Original df_horas logic for peak hours (can be added if needed for features)
    # For now, simplifying by removing it as per the plan to calculate time_activity_segs later.
    # If "es_hora_pico" is a desired feature, it should be calculated here based on "total_actividades_hora".

    return DataFrameWithMeta(
        df=df,
        primary_keys=llaves_primarias_id,
        categorical_columns=cs.expand_selector(df, cs_columnas_categoricas),
    )


def tranform_transactions_data(df: pl.DataFrame | pl.LazyFrame) -> DataFrameWithMeta:
    """
    Transforms raw transaction data by creating new features, casting types,
    and calculating estimated transaction times.
    """
    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    llaves_primarias_id = (
        "Emp_Id",
        "Suc_Id",
        "EmpSucDocCajFac_Id",
        "EmpSucCaj_Id",  # Will be created
        "hora_id",  # Will be created
        "Caja_Id",
        "Usuario_Id",  # Added for consistency
    )

    cs_columnas_categoricas = (
        cs.by_name("acumula_monedero")
        | cs.by_name("contiene_tengo")
        | cs.starts_with("es_")
        | cs.starts_with("contiene_")
        | cs.ends_with("_Id")
        | cs.by_dtype(pl.String)
        | cs.by_dtype(pl.Categorical)
    ) - cs.by_name(llaves_primarias_id)

    df = df.with_columns(
        pl.col("Factura_FechaHora").dt.hour().alias("hora_id"),
        pl.concat_str(
            pl.col("Emp_Id").cast(pl.String),
            pl.col("Suc_Id").cast(pl.String),
            pl.col("Caja_Id").cast(pl.String),
            separator="-",
        ).alias("EmpSucCaj_Id"),
    ).with_columns(
        # Ensure main IDs are correct type for joins later
        (cs.by_name(llaves_primarias_id) & cs.numeric()).cast(pl.Int64)
    )

    # Perform casting for categorical and numerical columns
    df = df.with_columns(
        (cs_columnas_categoricas & cs.numeric()).cast(pl.String),
        (cs_columnas_categoricas & cs.string()),
    ).with_columns(
        (
            cs.numeric()
            - cs.by_name(llaves_primarias_id)
            - (cs_columnas_categoricas & cs.numeric())
        ).cast(pl.Float64)
    )

    df = (
        df.with_columns(
            pl.col("cantidad_productos").abs().fill_null(0),
            pl.col("cantidad_unidades").abs().fill_null(0),
            pl.col("precio_unitario_prom").abs().fill_null(0),
            pl.col("valor_neto")
            .qcut(3, allow_duplicates=True)
            .alias("valor_neto_qcut"),
            pl.col("precio_unitario_prom")
            .qcut(3, allow_duplicates=True)
            .alias("precio_unitario_prom_qcut"),
            (pl.col("cantidad_unidades") - pl.col("cantidad_productos"))
            .clip(0)
            .alias("cantidad_unidades_relativa"),
            pl.lit(1).cast(pl.Float64).alias("ctd_transacciones"),
            (
                (pl.col("cantidad_productos"))
                .log1p()
                .fill_nan(0)
                .fill_null(0)
                .cast(pl.Float64)
            ).alias("log_cantidad_productos"),
            (
                pl.col("precio_unitario_prom")
                .log1p()
                .fill_nan(0)
                .fill_null(0)
                .cast(pl.Float64)
            ).alias("log_precio_unitario_prom"),
            # Cajas_Activas will be calculated in the final transform_data if needed for filtering
            # For now, this specific calculation is removed from here.
            # If it's a feature for the model, it should be calculated based on EmpSucCaj_Id
            # col("EmpSucCaj_Id")
            # .n_unique()
            # .over(["Emp_Id", "Suc_Id"]) # Original was partition_by=("Emp_Id", "Suc_Id")
            # .alias("Cajas_Activas"),
        )
        .with_columns(
            pl.col("cantidad_unidades_relativa")
            .log1p()
            .fill_nan(0)
            .fill_null(0)
            .cast(pl.Float64)
            .alias("log_cantidad_unidades_relativa"),
        )
        .with_columns(
            (
                143.407  # Base transaction time
                + 98.564 * pl.col("log_cantidad_productos")
                + 20.968 * pl.col("contiene_farma").cast(pl.Int64).fill_null(0)
                + 13.807 * pl.col("es_tercera_edad").cast(pl.Int64).fill_null(0)
                + 11.722 * pl.col("log_cantidad_unidades_relativa")
                + 11.335 * pl.col("log_precio_unitario_prom")
                + 6.027 * pl.col("acumula_monedero").cast(pl.Int64).fill_null(0)
            )
            .fill_null(216.4261)  # Default estimated time
            .alias(
                "tiempo_transaccion_estimado"
            )  # Renamed from tiempo_actividad_estimado
        )
    )

    return DataFrameWithMeta(
        df=df,
        primary_keys=llaves_primarias_id,
        categorical_columns=cs.expand_selector(df, cs_columnas_categoricas),
    )


def merge_transactions_and_movements(
    dfm_movements: DataFrameWithMeta, dfm_transactions: DataFrameWithMeta
) -> DataFrameWithMeta:
    """
    Merges transformed movement and transaction data.
    Aligns schemas, filters transactions based on movement activity, and concatenates.
    """
    print("Merging transformed transactions with movements...")

    # df
    df_movements = dfm_movements.dataframe
    df_transactions = dfm_transactions.dataframe

    # Prepare transactions for merge
    df_transactions_prepared = df_transactions.with_columns(
        [
            pl.col("Factura_FechaHora").alias("Timestamp_Aplicado"),
            pl.col("Factura_Fecha").alias("Fecha_Actividad_Original"),
            pl.lit("TRANSACCION").alias("Tipo_Actividad"),
            pl.col("Factura_Origen").alias("Origen_Actividad"),
            pl.lit("TRANSACCION_VENTA").alias("Tipo_Nombre"),
            pl.lit(0)
            .cast(pl.Float64)
            .alias(
                "es_merma"
            ),  # Ensure consistent type, Float64 can hold 0/1 and nulls
            pl.lit(0).cast(pl.Float64).alias("contiene_merma"),
            pl.lit(0).cast(pl.Float64).alias("es_interbodega"),
            pl.lit(0).cast(pl.Float64).alias("es_ajuste"),
            pl.lit(1).cast(pl.Float64).alias("suma_inventario"),
            pl.lit(0).cast(pl.Float64).alias("es_tipo_interbodega"),
            pl.lit(0).cast(pl.Float64).alias("es_mov_interbodega"),
            pl.col("cantidad_productos").alias("cantidad_items"),
            pl.col("cantidad_unidades").alias("cantidad_total"),
            pl.col("valor_neto").alias("valor_total"),
            pl.col("precio_unitario_prom").alias("costo_unitario_prom"),
            pl.col("tiempo_transaccion_estimado").alias("tiempo_estimado_individual"),
            pl.concat_str(
                pl.col("Emp_Id").cast(pl.String),
                pl.col("Suc_Id").cast(pl.String),
                pl.col("Usuario_Id").cast(pl.String),
                separator="-",
            ).alias("EmpSucUsr_Id"),
        ]
    )

    # Prepare movements for merge
    df_movements_prepared = df_movements.with_columns(
        [
            pl.col("Mov_Fecha_Final").alias("Timestamp_Aplicado"),
            pl.lit("MOVIMIENTO").alias("Tipo_Actividad"),
            pl.col("Mov_Origen").alias("Origen_Actividad"),
            pl.col("tiempo_movimiento_estimado").alias("tiempo_estimado_individual"),
            # Cast flag columns to Float64 for consistency before merge, as transactions add them as Float64
            pl.col("es_merma").cast(pl.Float64),
            pl.col("contiene_merma").cast(pl.Float64),
            pl.col("es_interbodega").cast(pl.Float64),
            pl.col("es_ajuste").cast(pl.Float64),
            pl.col("suma_inventario").cast(pl.Float64),
            pl.col("es_tipo_interbodega").cast(pl.Float64),
            pl.col("es_mov_interbodega").cast(pl.Float64),
        ]
    )

    # Debug: Check schemas before concat
    # print("Schema df_movements_aligned:", df_movements_aligned.schema)
    # print("Schema df_transactions_aligned:", df_transactions_aligned.schema)
    # for col_name in combined_columns_ordered:
    #     if df_movements_aligned[col_name].dtype != df_transactions_aligned[col_name].dtype:
    #         print(f"Type mismatch remains for {col_name}: {df_movements_aligned[col_name].dtype} vs {df_transactions_aligned[col_name].dtype}")

    df_combined = pl.concat(
        [
            df_movements_prepared,
            df_transactions_prepared,
        ],
        how="diagonal_relaxed",
    ).sort(["Emp_Id", "Suc_Id", "Usuario_Id", "Timestamp_Aplicado"])

    print(f"Movements prepared: {df_movements_prepared.height}")
    print(f"Transactions prepared: {df_transactions_prepared.height}")
    print(f"Total combined records: {df_combined.height}")

    all_categorical_columns = dict.fromkeys(dfm_transactions.categorical_columns)
    all_categorical_columns.update(dict.fromkeys(dfm_movements.categorical_columns))

    return DataFrameWithMeta(
        df_combined,
        primary_keys=dfm_transactions.primary_keys,
        categorical_columns=tuple(all_categorical_columns.keys()),
    )


def process_categorical_columns(
    df: pl.DataFrame | pl.LazyFrame, columnas_categoricas: tuple[str, ...]
) -> pl.DataFrame:
    """
    Process categorical columns by grouping rare categories.
    """
    # --- Agrupar categorías con muestras insuficientes en "otros" ---
    n_min = calculate_min_sample_size(
        confidence_level=0.95, margin_of_error=0.1, proportion=0.5
    )
    # Nota: para estos parámetros, n_min ≈ 97

    # Procesar

    # print(df.sort( by=( "Factura_Fecha", "total_transacciones_hora"), descending = True).head(20))
    df_lazy = df.lazy()

    # Process each column one by one, but maintain lazy state throughout
    for column_name in columnas_categoricas:
        # debug_print(f"Procesando columna: {column_name}")
        # Calculate value counts in eager mode (small operation)
        counts_df = (
            df_lazy.select(pl.col(column_name)).collect().to_series().value_counts()
        )

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


def transform_data(
    dfm_combined: DataFrameWithMeta, df_marcador: pl.DataFrame
) -> DataFrameWithMeta:
    """
    Performs final transformations on the merged data:
    - Calculates time between activities.
    - Applies filters based on this time.
    - Joins with employee marker data and filters based on active users/employees.
    - Groups sparse categorical values.
    """
    debug_print("Starting final data transformation...")

    df = dfm_combined.dataframe.with_columns(
        [
            pl.col("Timestamp_Aplicado").dt.date().alias("Fecha_Actividad"),
        ]
    )

    # Calculate time between activities for each user within each hour
    debug_print("Calculating time between activities (tiempo_actividad_segs)...")
    df = (
        df.sort(("EmpSucUsr_Id", "Fecha_Actividad", "hora_id", "Timestamp_Aplicado"))
        .with_columns(
            pl.col("Timestamp_Aplicado")
            .diff()
            .dt.total_seconds()
            .alias("tiempo_actividad_segs")
        )
        .filter(pl.col("tiempo_actividad_segs").is_not_null())
    )
    debug_print("Data after time between activities calculation:")
    debug_print(df.select(["tiempo_actividad_segs"]).describe())

    # Basic filters on activity time
    df = df.filter(
        (pl.col("tiempo_actividad_segs") > 0)
        & (pl.col("tiempo_actividad_segs") < 3600)  # Max 1 hour
    )
    debug_print("Data after basic filters:")
    debug_print(df.select(["tiempo_actividad_segs"]).describe())

    # Agrega "eliminar por segmento"
    df = marcar_segmentos_baja_actividad(df, col_gap="tiempo_actividad_segs")
    # --- START: Inserted "Top User-Activity Hours" filtering logic ---
    debug_print("Calculating hourly activity metrics for top hours filtering...")
    # Agrupar por EmpSucUsr_Id, Fecha_Actividad, hora_id para contar actividades
    df_horas_empsucusr = df.group_by(
        "EmpSucUsr_Id", "Emp_Id", "Suc_Id", "Fecha_Actividad", "hora_id"
    ).agg(pl.count().alias("total_actividades_hora_empsucusr"))
    df_horas_empsuc = df.group_by("Emp_Id", "Suc_Id", "Fecha_Actividad", "hora_id").agg(
        pl.count().alias("total_actividades_hora_empsuc")
    )

    # Calcular el top_k por las horas de la empsucusr
    TOP_K_PERCENTAGE_EMPSUCUSR_HOURS = (
        0.20  # e.g., top 20% de las horas con más actividad por empsucusr y empsuc
    )

    df_suc_filtered_list = []
    for df_key_empsuc in df_horas_empsuc.partition_by("Emp_Id", "Suc_Id"):
        num_top_k_hours_suc = int(
            TOP_K_PERCENTAGE_EMPSUCUSR_HOURS * df_key_empsuc.height
        )
        df_key_empsuc = df_key_empsuc.top_k(
            num_top_k_hours_suc, by="total_actividades_hora_empsuc"
        )
        # Join the filtered top hours back to the main DataFrame
        df_suc_filtered = (
            df.lazy()
            .join(
                df_key_empsuc.lazy().select(
                    ["Emp_Id", "Suc_Id", "Fecha_Actividad", "hora_id"]
                ),
                on=["Emp_Id", "Suc_Id", "Fecha_Actividad", "hora_id"],
                how="inner",
            )
            .collect(engine="streaming")
        )

        df_suc_filtered = filter_inactivity_samples(df_suc_filtered).filter(
            ~pl.col("eliminar_por_segmento")
        )

        df_top_horas_empsucusr = df_horas_empsucusr.join(
            df_key_empsuc.select(["Emp_Id", "Suc_Id"]).unique(),
            on=["Emp_Id", "Suc_Id"],
            how="inner",
        )
        num_top_k_hours = int(
            TOP_K_PERCENTAGE_EMPSUCUSR_HOURS * df_top_horas_empsucusr.height
        )
        df_top_horas_empsucusr = df_top_horas_empsucusr.top_k(
            num_top_k_hours, by="total_actividades_hora_empsucusr"
        )

        df_suc_filtered = (
            df_suc_filtered.lazy()
            .join(
                df_top_horas_empsucusr.lazy().select(
                    ["EmpSucUsr_Id", "Fecha_Actividad", "hora_id"]
                ),
                on=["EmpSucUsr_Id", "Fecha_Actividad", "hora_id"],
                how="inner",
            )
            .collect(engine="streaming")
        )

        df_suc_filtered_list.append(df_suc_filtered)

    df = pl.concat(df_suc_filtered_list, how="diagonal_relaxed")

    # Clean up intermediate DataFrames
    del (
        df_horas_empsucusr,
        df_horas_empsuc,
    )

    if df.height == 0:
        debug_print(
            "DataFrame is empty after 'Top Empsucusr-Activity Hours' filtering. Subsequent steps might not run or yield empty results."
        )
        # return df # Optionally return early if the DataFrame is empty
    # --- END: Inserted "Top Empsucusr-Activity Hours" filtering logic ---

    if df.height == 0:
        raise ValueError("DataFrame is empty after initial time filters.")

    # Quantile-based filtering for outliers
    q1 = (
        df.select(pl.col("tiempo_actividad_segs"))
        .quantile(0.02, interpolation="linear")
        .item()
    )
    q3 = (
        df.select(pl.col("tiempo_actividad_segs"))
        .quantile(0.98, interpolation="linear")
        .item()
    )
    debug_print(f"Tiempo actividad segs - q1 (0.02): {q1}, q3 (0.98): {q3}")

    df = df.filter(
        (pl.col("tiempo_actividad_segs") >= q1)
        & (pl.col("tiempo_actividad_segs") <= q3)
    ).filter(pl.col("tiempo_actividad_segs").is_not_null())

    if df.height == 0:
        raise ValueError("DataFrame is empty after quantile filters.")

    # Calculate active users per hour for filtering against employee count
    debug_print("Calculating active users per hour...")
    df = df.with_columns(
        pl.col("EmpSucUsr_Id")
        .n_unique()
        .over(["Emp_Id", "Suc_Id", "Fecha_Actividad", "hora_id"])
        .alias("Usuarios_Activos_En_Hora")
    )

    # Join with marcador data
    debug_print("Joining with marcador data...")
    # df_marcador expects Fecha_Calendario, ensure our Fecha_Actividad matches its role    # Prepare df_marcador: Cast Cantidad_Empleados to Float64, but keep IDs as Int64
    df_marcador_prepared = df_marcador.with_columns(
        pl.col("Cantidad_Empleados").cast(
            pl.Float64
        ),  # Only cast the specific numeric column needed as float
        pl.col("Fecha_Calendario").cast(pl.Date),
        # Ensure join keys are Int64 to match the left side (df)
        pl.col("Emp_Id").cast(pl.Int64),
        pl.col("Suc_Id").cast(pl.Int64),
        pl.col("hora_id").cast(
            pl.Int64
        ),  # Assuming hora_id in df_marcador is also numeric and needs to be Int64
    ).rename({"Fecha_Calendario": "Fecha_Actividad_Marcador"})

    df = df.join(
        df_marcador_prepared,
        left_on=["Emp_Id", "Suc_Id", "hora_id", "Fecha_Actividad"],
        right_on=["Emp_Id", "Suc_Id", "hora_id", "Fecha_Actividad_Marcador"],
        how="left",  # Use left join to not lose activities if no marker data (though filter might drop them)
    )

    # Filter based on active users vs. employees
    # If Cantidad_Empleados is null (no marker data for that hour), keep the record for now.
    # Or, decide to filter them out: .filter(col("Cantidad_Empleados").is_not_null())
    debug_print("Filtering based on active users vs. employees...")
    df = df.filter(
        (pl.col("Usuarios_Activos_En_Hora") + 1 >= pl.col("Cantidad_Empleados"))
        | pl.col("Cantidad_Empleados").is_not_null()
    ).drop(cs.ends_with("_right") | cs.by_name("Cantidad_Empleados"))

    df = filtro_ransac_median(df)

    # Quantile-based filtering for outliers
    q1 = (
        df.select(pl.col("tiempo_actividad_segs"))
        .quantile(0.25, interpolation="linear")
        .item()
    )
    q3 = (
        df.select(pl.col("tiempo_actividad_segs"))
        .quantile(0.75, interpolation="linear")
        .item()
    )

    iqr = q3 - q1

    def clip(value, lower=None, upper=None):
        if lower is not None and value < lower:
            return lower
        if upper is not None and value > upper:
            return upper
        return value

    lower_bound = clip(q1 - 1.5 * iqr, 0)
    upper_bound = clip(q3 + 1.5 * iqr)
    debug_print(
        f"Tiempo actividad segs - q1: {q1}, q3: {q3}, IQR: {iqr}, lower_bound: {lower_bound}, upper_bound: {upper_bound}"
    )

    df = df.filter(
        (pl.col("tiempo_actividad_segs") >= lower_bound)
        & (pl.col("tiempo_actividad_segs") <= upper_bound)
        & (pl.col("tiempo_actividad_segs").is_not_null())
    )

    if df.height == 0:
        debug_print("DataFrame is empty after employee count filter. Returning empty.")
        return DataFrameWithMeta(
            df, primary_keys=tuple(""), categorical_columns=tuple("")
        )

    # Group sparse categorical values into "otros"
    debug_print("Grouping sparse categorical values...")
    # Define which columns are categorical for this step
    # This should include original categoricals from both movements and transactions
    # Exclude IDs and high cardinality text fields not meant for this grouping
    # Filter to only existing columns that are strings/categorical
    df = process_categorical_columns(df, df_combined.categorical_columns)

    debug_print("Finished final data transformation.")

    return DataFrameWithMeta(
        df,
        primary_keys=df_combined.primary_keys,
        categorical_columns=df_combined.categorical_columns,
    )


def visualize_data_distribution(
    df: pl.DataFrame,
    target_column: str = "tiempo_actividad_segs",
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
        target_column (str): The target variable to analyze (default: "tiempo_actividad_segs")
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
    if df.height == 0:
        print("DataFrame is empty. Cannot generate visualization.")
        # Create an empty HTML file or just return
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(
                "<html><body><h1>DataFrame is empty. No data to visualize.</h1></body></html>"
            )
        return output_file

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
            col_name  # Changed from col to col_name
            for col_name in df.columns  # Changed from col to col_name
            if df[col_name].dtype.is_numeric()
            and col_name != target_column  # Changed from col to col_name
        ]

        for col_name in numeric_cols:  # Changed from col to col_name
            try:
                # Ensure no all-null columns are passed to pl.corr
                if (
                    df.select(pl.col(target_column).is_not_null().all()).item()
                    and df.select(pl.col(col_name).is_not_null().all()).item()
                    and df.select(pl.col(target_column).drop_nulls().len() > 1).item()
                    and df.select(pl.col(col_name).drop_nulls().len() > 1).item()
                ):
                    corr = abs(df.select(pl.corr(target_column, col_name)).item())
                    if not np.isnan(corr):
                        correlations[col_name] = corr  # Changed from col to col_name
                else:
                    debug_print(
                        f"Skipping correlation for {col_name} due to all nulls or insufficient data."
                    )
            except Exception as e:
                debug_print(f"Could not calculate correlation for {col_name}: {e}")
                pass  # Changed from del e to pass

        # Select top correlated features
        feature_columns = sorted(correlations.items(), key=lambda x: x[1], reverse=True)
        feature_columns = [
            col_name for col_name, _ in feature_columns[:max_features]
        ]  # Changed from col to col_name
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
    if (
        target_column in df_pd.columns
        and df_pd[target_column].isnull().all() is not True
    ):
        print("Creating target distribution plot...")
        fig_target = px.histogram(
            df_pd,
            x=target_column,
            title=f"Distribution of {target_column}",
            marginal="box",
        )
        html_parts.append('<div class="container">')
        html_parts.append("<h2>Target Distribution</h2>")
        html_parts.append(fig_target.to_html(full_html=False, include_plotlyjs="cdn"))
        html_parts.append("</div>")
    else:
        html_parts.append(
            f"<p>Target column {target_column} not found or all nulls.</p>"
        )

    # 2. Feature Distributions
    print("Creating feature distribution plots...")
    html_parts.append('<div class="container">')
    html_parts.append("<h2>Feature Distributions</h2>")

    for col_name in feature_columns:  # Changed from col to col_name
        if col_name not in df_pd.columns or df_pd[col_name].isnull().all() is True:
            html_parts.append(f"<p>Feature {col_name} not found or all nulls.</p>")
            continue
        if df[col_name].dtype.is_numeric():  # Changed from col to col_name
            fig = px.histogram(
                df_pd, x=col_name, title=f"Distribution of {col_name}"
            )  # Changed from col to col_name
            html_parts.append(fig.to_html(full_html=False, include_plotlyjs="cdn"))
        else:
            # For categorical features
            value_counts = (
                df[col_name].value_counts().to_pandas()
            )  # Changed from col to col_name
            fig = px.bar(
                value_counts, x=col_name, y="count", title=f"Distribution of {col_name}"
            )  # Changed from col to col_name
            html_parts.append(fig.to_html(full_html=False, include_plotlyjs="cdn"))

    html_parts.append("</div>")

    # 3. Scatter plots for dispersion analysis
    print("Creating dispersion plots...")
    html_parts.append('<div class="container">')
    html_parts.append("<h2>Dispersion Analysis</h2>")

    for col_name in feature_columns:  # Changed from col to col_name
        if (
            col_name not in df_pd.columns
            or df_pd[col_name].isnull().all() is True
            or target_column not in df_pd.columns
            or df_pd[target_column].isnull().all() is True
        ):
            html_parts.append(
                f"<p>Cannot create dispersion plot for {col_name} vs {target_column} due to missing/all-null data.</p>"
            )
            continue

        if df[col_name].dtype.is_numeric():  # Changed from col to col_name
            fig = px.scatter(
                df_pd,
                x=col_name,  # Changed from col to col_name
                y=target_column,
                title=f"{col_name} vs {target_column}",  # Changed from col to col_name
                trendline="ols",
            )
            html_parts.append(fig.to_html(full_html=False, include_plotlyjs="cdn"))
        else:
            # For categorical features, use box plots
            fig = px.box(
                df_pd,
                x=col_name,
                y=target_column,
                title=f"{col_name} vs {target_column}",  # Changed from col to col_name
            )
            html_parts.append(fig.to_html(full_html=False, include_plotlyjs="cdn"))

    html_parts.append("</div>")

    # 4. Correlation Heatmap
    print("Creating correlation heatmap...")
    numeric_df_pd = df_sample.select(
        [
            col_name
            for col_name in df_sample.columns
            if df_sample[col_name].dtype.is_numeric()
            and df_sample[col_name].is_not_null().any()
        ]  # Changed from col to col_name
    ).to_pandas()

    if not numeric_df_pd.empty:
        corr_matrix = numeric_df_pd.corr()
        fig_heatmap = px.imshow(
            corr_matrix,
            text_auto=".2f",  # type: ignore
            aspect="auto",
            title="Correlation Heatmap",
        )
        html_parts.append('<div class="container">')
        html_parts.append("<h2>Correlation Heatmap</h2>")
        html_parts.append(fig_heatmap.to_html(full_html=False, include_plotlyjs="cdn"))
        html_parts.append("</div>")
    else:
        html_parts.append("<p>No numeric data available for correlation heatmap.</p>")

    # 5. Pair plot for selected features (limited to 4 for readability)
    print("Creating pair plot...")
    pair_features_present = [
        f
        for f in feature_columns
        if f in df_pd.columns and df_pd[f].isnull().all() is not True
    ]
    if (
        target_column in df_pd.columns
        and df_pd[target_column].isnull().all() is not True
    ):
        pair_features = pair_features_present[: min(4, len(pair_features_present))] + [
            target_column
        ]
    else:
        pair_features = pair_features_present[: min(4, len(pair_features_present))]

    if len(pair_features) > 1:  # Need at least 2 features for a pair plot
        fig_pair = px.scatter_matrix(
            df_pd.filter(items=pair_features),  # Use .filter to select existing columns
            dimensions=pair_features,
            title="Pair Plot of Selected Features",
            opacity=0.5,
        )
        html_parts.append('<div class="container">')
        html_parts.append("<h2>Pair Plot</h2>")
        html_parts.append(fig_pair.to_html(full_html=False, include_plotlyjs="cdn"))
        html_parts.append("</div>")
    else:
        html_parts.append("<p>Not enough valid features for a pair plot.</p>")

    # Close HTML
    html_parts.append("</body></html>")

    # Write HTML file
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(html_parts))

    print(f"Visualization saved to {output_file}")

    try:
        print("Opening visualization in browser...")
        webbrowser.open("file://" + os.path.abspath(output_file))
    except Exception as e:
        print(f"Could not open visualization in browser: {e}")

    return output_file


# Example usage in the script:
# if __name__ == "__main__":
#     visualize_data_distribution(df_sensibles, target_column="tiempo_actividad_segs")


def calcular_correlacion_variables(
    df: pl.DataFrame,
    columna_objectivo: str,
    columnas_a_excluir: list = [],
    variables_a_correlacionar: list = [],
    num_rangos: int = 30,
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

    variables_numericas = [
        col
        for col in df.columns
        if col not in columnas_a_excluir
        and df[col].dtype.is_numeric()
        and (not variables_a_correlacionar or col in variables_a_correlacionar)
        and col != columna_objectivo
    ]

    variables_categoricas = [
        col
        for col in df.columns
        if col not in columnas_a_excluir
        and not df[col].dtype.is_numeric()
        and (not variables_a_correlacionar or col in variables_a_correlacionar)
        and col != columna_objectivo
    ]

    df = df.with_columns(pl.col(columna_objectivo).cast(pl.Float64))

    resultados_resumen = []

    for variable in variables_numericas:
        try:
            corr_pearson = df.select(
                pl.corr(columna_objectivo, variable).alias("correlacion_pearson")
            ).item()
            corr_spearman = df.select(
                pl.corr(columna_objectivo, variable, method="spearman").alias(
                    "correlacion_spearman"
                )
            ).item()
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
                    "correlacion_ratio": None,
                    **stats,
                }
            )
        except Exception as e:
            resultados_resumen.append(
                {"variable": variable, "tipo": "numérica", "error": str(e)}
            )

    for variable in variables_categoricas:
        try:
            categorias_count = df[variable].value_counts()
            n_categorias = categorias_count.height
            n_muestras = categorias_count["count"].median()
            n_observaciones_totales = categorias_count["count"].sum()
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
                df_resample = pl.concat(
                    [
                        resample.sample(n=n_muestras, seed=42, with_replacement=True)
                        for resample in df.partition_by(variable)
                    ]
                )
                n_observaciones = df_resample.filter(
                    pl.col(variable).is_not_null()
                ).height
                try:
                    stats_por_categoria = df_resample.group_by(variable).agg(
                        [
                            pl.mean(columna_objectivo).alias(columna_promedio),
                            pl.count(columna_objectivo).alias("n_observaciones"),
                        ]
                    )
                    varianza_total = (
                        df_resample.get_column(columna_objectivo).var() or 0.0
                    )
                    assert isinstance(varianza_total, (int, float))
                    if varianza_total and varianza_total > 0:
                        media_global = df_resample.get_column(columna_objectivo).mean()
                        suma_cuadrados_entre = sum(
                            row["n_observaciones"]
                            * (row[columna_promedio] - media_global) ** 2
                            for row in stats_por_categoria.to_dicts()
                            if row[columna_promedio] is not None
                            and row["n_observaciones"] > 0
                        )
                        correlacion_ratio = suma_cuadrados_entre / (
                            varianza_total * n_observaciones
                        )
                except Exception:
                    correlacion_ratio = None
            resultados_resumen.append(
                {
                    "variable": variable,
                    "tipo": "categórica",
                    "correlacion_pearson": None,
                    "correlacion_spearman": None,
                    "correlacion_ratio": correlacion_ratio,
                    "media_variable": None,
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

    df_resumen = pl.DataFrame(
        resultados_resumen, strict=False, infer_schema_length=10000, nan_to_null=True
    )
    if df_resumen.height > 0:
        df_resumen = df_resumen.with_columns(
            (
                pl.when(pl.col("correlacion_pearson").is_not_null())
                .then(pl.col("correlacion_pearson").abs())
                .otherwise(
                    pl.when(pl.col("correlacion_ratio").is_not_null())
                    .then(pl.col("correlacion_ratio"))
                    .otherwise(0)
                )
                * 100.0
            )
            .round(6)
            .alias("valor_correlacion_0_100")
        ).sort("valor_correlacion_0_100", descending=True)

    resultados_detalle = []

    for variable in variables_numericas:
        try:
            min_val = df[variable].min()
            max_val = df[variable].max()
            assert isinstance(min_val, (int, float))
            assert isinstance(max_val, (int, float))
            n_min_rangos = calculate_min_sample_size(
                confidence_level=0.95, margin_of_error=0.05
            )
            if min_val != max_val:
                valores_ordenados = (
                    df.select(pl.col(variable))
                    .sort(variable)
                    .filter(pl.col(variable).is_not_null())
                )
                total_muestras = valores_ordenados.height
                max_rangos_posibles = min(num_rangos, total_muestras // n_min_rangos)
                if max_rangos_posibles > 1:
                    muestras_por_rango = total_muestras // max_rangos_posibles
                    puntos_corte = [
                        valores_ordenados[i * muestras_por_rango - 1, variable]
                        for i in range(1, max_rangos_posibles)
                        if i * muestras_por_rango - 1 < total_muestras
                    ]

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
                    conteo_rangos = df_con_rangos_inicial.group_by(
                        "rango_idx_inicial"
                    ).agg(
                        pl.count().alias("n_muestras"),
                        pl.min(variable).alias("rango_min"),
                        pl.max(variable).alias("rango_max"),
                    )
                    rangos_finales = []
                    rango_actual = None
                    muestras_acumuladas = 0
                    for row in conteo_rangos.sort("rango_idx_inicial").to_dicts():
                        if rango_actual is None:
                            rango_actual = dict(row)
                            muestras_acumuladas = row["n_muestras"]
                        elif (
                            muestras_acumuladas < n_min_rangos
                            or row["n_muestras"] < n_min_rangos
                        ):
                            rango_actual["rango_max"] = row["rango_max"]
                            muestras_acumuladas += row["n_muestras"]
                        else:
                            rangos_finales.append(rango_actual)
                            rango_actual = dict(row)
                            muestras_acumuladas = row["n_muestras"]
                    if rango_actual is not None:
                        rangos_finales.append(rango_actual)
                    mapeo_rangos = {}
                    for idx, rango in enumerate(rangos_finales):
                        rango_inicial_min = rango["rango_idx_inicial"]
                        rango_inicial_max = (
                            rangos_finales[idx + 1]["rango_idx_inicial"] - 1
                            if idx < len(rangos_finales) - 1
                            else rango_inicial_min
                        )
                        for i in range(rango_inicial_min, int(rango_inicial_max) + 1):
                            mapeo_rangos[i] = idx
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
                    df_con_rangos = df_con_rangos_inicial.with_columns(
                        pl.col("rango_idx_inicial")
                        .map_elements(
                            lambda x: mapeo_rangos.get(x) if x is not None else None
                        )
                        .alias("rango_idx")
                    )
                    df_con_rangos = df_con_rangos.join(
                        df_limites, on="rango_idx", how="left"
                    ).drop("rango_idx_inicial")
                else:
                    df_con_rangos = df.with_columns(
                        [
                            pl.lit(0).alias("rango_idx"),
                            pl.lit(min_val).alias("rango_min"),
                            pl.lit(max_val).alias("rango_max"),
                        ]
                    )
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
                for rango_row in stats_por_rango.to_dicts():
                    etiqueta_rango = (
                        f"{rango_row['rango_min']:.2f} - {rango_row['rango_max']:.2f}"
                    )
                    resultados_detalle.append(
                        {
                            "variable": variable,
                            "tipo": "numérica",
                            "categoria_o_rango": etiqueta_rango,
                            "rango_min": str(rango_row["rango_min"]),
                            "rango_max": str(rango_row["rango_max"]),
                            columna_promedio: float(rango_row[columna_promedio]),
                            columna_mediana: float(rango_row[columna_mediana]),
                            columna_desviacion: float(rango_row[columna_desviacion]),
                            columna_minimo: float(rango_row[columna_minimo]),
                            columna_maximo: float(rango_row[columna_maximo]),
                            "n_observaciones": float(rango_row["n_observaciones"]),
                            "media_variable": float(rango_row["media_variable"]),
                            "desviacion_e_variable": float(
                                rango_row["desviacion_e_variable"]
                            ),
                            "minimo_variable": float(rango_row["minimo_variable"]),
                            "maximo_variable": float(rango_row["maximo_variable"]),
                            "error": None,
                        }
                    )
            else:
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

    for variable in variables_categoricas:
        try:
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
                        columna_promedio: float(cat_row[columna_promedio]),
                        columna_mediana: float(cat_row[columna_mediana]),
                        columna_desviacion: float(cat_row[columna_desviacion]),
                        columna_minimo: float(cat_row[columna_minimo]),
                        columna_maximo: float(cat_row[columna_maximo]),
                        "n_observaciones": float(cat_row["n_observaciones"]),
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

    df_detalle = pl.DataFrame(
        resultados_detalle, strict=False, infer_schema_length=10000, nan_to_null=True
    )
    if df_detalle.height > 0:
        df_detalle = df_detalle.sort(
            ["variable", "n_observaciones"], descending=[False, True]
        )

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
    print(f"tiempo_actividad_segs ≈ {resultado['formula']}")

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


def build_nonlinear_activity_time_model(df, columnas_a_excluir):
    """
    Construye un modelo XGBoost para predecir el tiempo de movimiento,
    capturando relaciones no lineales entre las variables.
    """
    print("Construyendo modelo XGBoost para tiempo de movimiento...")

    # Preparar datos para el modelo
    X = df.drop(
        cs.by_name(columnas_a_excluir + ["tiempo_actividad_segs"], require_all=False)
    ).fill_null(0)
    y = df["tiempo_actividad_segs"]

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
    print("\nModelo XGBoost para Tiempo de Movimiento:")
    print(f"RMSE (train): {rmse_train:.4f}, R² (train): {r2_train:.4f}")
    print(f"RMSE (test): {rmse_test:.4f}, R² (test): {r2_test:.4f}")
    if rmse_cv_mean is not None:
        print(f"RMSE (CV 5-fold): {rmse_cv_mean:.4f} ± {rmse_cv_std:.4f}")

    print("\nVariables más importantes (top 10):")
    print(importance_df.head(10))

    # Guardar resultados en Excel
    importance_df.write_excel(
        ".cache/importancia_modelo_xgboost_tiempo_movimiento.xlsx"
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
    assert linear_model is not None, "El modelo lineal no fue creado correctamente."

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

    print(f"tiempo_actividad_segs ≈ {formula} + ...")

    # Guardar resultados
    linear_approx_df.write_excel(".cache/aproximacion_lineal_xgboost_movimientos.xlsx")

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
        rules_df.write_excel(".cache/reglas_arbol_xgboost_movimientos.xlsx")

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
    Builds linear models for both XGBoost predictions and original target data.

    Args:
        df: Original DataFrame (polars)
        X_pd: Features DataFrame (pandas)
        y_pred_xgb: Target predictions from XGBoost
        feature_names: Names of features
        alpha: Significance level (default 0.05)

    Returns:
        Dictionary with model and statistics for XGBoost approximation
    """
    import statsmodels.api as sm
    from sklearn.preprocessing import StandardScaler

    print("\nBuilding statistically significant linear models...")

    # Get original target data
    if "tiempo_actividad_segs" in df.columns:
        y_original = df.select("tiempo_actividad_segs").to_pandas().values.flatten()
        # Align with X_pd indices if needed
        if len(y_original) != len(X_pd):
            print(
                f"Warning: Original target length ({len(y_original)}) != features length ({len(X_pd)})"
            )
            min_len = min(len(y_original), len(X_pd))
            y_original = y_original[:min_len]
            X_pd_aligned = X_pd.iloc[:min_len]
            y_pred_xgb_aligned = y_pred_xgb[:min_len]
        else:
            X_pd_aligned = X_pd
            y_pred_xgb_aligned = y_pred_xgb
    else:
        print("Warning: Original target 'tiempo_actividad_segs' not found in df")
        y_original = None
        X_pd_aligned = X_pd
        y_pred_xgb_aligned = y_pred_xgb

    def build_single_linear_model(X_data, y_data, model_name):
        """Helper function to build a single linear model"""
        print(f"\n--- Building {model_name} ---")

        # Validate inputs
        if len(X_data) != len(y_data):
            raise ValueError(
                f"X_data and y_data must have the same length for {model_name}"
            )

        if X_data.empty:
            raise ValueError(f"X_data cannot be empty for {model_name}")

        # Create working copy
        X_work = X_data.copy()
        y_work = np.array(y_data).copy()

        # Remove any rows with NaN targets
        valid_idx = ~np.isnan(y_work)
        X_work = X_work.loc[valid_idx].reset_index(drop=True)
        y_work = y_work[valid_idx]

        print(f"Working with {len(X_work)} samples after removing NaN targets")

        # Identify column types
        categorical_columns = []
        numeric_columns = []

        for col in X_work.columns:
            if X_work[col].dtype in [
                "object",
                "category",
            ] or isinstance(X_work[col].dtype, pd.CategoricalDtype):
                # Only include categorical columns with reasonable number of categories
                n_unique = X_work[col].nunique()
                if 1 < n_unique <= 20:  # Avoid constant columns and high cardinality
                    categorical_columns.append(col)
                elif n_unique == 1:
                    print(f"Dropping constant categorical column: {col}")
                else:
                    print(
                        f"Dropping high cardinality categorical column: {col} ({n_unique} unique values)"
                    )
            elif pd.api.types.is_numeric_dtype(X_work[col]):
                numeric_columns.append(col)

        # Process numeric columns
        X_numeric = pd.DataFrame()
        if numeric_columns:
            X_numeric = X_work[numeric_columns].copy()

            # Handle infinite values
            X_numeric = X_numeric.replace([np.inf, -np.inf], np.nan)

            # Fill NaN with median
            for col in X_numeric.columns:
                if X_numeric[col].isnull().any():
                    median_val = X_numeric[col].median()
                    X_numeric[col] = X_numeric[col].fillna(median_val)

            # Remove columns with zero variance
            zero_var_cols = [
                col for col in X_numeric.columns if X_numeric[col].var() == 0
            ]
            if zero_var_cols:
                print(f"Dropping zero variance numeric columns: {zero_var_cols}")
                X_numeric = X_numeric.drop(columns=zero_var_cols)
                numeric_columns = [
                    col for col in numeric_columns if col not in zero_var_cols
                ]

        # Process categorical columns
        X_categorical = pd.DataFrame()
        categorical_mappings = {}

        if categorical_columns:
            X_cat_processed = X_work[categorical_columns].copy()

            # Convert to string and handle missing values
            for col in categorical_columns:
                X_cat_processed[col] = X_cat_processed[col].astype(str)
                X_cat_processed[col] = X_cat_processed[col].replace("nan", "missing")
                X_cat_processed[col] = X_cat_processed[col].replace("None", "missing")

            # One-hot encode
            X_categorical = pd.get_dummies(
                X_cat_processed,
                columns=categorical_columns,
                drop_first=True,
                prefix_sep="_",
            )

            # Store mappings
            for col in categorical_columns:
                categories = X_cat_processed[col].unique()
                mapping = {}
                for cat in categories:
                    dummy_col = f"{col}_{cat}"
                    if dummy_col in X_categorical.columns:
                        mapping[dummy_col] = cat
                categorical_mappings[col] = mapping

        # Combine all features
        if not X_numeric.empty and not X_categorical.empty:
            X_processed = pd.concat([X_numeric, X_categorical], axis=1)
        elif not X_numeric.empty:
            X_processed = X_numeric
        elif not X_categorical.empty:
            X_processed = X_categorical
        else:
            raise ValueError("No valid features remaining after preprocessing")

        # Handle remaining edge cases
        X_processed = X_processed.loc[:, ~X_processed.columns.duplicated()]

        # Remove any remaining columns with NaN or infinite values
        problematic_cols = []
        for col in X_processed.columns:
            if X_processed[col].isnull().any() or np.isinf(X_processed[col]).any():
                problematic_cols.append(col)

        if problematic_cols:
            print(f"Dropping columns with remaining NaN/inf values: {problematic_cols}")
            X_processed = X_processed.drop(columns=problematic_cols)

        if X_processed.empty:
            raise ValueError("No features remaining after preprocessing")

        # Try statsmodels OLS first
        try:
            # Add constant for intercept
            X_with_const = sm.add_constant(X_processed)

            # Fit initial model
            model = sm.OLS(y_work, X_with_const)
            results = model.fit()

            print(
                f"Initial OLS model - R²: {results.rsquared:.4f}, Adj R²: {results.rsquared_adj:.4f}"
            )

            # Feature selection based on p-values
            significant_features = ["const"]
            for feature, pvalue in results.pvalues.items():
                if pvalue <= alpha:
                    significant_features.append(feature)

            # Ensure minimum features
            if len(significant_features) < 3:
                top_features = (
                    results.params.abs().sort_values(ascending=False).index[:6].tolist()
                )
                for feature in top_features:
                    if feature not in significant_features:
                        significant_features.append(feature)
                        if len(significant_features) >= 6:
                            break

            # Refit with significant features only
            assert isinstance(X_with_const, pd.DataFrame)
            if len(significant_features) < len(X_with_const.columns):
                X_significant = X_with_const[significant_features]
                final_model = sm.OLS(y_work, X_significant)
                final_results = final_model.fit()
            else:
                final_results = results
                X_significant = X_with_const

            print(
                f"Final OLS model - R²: {final_results.rsquared:.4f}, Adj R²: {final_results.rsquared_adj:.4f}"
            )

            # Build formula
            intercept = final_results.params["const"]
            formula_parts = [f"{intercept:.4f}"]

            for param_name, coef in final_results.params.items():
                if param_name == "const" or abs(coef) < 1e-6:
                    continue

                sign = "+" if coef >= 0 else ""
                original_category = get_original_category(
                    param_name, categorical_mappings, return_parts=True
                )

                if original_category != param_name and isinstance(
                    original_category, tuple
                ):
                    base_var, category = original_category
                    formula_parts.append(f"{sign}{coef:.4f} * {base_var}={category}")
                else:
                    formula_parts.append(f"{sign}{coef:.4f} * {param_name}")

            formula = " ".join(formula_parts)

            return {
                "r2": final_results.rsquared,
                "adj_r2": final_results.rsquared_adj,
                "formula": formula,
                "n_features": len(significant_features) - 1,  # Exclude const
                "method": "statsmodels_ols",
                "model": final_results,
                "categorical_mappings": categorical_mappings,
                "feature_columns": X_significant.columns.tolist(),
            }

        except Exception as e:
            print(f"OLS failed for {model_name}: {e}")
            print("Falling back to regularized linear regression...")

            # Fallback to sklearn
            try:
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(X_processed)
                X_scaled_df = pd.DataFrame(X_scaled, columns=X_processed.columns)

                lr = LinearRegression()
                lr.fit(X_scaled_df, y_work)

                r2 = lr.score(X_scaled_df, y_work)
                print(f"Linear Regression R²: {r2:.4f}")

                # Build formula (simplified)
                formula_parts = [f"{lr.intercept_:.4f}"]
                for i, (var, coef) in enumerate(zip(X_processed.columns, lr.coef_)):
                    if abs(coef) > 1e-6:
                        sign = "+" if coef >= 0 else ""
                        formula_parts.append(f"{sign}{coef:.4f} * {var}")

                formula = " ".join(formula_parts)

                return {
                    "r2": r2,
                    "adj_r2": None,  # Not available for sklearn
                    "formula": formula,
                    "n_features": len(X_processed.columns),
                    "method": "sklearn_linear_regression",
                    "model": lr,
                    "categorical_mappings": categorical_mappings,
                    "feature_columns": X_processed.columns.tolist(),
                    "scaler": scaler,
                }

            except Exception as e2:
                print(f"Linear regression also failed for {model_name}: {e2}")
                return {
                    "r2": 0.0,
                    "adj_r2": 0.0,
                    "formula": f"Error: Could not fit {model_name}",
                    "n_features": 0,
                    "method": "failed",
                    "model": None,
                    "categorical_mappings": {},
                    "feature_columns": [],
                }

    # Build both models
    xgb_model_result = build_single_linear_model(
        X_pd_aligned, y_pred_xgb_aligned, "XGBoost Linear Approximation"
    )

    if y_original is not None:
        original_model_result = build_single_linear_model(
            X_pd_aligned, y_original, "Direct Linear Model (Original Data)"
        )
    else:
        original_model_result = None

    # Print comparison
    print("\n" + "=" * 80)
    print("LINEAR MODEL COMPARISON")
    print("=" * 80)

    if original_model_result is not None:
        print(
            f"\n{'Metric':<25} {'Direct Linear':<20} {'XGBoost Approx':<20} {'Difference':<15}"
        )
        print("-" * 80)

        # R² comparison
        r2_direct = original_model_result["r2"]
        r2_xgb = xgb_model_result["r2"]
        r2_diff = r2_xgb - r2_direct
        print(f"{'R² Score':<25} {r2_direct:<20.4f} {r2_xgb:<20.4f} {r2_diff:<+15.4f}")

        # Adjusted R² comparison (if available)
        if (
            original_model_result["adj_r2"] is not None
            and xgb_model_result["adj_r2"] is not None
        ):
            adj_r2_direct = original_model_result["adj_r2"]
            adj_r2_xgb = xgb_model_result["adj_r2"]
            adj_r2_diff = adj_r2_xgb - adj_r2_direct
            print(
                f"{'Adjusted R²':<25} {adj_r2_direct:<20.4f} {adj_r2_xgb:<20.4f} {adj_r2_diff:<+15.4f}"
            )

        # Number of features
        n_feat_direct = original_model_result["n_features"]
        n_feat_xgb = xgb_model_result["n_features"]
        feat_diff = n_feat_xgb - n_feat_direct
        print(
            f"{'Number of Features':<25} {n_feat_direct:<20} {n_feat_xgb:<20} {feat_diff:<+15}"
        )

        # Method used
        print(
            f"{'Method Used':<25} {original_model_result['method']:<20} {xgb_model_result['method']:<20}"
        )

        print("\nDIRECT LINEAR MODEL FORMULA:")
        print(f"tiempo_actividad_segs = {original_model_result['formula']}")

        print("\nXGBOOST APPROXIMATION FORMULA:")
        print(f"tiempo_actividad_segs ≈ {xgb_model_result['formula']}")

        # Analysis
        print("\nANALYSIS:")
        if r2_xgb > r2_direct + 0.05:
            print(
                "• XGBoost approximation shows significantly better fit than direct linear model"
            )
            print("• This suggests XGBoost captured important non-linear relationships")
        elif r2_direct > r2_xgb + 0.05:
            print("• Direct linear model outperforms XGBoost approximation")
            print("• XGBoost may have overfitted or captured noise")
        else:
            print("• Both models show similar performance")
            print("• The relationship appears to be largely linear")

        if abs(feat_diff) > 2:
            if feat_diff > 0:
                print(
                    f"• XGBoost approximation uses {feat_diff} more features than direct model"
                )
            else:
                print(
                    f"• Direct model uses {abs(feat_diff)} more features than XGBoost approximation"
                )

    else:
        print(
            "Could not build direct linear model - original target data not available"
        )
        print("\nXGBOOST APPROXIMATION FORMULA:")
        print(f"tiempo_actividad_segs ≈ {xgb_model_result['formula']}")

    print("=" * 80)

    # Return the XGBoost approximation result in the original format for compatibility
    # Create coefficients DataFrame for XGBoost model
    if xgb_model_result["model"] is not None and hasattr(
        xgb_model_result["model"], "params"
    ):
        # statsmodels case
        model = xgb_model_result["model"]
        coef_df = pl.DataFrame(
            {
                "variable": model.params.index.tolist(),
                "coeficiente": model.params.values.tolist(),
                "p_valor": model.pvalues.values.tolist()
                if hasattr(model, "pvalues")
                else [None] * len(model.params),
                "error_estandar": model.bse.values.tolist()
                if hasattr(model, "bse")
                else [None] * len(model.params),
                "coef_abs": np.abs(model.params.values).tolist(),
            }
        )
    else:
        # sklearn case or failed case
        coef_df = pl.DataFrame(
            {
                "variable": ["const"],
                "coeficiente": [0.0],
                "p_valor": [None],
                "error_estandar": [None],
                "coef_abs": [0.0],
            }
        )

    # Calculate relative importance
    total_abs = coef_df.filter(pl.col("variable") != "const")["coef_abs"].sum()
    if total_abs > 0:
        coef_df = coef_df.with_columns(
            [
                pl.when(pl.col("variable") != "const")
                .then(pl.col("coef_abs") / total_abs * 100)
                .otherwise(0.0)
                .alias("importancia_porcentaje")
            ]
        )
    else:
        coef_df = coef_df.with_columns([pl.lit(0.0).alias("importancia_porcentaje")])

    # Add category information
    coef_df = coef_df.with_columns(
        [
            pl.col("variable")
            .map_elements(
                lambda var: get_original_category(
                    var, xgb_model_result["categorical_mappings"]
                ),
                return_dtype=pl.String,
            )
            .alias("variable_with_category")
        ]
    )

    # Sort by absolute coefficient
    coef_df = coef_df.sort("coef_abs", descending=True)

    return {
        "model": xgb_model_result["model"],
        "coefficients": coef_df,
        "formula": xgb_model_result["formula"],
        "significant_features": xgb_model_result["feature_columns"],
        "categorical_mappings": xgb_model_result["categorical_mappings"],
        "feature_columns": xgb_model_result["feature_columns"],
        "r2_linear_approx": xgb_model_result["r2"],
        "method": xgb_model_result["method"],
        "comparison_results": {
            "xgb_approximation": xgb_model_result,
            "direct_linear": original_model_result,
        },
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
    # Drop any dummy column that has only one unique value
    const_cols = [c for c in X_dummies.columns if X_dummies[c].nunique() <= 1]
    if const_cols:
        X_dummies = X_dummies.drop(columns=const_cols)

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


def identify_high_movement_time_factors(modelo_resultado, df, threshold_percentile=90):
    """
    Automatically identifies factors that explain higher movement times.

    Args:
        modelo_resultado: Dictionary with model results
        df: DataFrame with movement data
        threshold_percentile: Percentile to define "high" movement times

    Returns:
        Dictionary with factors that explain high movement times
    """
    print("\n=== FACTORS EXPLAINING HIGH MOVEMENT TIMES ===\n")

    # 1. Define what constitutes a "high" movement time
    high_time_threshold = df["tiempo_actividad_segs"].quantile(
        threshold_percentile / 100
    )
    print(
        f"High movement time threshold (P{threshold_percentile}): {high_time_threshold:.2f} seconds\n"
    )

    # 2. Get top features from XGBoost importance
    top_features = modelo_resultado["feature_importance"].head(10)
    print("Top 10 features by importance:")
    for row in top_features.to_dicts():
        print(f"  • {row['variable']}: {row['importancia_porcentaje']:.2f}%")

    # 3. Get top coefficients from linear approximation
    top_coefs = (
        modelo_resultado["linear_approximation"]
        .filter(pl.col("variable") != "const")
        .sort("coef_abs", descending=True)
        .head(10)
    )

    print("\nTop 10 factors by linear coefficient magnitude:")
    for row in top_coefs.to_dicts():
        sign = "+" if row["coeficiente"] > 0 else "-"
        print(
            f"  • {row['variable']}: {sign}{abs(row['coeficiente']):.4f} ({row['importancia_porcentaje']:.2f}%)"
        )

    # 4. Identify rules that lead to high movement times
    high_time_rules = (
        modelo_resultado["tree_rules"]
        .filter(pl.col("prediccion") > high_time_threshold)
        .sort("prediccion", descending=True)
    )

    if not high_time_rules.is_empty():
        print("\nRules that predict high movement times:")
        for row in high_time_rules.head(3).to_dicts():
            print(
                f"  • IF {row['condiciones']} THEN tiempo ≈ {row['prediccion']:.2f} seconds"
            )

    # 5. Analyze high movement time samples directly
    high_time_samples = df.filter(pl.col("tiempo_actividad_segs") > high_time_threshold)
    low_time_samples = df.filter(pl.col("tiempo_actividad_segs") <= high_time_threshold)

    # Compare averages for numeric features
    numeric_cols = [
        col
        for col in df.columns
        if df[col].dtype.is_numeric()
        and col != "tiempo_actividad_segs"
        and col in top_features["variable"].to_list()
    ]

    print("\nNumeric feature comparison (high vs. normal movement times):")
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

    print("\nCategorical feature comparison (high vs. normal movement times):")
    for col_name in categorical_cols[:5]:  # Limit to top 5 categorical features
        # Get value counts for high movement times
        high_counts = (
            high_time_samples.group_by(col_name)
            .agg(
                pl.count().alias("count"),
                pl.mean("tiempo_actividad_segs").alias("avg_time"),
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
                f"    - {category}: {row['percentage']:.1f}% of high-time movements, avg: {row['avg_time']:.2f}s"
            )

    # 6. Identify key combinations using decision tree rules
    print("\nKey combinations of factors leading to high movement times:")
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
            y = (df["tiempo_actividad_segs"] > high_time_threshold).to_pandas()

            # Train a simple decision tree
            dt = DecisionTreeClassifier(max_depth=3, random_state=42)
            dt.fit(X, y)

            # Extract rules
            tree_rules = export_text(dt, feature_names=X_cols)
            print(f"  Decision tree for high movement times:\n{tree_rules}")
        except Exception as e:
            print(f"  Could not create decision tree: {e}")

    print("\n=== END OF HIGH MOVEMENT TIME FACTORS ANALYSIS ===")

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


# ----------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------
# Main execution block (if __name__ == "__main__":)
# ----------------------------------------------------------------------------------
if __name__ == "__main__":
    from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_bi
    import joblib

    with pl.Config(
        set_tbl_cols=40, set_tbl_rows=20, set_fmt_str_lengths=100
    ) as cfg:  # Increased displayed cols
        days_run = 93
        use_cache = True
        fecha_desde = pdt.today().subtract(days=days_run)
        fecha_hasta = pdt.today().subtract(days=1)
        # fecha_desde = pdt.date(year=2025, month=5, day=12)
        # fecha_hasta = pdt.date(year=2025, month=5, day=24)
        sucursales = None
        # sucursales = [107, 137, 205, 214]

        # 1. Get raw data
        df_movements_raw = get_movements_data(
            dwh_farinter_bi,
            config=ConfigGetData(
                fecha_desde=fecha_desde,  # Using 31 days for more data
                use_cache=use_cache,  # Set to False to refresh cache if needed
                cache_max_age_seconds=3600 * 3600,
                cache_name=f"ut_mov_data_{days_run}",
                sucursales=sucursales,
            ),
        )

        df_transactions_raw = get_transactions_data(
            dwh_farinter_bi,
            config=ConfigGetData(
                fecha_desde=fecha_desde,
                use_cache=False,
                cache_max_age_seconds=3600 * 3600,
                cache_name=f"ut_trx_data_{days_run}",
                sucursales=sucursales,
            ),
        )

        df_marcador = get_marcador_data(
            dwh_farinter_bi,
            config=ConfigGetData(
                fecha_desde=fecha_desde,
                use_cache=use_cache,
                cache_max_age_seconds=3600 * 3600,
                cache_name=f"ut_marcador_mov_data_{days_run}",
                sucursales=sucursales,
            ),
        )

        print(
            f"Raw movements: {df_movements_raw.height}, Raw transactions: {df_transactions_raw.height}, Marcador: {df_marcador.height}"
        )
        if (
            df_movements_raw.height == 0
            or df_transactions_raw.height == 0
            or df_marcador.height == 0
        ):
            print("One of the initial dataframes is empty. Exiting.")
            exit()

        # 2. Perform initial transformations for movements and transactions separately
        df_movements_transformed = tranform_movements_data(df_movements_raw)
        df_transactions_transformed = tranform_transactions_data(df_transactions_raw)

        print(
            f"Transformed movements: {df_movements_transformed.dataframe.height}, Transformed transactions: {df_transactions_transformed.dataframe.height}"
        )
        if (
            df_movements_transformed.dataframe.height == 0
            or df_transactions_transformed.dataframe.height == 0
        ):
            print(
                "One of the transformed dataframes is empty after initial transform. Exiting."
            )
            exit()

        # 3. Merge the transformed data
        df_combined = merge_transactions_and_movements(
            df_movements_transformed, df_transactions_transformed
        )

        print(f"Combined data before final transform: {df_combined.dataframe.height}")
        if df_combined.dataframe.height == 0:
            print("Combined dataframe is empty before final transform. Exiting.")
            exit()

        # 4. Perform final transformations on the combined data
        dfm_final = transform_data(df_combined, df_marcador)
        df_final = dfm_final.dataframe.filter(pl.col("Tipo_Actividad") == "TRANSACCION")

        print(f"Final processed data: {df_final.height}")

        df_final.write_parquet(".cache/df_final.parquet")

        if df_final.height < 100:  # Arbitrary threshold for "too little data"
            print(
                "Final dataframe has very few rows. Analysis might not be meaningful."
            )
            print(df_final.head())
            if df_final.height == 0:
                print("Exiting due to empty final dataframe.")
                exit()
        else:
            print("Sample of final data:")
            print(
                df_final.sample(min(5, df_final.height)).select(
                    "Emp_Id",
                    "Suc_Id",
                    "Usuario_Id",
                    "Timestamp_Aplicado",
                    "Tipo_Actividad",
                    "cantidad_items",
                    "cantidad_total",
                    "tiempo_estimado_individual",
                    "tiempo_actividad_segs",
                    "Usuarios_Activos_En_Hora",
                )
            )

        # 5. Visualize data distribution
        # Select a subset of potentially interesting columns for visualization
        # These should exist in df_final after merging and transformations
        viz_columns = [
            "Tipo_Actividad",
            "Origen_Actividad",
            "Tipo_Nombre",
            "cantidad_items",
            "cantidad_total",
            "valor_total",
            "costo_unitario_prom",
            "precio_unitario_prom",
            "contiene_merma",
            "es_interbodega",
            "es_ajuste",
            "suma_inventario",
            "contiene_servicios",
            "contiene_farma",
            "es_tercera_edad",
            "acumula_monedero",
            "log_cantidad_items",
            "log_costo_unitario_prom",
            "log_cantidad_total_relativa",  # from movements
            "log_cantidad_productos",
            "log_precio_unitario_prom",
            "log_cantidad_unidades_relativa",  # from transactions
            "tiempo_estimado_individual",
            "Usuarios_Activos_En_Hora",
        ]
        # Filter to existing columns in df_final
        viz_columns_existing = [col for col in viz_columns if col in df_final.columns]

        visualize_data_distribution(
            df_final,
            target_column="tiempo_actividad_segs",
            feature_columns=viz_columns_existing,
            max_features=10,  # Increased max features for viz
        )

        # 6. Build and evaluate the model (using a subset of features for simplicity)
        # Ensure these features exist in df_final
        # This 'df_sensibles' selection needs to be robust to missing columns
        model_feature_candidates = []
        if (
            df_final["Tipo_Actividad"]
            .filter(df_final["Tipo_Actividad"] == "MOVIMIENTO")
            .len()
            > 0
        ):
            model_feature_candidates = [
                "Tipo_Actividad",  # Categorical: MOVIMIENTO or TRANSACCION
                "es_merma",
                "contiene_merma",
                "es_interbodega",
                "suma_inventario",
                "es_ajuste",
                "Tipo_Nombre"
                # "contiene_farma", # From transactions
                # "es_tercera_edad", # From transactions
                # "acumula_monedero", # From transactions
                "log_cantidad_items",  # Primarily from movements, might be null for transactions if not mapped
                "log_costo_unitario_prom",  # Primarily from movements
                "log_cantidad_total_relativa",  # Primarily from movements
                # Transaction specific log features (if they were not aliased to common names)
                # "log_cantidad_productos",
                # "log_precio_unitario_prom",
                # "log_cantidad_unidades_relativa",
                # "tiempo_estimado_individual" # The pre-calculated estimate
            ]
        if (
            df_final["Tipo_Actividad"]
            .filter(df_final["Tipo_Actividad"] == "TRANSACCION")
            .len()
            > 0
        ):
            model_feature_candidates += (
                "es_tercera_edad",
                # "cantidad_productos",
                # "valor_neto_qcut",
                # "cantidad_unidades_relativa",
                # "es_clinica",
                "es_asegurado",
                # "es_autoservicio",
                # "prop_autoservicio",
                "es_suc_autoservicio",
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

        df_sensibles_cols = ["tiempo_actividad_segs"]
        for col_name in model_feature_candidates:
            if col_name in df_final.columns:
                df_sensibles_cols.append(col_name)
            else:
                print(
                    f"Warning: Model feature candidate '{col_name}' not found in final DataFrame."
                )

        df_sensibles = df_final.select(df_sensibles_cols)

        # Fill NaNs for modeling (XGBoost can handle them, but explicit is often better)
        # For categorical (string) columns, fill with a placeholder like "missing"
        # For numeric, fill with 0 or median/mean.
        for col_name in df_sensibles.columns:
            if df_sensibles[col_name].dtype == pl.String:
                df_sensibles = df_sensibles.with_columns(
                    pl.col(col_name).fill_null("missing")
                )
            elif df_sensibles[col_name].dtype.is_numeric():
                df_sensibles = df_sensibles.with_columns(
                    pl.col(col_name).fill_null(0)
                )  # Simple fill with 0 for now

        # Columnas a excluir
        columnas_a_excluir = [
            "Emp_Id",
            "Suc_Id",
            "Bodega_Id",
            "Mov_Id",
            "Usuario_Id",
            "Mov_Fecha",
            "Mov_Fecha_Final",
            "Mov_Fecha_Aplicado",
            "Mov_Fecha_Recibido",
            "EmpSucUsr_Id",
            "tiempo_actividad_segs",
            "es_hora_pico",
            "max_movimientos_dia",
            "total_actividades_hora",
            "max_actividades_dia",
            "total_actividades_dia",
            "log_cantidad_items",
            "Caja_Id",
            "TipoDoc_id",
            "Factura_Id",
            "EmpSucDocCajFac_Id",
            "EmpSucCaj_Id",
            "Timestamp_Aplicado",
            "Fecha_Actividad",
            "hora_id",
            "Mov_Estado",
            "Mov_Interbodega",
            "Factura_FechaHora",
            "Usuarios_Activos_En_Hora",
            "Usuario_Nombre",
            "Tipo_Estadistica",
        ]

        if df_sensibles.height > 50:  # Need enough data for modeling
            print(
                "\n--- Construyendo modelo no lineal para tiempo de actividad (combinado) ---"
            )
            modelo_resultado = build_nonlinear_activity_time_model(
                df_sensibles, columnas_a_excluir
            )  # df_sensibles should be passed

            importancia = modelo_resultado["feature_importance"]
            print("\nImportancia de variables en el modelo XGBoost (combinado):")
            print(importancia.head(15))

            joblib.dump(
                modelo_resultado["model"],
                ".cache/modelo_xgboost_tiempo_actividad_combinado.pkl",
            )
            print(
                "Modelo combinado guardado en .cache/modelo_xgboost_tiempo_actividad_combinado.pkl"
            )

            try:
                import matplotlib.pyplot as plt

                top_features = importancia.head(15)
                plt.figure(figsize=(12, 10))  # Adjusted size
                plt.barh(
                    top_features["variable"].to_list()[::-1],
                    top_features["importancia_porcentaje"].to_list()[::-1],
                )
                plt.xlabel("Importancia (%)")
                plt.title("Importancia de Variables en Tiempo de Actividad (Combinado)")
                plt.tight_layout()
                plt.savefig(
                    ".cache/importancia_variables_tiempo_actividad_combinado.png"
                )
                print(
                    "Gráfico guardado en .cache/importancia_variables_tiempo_actividad_combinado.png"
                )
                plt.close()  # Close plot
            except Exception as e:
                print(f"No se pudo generar el gráfico de importancia: {e}")

            print("\n--- Aproximación lineal al modelo XGBoost (combinado) ---")
            print(modelo_resultado["linear_approximation"].head(10))
            print("\n--- Reglas extraídas del primer árbol (combinado) ---")
            print(modelo_resultado["tree_rules"].head(10))

            print_nonlinear_formula(modelo_resultado)

            high_time_factors = identify_high_movement_time_factors(  # Function name is specific, but concept applies
                modelo_resultado,
                df_final,
                threshold_percentile=90,  # Use df_final for context
            )
            # print("\nFactores con tiempo de actividad alto (combinado):") # Commented out to avoid large print
            # print(high_time_factors)
            print("High time factors analysis complete.")

            # 7. Correlation Analysis (optional, can be time-consuming)
            print("\n--- Iniciando análisis de correlación (puede tardar) ---")
            # Define columns to exclude for correlation analysis from df_final

            # Filter to existing columns
            correl_exclude_cols_existing = [
                col for col in columnas_a_excluir if col in df_final.columns
            ]

            resultados_correlacion = calcular_correlacion_variables(
                df_final,
                columna_objectivo="tiempo_actividad_segs",
                columnas_a_excluir=correl_exclude_cols_existing,
            )
            print("Resumen de Correlaciones (Top 10):")
            print(resultados_correlacion["resumen_correlaciones"].head(10))
            # print("Detalle de Variables (Sample):") # Can be very long
            # print(resultados_correlacion["detalle_variables"].head(20))

            resultados_correlacion["resumen_correlaciones"].write_excel(
                ".cache/resumen_correlaciones_combinado.xlsx"
            )
            resultados_correlacion["detalle_variables"].write_excel(
                ".cache/detalle_variables_combinado.xlsx"
            )
            print("Resultados de correlación guardados.")

        else:
            print(
                "Skipping model building and advanced analysis due to insufficient data after processing."
            )

        print(
            "\n--- Script execution finished ---"
        )  # ----------------------------------------------------------------------------------()
