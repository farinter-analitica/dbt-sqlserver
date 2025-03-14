import gc
import math
import os
import time
from collections import deque
from collections.abc import Sequence
from datetime import date, datetime
from typing import List, Optional, Tuple, Union

import numpy as np
import polars as pl
from statstools_gf.interpolate import interpolate_auto
from statstools_gf.ts_outliers.ts_outliers_simple import (
    assert_is_number,
    detect_ts_outliers,
)
from statstools_gf.utils import logging, setup_logging

logger = setup_logging(
    level=logging.DEBUG if __name__ == "__main__" else logging.ERROR, name=__name__
)
pl.Config().set_tbl_rows(100)

# Algoritmo llamado modelado y depuracion de la demanda mediante estimacion mddme

# =============================================================================
# Funciones principales (traducción de R a Python)
# =============================================================================

# def detect_ts_outliers(df_in: np.ndarray, *args):
#         q1 = np.percentile(df_in,25)
#         q2 = np.percentile(df_in,50)
#         q3 = np.percentile(df_in,75)
#         iqr = q3 -q1
#         fence_low  = q1 -1.5 * max((iqr,q2*0.1))
#         fence_high = q3 +1.5 * max((iqr,q2*0.1))
#         index = np.where((df_in > fence_low) & (df_in < fence_high))
#         return {"index": index}


def marginalizacion_demanda(
    demanda_original: Union[np.ndarray, List[float]],
    stock_acum: Union[np.ndarray, List[float]],
    demanda_sku: Union[np.ndarray, List[float]],
    dias_sin_stock: Union[np.ndarray, List[float]],
) -> dict:
    """
    Realiza la marginalización de la demanda (DC) según condiciones en SA, VA y Dss.
    Se calcula una cota inferior y se reemplazan valores que no la cumplen.
    El resultado del proceso es la demanda considerada probablemente valida.
    """
    # Convert inputs to numpy arrays
    demanda_original = np.asarray(demanda_original, dtype=float)
    stock_acum = np.asarray(stock_acum, dtype=float)
    demanda_sku = np.asarray(demanda_sku, dtype=float)
    dias_sin_stock = np.asarray(dias_sin_stock, dtype=float)
    n = len(demanda_original)

    # Calcular el lag de SA (primer elemento será NaN)
    SA_ant = np.pad(stock_acum[:-1], (1, 0), "constant", constant_values=np.nan)

    valid_idx = stock_acum != 0
    if np.sum(valid_idx) > 0:
        p15VA = max(1.0, float(np.percentile(demanda_sku[valid_idx], 15.0)))
    else:
        p15VA = 1.0

    # Find indices where conditions are met
    R = np.where((stock_acum >= p15VA) & (dias_sin_stock <= 15))[0]

    # Percentiles que incrementan la cota inferior pero están censurados
    nonzero_DC = demanda_original != 0
    if np.sum(nonzero_DC) > 0:
        p25cen = np.percentile(demanda_original[nonzero_DC], 25)
        p75cen = np.percentile(demanda_original[nonzero_DC], 75)
    else:
        p25cen = p75cen = 0

    # Incluir el primer elemento (índice 0)
    R = np.union1d(np.array([0]), R)
    R = np.union1d(
        R, np.where((demanda_original > p25cen) & (demanda_original < p75cen))[0]
    )

    if len(R) > 0:
        p20DC = max(1.0, float(np.percentile(demanda_original[R], 20.0)))
        p50DC = max(1.0, float(np.percentile(demanda_original[R], 50.0)))
    else:
        p20DC = p50DC = 1

    # NOTA:______________________________________________________________________________________________________________________
    # Condición p20DC/p50DC <= 1/10, dice que la mediana es 10 veces mas grande que el percentil 20%
    # Condiciones en las que (length(which(SA <= 1 ))/n) < 0.5 y (length(which(SA <= 1))/n) > 0.1, se piden ya que
    # como se buscan en las posiciones preliminarmente ya seleccionadas, se puede permitir que como mucho el 50% de los datos
    # (lo cual es bastante) Sean menor o igual a 1, ya que en el caso en el que la SA = 1 sea un dato no quiebre de stock
    # el percentil que está basado en los seleccionados, no estará por encima
    cheq = np.sum(stock_acum <= p20DC) / n

    if (p20DC / p50DC <= 1 / 10) and (cheq < 0.5):
        count_le = np.sum(stock_acum <= p20DC)
        quantile_val = ((count_le + 1) / n) * 100
        new_percentil = np.percentile(demanda_original[R], quantile_val)
        if new_percentil > p20DC:
            p20DC = new_percentil

    if len(R) / n >= 0.5:
        inter = np.intersect1d(demanda_original[R], demanda_original[nonzero_DC])
        if len(inter) == 0:
            venta_minima_esperada = (
                np.min(demanda_original[nonzero_DC]) if np.sum(nonzero_DC) > 0 else 0
            )
        else:
            venta_minima_esperada = max(
                np.min(inter), np.min(demanda_original[nonzero_DC])
            )
    else:
        venta_minima_esperada = (
            np.min(demanda_original[nonzero_DC]) if np.sum(nonzero_DC) > 0 else 0
        )

    venta_minima_esperada = max(float(p20DC), float(venta_minima_esperada))

    Final = np.where(
        (stock_acum >= venta_minima_esperada)
        & (dias_sin_stock <= 15)
        & (SA_ant >= venta_minima_esperada)
    )[0]
    if len(Final) > 0:
        p50DC = max(1.0, float(np.percentile(demanda_original[Final], 50.0)))
        p25DC = max(1.0, float(np.percentile(demanda_original[Final], 25.0)))
    else:
        p50DC = p25DC = 1

    # Condición en la que el percentil 25 es demasiado cercano respecto a la mediana
    if p50DC != 0 and (p25DC / p50DC) >= 0.85:
        p25DC = math.ceil(p50DC * 0.7)

    ADD = np.where(demanda_original >= p25DC)[0]
    Final = np.union1d(Final, ADD)
    Final = np.union1d(np.array([0]), Final)
    Final = np.sort(Final).astype(int)

    # Create output array with NaN where values should be filtered out
    demanda_valida = demanda_original.copy()
    mask = np.ones(n, dtype=bool)
    mask[Final] = False
    demanda_valida[mask] = np.nan

    return {"D": demanda_valida, "cota_inf": venta_minima_esperada}


def detectar_rachas_na(
    serie: Union[np.ndarray, Sequence[float]], l_racha: int
) -> deque[np.ndarray]:
    """
    Detects consecutive sequences (rachas) of NA (NaN) values in 'serie'.

    A sequence is considered valid if it has a length greater than or equal to 'l_racha'.
    The function returns a deque with each element being a numpy array of the indices
    in 'serie' that form a long consecutive NA sequence.

    Parameters:
        serie (Union[np.ndarray, Sequence[float]]): The input numeric series.
        l_racha (int): The minimum number of consecutive NaN values required.

    Returns:
        deque[np.ndarray]: A deque of numpy arrays containing the indices for each valid racha.
        If the input series is empty, returns an empty deque with maxlen=0.
    """
    # Convert the input to a NumPy array of floats.
    serie = np.asarray(serie, dtype=float)

    # Handle empty series gracefully.
    if serie.size == 0:
        return deque(maxlen=0)

    # Identify the indices where NaN values occur.
    pos_na = np.where(np.isnan(serie))[0]

    # Not enough NaNs to meet the minimum length requirement.
    if pos_na.size < l_racha:
        return deque(maxlen=0)

    result: deque[np.ndarray] = deque()
    start = 0  # Start index for the current run in pos_na.

    # Iterate through the positions to group consecutive indices.
    for i in range(1, len(pos_na)):
        # Check if the current index is not consecutive to the previous one.
        if pos_na[i] != pos_na[i - 1] + 1:
            # If the current group length meets the minimum requirement, add it.
            if i - start >= l_racha:
                result.append(pos_na[start:i])
            start = i  # Reset start for the next group.

    # Check the final group after the loop ends.
    if len(pos_na) - start >= l_racha:
        result.append(pos_na[start:])

    return result


def encontrar_hills_idx(
    demanda_original: Union[np.ndarray, List[float]],
    demanda_valida: Union[np.ndarray, List[float]],
    atp_est_idx: Union[np.ndarray, List[int]],
) -> np.ndarray:
    """
    Identifica "hills" (picos) en la serie D basándose en DC, excluyendo los índices
    presentes en 'atp'.
    """
    logger.debug(
        f"Entran find_hills con {demanda_original=}, {demanda_valida=}, {atp_est_idx=}"
    )
    # Convert all inputs to numpy arrays
    demanda_original = np.asarray(demanda_original, dtype=float)
    demanda_valida = np.asarray(demanda_valida, dtype=float)
    atp_est_idx = np.asarray(atp_est_idx, dtype=int)

    # Calculate threshold and find points above it
    threshold = max(1.0, float(np.nanpercentile(demanda_valida, 50.0)))
    p50 = np.where(demanda_valida > threshold)[0]

    # Find NA values in D
    NAS = np.where(np.isnan(demanda_valida))[0]

    # Find neighbors of NA values
    neighbors_before = NAS - 1
    neighbors_after = NAS + 1

    # Filter valid indices (prevent out of bounds)
    neighbors_before = neighbors_before[neighbors_before >= 0]
    neighbors_after = neighbors_after[neighbors_after < len(demanda_valida)]

    # Combine and get unique neighbors
    neighbors = np.union1d(neighbors_before, neighbors_after)

    # Find the intersection of neighbors and p50
    hills_idx = np.intersect1d(neighbors, p50)

    # Exclude atp and t_list indices
    if len(atp_est_idx) > 0:
        hills_idx = np.setdiff1d(hills_idx, atp_est_idx)

    logger.debug(f"Salen find_hills con hills: {hills_idx}")
    # Return sorted hills
    return np.sort(hills_idx)


def atp_ests_idx(
    atp_idx: Union[np.ndarray, List[int]],
    D: Union[np.ndarray, List[float]],
    li: int = 10,
    ls: int = 14,
) -> np.ndarray:
    """
    Devuelve los índices de atípicos estacionales, es decir, aquellos en 'atp'
    que cumplen condiciones basadas en la diferencia (entre 10 y 14 unidades).
    """
    # Convert to numpy array
    atp_idx = np.asarray(atp_idx, dtype=np.int32)
    logger.debug(f"atp est entra: {atp_idx}")

    if len(atp_idx) == 0:
        return np.array([], dtype=np.int32)

    # Generate arrays of indices offset by 10-14 periods (forward and backward)
    pmo = np.add.outer(atp_idx, np.arange(10, 15)).flatten()
    plo = np.add.outer(atp_idx, -np.arange(10, 15)).flatten()

    # Find which values in atp are also in either pmo or plo
    mask = np.isin(atp_idx, pmo) | np.isin(atp_idx, plo)

    # Filter and return sorted unique values
    atp_est_val = np.sort(np.unique(atp_idx[mask]))
    logger.debug(f"atp est sale: {atp_est_val}")

    return atp_est_val


def obtener_atp(
    DC: Union[np.ndarray, List[float]],
    D: Union[np.ndarray, List[float]],
    start: Tuple[int, int],
) -> np.ndarray:
    """
    Obtiene los índices de los valores atípicos altos comparando la serie DC con D.
    """
    # Convert inputs to numpy arrays
    DC_np = np.asarray(DC, dtype=float)
    D_np = np.asarray(D, dtype=float)

    # Find indexes where D is not null and above 75th percentile
    D_notna = ~np.isnan(D_np)
    if np.sum(D_notna) > 0:
        p75D = np.percentile(D_np[D_notna], 75)
        R = np.where((D_notna) & (D_np > p75D))[0]
    else:
        R = np.array([], dtype=int)

    # Use detect_ts_outliers function, but we need to convert to Polars Series for it
    # since we're relying on the existing implementation
    outliers, _ = detect_ts_outliers(
        ts=pl.Series(values=DC_np), variability_threshold=2.5
    )

    # Convert back to numpy array if needed
    outliers_np = np.array(outliers, dtype=int)

    # Find intersection of outliers and R
    atp = np.intersect1d(outliers_np, R)

    return atp


def calcular_decaimiento(
    ts: Union[np.ndarray, List[float]],
    calcular_idx: Union[np.ndarray, List[int]],
    factor_p: float,
    exclude_idx: Union[np.ndarray, List[int]],
    max_samples: int = 8,
) -> np.ndarray:
    """
    Calcula el decaimiento en una racha (conjunto 'r'), usando la media de los valores
    excluyendo los índices en 'calcular_idx' y 'exclude_idx'.

    Parameters:
    -----------
    ts : array-like
        Serie temporal original con valores numéricos.
    calcular_idx : array-like
        Índices de la serie donde se calculará el decaimiento (usualmente rachas de NaN).
    factor_p : float
        Factor de probabilidad que controla la velocidad del decaimiento (0 < factor_p < 1).
    exclude_idx : array-like
        Índices adicionales a excluir del cálculo de la media (típicamente atípicos).
    max_samples : int, default=8
        Número máximo de muestras de decaimiento a generar, independientemente
        de la longitud de 'calcular_idx'.

    Returns:
    --------
    np.ndarray:
        Vector con los valores de decaimiento calculados para los índices en 'calcular_idx'.
        La longitud del vector será como máximo 'max_samples'.

    Notes:
    ------
    El algoritmo:
    1. Limita el decaimiento a max_samples o la longitud de 'calcular_idx', lo que sea menor
    2. Calcula un valor p_val que determinará el punto final del decaimiento
    3. Calcula la media de referencia excluyendo los índices de 'calcular_idx' y 'exclude_idx'
    4. Genera una secuencia lineal decreciente desde 1 hasta p_val
    5. Multiplica esta secuencia por la media para obtener los valores de decaimiento
    6. Si sobran posiciones de calcular_idx rellena con ceros.
    """
    # Convert inputs to numpy arrays
    ts = np.asarray(ts, dtype=float)
    calcular_idx = np.asarray(calcular_idx, dtype=int)
    exclude_idx = np.asarray(exclude_idx, dtype=int)

    if len(calcular_idx) == 0:
        return np.array([])

    factor_p = min(1.0, max(0.0, factor_p))

    # Limit decay length to max_samples or length of r, whichever is smaller
    # This ensures we don't exceed the requested max decay length
    l_dec = min(len(calcular_idx), max_samples)

    # Calculate the terminal decay value (p_val)
    # Use factor_p^l_dec or 1/l_dec, whichever is larger (factor_p^l_dec gets very small for long sequences)
    # This ensures the decay reaches a reasonably small value by the end
    p_val = max(factor_p**l_dec, 1 / l_dec)

    # Create mask of indices to exclude (both racha indices and atypical indices)
    exclude = np.union1d(exclude_idx, calcular_idx)

    # Find indices to include for mean calculation
    include = np.setdiff1d(np.arange(len(ts)), exclude)

    # Calculate mean of valid values (values not in the racha or atypical)
    # This becomes our starting point for the decay
    m = np.nanmean(ts[include]) if len(include) > 0 else 0

    # Create linear decay sequence from 1 to p_val
    # 1 means "100% of the mean" at the start
    # p_val means "p_val * 100% of the mean" at the end
    seq1 = np.linspace(1, p_val, l_dec)

    # Calculate final decay values by multiplying mean by decay factors
    final = np.append(m * seq1, np.zeros(len(calcular_idx) - l_dec))

    # Round to nearest integer (since we're typically dealing with counts)
    return np.round(final, decimals=1)


def rpd(
    demanda_valida: Union[np.ndarray, List[float]],
    start: Tuple[int, int],
    demanda_original: Union[np.ndarray, List[float]],
    umbral_min: float,
    atp_idx: Union[np.ndarray, List[int]],
    q60_no_na: float,
    hills_idx: Union[np.ndarray, List[int]],
    atp_ests_vals_idx: Optional[Union[np.ndarray, List[int]]] = None,
    decaimiento: bool = False,
    only_rachas: bool = False,
) -> np.ndarray:
    """
    Función que limpia y ajusta la serie D (a partir de DC) para corregir outliers,
    hills, atípicos y detectar rachas. Se aplican múltiples reglas según el umbral y
    la cantidad de datos. RPD = Reemplazo y depuracion de la demanda

    reemplazar y depurar demanda
    TODO: Esto deberia hacerlo paso a paso en columnas diferentes del dataframe en modo debug
    """
    # Convert all inputs to numpy arrays
    demanda_valida = np.asarray(demanda_valida, dtype=float, copy=True)
    demanda_original = np.asarray(demanda_original, dtype=float)
    atp_idx = np.asarray(atp_idx, dtype=int).astype(np.int64)
    hills_idx = np.asarray(hills_idx, dtype=int).astype(np.int64)

    if atp_ests_vals_idx is None:
        atp_ests_vals_idx = np.array([], dtype=int)
    else:
        atp_ests_vals_idx = np.asarray(atp_ests_vals_idx, dtype=int)

    series_n = len(demanda_valida)
    logger.debug(f"Entra rpd: {demanda_valida=}")

    # Find positions with NaN values and valid values
    na_idx = np.where(np.isnan(demanda_valida))[0]
    valid_idx = np.where(~np.isnan(demanda_valida))[0]
    factor_disp = float(
        np.sum((~np.isnan(demanda_valida)) & (demanda_valida > 0.0)) / len(valid_idx)
        if len(valid_idx) > 0
        else 0
    )

    umbral_val = 1 - umbral_min
    if series_n <= 6:
        umbral_val = 1 - (2 / series_n)

    # Calculate median of non-NaN values
    q50_no_na = float(np.nanmedian(demanda_valida))
    aux_na_idx = na_idx.copy()

    if (len(na_idx) / series_n <= umbral_val and q50_no_na > 0) or only_rachas:
        rachas_na_list = detectar_rachas_na(demanda_valida, l_racha=5)

        if only_rachas:
            if rachas_na_list:
                # Combine all racha indices into one array
                rachas = np.array([], dtype=int)
                for racha in rachas_na_list:
                    rachas = np.union1d(rachas, racha)
                return rachas
            else:
                return np.array([], dtype=int)

        atp_idx = np.setdiff1d(atp_idx, atp_ests_vals_idx)
        # logger.debug(f"ATP np.setdiff1d(atp, hill_y_atp): {atp}")

        # Identify elements that are not hills or NaN
        el_no_hills = np.setdiff1d(np.arange(series_n), np.union1d(na_idx, hills_idx))
        hill_y_atp = np.intersect1d(hills_idx, atp_idx)
        atp_idx = np.setdiff1d(atp_idx, hill_y_atp)

        # _____________________________HILLS______________________________
        atp_idx, aux_na_idx = procesar_hills(
            demanda_valida,
            demanda_original,
            atp_idx,
            hills_idx,
            series_n,
            q50_no_na,
            aux_na_idx,
            el_no_hills,
            hill_y_atp,
        )

        # _________________________Atipicos________________________________
        aux_na_idx, considerar_atp = procesar_atipicos(
            demanda_valida,
            demanda_original,
            atp_idx,
            hills_idx,
            atp_ests_vals_idx,
            series_n,
            aux_na_idx,
        )
        # _______________Se detectan rachas sin extremos hills ni atipicos ___________________________

        demanda_valida = procesar_rachas(
            demanda_valida,
            demanda_original,
            atp_ests_vals_idx,
            decaimiento,
            series_n,
            factor_disp,
            rachas_na_list,
        )
        demanda_valida_cp: np.ndarray = demanda_valida.copy()

        # Calculate percentiles for non-NaN percentil 75
        non_nan_vals = demanda_valida_cp[~np.isnan(demanda_valida_cp)]
        if len(non_nan_vals) > 0:
            q75_no_na = float(np.percentile(non_nan_vals, 75))
        else:
            q75_no_na = 0.0

        logger.debug(f"previo interpolar atps: {demanda_valida_cp=}")
        if len(atp_ests_vals_idx) > 0:
            # Reducir outliers
            demanda_valida_cp[atp_ests_vals_idx] = np.clip(
                demanda_valida[atp_ests_vals_idx], 0, q75_no_na
            )
            demanda_valida_cp = interpolate_auto(demanda_valida_cp).to_numpy()
            demanda_valida_cp = np.round(np.clip(demanda_valida_cp, 0, np.inf), 1)
            # Devolver los outliers
            demanda_valida_cp[atp_ests_vals_idx] = np.clip(
                demanda_valida[atp_ests_vals_idx], 0, np.percentile(non_nan_vals, 99)
            )
        else:
            demanda_valida_cp = interpolate_auto(demanda_valida_cp).to_numpy()
            demanda_valida_cp = np.round(np.clip(demanda_valida_cp, 0, np.inf), 1)

        logger.debug(f"post interpolar: {demanda_valida_cp=}")
        # Control de la tendencia de cola derecha
        # No generar tendencias o estacionalidades inexistentes
        # (Solo disponible para historicos de mas de 10 unidades)
        demanda_valida_cp = control_tendencia_derecha(
            demanda_valida,
            demanda_original,
            atp_ests_vals_idx,
            series_n,
            aux_na_idx,
            considerar_atp,
            demanda_valida_cp,
            q75_no_na,
        )

        if np.all(demanda_valida_cp == 0):
            demanda_valida_cp = demanda_original

        return np.clip(np.round(demanda_valida_cp, 1), 0, np.inf)
    else:
        return demanda_original


def control_tendencia_derecha(
    demanda_valida: np.ndarray,
    demanda_original: np.ndarray,
    atp_ests_vals_idx: np.ndarray,
    series_n: int,
    aux_na_idx: np.ndarray,
    considerar_atp: np.ndarray,
    demanda_valida_cp: np.ndarray,
    q75_no_na: float,
) -> np.ndarray:
    """
    Controls right-tail trend in demand data by applying various constraints and adjustments.

    Args:
        demanda_valida: Array of validated demand values
        demanda_original: Array of original demand values
        atp_ests_vals_idx: Array of ATP estimated value indices
        series_n: Length of the time series
        aux_na_idx: Array of indices for NA values
        considerar_atp: Array of ATP values to consider
        demanda_valida_cp: Copy of validated demand array to modify
        q75_no_na: 75th percentile of non-NA values

    Returns:
        Modified copy of demand array with controlled right-tail trend
    """
    for n_index in aux_na_idx:
        if series_n >= 10 and n_index > math.floor((9 / 10) * series_n):
            # Ventana movil
            window_start = max(0, n_index - 6)
            window_end = min(series_n, n_index + 7)
            window_indices = np.concatenate(
                [
                    np.arange(window_start, n_index),
                    np.arange(n_index + 1, window_end),
                ]
            )
            vm = demanda_valida[window_indices]
            vm = vm[~np.isnan(vm)]

            if len(vm) > 1:
                q75vm = np.percentile(vm, 75)
            else:
                q75vm = q75_no_na

            ret_val_at_idx = (
                np.inf
                if np.isnan(demanda_valida_cp[n_index])
                else demanda_valida_cp[n_index]
            )
            if ret_val_at_idx > q75_no_na and ret_val_at_idx > q75vm:
                demanda_valida_cp[n_index] = max(float(q75_no_na), float(q75vm))

        d_not_null_min = np.nanmin(demanda_valida)

        ret_val_at_idx = (
            0.0 if np.isnan(demanda_valida_cp[n_index]) else demanda_valida_cp[n_index]
        )
        if ret_val_at_idx < d_not_null_min and d_not_null_min < np.nanpercentile(
            demanda_valida_cp, 50
        ):
            demanda_valida_cp[n_index] = d_not_null_min

        x_val = 2 + len(considerar_atp) + len(atp_ests_vals_idx)
        cota_maxima_reemplazo = max(
            float(np.nanpercentile(demanda_valida_cp, 100 * (1 - x_val / series_n))),
            float(np.nanmedian(demanda_valida_cp)),
        )

        if (
            series_n >= 12
            and not np.isnan(demanda_valida_cp[n_index])
            and demanda_valida_cp[n_index] > cota_maxima_reemplazo
        ):
            demanda_valida_cp[n_index] = min(
                float(cota_maxima_reemplazo), float(demanda_valida_cp[n_index])
            )

        if demanda_original[n_index] > demanda_valida_cp[n_index]:
            demanda_valida_cp[n_index] = demanda_original[n_index]

    return demanda_valida_cp


def procesar_rachas(
    demanda_valida: np.ndarray,
    demanda_original: np.ndarray,
    atp_ests_vals_idx: np.ndarray,
    decaimiento: bool,
    series_n: int,
    factor_disp: float,
    rachas_na_list: deque[np.ndarray] | None,
) -> np.ndarray:
    """
    Process streaks of missing values in demand data.

    Args:
        demanda_valida: Array of validated demand values
        demanda_original: Array of original demand values
        atp_ests_vals_idx: Array of ATP estimated value indices
        decaimiento: Boolean indicating whether to apply decay calculation
        series_n: Length of the time series
        factor_disp: Dispersion factor for decay calculation
        rachas_na_list: deque of arrays containing indices of NA streaks

    Returns:
        Array of processed demand values with streaks handled
    """
    logger.debug(f"previo rachas: {demanda_valida=}")
    if rachas_na_list is not None and len(rachas_na_list) > 0:
        for racha in rachas_na_list:
            extremos_idx = [racha[0], racha[-1]]
            if extremos_idx[1] < series_n - 1 and np.any(
                demanda_original[extremos_idx[1] :] != 0
            ):
                if extremos_idx[1] - extremos_idx[0] > 1:
                    idx_range = range(extremos_idx[0] + 1, extremos_idx[1])
                    demanda_valida[idx_range] = np.nanmedian(demanda_valida)
            elif extremos_idx[1] == series_n - 1 or np.all(
                demanda_original[extremos_idx[1] :] == 0
            ):
                if decaimiento:
                    demanda_valida[racha] = calcular_decaimiento(
                        demanda_valida, racha, factor_disp, atp_ests_vals_idx
                    )
                else:
                    demanda_valida[racha] = demanda_original[racha]

    logger.debug(f"post rachas: {demanda_valida=}")
    return demanda_valida


def procesar_atipicos(
    demanda_valida: np.ndarray,
    demanda_original: np.ndarray,
    atp_idx: np.ndarray,
    hills_idx: np.ndarray,
    atp_ests_vals_idx: np.ndarray,
    series_n: int,
    aux_na_idx: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Process atypical values in demand data by smoothing them using neighboring points.

    Args:
        demanda_valida: Array of validated demand values
        demanda_original: Array of original demand values
        atp_idx: Array of outliers indices
        hills_idx: Array of indices identified as hills
        atp_ests_vals_idx: Array of ATP estimated value indices
        series_n: Length of the time series
        aux_na_idx: Array of auxiliary NA indices

    Returns:
        tuple containing:
            - Updated auxiliary NA indices array
            - Array of ATP indices to consider
    """
    considerar_atp = np.array([], dtype=int)
    if len(atp_idx) > 0 and series_n > 3:
        q80_aux = round(np.nanpercentile(demanda_valida, 80), 1)
        delete = np.union1d(atp_idx, atp_ests_vals_idx)
        remaining = np.setdiff1d(np.arange(series_n), delete)
        max_cot_atp = np.nanmax(demanda_valida[remaining]) if len(remaining) > 0 else 0
        q50_aux = math.ceil(np.nanmedian(demanda_valida))
        demanda_valida_cp = demanda_valida.copy()

        logger.debug(f"D incial atipicos: {demanda_valida}")
        for index_atp in atp_idx:
            if index_atp == 0:
                vecinos_idx = np.array([index_atp + 1, index_atp + 2])
            elif index_atp == series_n - 1:
                vecinos_idx = np.array([index_atp - 2, index_atp - 1])
            else:
                vecinos_idx = np.array([index_atp - 1, index_atp + 1])

            logger.debug(
                f"vecinos_idx: {vecinos_idx}, index_atp: {index_atp}, n: {series_n}"
            )
            pos_set = np.union1d(vecinos_idx, np.array([index_atp]))
            vecinos_idx = np.setdiff1d(vecinos_idx, hills_idx)

            # In Polars: cond = aux.is_null() | (aux < q50_aux)
            cond = np.isnan(demanda_valida_cp) | (demanda_valida_cp < q50_aux)
            vecinos_idx = np.intersect1d(vecinos_idx, np.where(cond)[0])
            vecinos_idx = np.union1d(vecinos_idx, np.array([index_atp]))

            vecinos_idx = vecinos_idx.astype(np.int32)
            index_atp = int(index_atp)
            if len(vecinos_idx) <= 1:
                if index_atp < series_n - 2:
                    candidate = max(
                        np.nanmean(demanda_original[pos_set]),
                        np.nansum(demanda_valida[pos_set]) / len(pos_set)
                        if len(pos_set) > 0
                        else 0,
                    )
                    demanda_valida[index_atp] = min(candidate, max_cot_atp)
                if demanda_valida[index_atp] > q80_aux:
                    considerar_atp = np.union1d(considerar_atp, [index_atp])
            else:
                val = max(
                    np.nanmean(demanda_original[vecinos_idx]),
                    np.nansum(demanda_valida[vecinos_idx]) / len(vecinos_idx)
                    if len(vecinos_idx) > 0
                    else 0,
                )
                demanda_valida[vecinos_idx] = val
                if demanda_valida[index_atp] > max_cot_atp and index_atp < series_n - 1:
                    demanda_valida[vecinos_idx] = max_cot_atp
                if demanda_valida[index_atp] > q80_aux:
                    considerar_atp = np.union1d(considerar_atp, vecinos_idx)
                aux_na_idx = np.setdiff1d(aux_na_idx, vecinos_idx)

    logger.debug(f"D post Atipicos: {demanda_valida}")
    return aux_na_idx, considerar_atp


def procesar_hills(
    demanda_valida: np.ndarray,
    demanda_original: np.ndarray,
    atp_idx: np.ndarray,
    hills_idx: np.ndarray,
    series_n: int,
    q50_no_na: float,
    aux_na_idx: np.ndarray,
    el_no_hills: np.ndarray,
    hill_y_atp: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Process hills in demand data by smoothing values using neighboring points.

    Args:
        demanda_valida: Array of validated demand values
        demanda_original: Array of original demand values
        atp_idx: Array of outliers indices
        hills_idx: Array of indices identified as hills
        series_n: Length of the time series
        q50_no_na: 50th percentile of non-NA values
        aux_na_idx: Array of auxiliary NA indices
        el_no_hills: Array of indices not identified as hills
        hill_y_atp: Array of indices that are both hills and ATP

    Returns:
        tuple containing:
            - Updated ATP indices array
            - Updated auxiliary NA indices array
    """
    if len(hills_idx) > 0:
        for h in hills_idx:
            val = 0
            if h in hill_y_atp:
                vecinos_idx_candidates = np.array([h - 2, h - 1, h + 1, h + 2])
                # Keep only valid indices within range
                vecinos_idx_candidates = vecinos_idx_candidates[
                    (vecinos_idx_candidates >= 0) & (vecinos_idx_candidates < series_n)
                ]
                vecinos_idx = np.union1d(
                    np.intersect1d(vecinos_idx_candidates, aux_na_idx),
                    np.array([h]),
                )

                if len(vecinos_idx) > 1:
                    if (len(el_no_hills) / (series_n - len(aux_na_idx))) > 0.5:
                        val = max(
                            float(np.nanmean(demanda_original[vecinos_idx])),
                            float(
                                np.nansum(demanda_valida[vecinos_idx])
                                / len(vecinos_idx)
                            )
                            if len(vecinos_idx) > 0
                            else 0.0,
                            float(np.nanpercentile(demanda_original[el_no_hills], 50.0))
                            if len(el_no_hills) > 0
                            else 0.0,
                        )
                    else:
                        val = max(
                            float(np.nanmean(demanda_original[vecinos_idx])),
                            np.nansum(demanda_valida[vecinos_idx]) / len(vecinos_idx)
                            if len(vecinos_idx) > 0
                            else 0,
                            q50_no_na,
                        )
                    demanda_valida[vecinos_idx] = val
                else:
                    atp_idx = np.union1d(atp_idx, np.array([h]))
            else:
                vecinos_idx_candidates = np.array([h - 1, h + 1])
                vecinos_idx = np.union1d(
                    np.intersect1d(vecinos_idx_candidates, aux_na_idx),
                    np.array([h]),
                )

                if len(vecinos_idx) > 1:
                    if (len(el_no_hills) / (series_n - len(aux_na_idx))) > 0.5:
                        val = max(
                            float(
                                np.nansum(demanda_original[vecinos_idx])
                                / len(vecinos_idx)
                            ),
                            float(
                                np.nansum(demanda_valida[vecinos_idx])
                                / len(vecinos_idx)
                            ),
                            float(np.nanpercentile(demanda_original[el_no_hills], 50.0))
                            if len(el_no_hills) > 0
                            else 0.0,
                        )
                    else:
                        val = max(
                            float(
                                np.nansum(demanda_original[vecinos_idx])
                                / len(vecinos_idx)
                            ),
                            float(
                                np.nansum(demanda_valida[vecinos_idx])
                                / len(vecinos_idx)
                            ),
                            float(q50_no_na),
                        )
                    demanda_valida[vecinos_idx] = val

            logger.debug(
                f"val: {val}, vecinos_hills_idx: {vecinos_idx}, D[vecinos_hills_idx]: {demanda_valida[vecinos_idx]}"
            )
            el_no_hills = np.union1d(el_no_hills, vecinos_idx)
            aux_na_idx = np.setdiff1d(aux_na_idx, vecinos_idx)
    return atp_idx, aux_na_idx


def read_from_files(
    ruta_read_arch_demanda: str,
    ruta_read_arch_stock: str,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if os.path.exists(ruta_read_arch_demanda) and os.path.exists(ruta_read_arch_stock):
        current_hist = pl.read_parquet(ruta_read_arch_demanda)
        current_stock = pl.read_parquet(ruta_read_arch_stock)
    else:
        raise FileNotFoundError(
            "No se encontró alguno de los archivos de demanda o stock, revisar archivos"
        )

    return current_hist, current_stock


def process_dataframes(
    current_hist: pl.DataFrame, current_stock: pl.DataFrame, main_included=False
) -> pl.DataFrame:
    """
    Función que:
      - Realiza filtrados y transformaciones (similares a las operaciones de dplyr y tidyr en R).
      - Realiza joins, expansiones de fechas.
      - Finalmente, Devuelve el dataframe procesado.

    Se calculara main si no esta incluido en el dataframe y la variable no es False.
    """

    # Filtrado por centros de almacén
    # Almacen exclusivo de entradas no de salidas
    almacenes_excluidos = ("1009",)
    llaves_grupo_hist = ["Gpo_Plan", "Material_Id", "Gpo_Cliente", "Centro_Almacen_Id"]
    llaves_fecha_hist = ["Fecha_Id"]
    metricas_hist = ["Demanda_Positiva", "Demanda_Negativa", "Vencidos_Entrada"]
    extras_hist = ["Material_Nombre", "Articulo_Id"]
    extras_hist = [ex for ex in extras_hist if ex in current_hist.columns]
    llaves_grupo_stock = [
        "Gpo_Plan",
        "Material_Id",
        "Centro_Almacen_Id",
    ]
    llaves_fecha_stock = ["Fecha_Id"]
    metricas_stock = ["Stock_Cierre", "DiasSin_Stock"]
    extras_stock = ["Material_Nombre", "Articulo_Id"]
    extras_stock = [ex for ex in extras_stock if ex in current_stock.columns]

    # Añadir main cuando sea provisto.
    if any(("main" in current_stock.columns, "main_stock" in current_stock.columns)):
        extras_hist.append("main")
        extras_hist.append("main_stock")
        extras_stock.append("main_stock")
        main_included = True

    current_stock = (
        current_stock.select(
            *llaves_grupo_stock, *llaves_fecha_stock, *metricas_stock, *extras_stock
        )
        .cast(
            {
                **{key: pl.String for key in llaves_grupo_stock},
                **{key: pl.Date for key in llaves_fecha_stock},
                **{key: pl.Float32 for key in metricas_stock},
                **{key: pl.String for key in extras_stock},
            }
        )
        .filter(
            ~pl.col("Material_Nombre").str.starts_with("#"),
            ~pl.col("Centro_Almacen_Id").str.contains(".*-R.*"),
            ~pl.col("Centro_Almacen_Id").is_in(almacenes_excluidos),
        )
    ).with_columns(
        Fecha_Id=pl.col("Fecha_Id").dt.month_start(),
    )

    if not main_included:
        current_stock = current_stock.with_columns(
            main_stock=(pl.concat_str(llaves_grupo_stock, separator="/")),
        )
        if (
            current_stock.n_unique(subset=(*llaves_grupo_stock, *llaves_fecha_stock))
            != current_stock.height
        ):
            raise ValueError("Hay duplicados en current_stock")

    # Procesar current_hist de forma similar

    current_hist = (
        current_hist.select(
            *llaves_grupo_hist, *llaves_fecha_hist, *metricas_hist, *extras_hist
        )
        .cast(
            {
                **{key: pl.String for key in llaves_grupo_hist},
                **{key: pl.Date for key in llaves_fecha_hist},
                **{key: pl.Float32 for key in metricas_hist},
                **{key: pl.String for key in extras_hist},
            }
        )
        .filter(
            ~pl.col("Material_Nombre").str.starts_with("#"),
            ~pl.col("Centro_Almacen_Id").str.contains(".*-R.*"),
            ~pl.col("Centro_Almacen_Id").is_in(almacenes_excluidos),
            ~pl.col("Material_Nombre").str.contains("INST"),
            ~pl.col("Gpo_Cliente").eq("INST"),
        )
    ).with_columns(
        Fecha_Id=pl.col("Fecha_Id").dt.month_start(),
    )

    if not main_included:
        current_hist = current_hist.with_columns(
            main=(pl.concat_str(llaves_grupo_hist, separator="/")),
            main_stock=(pl.concat_str(llaves_grupo_stock, separator="/")),
        )

        # validar duplicados
        if (
            current_hist.n_unique(subset=(*llaves_grupo_hist, *llaves_fecha_hist))
            != current_hist.height
        ):
            raise ValueError("Hay duplicados en current_hist")

    # Función para calcular la fecha inicial y final y rellenar
    # Fecha Final para el main
    # dates# logger.debug(f"current_stock antes fechas_main_max: {current_stock.head(40)}")
    fechas_main_max = (
        current_hist.select("main", "main_stock", *llaves_grupo_hist)
        .unique()
        .with_columns(pl.lit(current_hist["Fecha_Id"].max()).alias("Fecha_Id"))
    )
    # dates# logger.debug(f"fechas_main_max: {fechas_main_max.head(40)}")
    fechas_main_min = (
        (
            (
                current_stock.group_by("main_stock").agg(
                    pl.col("Fecha_Id").min().alias("Fecha_Id")
                )
            )
            .join(fechas_main_max, on="main_stock", how="inner", validate="1:m")
            .select("main", "main_stock", *llaves_grupo_hist, "Fecha_Id")
        )
        .join(current_hist, on=["main", "main_stock", "Fecha_Id"], how="anti")
        .unique()
    )
    # dates# logger.debug(f"fechas_main_min: {fechas_main_min.head(2)}")
    fechas_main_max = fechas_main_max.join(
        current_hist, on=["main", "main_stock", "Fecha_Id"], how="anti"
    ).unique()

    # dates# logger.debug(f"fechas_main_max: {fechas_main_max.head(2)}")
    # Expandir fechas
    current_hist = (
        pl.concat([current_hist, fechas_main_max, fechas_main_min], how="diagonal")
        .sort("Fecha_Id", "main")
        .upsample(
            time_column="Fecha_Id",
            every="1mo",
            group_by="main",
        )
        .with_columns(pl.exclude(metricas_hist).forward_fill())
        .with_columns(
            pl.col("Fecha_Id")
            .dt.month_end()
            .alias("Fecha_Id"),  # Volver a Fecha Final para joins
        )
    ).unique(subset=("Fecha_Id", "main"))
    # dates# logger.debug(f"current_hist despues ups: {current_hist.head(40)}")

    # with pl.Config() as cfg:
    #     cfg.set_tbl_cols(-1)
    #     logger.debug(
    #         f"process_dataframes current_hist entra: {
    #             current_hist.filter(pl.col('main').str.starts_with('A/123'))
    #             .select(['main', 'main_stock', 'Fecha_Id'])
    #             .head(30)
    #         }"
    #     )
    current_hist = current_hist
    # Función para calcular la fecha final en stock
    current_stock = current_stock.with_columns(
        pl.col("Fecha_Id").dt.month_end().alias("Fecha_Id"),
    )

    # Agregar cierre de stock y stock acumulado
    stock_cols = [
        "main_stock",
        "Material_Id",
        "Fecha_Id",
        "Stock_Cierre",
        "DiasSin_Stock",
    ]
    stock_subset = current_stock[stock_cols]
    current_hist = current_hist.join(
        stock_subset,
        on=["main_stock", "Fecha_Id"],
        how="left",
    ).drop(pl.selectors.ends_with("_right"))

    current_hist = current_hist.with_columns(
        pl.col(metricas_hist).fill_nan(0).fill_null(0),
        Stock_Cierre=pl.col("Stock_Cierre").fill_nan(0).fill_null(0),
        DiasSin_Stock=pl.col("DiasSin_Stock").fill_nan(30).fill_null(30),
    )

    # Agregar resumen (acumulado de demanda positiva)
    C_H = current_hist.group_by(["main_stock", "Fecha_Id"]).agg(
        pl.col("Demanda_Positiva").sum().alias("Demanda_Total_Sku")
    )
    current_hist = current_hist.join(
        C_H,
        on=["main_stock", "Fecha_Id"],
        how="left",
    ).drop(pl.selectors.ends_with("_right"))

    # print(current_hist.filter(pl.col("main").str.starts_with("G")).head(100))
    # print(current_hist.filter(pl.col("main").str.starts_with("G")).describe())
    # se agrega la variable de stock acumulado que cuenta el numero de unidades que estaban a inicio de mes
    # asi como las unidades que entraron
    current_hist = (
        current_hist.with_columns(
            Stock_Acum=pl.col("Stock_Cierre") + pl.col("Demanda_Total_Sku"),
        )
        .with_columns(
            # Agregar variable Primer_Demanda: primer mes con demanda positiva
            Primer_Demanda=pl.col("Fecha_Id")
            .filter(pl.col("Stock_Acum") > 0)
            .min()
            .over("main")
        )
        .filter(
            # eliminar los registros para los que se identifico que antes habia 0
            pl.col("Primer_Demanda") <= pl.col("Fecha_Id")
        )
        .with_columns(
            Anio_Id=pl.col("Fecha_Id").dt.year(),
            Mes_Id=pl.col("Fecha_Id").dt.month(),
        )
    )

    # with pl.Config() as cfg:
    #     cfg.set_tbl_cols(-1)
    #     logger.debug(
    #         f"process_dataframes current_hist sale: {
    #             current_hist.filter(pl.col('main').str.starts_with('C'))
    #             .select(['main', 'Fecha_Id', 'Demanda_Positiva', 'Stock_Cierre'])
    #             .head(30)
    #         }"
    #     )

    # validar duplicados
    if (
        current_hist.n_unique(subset=(*llaves_fecha_hist, "main"))
        != current_hist.height
    ):
        raise ValueError("Hay duplicados en current_hist")

    return current_hist  # .head(10000)


def write_to_parquet(df: pl.DataFrame, ruta_write: str) -> bool:
    if ruta_write is not None:
        df.write_parquet(ruta_write)

    return True


def procesar_con_mddme(
    current_hist: pl.DataFrame,
    current_stock: pl.DataFrame,
    procesar_stats: bool = True,
    umbral_min: float = 0.4,
    ruta_write: Optional[str] = None,
) -> pl.DataFrame:
    """
    Función principal que:
      - Lee archivos Parquet de demanda y stock.
      - Realiza filtrados y transformaciones (similares a las operaciones de dplyr y tidyr en R).
      - Realiza joins, expansiones de fechas y agrega variables (incluyendo llamadas a las funciones definidas arriba).
      - Finalmente, escribe el resultado a un archivo Parquet (si 'ruta_write' es proporcionada).
    """

    current_hist = process_dataframes(
        current_hist=current_hist, current_stock=current_stock
    )

    if procesar_stats:

        def process_group(df: pl.DataFrame) -> pl.DataFrame:
            """Polars version of process_group function"""
            logger.debug(f"Entra process group df: {df.head(40)}")
            # Sort by date
            df = df.sort("Fecha_Id")
            # with pl.Config() as cfg:
            #     cfg.set_tbl_cols(-1)
            #     logger.debug(f"res: {df.select(['main', 'Fecha_Id', 'Demanda_Positiva']).head(10)}")
            # Get numpy arrays for calculations
            demanda_original = df.get_column("Demanda_Positiva").to_numpy()
            stock_acum = df.get_column("Stock_Acum").to_numpy()
            demanda_sku = df.get_column("Demanda_Total_Sku").to_numpy()
            dias_sin_stock = df.get_column("DiasSin_Stock").to_numpy()

            # Marginalization
            demanda_valida = marginalizacion_demanda(
                demanda_original=demanda_original,
                stock_acum=stock_acum,
                demanda_sku=demanda_sku,
                dias_sin_stock=dias_sin_stock,
            )
            logger.debug(f"Entran desde res_marginalizados: {demanda_valida}")
            # Get first date for start tuple
            first_date = df.get_column("Primer_Demanda").first()
            if isinstance(first_date, (datetime, date)):
                start_tuple = (first_date.year, first_date.month)
            else:
                raise TypeError(
                    f"Unexpected date type: {type(first_date)}. Expected pl.Datetime or datetime.datetime"
                )

            # Get atypical values
            atp_vals = obtener_atp(demanda_original, demanda_valida["D"], start_tuple)

            # Create atypical indicators array
            atipicos = np.zeros(len(df))
            if len(atp_vals) > 0:
                atipicos[atp_vals] = 1

            # Get seasonal atypicals
            atp_est_idx = atp_ests_idx(atp_vals, demanda_valida["D"])
            atipicos_est_idx = np.zeros(len(df))
            if len(atp_est_idx) > 0:
                atipicos_est_idx[atp_est_idx] = 1

            # Get hills
            logger.debug(
                f"Entran A hills_vals: {demanda_original=}, {demanda_valida['D']=}, {atipicos_est_idx== 1=}"
            )
            hills_vals = encontrar_hills_idx(
                demanda_original,
                demanda_valida["D"],
                atipicos_est_idx == 1,
            )

            hills = np.zeros(len(df))
            if len(hills_vals) > 0:
                hills[hills_vals] = 1
            # print(df.head(100))
            # print(df.describe())
            # Return processed dataframe with new columns
            df = df.with_columns(
                [
                    pl.Series("Seleccionados", demanda_valida["D"]),
                    pl.Series("Atipicos", atipicos).cast(pl.Boolean),
                    pl.Series("Atipicos_Estacionales", atipicos_est_idx).cast(
                        pl.Boolean
                    ),
                    pl.Series("Hills", hills).cast(pl.Boolean),
                ]
            )
            # logger.debug(f"Salen process group df: {df.head(100)}")
            return df

        processed_groups: deque = deque()
        for group in current_hist.partition_by("main"):
            processed_groups.append(process_group(group))

        current_hist = pl.concat(processed_groups) if processed_groups else current_hist

        def process_group_forecast(df: pl.DataFrame) -> pl.DataFrame:
            """Polars version of process_group_forecast function"""
            # Sort by date
            df = df.sort("Fecha_Id")

            # Get numpy arrays for calculations
            atp = df["Atipicos"].arg_true().to_numpy()
            atp_ests_vals_idx = (
                df.get_column("Atipicos_Estacionales").arg_true().to_numpy()
            )
            hills_idx = df.get_column("Hills").arg_true().to_numpy()
            demanda_valida = df.get_column("Seleccionados").to_numpy()
            demanda_original = df.get_column("Demanda_Positiva").to_numpy()
            # SA = df.get_column("Stock_Acum").to_numpy()
            # VA = df.get_column("Demanda_Total_Sku").to_numpy()
            # Dss = df.get_column("DiasSin_Stock").to_numpy()

            # Calculate q60noNA
            seleccionados = assert_is_number(
                df.get_column("Seleccionados").drop_nulls().drop_nans().quantile(0.60)
            )
            q60noNA = math.floor(seleccionados) if seleccionados else 0

            # Get start tuple from first date with proper type handling
            first_date = df.get_column("Primer_Demanda").first()
            if isinstance(first_date, (datetime, date)):
                start_tuple = (first_date.year, first_date.month)
            else:
                raise TypeError(
                    f"Unexpected date type: {type(first_date)}. Expected pl.Datetime or datetime.datetime"
                )

            # Process RPD calculations

            limpios_forecast = rpd(
                demanda_valida=demanda_valida,
                start=start_tuple,
                demanda_original=demanda_original,
                umbral_min=umbral_min,
                atp_idx=atp,
                q60_no_na=q60noNA,
                hills_idx=hills_idx,
                atp_ests_vals_idx=atp_ests_vals_idx,
                decaimiento=False,
            )
            logger.debug(f"Entran desde limpios_forecast: {limpios_forecast=}")

            limpios_promedio = rpd(
                demanda_valida=demanda_valida,
                start=start_tuple,
                demanda_original=demanda_original,
                umbral_min=0,
                atp_idx=atp,
                q60_no_na=1,
                hills_idx=hills_idx,
                atp_ests_vals_idx=np.array([]),
                decaimiento=True,
            )

            rachas_idx = rpd(
                demanda_valida=demanda_valida,
                start=start_tuple,
                demanda_original=demanda_original,
                umbral_min=0,
                atp_idx=atp,
                q60_no_na=1,
                hills_idx=hills_idx,
                atp_ests_vals_idx=atp_ests_vals_idx,
                decaimiento=False,
                only_rachas=True,
            )

            # Create rachas indicator array
            es_racha = np.zeros(len(df))
            es_racha[rachas_idx] = 1

            # Calculate se_limpio
            seleccionados_len = len(df) - (df.get_column("Seleccionados").null_count())
            se_limpio = (
                1 if (seleccionados_len / len(df) >= umbral_min and q60noNA > 0) else 0
            )

            # Return processed dataframe with new columns
            return df.with_columns(
                [
                    pl.Series("Limpios_Forecast", limpios_forecast).cast(pl.Float64),
                    pl.Series("Limpios_Para_Promedio", limpios_promedio).cast(
                        pl.Float64
                    ),
                    pl.Series("Rachas", es_racha).cast(pl.Boolean),
                    pl.Series("Se_limpio", [se_limpio] * len(df)).cast(pl.Boolean),
                ]
            )

        processed_groups: deque = deque()
        for group in current_hist.partition_by("main"):
            processed_groups.append(process_group_forecast(group))

        current_hist = pl.concat(processed_groups) if processed_groups else current_hist

    if ruta_write:
        write_to_parquet(current_hist, ruta_write)

    return current_hist


# =============================================================================
# Bloque principal
# =============================================================================

if __name__ == "__main__":
    import psutil

    start_time = time.time()
    gc.collect()
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / (1024 * 1024)

    # Simulación de argumentos (puedes sustituir por sys.argv si se requiere)
    args = [
        r"py_algoritmo_mddme_gf\tests_algoritmo_mddme_gf\demanda_prueba_temp.parquet",
        r"py_algoritmo_mddme_gf\tests_algoritmo_mddme_gf\demanda_prueba_stock_temp.parquet",
        0.4,
        r"py_algoritmo_mddme_gf\tests_algoritmo_mddme_gf\prueba_resultado.parquet",
    ]
    ruta_read_arch_demanda = os.path.abspath(str(args[0]))
    ruta_read_arch_stock = os.path.abspath(str(args[1]))
    umbral = float(str(args[2])) if args[2] is not None else 0.4
    ruta_write = os.path.abspath(str(args[3]))

    current_hist, current_stock = read_from_files(
        ruta_read_arch_demanda=ruta_read_arch_demanda,
        ruta_read_arch_stock=ruta_read_arch_stock,
    )

    resultado = procesar_con_mddme(
        current_hist,
        current_stock,
        procesar_stats=True,
        umbral_min=umbral,
        ruta_write=ruta_write,
    )

    end_time = time.time()
    gc.collect()
    mem_after = process.memory_info().rss / (1024 * 1024)

    time_taken = (end_time - start_time) / 60.0
    mem_used = mem_after - mem_before

    time_str = f"El tiempo que se tardó fue: {time_taken:.3f} minutos"
    mem_str = f"La memoria usada fue de: {round(mem_used, 1)} MB"

    print(time_str)
    print(mem_str)
