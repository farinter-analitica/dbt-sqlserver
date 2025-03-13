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
from statstools_gf import rle  # , detect_ts_outliers
from statstools_gf.interpolate import interpolate_auto
from statstools_gf.ts_outliers.ts_outliers_simple import (
    assert_is_number,
    detect_ts_outliers,
)
from statstools_gf.utils import logging, setup_logging

logger = setup_logging(
    level=logging.INFO if __name__ == "__main__" else logging.INFO, name=__name__
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
    DC: Union[np.ndarray, List[float]],
    SA: Union[np.ndarray, List[float]],
    VA: Union[np.ndarray, List[float]],
    Dss: Union[np.ndarray, List[float]],
) -> dict:
    """
    Realiza la marginalización de la demanda (DC) según condiciones en SA, VA y Dss.
    Se calcula una cota inferior y se reemplazan valores que no la cumplen.
    """
    # Convert inputs to numpy arrays
    DC = np.asarray(DC, dtype=float)
    SA = np.asarray(SA, dtype=float)
    VA = np.asarray(VA, dtype=float)
    Dss = np.asarray(Dss, dtype=float)
    n = len(DC)

    # Calcular el lag de SA (primer elemento será NaN)
    SA_ant = np.pad(SA[:-1], (1, 0), "constant", constant_values=np.nan)

    comp = 0
    valid_idx = SA != 0
    if np.sum(valid_idx) > 0:
        p15VA = max(1.0, float(np.percentile(VA[valid_idx], 15.0)))
    else:
        p15VA = 1.0

    # Find indices where conditions are met
    R = np.where((SA >= p15VA) & (Dss <= 15))[0]

    # Percentiles que incrementan la cota inferior pero están censurados
    nonzero_DC = DC != 0
    if np.sum(nonzero_DC) > 0:
        p25cen = np.percentile(DC[nonzero_DC], 25)
        p75cen = np.percentile(DC[nonzero_DC], 75)
    else:
        p25cen = p75cen = 0

    # Incluir el primer elemento (índice 0)
    R = np.union1d(np.array([0]), R)
    R = np.union1d(R, np.where((DC > p25cen) & (DC < p75cen))[0])

    if len(R) > 0:
        p20DC = max(1.0, float(np.percentile(DC[R], 20.0)))
        p50DC = max(1.0, float(np.percentile(DC[R], 50.0)))
    else:
        p20DC = p50DC = 1

    # NOTA:______________________________________________________________________________________________________________________
    # Condición p20DC/p50DC <= 1/10, dice que la mediana es 10 veces mas grande que el percentil 20%
    # Condiciones en las que (length(which(SA <= 1 ))/n) < 0.5 y (length(which(SA <= 1))/n) > 0.1, se piden ya que
    # como se buscan en las posiciones preliminarmente ya seleccionadas, se puede permitir que como mucho el 50% de los datos
    # (lo cual es bastante) Sean menor o igual a 1, ya que en el caso en el que la SA = 1 sea un dato no quiebre de stock
    # el percentil que está basado en los seleccionados, no estará por encima
    cheq = np.sum(SA <= p20DC) / n

    if (p20DC / p50DC <= 1 / 10) and (cheq < 0.5):
        count_le = np.sum(SA <= p20DC)
        quantile_val = ((count_le + 1) / n) * 100
        new_percentil = np.percentile(DC[R], quantile_val)
        if new_percentil > p20DC:
            p20DC = new_percentil
            comp = 1

    if len(R) / n >= 0.5:
        inter = np.intersect1d(DC[R], DC[nonzero_DC])
        if len(inter) == 0:
            Venta_infima = np.min(DC[nonzero_DC]) if np.sum(nonzero_DC) > 0 else 0
        else:
            Venta_infima = max(np.min(inter), np.min(DC[nonzero_DC]))
    else:
        Venta_infima = np.min(DC[nonzero_DC]) if np.sum(nonzero_DC) > 0 else 0

    Venta_infima = max(float(p20DC), float(Venta_infima))

    Final = np.where((SA >= Venta_infima) & (Dss <= 15) & (SA_ant >= Venta_infima))[0]
    if len(Final) > 0:
        p50DC = max(1.0, float(np.percentile(DC[Final], 50.0)))
        p25DC = max(1.0, float(np.percentile(DC[Final], 25.0)))
    else:
        p50DC = p25DC = 1

    # Condición en la que el percentil 25 es demasiado cercano respecto a la mediana
    if p50DC != 0 and (p25DC / p50DC) >= 0.85:
        p25DC = math.ceil(p50DC * 0.7)

    ADD = np.where(DC >= p25DC)[0]
    Final = np.union1d(Final, ADD)
    Final = np.union1d(np.array([0]), Final)
    Final = np.sort(Final).astype(int)

    # Create output array with NaN where values should be filtered out
    D_out = DC.copy()
    mask = np.ones(n, dtype=bool)
    mask[Final] = False
    D_out[mask] = np.nan

    return {"D": D_out, "comp": comp, "cota_inf": Venta_infima}


def detect_rachas_na(
    serie: Union[np.ndarray, Sequence[float]], l_racha: int
) -> Optional[deque[np.ndarray]]:
    """
    Detecta secuencias consecutivas (rachas) de valores NA en 'serie'.
    Retorna una lista con los índices de cada racha larga (>= l_racha).
    """
    serie = np.asarray(serie, dtype=float)
    # Identificar las posiciones donde hay valores NA
    pos_na = np.isnan(serie).nonzero()[0]
    if len(pos_na) >= l_racha:
        # Calcular las diferencias entre posiciones consecutivas
        diff_na = np.diff(pos_na)
        # Encontrar los índices donde las diferencias son iguales a 1 (valores faltantes consecutivos)
        cond = diff_na == 1
        values, lengths = rle(cond)
        # Identificar las posiciones donde la racha de valores faltantes es mayor o igual a 3 consecutivos
        long_rachas = np.where((values is True) & (lengths >= l_racha - 1))[0]
        if len(long_rachas) > 0:
            # Si existen rachas largas, devolver una lista con las posiciones de los valores faltantes en cada racha
            pos_list: deque = deque()
            cumulative = np.cumsum(np.insert(lengths, 0, 0))
            for i in long_rachas:
                start = cumulative[i]
                end = cumulative[i + 1] + 1
                pos_list.append(pl.Series(pos_na[start:end]))

            return pos_list
        else:
            return None
    return None


def find_hills(
    DC: Union[np.ndarray, List[float]],
    D: Union[np.ndarray, List[float]],
    atp: Union[np.ndarray, List[int]],
    t_list: Optional[Union[pl.Series, np.ndarray]] = None,
) -> np.ndarray:
    """
    Identifica "hills" (picos) en la serie D basándose en DC, excluyendo los índices
    presentes en 'atp' y en 't_list'.
    """
    logger.debug(
        f"Entran find_hills con DC: {DC}, D: {D}, atp: {atp}, t_list: {t_list}"
    )
    # Convert all inputs to numpy arrays
    DC = np.asarray(DC, dtype=float)
    D = np.asarray(D, dtype=float)
    atp = np.asarray(atp, dtype=int)

    if t_list is None:
        t_list = np.array([], dtype=int)
    else:
        t_list = np.asarray(t_list, dtype=int)

    # Calculate threshold and find points above it
    threshold = max(1.0, float(np.nanpercentile(D, 50.0)))
    p50 = np.where(D > threshold)[0]

    # Find NA values in D
    NAS = np.where(np.isnan(D))[0]

    # Find neighbors of NA values
    neighbors_before = NAS - 1
    neighbors_after = NAS + 1

    # Filter valid indices (prevent out of bounds)
    neighbors_before = neighbors_before[neighbors_before >= 0]
    neighbors_after = neighbors_after[neighbors_after < len(D)]

    # Combine and get unique neighbors
    neighbors = np.union1d(neighbors_before, neighbors_after)

    # Find the intersection of neighbors and p50
    hills = np.intersect1d(neighbors, p50)

    # Exclude atp and t_list indices
    if len(atp) > 0:
        hills = np.setdiff1d(hills, atp)

    hills = np.setdiff1d(hills, t_list)
    logger.debug(f"Salen find_hills con hills: {hills}")
    # Return sorted hills
    return np.sort(hills)


def atp_ests(
    atp: Union[np.ndarray, List[int]],
    D: Union[np.ndarray, List[float]],
    li: int = 10,
    ls: int = 14,
) -> np.ndarray:
    """
    Devuelve los índices de atípicos estacionales, es decir, aquellos en 'atp'
    que cumplen condiciones basadas en la diferencia (entre 10 y 14 unidades).
    """
    # Convert to numpy array
    atp = np.asarray(atp, dtype=np.int64)
    logger.debug(f"atp est entra: {atp}")

    if len(atp) == 0:
        return np.array([], dtype=np.int64)

    # Generate arrays of indices offset by 10-14 periods (forward and backward)
    pmo = np.add.outer(atp, np.arange(10, 15)).flatten()
    plo = np.add.outer(atp, -np.arange(10, 15)).flatten()

    # Find which values in atp are also in either pmo or plo
    mask = np.isin(atp, pmo) | np.isin(atp, plo)

    # Filter and return sorted unique values
    atp_est_val = np.sort(np.unique(atp[mask]))
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
    outliers, _ = detect_ts_outliers(ts=pl.Series(values=DC_np))

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

    if not 0.0 < factor_p <= 1.0:
        raise ValueError("factor_p debe estar entre 0 y 1")

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
    return np.round(final)


def rpd(
    D: Union[np.ndarray, List[float]],
    start: Tuple[int, int],
    DC: Union[np.ndarray, List[float]],
    umbral: float,
    atp: Union[np.ndarray, List[int]],
    q60_no_na: float,
    hills: Union[np.ndarray, List[int]],
    atp_ests_vals: Optional[Union[np.ndarray, List[int]]] = None,
    para_prom: bool = False,
    only_rachas: bool = False,
) -> np.ndarray:
    """
    Función que limpia y ajusta la serie D (a partir de DC) para corregir outliers,
    hills, atípicos y detectar rachas. Se aplican múltiples reglas según el umbral y
    la cantidad de datos.

    reemplazar y depurar demanda
    """
    # Convert all inputs to numpy arrays
    D = np.asarray(D, dtype=float, copy=True)
    DC = np.asarray(DC, dtype=float)
    atp = np.asarray(atp, dtype=int).astype(np.int64)
    hills = np.asarray(hills, dtype=int).astype(np.int64)

    if atp_ests_vals is None:
        atp_ests_vals = np.array([], dtype=int)
    else:
        atp_ests_vals = np.asarray(atp_ests_vals, dtype=int)

    n = len(D)
    logger.debug(f"D: {D}")

    # Find positions with NaN values and valid values
    pos = np.where(np.isnan(D))[0]
    valid = np.where(~np.isnan(D))[0]
    prob = float(
        np.sum((~np.isnan(D)) & (D > 0.0)) / len(valid) if len(valid) > 0 else 0
    )

    umbral_val = 1 - umbral
    if n <= 6:
        umbral_val = 1 - (2 / n)

    # Calculate median of non-NaN values
    q50_no_na = np.nanmedian(D)
    aux_pos = pos.copy()

    if (len(pos) / n <= umbral_val and q50_no_na > 0) or only_rachas:
        atp = np.setdiff1d(atp, atp_ests_vals)
        # logger.debug(f"ATP np.setdiff1d(atp, hill_y_atp): {atp}")

        # Identify elements that are not hills or NaN
        el_no_hills = np.setdiff1d(np.arange(n), np.union1d(pos, hills))
        hill_y_atp = np.intersect1d(hills, atp)
        atp = np.setdiff1d(atp, hill_y_atp)

        # _____________________________HILLS______________________________
        if len(hills) > 0:
            for h in hills:
                val = 0
                if h in hill_y_atp:
                    mod_candidates = np.array([h - 2, h - 1, h + 1, h + 2])
                    # Keep only valid indices within range
                    mod_candidates = mod_candidates[
                        (mod_candidates >= 0) & (mod_candidates < n)
                    ]
                    mod_candidates = np.intersect1d(mod_candidates, aux_pos)
                    mod = np.union1d(mod_candidates, np.array([h]))

                    if len(mod) > 1:
                        if (len(el_no_hills) / (n - len(aux_pos))) > 0.5:
                            val = max(
                                float(np.nanmean(DC[mod])),
                                float(np.nansum(D[mod]) / len(mod))
                                if len(mod) > 0
                                else 0.0,
                                float(np.nanpercentile(DC[el_no_hills], 50.0))
                                if len(el_no_hills) > 0
                                else 0.0,
                            )
                        else:
                            val = max(
                                np.nanmean(DC[mod]),
                                np.nansum(D[mod]) / len(mod) if len(mod) > 0 else 0,
                                q50_no_na,
                            )
                        D[mod] = val
                    else:
                        atp = np.union1d(atp, np.array([h]))
                else:
                    mod_candidates = np.intersect1d(np.array([h - 1, h + 1]), aux_pos)
                    mod = np.union1d(mod_candidates, np.array([h]))

                    if len(mod) > 1:
                        if (len(el_no_hills) / (n - len(aux_pos))) > 0.5:
                            val = max(
                                float(np.nansum(DC[mod]) / 2),
                                float(np.nansum(D[mod]) / 2),
                                float(np.nanpercentile(DC[el_no_hills], 50.0))
                                if len(el_no_hills) > 0
                                else 0.0,
                            )
                        else:
                            val = max(
                                float(np.nansum(DC[mod]) / 2),
                                float(np.nansum(D[mod]) / 2),
                                float(q50_no_na),
                            )
                        D[mod] = val

                logger.debug(f"val: {val}, mod: {mod}, D[mod]: {D[mod]}")
                el_no_hills = np.union1d(el_no_hills, mod)
                aux_pos = np.setdiff1d(aux_pos, mod)

        # _________________________Atipicos________________________________
        considerar_atp = np.array([], dtype=int)
        if len(atp) > 0 and n > 3:
            q80_aux = round(np.nanpercentile(D, 80))
            delete = np.union1d(atp, atp_ests_vals)
            remaining = np.setdiff1d(np.arange(n), delete)
            max_cot_atp = np.nanmax(D[remaining]) if len(remaining) > 0 else 0
            q50_aux = math.ceil(np.nanmedian(D))
            aux = D.copy()

            logger.debug(f"D incial atipicos: {D}")
            for index_atp in atp:
                if index_atp == 0:
                    mod = np.array([index_atp + 1, index_atp + 2])
                elif index_atp == n - 1:
                    mod = np.array([index_atp - 2, index_atp - 1])
                else:
                    mod = np.array([index_atp - 1, index_atp + 1])

                logger.debug(f"mod: {mod}, index_atp: {index_atp}, n: {n}")
                pos_set = np.union1d(mod, np.array([index_atp]))
                mod = np.setdiff1d(mod, hills)

                # In Polars: cond = aux.is_null() | (aux < q50_aux)
                cond = np.isnan(aux) | (aux < q50_aux)
                mod = np.intersect1d(mod, np.where(cond)[0])
                mod = np.union1d(mod, np.array([index_atp]))

                mod = mod.astype(np.int64)
                index_atp = int(index_atp)
                if len(mod) <= 1:
                    if index_atp < n - 2:
                        candidate = max(
                            np.nanmean(DC[pos_set]),
                            np.nansum(D[pos_set]) / len(pos_set)
                            if len(pos_set) > 0
                            else 0,
                        )
                        D[index_atp] = min(candidate, max_cot_atp)
                    if D[index_atp] > q80_aux:
                        considerar_atp = np.union1d(considerar_atp, [index_atp])
                else:
                    val = max(
                        np.nanmean(DC[mod]),
                        np.nansum(D[mod]) / len(mod) if len(mod) > 0 else 0,
                    )
                    D[mod] = val
                    if D[index_atp] > max_cot_atp and index_atp < n - 1:
                        D[mod] = max_cot_atp
                    if D[index_atp] > q80_aux:
                        considerar_atp = np.union1d(considerar_atp, mod)
                    aux_pos = np.setdiff1d(aux_pos, mod)

        logger.debug(f"D post Atipicos: {D}")
        # _______________Se detectan rachas sin extremos hills ni atipicos ___________________________
        logger.debug(f"D previo rachas: {D}")

        pos_na_list = detect_rachas_na(D, l_racha=4)

        if only_rachas:
            if pos_na_list is not None:
                # Combine all racha indices into one array
                rachas = np.array([], dtype=int)
                for racha in pos_na_list:
                    rachas = np.union1d(rachas, racha)
                return rachas
            else:
                return np.array([], dtype=int)

        if pos_na_list is not None and len(pos_na_list) > 0:
            for r in pos_na_list:
                extremos = [r[0], r[-1]]
                if extremos[1] < n - 1 and np.any(DC[extremos[1] :] != 0):
                    if extremos[1] - extremos[0] > 1:
                        idx_range = range(extremos[0] + 1, extremos[1])
                        D[idx_range] = np.nanmedian(D)
                elif extremos[1] == n - 1 or np.all(DC[extremos[1] :] == 0):
                    if para_prom:
                        D[r] = calcular_decaimiento(D, r, prob, atp_ests_vals)
                    else:
                        return DC

        logger.debug(f"D post rachas: {D}")
        ret: np.ndarray = D.copy()

        # Calculate percentiles for non-NaN values
        non_nan_vals = ret[~np.isnan(ret)]
        if len(non_nan_vals) > 0:
            q75_no_na = np.percentile(non_nan_vals, 75)
        else:
            q75_no_na = 0

        ret_series = ret.copy()
        logger.debug(f"ret_series previo interpolar: {ret_series}")
        if len(atp_ests_vals) > 0:
            aux_ests = ret[atp_ests_vals].copy()
            ret[atp_ests_vals] = q75_no_na
            ret = interpolate_auto(ret).to_numpy()
            ret = np.round(np.abs(ret))
            ret[atp_ests_vals] = aux_ests
        else:
            ret = interpolate_auto(ret).to_numpy()
            ret = np.round(np.abs(ret))

        logger.debug(f"ret_series post interpolar: {ret}")

        for p_idx in aux_pos:
            if n >= 10 and p_idx > math.floor((9 / 10) * n):
                # Ventana movil
                window_start = max(0, p_idx - 6)
                window_end = min(n, p_idx + 7)
                window_indices = np.concatenate(
                    [np.arange(window_start, p_idx), np.arange(p_idx + 1, window_end)]
                )
                vm = D[window_indices]
                vm = vm[~np.isnan(vm)]

                if len(vm) > 1:
                    q75vm = np.percentile(vm, 75)
                else:
                    q75vm = q75_no_na

                # Replace np.inf with a very large value for comparison
                ret_val_at_idx = np.inf if np.isnan(ret[p_idx]) else ret[p_idx]
                if ret_val_at_idx > q75_no_na and ret_val_at_idx > q75vm:
                    ret[p_idx] = max(float(q75_no_na), float(q75vm))

            d_not_null_min = np.nanmin(D)
            # Use 0 instead of NaN for comparison
            ret_val_at_idx = 0 if np.isnan(ret[p_idx]) else ret[p_idx]
            if ret_val_at_idx < d_not_null_min and d_not_null_min < np.nanpercentile(
                ret, 50
            ):
                ret[p_idx] = d_not_null_min

            x_val = 2 + len(considerar_atp) + len(atp_ests_vals)
            cota_maxima_reemplazo = max(
                float(np.nanpercentile(ret, 100 * (1 - x_val / n))),
                float(np.nanmedian(ret)),
            )

            if (
                n >= 12
                and not np.isnan(ret[p_idx])
                and ret[p_idx] > cota_maxima_reemplazo
            ):
                ret[p_idx] = min(float(cota_maxima_reemplazo), float(ret[p_idx]))

            if DC[p_idx] > ret[p_idx]:
                ret[p_idx] = DC[p_idx]

        if np.all(ret == 0):
            ret = DC

        return np.abs(np.round(ret))
    else:
        return DC


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
    fechas_main_max = (
        current_hist.select("main", "main_stock", *llaves_grupo_hist)
        .unique()
        .with_columns(pl.lit(current_hist["Fecha_Id"].max()).alias("Fecha_Id"))
    )
    fechas_main_min = (
        (
            current_stock.group_by("main_stock")
            .agg(pl.col("Fecha_Id").min().alias("Fecha_Id"))
            .join(fechas_main_max, on="main_stock", how="left")
            .select("main", "main_stock", *llaves_grupo_hist, "Fecha_Id")
        )
        .join(current_hist, on=["main", "main_stock", "Fecha_Id"], how="anti")
        .unique()
    )

    fechas_main_max = fechas_main_max.join(
        current_hist, on=["main", "main_stock", "Fecha_Id"], how="anti"
    ).unique()

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
        pl.col("Demanda_Positiva").sum().alias("Demanda_Acum_Pos")
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
            Stock_Acum=pl.col("Stock_Cierre") + pl.col("Demanda_Acum_Pos"),
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
    umbral: float = 0.4,
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
            # Sort by date
            df = df.sort("Fecha_Id")
            # with pl.Config() as cfg:
            #     cfg.set_tbl_cols(-1)
            #     logger.debug(f"res: {df.select(['main', 'Fecha_Id', 'Demanda_Positiva']).head(10)}")
            # Get numpy arrays for calculations
            DC = df.get_column("Demanda_Positiva").to_numpy()
            SA = df.get_column("Stock_Acum").to_numpy()
            VA = df.get_column("Demanda_Acum_Pos").to_numpy()
            Dss = df.get_column("DiasSin_Stock").to_numpy()

            # Marginalization
            res = marginalizacion_demanda(
                DC=DC,
                SA=SA,
                VA=VA,
                Dss=Dss,
            )
            logger.debug(f"res: {res}")
            # Get first date for start tuple
            first_date = df.get_column("Primer_Demanda").first()
            if isinstance(first_date, (datetime, date)):
                start_tuple = (first_date.year, first_date.month)
            else:
                raise TypeError(
                    f"Unexpected date type: {type(first_date)}. Expected pl.Datetime or datetime.datetime"
                )

            # Get atypical values
            atp_vals = obtener_atp(DC, res["D"], start_tuple)

            # Create atypical indicators array
            atipicos = np.zeros(len(df))
            if len(atp_vals) > 0:
                atipicos[atp_vals] = 1

            # Get seasonal atypicals
            atp_est = atp_ests(atp_vals, res["D"])
            atipicos_est = np.zeros(len(df))
            if len(atp_est) > 0:
                atipicos_est[atp_est] = 1

            # Get hills
            hills_vals = find_hills(
                DC,
                res["D"],
                atipicos_est == 1,
            )

            hills = np.zeros(len(df))
            if len(hills_vals) > 0:
                hills[hills_vals] = 1
            # print(df.head(100))
            # print(df.describe())
            # Return processed dataframe with new columns
            df = df.with_columns(
                [
                    pl.Series("Seleccionados", res["D"]),
                    pl.Series("Atipicos", atipicos).cast(pl.Boolean),
                    pl.Series("Atipicos_Estacionales", atipicos_est).cast(pl.Boolean),
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
            atp_ests_vals = df.get_column("Atipicos_Estacionales").arg_true().to_numpy()
            hills = df.get_column("Hills").arg_true().to_numpy()
            D = df.get_column("Seleccionados").to_numpy()
            DC = df.get_column("Demanda_Positiva").to_numpy()
            # SA = df.get_column("Stock_Acum").to_numpy()
            # VA = df.get_column("Demanda_Acum_Pos").to_numpy()
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
                D=D,
                start=start_tuple,
                DC=DC,
                umbral=umbral,
                atp=atp,
                q60_no_na=q60noNA,
                hills=hills,
                atp_ests_vals=atp_ests_vals,
                para_prom=False,
            )

            limpios_promedio = rpd(
                D=D,
                start=start_tuple,
                DC=DC,
                umbral=0,
                atp=atp,
                q60_no_na=1,
                hills=hills,
                atp_ests_vals=np.array([]),
                para_prom=True,
            )

            rachas = rpd(
                D=D,
                start=start_tuple,
                DC=DC,
                umbral=0,
                atp=atp,
                q60_no_na=1,
                hills=hills,
                atp_ests_vals=atp_ests_vals,
                para_prom=False,
                only_rachas=True,
            )

            # Create rachas indicator array
            ret = np.zeros(len(df))
            ret[rachas] = 1

            # Calculate se_limpio
            seleccionados_len = len(df) - (df.get_column("Seleccionados").null_count())
            se_limpio = (
                1 if (seleccionados_len / len(df) >= umbral and q60noNA > 0) else 0
            )

            # Return processed dataframe with new columns
            return df.with_columns(
                [
                    pl.Series("Limpios_Forecast", limpios_forecast).cast(pl.Float64),
                    pl.Series("Limpios_Para_Promedio", limpios_promedio).cast(
                        pl.Float64
                    ),
                    pl.Series("Rachas", ret).cast(pl.Boolean),
                    pl.Series("Se_limpio", [se_limpio] * len(df)).cast(pl.Boolean),
                ]
            )

        # Replace the pandas groupby with polars
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
        umbral=umbral,
        ruta_write=ruta_write,
    )

    end_time = time.time()
    gc.collect()
    mem_after = process.memory_info().rss / (1024 * 1024)

    time_taken = (end_time - start_time) / 60.0
    mem_used = mem_after - mem_before

    time_str = f"El tiempo que se tardó fue: {time_taken:.3f} minutos"
    mem_str = f"La memoria usada fue de: {round(mem_used)} MB"

    print(time_str)
    print(mem_str)
