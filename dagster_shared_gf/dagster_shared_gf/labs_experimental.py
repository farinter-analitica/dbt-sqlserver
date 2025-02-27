from collections import deque
import scipy.sparse as sp
import numpy as np
import polars as pl
import pendulum as pdl
from typing import Tuple
from dagster_shared_gf.shared_variables import env_str

from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_database_admin

dwh_farinter_database_admin.default_database = "DL_FARINTER"


def get_customer_purchases(dwh_farinter_dl) -> pl.DataFrame:
    """
    Ejecuta la consulta para obtener las compras de clientes.
    Se filtran los últimos 'meses_muestra' meses.
    La consulta retorna los campos: Monedero_Id, ArticuloPadre_Id y Frecuencia (cantidad de compras).
    """
    meses_muestra = 2
    lista_fechas_muestra = [
        pdl.today().subtract(months=i + 1) for i in range(meses_muestra)
    ]
    lista_aniomes = [fecha.year * 100 + fecha.month for fecha in lista_fechas_muestra]

    sql_query = f"""
    SELECT 
        DC.Monedero_Id,
        FA.ArticuloPadre_Id,
        COUNT(*) AS Frecuencia
    FROM
        DL_FARINTER.dbo.DL_Kielsa_FacturasPosiciones FA WITH(NOLOCK)
    INNER JOIN
        (SELECT {"TOP 100000" if env_str == "local" else ""}
            M.Monedero_Id,
            COUNT(DISTINCT ArticuloPadre_Id) AS Articulos
        FROM
            DL_FARINTER.dbo.DL_Kielsa_FacturasPosiciones F WITH(NOLOCK)
        INNER JOIN 
            DL_FARINTER.dbo.DL_Kielsa_Monedero M WITH(NOLOCK)
            ON F.MonederoTarj_Id_Limpio = M.Monedero_Id
            AND F.Emp_Id = M.Emp_Id
        WHERE
            F.Emp_Id = 1 AND 
            F.AnioMes_Id IN ({", ".join(map(str, lista_aniomes))}) AND 
            F.TipoDoc_Id = 1 AND
            M.Activo_Indicador = 1 
        GROUP BY
            M.Monedero_Id
        HAVING
            COUNT(DISTINCT ArticuloPadre_Id) > 1) DC 
    ON FA.MonederoTarj_Id_Limpio = DC.Monedero_Id
    WHERE
        FA.Emp_Id = 1 AND 
        FA.AnioMes_Id IN ({", ".join(map(str, lista_aniomes))}) AND 
        FA.TipoDoc_Id = 1
    GROUP BY
        DC.Monedero_Id,
        FA.ArticuloPadre_Id,
        DC.Articulos
    ORDER BY DC.Monedero_Id, COUNT(*) DESC
    """
    main_query = (
        pl.read_database(sql_query, dwh_farinter_dl.get_arrow_odbc_conn_string())
        .lazy()
        .collect(streaming=True)
    )
    return main_query


def create_user_item_matrix(
    purchases_df: pl.DataFrame,
) -> Tuple[sp.csr_matrix, dict, dict]:
    """
    Convierte los datos de compras en una matriz dispersa usuario–artículo.

    Se utilizan los campos:
      - Monedero_Id (identificador del cliente)
      - ArticuloPadre_Id (identificador del artículo)
      - Frecuencia (cantidad de compras; se utiliza como peso en la matriz)

    Se asume que los IDs son textos.
    """
    # Extraer los IDs únicos y ordenados de usuario y artículo
    user_ids = purchases_df.get_column("Monedero_Id").unique().sort().to_numpy()
    item_ids = purchases_df.get_column("ArticuloPadre_Id").unique().sort().to_numpy()

    # Convertir las columnas originales a arrays de NumPy
    monedero_array = purchases_df.get_column("Monedero_Id").to_numpy()
    articulo_array = purchases_df.get_column("ArticuloPadre_Id").to_numpy()
    # Utilizar la columna "Frecuencia" para asignar el peso de cada compra
    frecuencia_array = purchases_df.get_column("Frecuencia").to_numpy()

    # Mapeo vectorizado: usar np.searchsorted ya que los arrays están ordenados
    user_indices = np.searchsorted(user_ids, monedero_array)
    item_indices = np.searchsorted(item_ids, articulo_array)

    # Utilizar la frecuencia real (convertida a int32)
    data = frecuencia_array.astype(np.int32)

    # Construir la matriz dispersa (filas: usuarios, columnas: artículos)
    matrix = sp.csr_matrix(
        (data, (user_indices, item_indices)),
        shape=(len(user_ids), len(item_ids)),
        dtype=np.int32,
    )

    # Crear diccionarios de mapeo para búsquedas inversas
    user_to_idx = {uid: i for i, uid in enumerate(user_ids)}
    item_to_idx = {iid: i for i, iid in enumerate(item_ids)}

    return matrix, user_to_idx, item_to_idx


def compute_cooccurrence_matrix(
    user_item_matrix: sp.csr_matrix, chunk_size: int = 10_000
) -> sp.csr_array:
    """
    Calcula la matriz de coocurrencia de artículos de forma eficiente en memoria.
    Cada elemento (i, j) indica la suma de las coocurrencias (ponderadas por Frecuencia)
    entre el artículo i y el artículo j.
    """
    n_items = user_item_matrix.shape[1]
    # Inicializar la matriz de coocurrencia en formato LIL (fácil de modificar)
    cooccurrence = sp.lil_matrix((n_items, n_items), dtype=np.int32)

    # Procesar en bloques para gestionar la memoria
    for i in range(0, n_items, chunk_size):
        end = min(i + chunk_size, n_items)
        # Obtener un bloque de la matriz transpuesta (artículos)
        chunk = user_item_matrix.T[i:end]
        # Acumular la coocurrencia multiplicando por la matriz original
        cooccurrence[i:end] = chunk @ user_item_matrix

    # Convertir a CSR para operaciones rápidas y poner a cero la diagonal (sin auto-coocurrencia)
    cooccurrence = cooccurrence.tocsr()
    cooccurrence.setdiag(0)

    return cooccurrence


def compute_lift_matrix(
    user_item_matrix: sp.csr_matrix, cooccurrence: sp.csr_array
) -> sp.csr_array:
    """
    Calcula la matriz de lift a partir de la matriz de coocurrencia.

    La fórmula utilizada es:
       lift(i, j) = (coocurrencia observada para i y j) / ((frecuencia(i) * frecuencia(j)) / total_usuarios)

    Donde:
      - frecuencia(i) es el número (ponderado) de usuarios que compraron el artículo i.
      - total_usuarios es el número total de clientes.
    """
    total_users = user_item_matrix.shape[0]
    # Calcular la frecuencia de cada artículo (suma de los pesos por columna)
    item_counts = np.array(
        user_item_matrix.sum(axis=0)
    ).ravel()  # Vector de frecuencias

    # Convertir la matriz de coocurrencia a formato COO para iterar sobre sus elementos no nulos
    cooc_coo = cooccurrence.tocoo()
    lift_data = []

    for i, j, observed in zip(cooc_coo.row, cooc_coo.col, cooc_coo.data):
        # Calcular la coocurrencia esperada si fueran independientes
        expected = (item_counts[i] * item_counts[j]) / total_users
        # Evitar división por cero
        lift_value = observed / expected if expected > 0 else 0
        lift_data.append(lift_value)

    # Reconstruir la matriz de lift en formato COO y convertir a CSR
    lift_matrix = sp.coo_matrix(
        (lift_data, (cooc_coo.row, cooc_coo.col)), shape=cooccurrence.shape
    )
    return lift_matrix.tocsr()


def generate_recommendations(
    purchases_df: pl.DataFrame,
    n_recommendations: int = 5,
    batch_size: int = 1000,
    lift_threshold: float = 1.0,
) -> pl.DataFrame:
    """
    Genera recomendaciones de productos para cada cliente basadas en lo que otros han comprado.

    Se utilizan dos métricas:
      - Lift: Mide la fuerza de la asociación entre artículos.
      - Clientes_Compraron: Suma de las coocurrencias (ponderadas por frecuencia) entre los artículos
        que el cliente ya compró y el artículo candidato.

    Se filtran aquellas asociaciones cuyo lift sea inferior al umbral (por defecto 1.0) y, además, se
    descartan recomendaciones cuya métrica 'Clientes_Compraron' sea menor que el percentil 10 de todos los
    valores no cero o 5, lo que sea mayor.
    """
    # Construir la matriz usuario–artículo y obtener los mapeos
    user_item_matrix, user_to_idx, item_to_idx = create_user_item_matrix(purchases_df)

    # Calcular la matriz de coocurrencia
    cooccurrence = compute_cooccurrence_matrix(user_item_matrix)

    # Calcular la matriz de lift a partir de la coocurrencia
    lift_matrix = compute_lift_matrix(user_item_matrix, cooccurrence)

    # ----------------------------
    # Filtrar asociaciones débiles: descartar aquellas con lift inferior al umbral.
    # Se trabaja en formato COO para filtrar de forma vectorizada.
    lift_coo = lift_matrix.tocoo()
    mask = lift_coo.data >= lift_threshold
    filtered_data = np.where(mask, lift_coo.data, 0)
    lift_matrix = sp.coo_matrix(
        (filtered_data, (lift_coo.row, lift_coo.col)), shape=lift_coo.shape
    )
    lift_matrix = lift_matrix.tocsr()
    lift_matrix.eliminate_zeros()
    # ----------------------------

    # Calcular el umbral global para 'Clientes_Compraron':
    # Se toma el percentil 10 de los valores no cero de la matriz de coocurrencia y se compara con 5.
    nonzero_cooccur = cooccurrence.data
    if nonzero_cooccur.size > 0:
        p10 = np.percentile(nonzero_cooccur, 10)
    else:
        p10 = 0
    min_cooccur_threshold = max(5, int(p10))

    # Crear diccionarios inversos para la salida final
    idx_to_user = {i: uid for uid, i in user_to_idx.items()}
    idx_to_item = {i: iid for iid, i in item_to_idx.items()}

    recommendations = deque()
    n_users = user_item_matrix.shape[0]

    # Procesar usuarios en bloques para mantener escalabilidad
    for batch_start in range(0, n_users, batch_size):
        batch_end = min(batch_start + batch_size, n_users)
        batch_matrix = user_item_matrix[batch_start:batch_end]

        # Calcular los puntajes de lift para cada usuario en el bloque
        batch_lift_scores = batch_matrix.dot(lift_matrix).toarray()
        # Calcular los puntajes de coocurrencia (Clientes_Compraron) para el bloque
        batch_cooccur_scores = batch_matrix.dot(cooccurrence).toarray()

        # Para cada usuario en el bloque
        for i in range(batch_end - batch_start):
            user_idx = batch_start + i
            # Obtener los índices de los artículos que ya compró el usuario
            user_items = batch_matrix[i].nonzero()[1]
            if user_items.size == 0:
                continue  # Saltar usuarios sin compras

            # Excluir los artículos ya comprados (asignar puntaje negativo)
            batch_lift_scores[i, user_items] = -1
            batch_cooccur_scores[i, user_items] = -1

            # Seleccionar los mejores n_recommendations basados en lift
            if n_recommendations < batch_lift_scores.shape[1]:
                top_unsorted = np.argpartition(
                    -batch_lift_scores[i], n_recommendations
                )[:n_recommendations]
                top_item_indices = top_unsorted[
                    np.argsort(batch_lift_scores[i][top_unsorted])[::-1]
                ]
            else:
                top_item_indices = np.argsort(batch_lift_scores[i])[::-1][
                    :n_recommendations
                ]

            # Evaluar cada recomendación candidata
            for item_idx in top_item_indices:
                lift_score = batch_lift_scores[i, item_idx]
                if lift_score > 0:
                    cooccur_count = int(batch_cooccur_scores[i, item_idx])
                    # Solo incluir recomendaciones con Clientes_Compraron >= umbral
                    if cooccur_count >= min_cooccur_threshold:
                        recommendations.append(
                            {
                                "Monedero_Id": idx_to_user[user_idx],
                                "Articulo_Id_Recomendado": idx_to_item[item_idx],
                                "Lift_Score": lift_score,
                                "Clientes_Compraron": cooccur_count,
                                "Articulos_Id_Relacionados": ",".join(
                                    str(idx_to_item[j]) for j in user_items
                                ),
                            }
                        )

    return pl.DataFrame(recommendations)


# Ejemplo de uso:
if __name__ == "__main__":
    import time
    # En caso de no ejecutar la consulta real, se puede usar un DataFrame de ejemplo.
    # sample_purchases = pl.DataFrame({
    #     'Monedero_Id': [1, 1, 2, 2, 3, 3, 4],
    #     'ArticuloPadre_Id': [101, 102, 102, 103, 101, 103, 102],
    #     'Frecuencia': [1, 1, 1, 1, 1, 1, 1]
    # })

    start_time = time.time()
    # Obtener los datos de compras (en producción se usa get_customer_purchases)
    sample_purchases_pl = get_customer_purchases(dwh_farinter_database_admin)
    # Generar recomendaciones utilizando lift como criterio
    recommendations_df = generate_recommendations(
        sample_purchases_pl,
        n_recommendations=3,
        batch_size=2,
        lift_threshold=1.0,  # Se filtran asociaciones con lift inferior a 1.0
    )
    end_time = time.time()

    print(f"Tiempo de procesamiento: {end_time - start_time:.2f} segundos")
    print("\nRecomendaciones:")
    print(recommendations_df)
    print(recommendations_df.describe())
