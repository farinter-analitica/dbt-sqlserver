import warnings
from collections import deque
from datetime import datetime
from typing import Sequence

import numpy as np
import pendulum as pdl
import polars as pl
import scipy.sparse as sp
from dagster import (
    AssetChecksDefinition,
    AssetKey,
    AssetsDefinition,
    In,
    Nothing,
    Out,
    asset,
    graph,
    instance_for_test,
    load_asset_checks_from_current_module,
    load_assets_from_current_module,
    materialize,
    op,
)

from dagster_shared_gf.automation import automation_weekly_7_delta_1_cron
from dagster_shared_gf.resources.smb_resources import (
    smb_resource_staging_dagster_dwh,
)
from dagster_shared_gf.resources.sql_server_resources import (
    SQLServerResource,
    dwh_farinter_dl,
)
from dagster_shared_gf.shared_functions import SQLScriptGenerator
from dagster_shared_gf.shared_variables import env_str, tags_repo


@op(
    ins={
        "DL_Kielsa_FacturasPosiciones": In(
            dagster_type=Nothing,
            asset_key=AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_FacturasPosiciones"]),
        )
    },
    out={
        "df_purchases": Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"),
    },
)
def get_customer_purchases_for_recom(
    dwh_farinter_dl: SQLServerResource,
) -> pl.DataFrame:
    meses_muestra = 2
    lista_fechas_muestra = [
        pdl.today().subtract(months=i) for i in range(meses_muestra + 1)
    ]
    lista_aniomes = [fecha.year * 100 + fecha.month for fecha in lista_fechas_muestra]

    sql_query = f"""
    SELECT 
        DC.Monedero_Id,
        FA.ArticuloPadre_Id,
        --DC.Articulos,
        COUNT(*) AS Frecuencia
    FROM
        DL_FARINTER.dbo.DL_Kielsa_FacturasPosiciones FA WITH(NOLOCK)
    INNER JOIN
        (SELECT {"TOP 10000" if env_str == "local" else ""}
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
            F.Factura_Fecha >= '{min(lista_fechas_muestra).strftime("%Y%m%d")}' AND
            F.TipoDoc_Id = 1 AND
            M.Activo_Indicador = 1 
        GROUP BY
            M.Monedero_Id
        HAVING COUNT(DISTINCT ArticuloPadre_Id) > 1
        ) DC 
    ON FA.MonederoTarj_Id_Limpio = DC.Monedero_Id
    WHERE
        FA.Emp_Id = 1 AND 
        FA.AnioMes_Id IN ({", ".join(map(str, lista_aniomes))}) AND 
        FA.Factura_Fecha >= '{min(lista_fechas_muestra).strftime("%Y%m%d")}' AND
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


@op
def create_user_item_matrix(
    purchases_df: pl.DataFrame,
) -> tuple[sp.csr_matrix, dict, dict]:
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

    # Mapeo vectorizado: usar np.searchsorted ya que los arrays están ordenados
    user_indices = np.searchsorted(user_ids, monedero_array)
    item_indices = np.searchsorted(item_ids, articulo_array)

    # Utilizar la columna "Frecuencia" para asignar el peso de cada compra
    frecuencia_array = purchases_df.get_column("Frecuencia").to_numpy()
    # Utilizar la frecuencia real (convertida a int32)
    data = (frecuencia_array > 0).astype(np.int32)

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


@op
def compute_cooccurrence_matrix(
    user_item_matrix: sp.csr_matrix, chunk_size: int = 10_000
) -> sp.csr_matrix:
    """
    Calcula la matriz de coocurrencia de artículos de forma eficiente en memoria.
    Cada elemento (i, j) indica la suma de las coocurrencias (ponderadas por Frecuencia)
    entre el artículo i y el artículo j.
    """
    if user_item_matrix.shape is None:
        raise ValueError("La matriz de usuario-artículo está vacía.")

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

    return cooccurrence  # type: ignore


@op
def filter_cooccurrence(
    cooccurrence: sp.csr_matrix, min_cooccur_count: int = 3
) -> sp.csr_matrix:
    """
    Elimina pares (i, j) cuya co-ocurrencia sea < min_cooccur_count.
    El primer filtro (en la matriz global) se asegura de no incluir pares (i, j) con evidencia muy escasa en toda la data (menos de X clientes).
    """

    cooc_coo = cooccurrence.tocoo()
    mask = cooc_coo.data >= min_cooccur_count

    cooc_filtered = sp.coo_matrix(
        (cooc_coo.data[mask], (cooc_coo.row[mask], cooc_coo.col[mask])),
        shape=cooc_coo.shape,
    ).tocsr()

    cooc_filtered.eliminate_zeros()
    return cooc_filtered  # type: ignore


@op
def compute_lift_matrix(
    user_item_matrix: sp.csr_matrix, cooccurrence: sp.csr_matrix
) -> sp.csr_matrix:
    """
    Calcula la matriz de lift a partir de la matriz de coocurrencia.

    La fórmula utilizada es:
       lift(i, j) = (coocurrencia observada para i y j) / ((frecuencia(i) * frecuencia(j)) / total_usuarios)

    Donde:
      - frecuencia(i) es el número (ponderado) de usuarios que compraron el artículo i.
      - total_usuarios es el número total de clientes.
    """
    if user_item_matrix.shape is None:
        raise ValueError("La matriz de usuario-artículo está vacía.")

    total_users = user_item_matrix.shape[0]
    # Calcular la frecuencia de cada artículo (suma de los pesos por columna)
    item_counts = np.array(
        user_item_matrix.sum(axis=0)
    ).ravel()  # Vector de frecuencias

    # Convertir la matriz de coocurrencia a formato COO para iterar sobre sus elementos no nulos
    cooc_coo = cooccurrence.tocoo()
    lift_data = deque()

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
    return lift_matrix.tocsr()  # type: ignore


@op(
    out={
        "recommendations": Out(
            pl.DataFrame, io_manager_key="polars_parquet_io_manager"
        ),
    }
)
def generate_customer_recommendations(
    purchases_df: pl.DataFrame,
    n_recommendations: int = 5,
    batch_size: int = 1000,
    lift_threshold: float = 1.0,  # Lo normal es 1.0, pero soportaremos menores con una co-ocurrencia alta para obtener mas resultados
    min_cooccur_threshold: int = 5,
) -> pl.DataFrame:
    """
    Genera recomendaciones de productos para cada cliente basadas en lo que otros han comprado.

    Se utilizan dos métricas:
      - Lift: Mide la fuerza de la asociación entre artículos.
      - Clientes_Compraron: Suma de las coocurrencias (ponderadas por frecuencia) entre los artículos
        que el cliente ya compró y el artículo candidato.

    Se filtran u ordenan aquellas asociaciones cuyo lift sea inferior al umbral (por defecto 1.0) y, además, se
    descartan recomendaciones cuya métrica 'Clientes_Compraron' sea menor que el percentil 10 de todos los
    valores no cero o 5, lo que sea mayor.
    """
    # Construir la matriz usuario–artículo y obtener los mapeos
    user_item_matrix, user_to_idx, item_to_idx = create_user_item_matrix(purchases_df)
    raw_cooccurrence = compute_cooccurrence_matrix(user_item_matrix)
    # -- Determinar umbral de co-ocurrencia: tomamos p10 de valores no cero o 5, lo que sea mayor --
    nonzero_cooccur = raw_cooccurrence.data
    if nonzero_cooccur.size > 0:
        p10 = int(np.percentile(nonzero_cooccur, 10))
    else:
        p10 = 0
    min_cooccur_threshold = max(min_cooccur_threshold, p10)
    # Calcular la matriz de coocurrencia
    cooccurrence = filter_cooccurrence(
        raw_cooccurrence, min_cooccur_count=min_cooccur_threshold
    )

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

    # Crear diccionarios inversos para la salida final
    idx_to_user = {i: uid for uid, i in user_to_idx.items()}
    idx_to_item = {i: iid for iid, i in item_to_idx.items()}

    recommendations = deque()
    n_users = user_item_matrix.shape[0]

    # Procesar usuarios en bloques para mantener escalabilidad
    for batch_start in range(0, n_users, batch_size):
        batch_end = min(batch_start + batch_size, n_users)
        batch_matrix = user_item_matrix[batch_start:batch_end]

        # Puntajes de lift y coocurrencia para este bloque de usuarios
        batch_lift_scores = batch_matrix.dot(lift_matrix).toarray()
        batch_cooccur_scores = batch_matrix.dot(cooccurrence).toarray()

        # Por cada usuario en el bloque
        for i in range(batch_end - batch_start):
            user_idx = batch_start + i
            # Artículos que ya compró
            user_items = batch_matrix[i].nonzero()[1]
            if user_items.size < 2:
                continue  # no tiene compras suficientes, no sugerimos nada

            # Quitar los artículos ya comprados (asignar -1)
            batch_lift_scores[i, user_items] = -1
            batch_cooccur_scores[i, user_items] = -1

            # Escoger los mejores N (top N) por lift
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

            # Validar cada artículo recomendado
            for item_idx in top_item_indices:
                lift_score = batch_lift_scores[i, item_idx]
                if lift_score > 0:
                    cooccur_count = int(batch_cooccur_scores[i, item_idx])
                    # Exigir que la coocurrencia sea >= min_cooccur_threshold
                    # (ya filtramos globalmente, pero aquí puedes reforzar la verificación)
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


@op
def save_customer_recommendations(
    dwh_farinter_dl: SQLServerResource, recommendations: pl.DataFrame
) -> None:
    print(f"Por guardar {len(recommendations)} recomendaciones")
    with dwh_farinter_dl.get_sqlalchemy_conn() as conn:
        if env_str == "local":
            return

        sg = SQLScriptGenerator(
            primary_keys=("Monedero_Id", "Articulo_Id_Recomendado"),
            db_schema="dbo",
            table_name="DL_Kielsa_Cliente_ArticuloRecomendado",
            df=recommendations,
            temp_table_name="DL_Kielsa_Cliente_ArticuloRecomendado_NEW",
        )

        dwh_farinter_dl.execute_and_commit(
            sg.drop_table_sql_script(temp=True), connection=conn
        )
        dwh_farinter_dl.execute_and_commit(
            sg.create_table_sql_script(temp=True), connection=conn
        )
        dwh_farinter_dl.execute_and_commit(
            sg.columnstore_table_sql_script(temp=True), connection=conn
        )

        # First write as regular table
        recommendations.write_database(
            table_name=sg.temp_table_name,
            connection=conn,
            if_table_exists="append",
        )

        dwh_farinter_dl.execute_and_commit(
            sg.primary_key_table_sql_script(temp=True), connection=conn
        )

        dwh_farinter_dl.execute_and_commit(
            sg.swap_table_with_temp(), connection=conn
        )


@graph(tags=tags_repo.Weekly | tags_repo.UniquePeriod | tags_repo.AutomationOnly)
def cliente_recomendacion_graph():
    df_purchases = get_customer_purchases_for_recom()
    recommendations = generate_customer_recommendations(df_purchases)
    return save_customer_recommendations(recommendations)


DL_Kielsa_Cliente_ArticuloRecomendado = AssetsDefinition.from_graph(
    graph_def=cliente_recomendacion_graph,
    keys_by_output_name={
        "result": AssetKey(
            ["DL_FARINTER", "dbo", "DL_Kielsa_Cliente_ArticuloRecomendado"]
        )
    },
    tags_by_output_name={
        "result": tags_repo.Weekly | tags_repo.UniquePeriod | tags_repo.AutomationOnly
    },
    automation_conditions_by_output_name={"result": automation_weekly_7_delta_1_cron},
)

all_assets = tuple(load_assets_from_current_module())
all_asset_checks: Sequence[AssetChecksDefinition] = tuple(
    load_asset_checks_from_current_module()
)


if __name__ == "__main__":
    from dagster import instance_for_test
    from dagster_polars import PolarsParquetIOManager

    start_time = datetime.now()
    with instance_for_test() as instance:
        from dagster import ResourceDefinition

        if env_str == "local":
            warnings.warn(
                "Running in local mode, using top 10000 rows and no loading to SQL Server"
            )

        @asset(name="between_asset")
        def mock_between_asset() -> int:
            return 1

        mock_dwh_farinter_bi = ResourceDefinition.mock_resource()
        mock_dwh_farinter_dl = ResourceDefinition.mock_resource()

        result = materialize(
            assets=[mock_between_asset, DL_Kielsa_Cliente_ArticuloRecomendado],
            instance=instance,
            resources={
                "dwh_farinter_dl": dwh_farinter_dl,
                "dwh_farinter_bi": mock_dwh_farinter_bi,
                "smb_resource_staging_dagster_dwh": smb_resource_staging_dagster_dwh,
                "polars_parquet_io_manager": PolarsParquetIOManager(),
            },
        )
        print(
            result.output_for_node(DL_Kielsa_Cliente_ArticuloRecomendado.node_def.name)
        )

    end_time = datetime.now()
    print(
        f"Tiempo de ejecución: {end_time - start_time}, desde {start_time}, hasta {end_time}"
    )
