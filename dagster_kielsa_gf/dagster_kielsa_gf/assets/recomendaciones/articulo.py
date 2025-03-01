import warnings
from collections import deque
from datetime import datetime

from dagster_shared_gf.shared_helpers import SQLScriptGenerator
import numpy as np
import pendulum as pdl
import polars as pl
import scipy.sparse as sp
from dagster import (
    AssetKey,
    AssetsDefinition,
    Field,
    In,
    Nothing,
    OpExecutionContext,
    Out,
    asset,
    graph,
    instance_for_test,
    materialize,
    op,
)
from dagster._core.definitions.input import InputDefinition

from dagster_shared_gf.automation import automation_weekly_7_delta_1_cron
from dagster_shared_gf.resources.smb_resources import (
    smb_resource_staging_dagster_dwh,
)
from dagster_shared_gf.resources.sql_server_resources import (
    SQLServerResource,
)
from dagster_shared_gf.shared_functions import (
    get_chi_square_threshold,
)
from dagster_shared_gf.shared_variables import env_str, tags_repo


@op(
    ins={
        "BI_Kielsa_Hecho_FacturaPosicion": In(
            dagster_type=Nothing,
            asset_key=AssetKey(
                ["BI_FARINTER", "dbo", "BI_Kielsa_Hecho_FacturaPosicion"]
            ),
        ),
        "BI_Kielsa_Dim_Articulo": In(
            dagster_type=Nothing,
            asset_key=AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Dim_Articulo"]),
        ),
    },
    out={
        "df_purchases": Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"),
    },
    config_schema={"meses_muestra": Field(int, is_required=False, default_value=6)},
)
def get_article_purchases_for_recom(
    context: OpExecutionContext,
    dwh_farinter_dl: SQLServerResource,
) -> pl.DataFrame:
    meses_muestra = context.op_config["meses_muestra"]
    lista_fechas_muestra = [
        pdl.today().subtract(months=i) for i in range(meses_muestra + 1)
    ]
    lista_aniomes = [fecha.year * 100 + fecha.month for fecha in lista_fechas_muestra]

    sql_query = f"""
    SELECT 
        FA.EmpSucDocCajFac_Id AS Factura_Id,
        A.Articulo_Codigo_Padre AS ArticuloPadre_Id,
        --DC.Articulos,
        COUNT(*) AS Frecuencia
    FROM
        BI_FARINTER.dbo.BI_Kielsa_Hecho_FacturaPosicion FA WITH(NOLOCK)
    INNER JOIN
        (SELECT {"TOP 10000" if env_str == "local" else ""}
            F.EmpSucDocCajFac_Id,
            COUNT(DISTINCT A.Articulo_Codigo_Padre) AS Articulos
        FROM
            BI_FARINTER.dbo.BI_Kielsa_Hecho_FacturaPosicion F WITH(NOLOCK)
        INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo A 
            ON F.Articulo_Id = A.Articulo_Id
            AND F.Emp_Id = A.Emp_Id
        WHERE
            F.Emp_Id = 1 AND 
            F.AnioMes_Id IN ({", ".join(map(str, lista_aniomes))}) AND
            F.Factura_Fecha >= '{min(lista_fechas_muestra).strftime("%Y%m%d")}' AND
            F.TipoDoc_Id = 1 AND
            A.Articulo_Activo = 1
        GROUP BY
            F.EmpSucDocCajFac_Id
        HAVING COUNT(DISTINCT A.Articulo_Codigo_Padre) > 1
        ) DC 
        ON FA.EmpSucDocCajFac_Id = DC.EmpSucDocCajFac_Id
    INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo A 
        ON FA.Articulo_Id = A.Articulo_Id
        AND FA.Emp_Id = A.Emp_Id
    WHERE
        FA.Emp_Id = 1 AND 
        FA.AnioMes_Id IN ({", ".join(map(str, lista_aniomes))}) AND 
        FA.Factura_Fecha >= '{min(lista_fechas_muestra).strftime("%Y%m%d")}' AND
        FA.TipoDoc_Id = 1 AND
        A.Articulo_Activo = 1
    GROUP BY
        FA.EmpSucDocCajFac_Id,
        A.Articulo_Codigo_Padre,
        DC.Articulos
    """
    main_query = (
        pl.read_database(sql_query, dwh_farinter_dl.get_arrow_odbc_conn_string())
        .lazy()
        .collect(streaming=True)
    )
    return main_query


@op
def create_invoice_item_matrix(
    purchases_df: pl.DataFrame,
) -> tuple[sp.csr_matrix, dict, dict]:
    """
    Convierte los datos de compras en una matriz dispersa factura-artículo.

    Se utilizan los campos:
      - Factura_Id (identificador de la factura)
      - ArticuloPadre_Id (identificador del artículo)
      - Frecuencia (cantidad de compras; se utiliza como peso en la matriz)

    Se asume que los IDs son textos.

    Parameters
    ----------
    purchases_df : pl.DataFrame
        Dataframe de compras.

    Returns
    -------
    tuple[sp.csr_matrix, dict, dict]
        Matriz dispersa factura-artículo, diccionario de mapeo de factura a índice, diccionario de mapeo de artículo a índice.
    """
    # Extraer los IDs únicos y ordenados de usuario y artículo
    fact_ids = purchases_df.get_column("Factura_Id").unique().sort().to_numpy()
    item_ids = purchases_df.get_column("ArticuloPadre_Id").unique().sort().to_numpy()

    # Convertir las columnas originales a arrays de NumPy
    factura_array = purchases_df.get_column("Factura_Id").to_numpy()
    articulo_array = purchases_df.get_column("ArticuloPadre_Id").to_numpy()

    # Mapeo vectorizado: usar np.searchsorted ya que los arrays están ordenados
    factura_indices = np.searchsorted(fact_ids, factura_array)
    item_indices = np.searchsorted(item_ids, articulo_array)

    # Utilizar cantidad o simplemente 1 para indicar presencia
    data = np.ones(len(factura_array), dtype=np.int32)  # o usar la cantidad

    # Construir la matriz dispersa (filas: usuarios, columnas: artículos)
    matrix = sp.csr_matrix(
        (data, (factura_indices, item_indices)),
        shape=(len(fact_ids), len(item_ids)),
        dtype=np.int32,
    )

    # Crear diccionarios de mapeo para búsquedas inversas
    fact_to_idx = {uid: i for i, uid in enumerate(fact_ids)}
    item_to_idx = {iid: i for i, iid in enumerate(item_ids)}

    return matrix, fact_to_idx, item_to_idx


@op
def compute_cooccurrence_matrix(
    fact_item_matrix: sp.csr_matrix, chunk_size: int = 10_000
) -> sp.csr_matrix:
    """
    Calcula la matriz de coocurrencia de artículos de forma eficiente en memoria.
    Cada elemento (i, j) indica la suma de las coocurrencias (ponderadas por Frecuencia)
    entre el artículo i y el artículo j.
    """
    if fact_item_matrix.shape is None:
        raise ValueError("La matriz de usuario-artículo está vacía.")

    n_items = fact_item_matrix.shape[1]
    # Inicializar la matriz de coocurrencia en formato LIL (fácil de modificar)
    cooccurrence = sp.lil_matrix((n_items, n_items), dtype=np.int32)

    # Procesar en bloques para gestionar la memoria
    for i in range(0, n_items, chunk_size):
        end = min(i + chunk_size, n_items)
        # Obtener un bloque de la matriz transpuesta (artículos)
        chunk = fact_item_matrix.T[i:end]
        # Acumular la coocurrencia multiplicando por la matriz original
        cooccurrence[i:end] = chunk @ fact_item_matrix

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
    fact_item_matrix: sp.csr_matrix, cooccurrence: sp.csr_matrix
) -> sp.csr_matrix:
    """
    Calcula la matriz de lift a partir de la matriz de coocurrencia.

    La fórmula utilizada es:
       lift(i, j) = (coocurrencia observada para i y j) / ((frecuencia(i) * frecuencia(j)) / total_usuarios)

    Donde:
      - frecuencia(i) es el número (ponderado) de usuarios que compraron el artículo i.
      - total_usuarios es el número total de clientes.
    """
    if fact_item_matrix.shape is None:
        raise ValueError("La matriz de usuario-artículo está vacía.")

    total_users = fact_item_matrix.shape[0]
    # Calcular la frecuencia de cada artículo (suma de los pesos por columna)
    item_counts = np.array(
        fact_item_matrix.sum(axis=0)
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
def generate_article_recommendations(
    purchases_df: pl.DataFrame,
    max_n_recommendations: int = 20,
    batch_size: int = 1000,
    min_lift_threshold: float = 1.0,
    min_cooccur_threshold: int = 5,
    min_confidence_level: float = 90.0,
) -> pl.DataFrame:
    """
    Genera recomendaciones de productos relacionados basados en compras conjuntas en facturas.

    Se utilizan estas metricas:
    - Lift: Mide la fuerza de la asociación entre artículos.
    - Facturas_Conjuntas: Número de facturas en las que ambos artículos aparecen juntos.
    - Significance_Score: Medida estadística (chi-cuadrado) que indica cuán significativa
      es la asociación considerando el tamaño de la muestra.

    Se filtran asociaciones cuyo lift sea inferior al umbral (por defecto 1.0) y se
    descartan recomendaciones con menos de min_cooccur_threshold facturas conjuntas.

    Parameters
    ----------
    purchases_df : pl.DataFrame
        Dataframe de compras.
    n_recommendations : int, optional
        Número de recomendaciones a generar, por defecto 5.
    batch_size : int, optional
        Tamaño del lote para procesar la matriz, por defecto 1000.
    min_lift_threshold : float, optional
        Umbral mínimo para el lift, por defecto 1.0.
    min_cooccur_threshold : int, optional
        Umbral mínimo para el número de facturas conjuntas, por defecto 5.
    min_confidence_level : float, optional
        Nivel de confianza para el cálculo de la significancia, por defecto 70.0.
    """
    min_significance = get_chi_square_threshold(min_confidence_level)

    # Construir la matriz factura-artículo y obtener los mapeos
    invoice_item_matrix, invoice_to_idx, item_to_idx = create_invoice_item_matrix(
        purchases_df
    )
    raw_cooccurrence = compute_cooccurrence_matrix(invoice_item_matrix)

    # Determinar umbral de co-ocurrencia: tomamos p10 de valores no cero o valor predefinido
    nonzero_cooccur = raw_cooccurrence.data
    if nonzero_cooccur.size > 0:
        p10 = int(np.percentile(nonzero_cooccur, 10))
    else:
        p10 = 0
    min_cooccur_threshold = max(min_cooccur_threshold, p10)

    # Filtrar matriz de co-ocurrencia
    cooccurrence = filter_cooccurrence(
        raw_cooccurrence, min_cooccur_count=min_cooccur_threshold
    )

    # Calcular la matriz de lift a partir de la coocurrencia
    lift_matrix = compute_lift_matrix(invoice_item_matrix, cooccurrence)

    # Calculate significance scores using Chi-Square statistics
    significance_matrix = sp.lil_matrix(lift_matrix.shape, dtype=np.float32)
    item_frequencies = np.array(invoice_item_matrix.sum(axis=0)).ravel()
    total_invoices = invoice_item_matrix.shape[0]

    # Convert to COO for easier iteration
    cooc_coo = cooccurrence.tocoo()
    for i, j, observed in zip(cooc_coo.row, cooc_coo.col, cooc_coo.data):
        # Expected co-occurrence under independence assumption
        expected = (item_frequencies[i] * item_frequencies[j]) / total_invoices

        # Chi-square statistic: (O-E)²/E
        if expected > 0:
            chi_square = ((observed - expected) ** 2) / expected
            # Store significance score
            significance_matrix[i, j] = chi_square

    significance_matrix = significance_matrix.tocsr()

    # Filtrar asociaciones débiles: descartar aquellas con lift inferior al umbral
    lift_coo = lift_matrix.tocoo()
    mask = lift_coo.data >= min_lift_threshold
    filtered_data = np.where(mask, lift_coo.data, 0)
    lift_matrix = sp.coo_matrix(
        (filtered_data, (lift_coo.row, lift_coo.col)), shape=lift_coo.shape
    ).tocsr()
    lift_matrix.eliminate_zeros()

    # Crear diccionario inverso para la salida final
    idx_to_item = {i: iid for iid, i in item_to_idx.items()}

    recommendations = deque()
    n_items = lift_matrix.shape[0] if lift_matrix.shape is not None else 0

    fecha_generacion = datetime.now().date()
    # Procesar artículos en bloques para mantener escalabilidad
    for batch_start in range(0, n_items, batch_size):
        batch_end = min(batch_start + batch_size, n_items)

        # Por cada artículo en el bloque
        for item_idx in range(batch_start, batch_end):
            # Artículos relacionados con este ítem según lift
            item_row = lift_matrix[item_idx]
            related_items = item_row.nonzero()[1]

            if related_items.size == 0:
                continue  # No hay artículos relacionados

            # Obtener puntajes de lift, coocurrencia y significancia
            lift_scores = item_row[0, related_items].toarray().flatten()
            cooccur_scores = cooccurrence[item_idx, related_items].toarray().flatten()
            significance_scores = (
                significance_matrix[item_idx, related_items].toarray().flatten()
            )

            # Create combined score: normalize each factor and use weighted sum
            # First normalize to [0,1] range
            if lift_scores.size > 0:
                norm_lift = (
                    lift_scores / lift_scores.max()
                    if lift_scores.max() > 0
                    else lift_scores
                )
                norm_cooccur = (
                    cooccur_scores / cooccur_scores.max()
                    if cooccur_scores.max() > 0
                    else cooccur_scores
                )
                norm_signif = (
                    significance_scores / significance_scores.max()
                    if significance_scores.max() > 0
                    else significance_scores
                )

                # Weighted combined score (adjust weights as needed)
                combined_scores = (
                    3 * norm_lift + 4 * norm_cooccur + 3 * norm_signif
                ) / 10.0
            else:
                combined_scores = np.array([])

            # Ordenar por combined score
            if max_n_recommendations < related_items.size:
                top_unsorted = np.argpartition(-combined_scores, max_n_recommendations)[
                    :max_n_recommendations
                ]
                sorted_indices = top_unsorted[
                    np.argsort(-combined_scores[top_unsorted])
                ]
            else:
                sorted_indices = (
                    np.argsort(-combined_scores)
                    if combined_scores.size > 0
                    else np.array([])
                )

            # Para cada artículo relacionado en el top N
            for idx in sorted_indices[:max_n_recommendations]:
                related_item_idx = related_items[idx]
                lift_score = lift_scores[idx]
                cooccur_count = int(cooccur_scores[idx])
                significance_score = float(significance_scores[idx])
                combined_score = float(combined_scores[idx])

                if (
                    lift_score > min_lift_threshold
                    and cooccur_count >= min_cooccur_threshold
                    and significance_score >= min_significance
                ):
                    recommendations.append(
                        {
                            "Articulo_Id": idx_to_item[item_idx],
                            "Articulo_Id_Relacionado": idx_to_item[related_item_idx],
                            "Lift_Score": float(lift_score),
                            "Facturas_Conjuntas": cooccur_count,
                            "Significance_Score": significance_score,
                            "Combined_Score": combined_score,
                            "Fecha_Generacion": fecha_generacion,
                        }
                    )

    df_recommendations = pl.DataFrame(recommendations)

    if df_recommendations.is_empty():
        raise ValueError("No se encontraron recomendaciones.")

    return df_recommendations.with_columns(
        pl.col("Combined_Score")
        .rank("ordinal", descending=True)
        .over("Articulo_Id")
        .alias("Rank")
    )


@op
def save_article_recommendations(
    dwh_farinter_dl: SQLServerResource, recommendations: pl.DataFrame
) -> None:
    if env_str == "local":
        with pl.Config() as c:
            c.set_tbl_rows(-1)
            c.set_tbl_cols(-1)
            print(recommendations.head(10))
            print(recommendations.describe())
        return
    print(f"Por guardar {len(recommendations)} recomendaciones entre artículos")
    with dwh_farinter_dl.get_sqlalchemy_conn() as conn:
        sg = SQLScriptGenerator(
            primary_keys=("Articulo_Id", "Articulo_Id_Relacionado"),
            db_schema="dbo",
            table_name="DL_Kielsa_Articulo_ArticuloRelacionado",
            df=recommendations,
            temp_table_name="DL_Kielsa_Articulo_ArticuloRelacionado_NEW",
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

        dwh_farinter_dl.execute_and_commit(sg.swap_table_with_temp(), connection=conn)


@graph(
    tags=tags_repo.Weekly | tags_repo.UniquePeriod | tags_repo.AutomationOnly,
    input_defs=[
        InputDefinition(
            name="BI_Kielsa_Hecho_FacturaPosicion",
            dagster_type=Nothing,
            description="Fact table with invoice positions",
            asset_key=AssetKey(
                ["BI_FARINTER", "dbo", "BI_Kielsa_Hecho_FacturaPosicion"]
            ),
        ),
        InputDefinition(
            name="BI_Kielsa_Dim_Articulo",
            dagster_type=Nothing,
            description="Dim table with articles",
            asset_key=AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Dim_Articulo"]),
        ),
    ],
)
def articulo_recomendacion_graph(**kwargs):
    df_purchases = get_article_purchases_for_recom(**kwargs)
    recommendations = generate_article_recommendations(df_purchases)
    return save_article_recommendations(recommendations)


DL_Kielsa_Articulo_ArticuloRelacionado = AssetsDefinition.from_graph(
    graph_def=articulo_recomendacion_graph,
    keys_by_output_name={
        "result": AssetKey(
            ["DL_FARINTER", "dbo", "DL_Kielsa_Articulo_ArticuloRelacionado"]
        )
    },
    tags_by_output_name={
        "result": tags_repo.Weekly | tags_repo.UniquePeriod | tags_repo.AutomationOnly
    },
    automation_conditions_by_output_name={"result": automation_weekly_7_delta_1_cron},
    group_name="recomendaciones",
)


if __name__ == "__main__":
    from dagster import instance_for_test
    from dagster_polars import PolarsParquetIOManager

    from dagster_shared_gf.resources.sql_server_resources import (
        dwh_farinter_dl,
    )

    # print(DL_Kielsa_Articulo_ArticuloRelacionado.asset_deps)
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
            assets=[mock_between_asset, DL_Kielsa_Articulo_ArticuloRelacionado],
            instance=instance,
            resources={
                "dwh_farinter_dl": dwh_farinter_dl,
                "dwh_farinter_bi": mock_dwh_farinter_bi,
                "smb_resource_staging_dagster_dwh": smb_resource_staging_dagster_dwh,
                "polars_parquet_io_manager": PolarsParquetIOManager(),
            },
        )
        print(
            result.output_for_node(DL_Kielsa_Articulo_ArticuloRelacionado.node_def.name)
        )

    end_time = datetime.now()
    print(
        f"Tiempo de ejecución: {end_time - start_time}, desde {start_time}, hasta {end_time}"
    )

    # SELECT TOP (1000) AR.*
    # 		,A.Articulo_Nombre
    # 	    ,A2.Articulo_Nombre AS Relacionado
    #   FROM [DL_FARINTER].[dbo].[DL_Kielsa_Articulo_ArticuloRelacionado] AR
    #   INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo A
    #   ON A.Emp_Id=1
    #   AND AR.Articulo_Id = A.Articulo_Id
    #     INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo A2
    #   ON A2.Emp_Id=1
    #   AND AR.Articulo_Id_Relacionado = A2.Articulo_Id
    #   WHERE A.Articulo_Id = '1110000125'
