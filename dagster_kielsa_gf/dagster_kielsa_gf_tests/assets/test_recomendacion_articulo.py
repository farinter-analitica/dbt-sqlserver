# test_recomendacion_articulo.py

import pytest
import polars as pl
import numpy as np
import scipy.sparse as sp
from dagster_kielsa_gf.assets.recomendacion_articulo import (
    create_invoice_item_matrix,
    compute_cooccurrence_matrix,
    compute_lift_matrix,
    filter_cooccurrence,
    generate_article_recommendations,
)


def test_create_invoice_item_matrix():
    """
    Validamos que la matriz se construya correctamente para un dataset pequeño.
    """
    data = {
        "Factura_Id": ["F1", "F1", "F2", "F2", "F3"],
        "ArticuloPadre_Id": ["A", "B", "B", "C", "A"],
        "Frecuencia": [1, 2, 1, 3, 1],
    }
    df = pl.DataFrame(data)
    mat, fact2idx, item2idx = create_invoice_item_matrix(df)

    assert mat.shape == (3, 3)  # 3 facturas (F1,F2,F3) x 3 artículos (A,B,C)
    # Revisa la fila de F1
    idx_f1 = fact2idx["F1"]
    idx_a = item2idx["A"]
    idx_b = item2idx["B"]
    idx_c = item2idx["C"]

    # Revisar que F1 contiene A y B (freq>0 =>1)
    row_f1 = mat[idx_f1].toarray().ravel()
    assert row_f1[idx_a] == 1
    assert row_f1[idx_b] == 1
    assert row_f1[idx_c] == 0  # F1 no tiene C


def test_compute_cooccurrence_matrix():
    """
    Probar que la co-ocurrencia se calcule correctamente en un ejemplo controlado.
    """
    # Simulamos 3 facturas x 3 items
    #   F1 -> A,B
    #   F2 -> B,C
    #   F3 -> A
    # Esperamos co-ocurrencia(A,B)=1 (de F1), co-ocurrencia(B,C)=1 (F2), etc.

    # Directamente creamos la matriz:
    data = np.array([1, 1, 1, 1, 1])  # 5 "1"s
    row_indices = np.array([0, 0, 1, 1, 2])  # factura
    col_indices = np.array([0, 1, 1, 2, 0])  # item
    # Interpretemos:
    # factura0 => items(0,1) => A,B
    # factura1 => items(1,2) => B,C
    # factura2 => item(0) => A

    fact_item = sp.csr_matrix((data, (row_indices, col_indices)), shape=(3, 3))

    coocc = compute_cooccurrence_matrix(fact_item)
    # coocc debería ser 3x3 (items x items)

    coocc_array = coocc.toarray()
    assert coocc_array.shape == (3, 3)
    assert coocc_array[0, 1] == 1  # A,B
    assert coocc_array[1, 0] == 1
    assert coocc_array[0, 2] == 0  # A,C
    assert coocc_array[1, 2] == 1  # B,C
    assert coocc_array[2, 1] == 1  # C,B
    assert coocc_array[2, 0] == 0  # C,A


def test_compute_lift_matrix():
    """
    Validar lift en un caso controlado.
    """
    # Reusamos la matriz del test anterior:
    data = np.array([1, 1, 1, 1, 1])
    row_indices = np.array([0, 0, 1, 1, 2])
    col_indices = np.array([0, 1, 1, 2, 0])
    fact_item = sp.csr_matrix((data, (row_indices, col_indices)), shape=(3, 3))
    coocc = compute_cooccurrence_matrix(fact_item)

    # freq(A) => factura0, factura2 => 2
    # freq(B) => factura0, factura1 => 2
    # freq(C) => factura1 => 1
    # total_facturas = 3

    # coocc(A,B)=1 => expected(A,B)= (2*2)/3 = 4/3 => lift= 1/(4/3) = 3/4=0.75
    # coocc(B,C)=1 => freq(B)=2, freq(C)=1 => expected= (2*1)/3=2/3 => lift= (1)/(2/3)=1.5
    # coocc(A,C)=0 => lift= 0

    lift = compute_lift_matrix(fact_item, coocc)
    arr_lift = lift.toarray()

    # Revisar (A,B) => (0,1)
    assert arr_lift[0, 1] == pytest.approx(0.75, 0.00001)
    # Revisar (B,C) => (1,2)
    assert arr_lift[1, 2] == pytest.approx(1.5, 0.00001)
    # Revisar diagonal=0
    assert arr_lift[0, 0] == 0


def test_filter_cooccurrence():
    """
    Probar el filtro de co-ocurrencia: eliminamos pares con < 2 coocurrencia
    """
    # Creamos coocurrencia "ficticia" 4x4
    # item(0,1,2,3)
    coocc_data = np.array([1, 2, 3, 1])
    rows = np.array([0, 1, 1, 2])
    cols = np.array([1, 0, 2, 3])
    # coocc(0,1)=1, coocc(1,0)=2, coocc(1,2)=3, coocc(2,3)=1
    coocc_m = sp.coo_matrix((coocc_data, (rows, cols)), shape=(4, 4)).tocsr()

    filtered = filter_cooccurrence(coocc_m, min_cooccur_count=2)
    arr_f = filtered.toarray()
    # Esperamos que coocc(0,1)=1 se elimine
    # coocc(1,0)=2 se conserve
    # coocc(1,2)=3 se conserve
    # coocc(2,3)=1 se elimine

    assert arr_f[0, 1] == 0
    assert arr_f[1, 0] == 2
    assert arr_f[1, 2] == 3
    assert arr_f[2, 3] == 0


def test_generate_article_recommendations():
    """
    Verificar el comportamiento de generate_article_recommendations con un dataset
    que permite generar hasta 5 recomendaciones por artículo, como en el caso real.
    """
    # Dataset expandido con relaciones más complejas entre artículos
    # Creamos 10 facturas con diferentes combinaciones de 7 artículos (A-G)
    data = {
        "Factura_Id": [
            # Facturas con pares de artículos
            "F1",
            "F1",  # A+B
            "F2",
            "F2",  # A+C
            "F3",
            "F3",  # A+D
            "F4",
            "F4",  # B+C
            "F5",
            "F5",  # B+D
            "F6",
            "F6",  # C+D
            # Facturas con ternas de artículos
            "F7",
            "F7",
            "F7",  # A+E+F
            "F8",
            "F8",
            "F8",  # B+E+G
            "F9",
            "F9",
            "F9",  # C+F+G
            # Factura con múltiples artículos
            "F10",
            "F10",
            "F10",
            "F10",
            "F10",  # A+B+C+D+E
        ],
        "ArticuloPadre_Id": [
            # Pares
            "A",
            "B",
            "A",
            "C",
            "A",
            "D",
            "B",
            "C",
            "B",
            "D",
            "C",
            "D",
            # Ternas
            "A",
            "E",
            "F",
            "B",
            "E",
            "G",
            "C",
            "F",
            "G",
            # Múltiples
            "A",
            "B",
            "C",
            "D",
            "E",
        ],
        "Frecuencia": [1] * 26,  # 26 registros total
    }
    df = pl.DataFrame(data)

    # Ejecutar la función con parámetros adecuados para este dataset
    recs = generate_article_recommendations(
        df,
        n_recommendations=5,  # Solicitamos 5 recomendaciones por artículo
        min_cooccur_threshold=1,  # Umbral mínimo de coocurrencia
        lift_threshold=0.1,  # Umbral de lift bajo para este test
    )

    # Verificaciones básicas
    assert not recs.is_empty(), "No se generaron recomendaciones"

    # Verificar columnas
    required_columns = [
        "Articulo_Id",
        "Articulo_Id_Relacionado",
        "Lift_Score",
        "Facturas_Conjuntas",
        "Fecha_Generacion",
    ]
    assert all(col in recs.columns for col in required_columns), (
        "Faltan columnas requeridas"
    )

    # Organizar recomendaciones por artículo
    rec_dict = {}
    for row in recs.select(["Articulo_Id", "Articulo_Id_Relacionado"]).iter_rows():
        if row[0] not in rec_dict:
            rec_dict[row[0]] = []
        rec_dict[row[0]].append(row[1])

    # Verificar artículos clave
    # Artículo A aparece en 5 facturas con B, C, D, E, F - debería tener 5 recomendaciones
    if "A" in rec_dict:
        # Verificar que A tenga recomendaciones
        assert len(rec_dict["A"]) > 0, "El artículo A no tiene recomendaciones"
        # Verificar que A recomiende algunos de los artículos esperados
        expected_for_A = {"B", "C", "D", "E", "F"}
        actual_for_A = set(rec_dict["A"])
        assert len(actual_for_A.intersection(expected_for_A)) > 0, (
            f"A debería recomendar algunos de {expected_for_A}, pero recomienda: {actual_for_A}"
        )

    # Verificar que se generan múltiples recomendaciones para algunos artículos
    articles_with_multiple_recs = [
        art for art, recs in rec_dict.items() if len(recs) > 1
    ]
    assert len(articles_with_multiple_recs) > 0, (
        "Ningún artículo tiene múltiples recomendaciones"
    )

    # Verificar que algunos artículos tengan cerca de 5 recomendaciones
    max_recs_count = max([len(recs) for recs in rec_dict.values()], default=0)
    assert max_recs_count > 2, (
        f"El máximo de recomendaciones por artículo es {max_recs_count}, esperábamos cerca de 5"
    )

    # Verificar validez de los puntajes
    assert all(recs["Lift_Score"] > 0), "Hay recomendaciones con Lift_Score <= 0"
    assert all(recs["Facturas_Conjuntas"] > 0), (
        "Hay recomendaciones con Facturas_Conjuntas <= 0"
    )

    # Opcional: Imprimir un resumen de recomendaciones para diagnóstico
    print(f"Total de recomendaciones generadas: {len(recs)}")
    print(f"Artículos con recomendaciones: {len(rec_dict)}")
    print(
        f"Artículos con 5 recomendaciones: {sum(1 for recs in rec_dict.values() if len(recs) == 5)}"
    )
    print(f"Máximo de recomendaciones por artículo: {max_recs_count}")
