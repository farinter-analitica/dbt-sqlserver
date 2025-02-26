# test_recomendacion_articulo.py

import pytest
import polars as pl
import numpy as np
import scipy.sparse as sp
from datetime import datetime, date
from dagster_kielsa_gf.assets.recomendaciones.articulo import (
    create_invoice_item_matrix,
    compute_cooccurrence_matrix,
    compute_lift_matrix,
    filter_cooccurrence,
    generate_article_recommendations,
)


def test_create_invoice_item_matrix():
    """
    Validate that the matrix is correctly constructed for a small dataset.
    """
    data = {
        "Factura_Id": ["F1", "F1", "F2", "F2", "F3"],
        "ArticuloPadre_Id": ["A", "B", "B", "C", "A"],
        "Frecuencia": [1, 2, 1, 3, 1],
    }
    df = pl.DataFrame(data)
    mat, fact2idx, item2idx = create_invoice_item_matrix(df)

    assert mat.shape == (3, 3)  # 3 invoices (F1,F2,F3) x 3 items (A,B,C)
    
    # Check F1 row
    idx_f1 = fact2idx["F1"]
    idx_a = item2idx["A"]
    idx_b = item2idx["B"]
    idx_c = item2idx["C"]

    # Verify that F1 contains A and B (binary presence, not frequency)
    row_f1 = mat[idx_f1].toarray().ravel()
    assert row_f1[idx_a] == 1
    assert row_f1[idx_b] == 1
    assert row_f1[idx_c] == 0  # F1 doesn't have C


def test_compute_cooccurrence_matrix():
    """
    Test that co-occurrence is calculated correctly in a controlled example.
    """
    # Simulate 3 invoices x 3 items
    #   F1 -> A,B
    #   F2 -> B,C
    #   F3 -> A
    data = np.array([1, 1, 1, 1, 1])  # 5 "1"s
    row_indices = np.array([0, 0, 1, 1, 2])  # invoice
    col_indices = np.array([0, 1, 1, 2, 0])  # item
    
    fact_item = sp.csr_matrix((data, (row_indices, col_indices)), shape=(3, 3))

    coocc = compute_cooccurrence_matrix(fact_item)
    
    coocc_array = coocc.toarray()
    assert coocc_array.shape == (3, 3)
    assert coocc_array[0, 1] == 1  # A,B
    assert coocc_array[1, 0] == 1  # B,A
    assert coocc_array[0, 2] == 0  # A,C
    assert coocc_array[1, 2] == 1  # B,C
    assert coocc_array[2, 1] == 1  # C,B
    assert coocc_array[2, 0] == 0  # C,A
    # Diagonal should be zero
    assert coocc_array[0, 0] == 0
    assert coocc_array[1, 1] == 0
    assert coocc_array[2, 2] == 0


def test_compute_lift_matrix():
    """
    Validate lift calculation in a controlled case.
    """
    # Reuse matrix from previous test
    data = np.array([1, 1, 1, 1, 1])
    row_indices = np.array([0, 0, 1, 1, 2])
    col_indices = np.array([0, 1, 1, 2, 0])
    fact_item = sp.csr_matrix((data, (row_indices, col_indices)), shape=(3, 3))
    coocc = compute_cooccurrence_matrix(fact_item)

    # freq(A) => invoice0, invoice2 => 2
    # freq(B) => invoice0, invoice1 => 2
    # freq(C) => invoice1 => 1
    # total_invoices = 3

    # coocc(A,B)=1 => expected(A,B)= (2*2)/3 = 4/3 => lift= 1/(4/3) = 3/4=0.75
    # coocc(B,C)=1 => freq(B)=2, freq(C)=1 => expected= (2*1)/3=2/3 => lift= (1)/(2/3)=1.5
    # coocc(A,C)=0 => lift= 0

    lift = compute_lift_matrix(fact_item, coocc)
    arr_lift = lift.toarray()

    # Check (A,B) => (0,1)
    assert arr_lift[0, 1] == pytest.approx(0.75, 0.00001)
    # Check (B,C) => (1,2)
    assert arr_lift[1, 2] == pytest.approx(1.5, 0.00001)
    # Check diagonal is zero
    assert arr_lift[0, 0] == 0
    assert arr_lift[1, 1] == 0
    assert arr_lift[2, 2] == 0


def test_filter_cooccurrence():
    """
    Test co-occurrence filtering: remove pairs with < min_threshold co-occurrence
    """
    # Create "fictitious" 4x4 co-occurrence matrix
    # items (0,1,2,3)
    coocc_data = np.array([1, 2, 3, 1])
    rows = np.array([0, 1, 1, 2])
    cols = np.array([1, 0, 2, 3])
    # coocc(0,1)=1, coocc(1,0)=2, coocc(1,2)=3, coocc(2,3)=1
    coocc_m = sp.coo_matrix((coocc_data, (rows, cols)), shape=(4, 4)).tocsr()

    filtered = filter_cooccurrence(coocc_m, min_cooccur_count=2)
    arr_f = filtered.toarray()
    
    # We expect coocc(0,1)=1 to be removed 
    # coocc(1,0)=2 to be kept
    # coocc(1,2)=3 to be kept
    # coocc(2,3)=1 to be removed

    assert arr_f[0, 1] == 0
    assert arr_f[1, 0] == 2
    assert arr_f[1, 2] == 3
    assert arr_f[2, 3] == 0


def test_generate_article_recommendations_complex():
    """
    Verify the behavior of generate_article_recommendations with a dataset
    that allows generating up to 5 recommendations per article, as in the real case.
    """
    # Expanded dataset with more complex relationships between articles
    data = {
        "Factura_Id": [
            # Invoices with pairs of articles
            "F1", "F1",  # A+B
            "F2", "F2",  # A+C
            "F3", "F3",  # A+D
            "F4", "F4",  # B+C
            "F5", "F5",  # B+D
            "F6", "F6",  # C+D
            "F1", "F1",  # A+B
            "F2", "F2",  # A+C
            "F3", "F3",  # A+D
            "F4", "F4",  # B+C
            "F5", "F5",  # B+D
            "F6", "F6",  # C+D
            # Invoices with triplets of articles
            "F7", "F7", "F7",  # A+E+F
            "F8", "F8", "F8",  # B+E+G
            "F9", "F9", "F9",  # C+F+G
            # Invoice with multiple articles
            "F10", "F10", "F10", "F10", "F10",  # A+B+C+D+E
        ],
        "ArticuloPadre_Id": [
            # Pairs
            "A", "B",
            "A", "C",
            "A", "D",
            "B", "C",
            "B", "D",
            "C", "D", 
            "A", "B",
            "A", "C",
            "A", "D",
            "B", "C",
            "B", "D",
            "C", "D",
            # Triplets
            "A", "E", "F",
            "B", "E", "G",
            "C", "F", "G",
            # Multiple
            "A", "B", "C", "D", "E",
        ],
        "Frecuencia": [1] * 38,  # 26 records total
    }
    df = pl.DataFrame(data)

    # Execute the function with appropriate parameters for this dataset
    recs = generate_article_recommendations(
        df,
        max_n_recommendations=5,  # Request 5 recommendations per article
        min_cooccur_threshold=2,  # Minimum co-occurrence threshold
        min_lift_threshold=0.01,  # Low lift threshold for this test
        min_confidence_level=0.01,  # Low significance threshold for this test
    )

    # Basic verifications
    assert not recs.is_empty(), "No recommendations were generated"

    # Verify columns
    required_columns = [
        "Articulo_Id", 
        "Articulo_Id_Relacionado", 
        "Lift_Score", 
        "Facturas_Conjuntas",
        "Fecha_Generacion"
    ]
    assert all(col in recs.columns for col in required_columns), "Missing required columns"

    # Organize recommendations by article
    rec_dict = {}
    for row in recs.select(["Articulo_Id", "Articulo_Id_Relacionado"]).iter_rows():
        if row[0] not in rec_dict:
            rec_dict[row[0]] = []
        rec_dict[row[0]].append(row[1])

    # Check key articles
    # Article A appears in 5 invoices with B, C, D, E, F - should have 5 recommendations
    if "A" in rec_dict:
        # Verify A has recommendations
        assert len(rec_dict["A"]) > 0, "Article A has no recommendations"
        # Verify A recommends some of the expected articles
        expected_for_A = {"B", "C", "D", "E", "F"}
        actual_for_A = set(rec_dict["A"])
        assert len(actual_for_A.intersection(expected_for_A)) > 0, (
            f"A should recommend some of {expected_for_A}, but recommends: {actual_for_A}"
        )

    # Verify multiple recommendations are generated for some articles
    articles_with_multiple_recs = [art for art, recs in rec_dict.items() if len(recs) > 1]
    assert len(articles_with_multiple_recs) > 0, "No article has multiple recommendations"

    # Verify some articles have close to 5 recommendations
    max_recs_count = max([len(recs) for recs in rec_dict.values()], default=0)
    assert max_recs_count > 2, (
        f"The maximum number of recommendations per article is {max_recs_count}, expected close to 5"
    )

    # Verify validity of scores
    assert all(recs["Lift_Score"] > 0), "There are recommendations with Lift_Score <= 0"
    assert all(recs["Facturas_Conjuntas"] > 0), "There are recommendations with Facturas_Conjuntas <= 0"
    
    # Verify Fecha_Generacion is a valid date
    assert all(isinstance(_date_, date) for _date_ in recs["Fecha_Generacion"].to_list()), (
        "Fecha_Generacion contains invalid dates"
    )


def test_generate_article_recommendations_empty():
    """
    Test behavior with an empty dataset.
    """
    df = pl.DataFrame({
        "Factura_Id": [],
        "ArticuloPadre_Id": [],
        "Frecuencia": []
    })
    
    # Llamar a la función con un umbral alto para asegurar que no haya recomendaciones
    with pytest.raises(ValueError):
        generate_article_recommendations(
            df, 
            max_n_recommendations=1, 
            min_cooccur_threshold=100, 
            min_lift_threshold=100.0,
            min_confidence_level=99.99
        )


def test_generate_article_recommendations_single_item():
    """
    Test behavior with a dataset where each invoice has only one item.
    """
    data = {
        "Factura_Id": ["F1", "F2", "F3"],
        "ArticuloPadre_Id": ["A", "B", "C"],
        "Frecuencia": [1, 1, 1]
    }
    df = pl.DataFrame(data)
    
    # Llamar a la función con un umbral alto para asegurar que no haya recomendaciones
    with pytest.raises(ValueError):
        generate_article_recommendations(
            df, 
            max_n_recommendations=1, 
            min_cooccur_threshold=100, 
            min_lift_threshold=100.0,
            min_confidence_level=99.99
        )
