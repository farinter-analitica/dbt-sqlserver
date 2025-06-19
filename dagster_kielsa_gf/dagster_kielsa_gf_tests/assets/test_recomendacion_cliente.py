# test_recommendations.py

import pytest
import polars as pl
import numpy as np
import scipy.sparse as sp
from datetime import datetime
from dagster_kielsa_gf.assets.recomendaciones.cliente_articulo import (
    create_user_item_matrix,
    compute_cooccurrence_matrix,
    compute_lift_matrix,
    filter_cooccurrence,
    generate_customer_recommendations,
)


def test_create_user_item_matrix():
    """
    Validamos que la matriz se construya correctamente para un dataset pequeño.
    """
    data = {
        "Monedero_Id": ["U1", "U1", "U2", "U2", "U3"],
        "ArticuloPadre_Id": ["A", "B", "B", "C", "A"],
        "Frecuencia": [1, 2, 1, 3, 1],
    }
    df = pl.DataFrame(data)
    mat, user2idx, item2idx = create_user_item_matrix(df)

    assert mat.shape == (3, 3)  # 3 usuarios (U1,U2,U3) x 3 artículos (A,B,C)
    # Revisa la fila de U1
    idx_u1 = user2idx["U1"]
    idx_a = item2idx["A"]
    idx_b = item2idx["B"]
    idx_c = item2idx["C"]

    # Revisar que U1 compre A y B => (freq>0 =>1)
    # mat[idx_u1, idx_a] y mat[idx_u1, idx_b] deben ser 1
    row_u1 = mat[idx_u1].toarray().ravel()
    assert row_u1[idx_a] == 1
    assert row_u1[idx_b] == 1
    assert row_u1[idx_c] == 0  # U1 no tiene C


def test_compute_cooccurrence_matrix():
    """
    Probar que la co-ocurrencia se calcule correctamente en un ejemplo controlado.
    """
    # Simulamos 3 usuarios x 3 items
    #   U1 -> A,B
    #   U2 -> B,C
    #   U3 -> A
    # Esperamos co-ocurrencia(A,B)=1 (de U1), co-ocurrencia(B,C)=1 (U2), etc.

    # Directamente creamos la matriz:
    data = np.array([1, 1, 1, 1, 1])  # 5 "1"s,
    row_indices = np.array([0, 0, 1, 1, 2])  # user
    col_indices = np.array([0, 1, 1, 2, 0])  # item
    # Interpretemos:
    # user0 => items(0,1) => A,B
    # user1 => items(1,2) => B,C
    # user2 => item(0) => A

    user_item = sp.csr_matrix((data, (row_indices, col_indices)), shape=(3, 3))

    coocc = compute_cooccurrence_matrix(user_item)
    # coocc debería ser 3x3 (items x items)

    # coocc(A,B) => item0 vs item1:
    # A = col0, B= col1
    # Cuántos usuarios compraron ambos? => user0
    # => coocc(0,1)= 1
    # coocc(1,0)=1 (simétrico)
    # coocc(A,C)=0 (ningún user compra A y C a la vez)
    # coocc(B,C)=1 (user1)

    # coocc(0,0)=0, coocc(1,1)=0, coocc(2,2)=0 (diagonal en 0)
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
    user_item = sp.csr_matrix((data, (row_indices, col_indices)), shape=(3, 3))
    coocc = compute_cooccurrence_matrix(user_item)

    # freq(A) => user0, user2 => 2
    # freq(B) => user0, user1 => 2
    # freq(C) => user1 => 1
    # total_users = 3

    # coocc(A,B)=1 => expected(A,B)= (2*2)/3 = 4/3 => lift= 1/(4/3) = 3/4=0.75
    # coocc(B,C)=1 => freq(B)=2, freq(C)=1 => expected= (2*1)/3=2/3 => lift= (1)/(2/3)=1.5
    # coocc(A,C)=0 => lift= 0

    lift = compute_lift_matrix(user_item, coocc)
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


def test_generate_recommendations_with_significance():
    """
    Verifica la generación de recomendaciones incluyendo la puntuación de significancia
    y el ranking de recomendaciones.
    """
    # Definimos las compras de los usuarios "clave":
    # U1 compra A y B
    # U2 compra B y C
    # U3 compra A y C
    # U4 compra B (únicamente, se descarta por tener <2 compras)
    # U5 compra A, B y C (se descarta por tener todos los ítems)
    monederos = [
        "U1",
        "U1",  # U1: A, B
        "U2",
        "U2",  # U2: B, C
        "U3",
        "U3",  # U3: A, C
        "U4",  # U4: B
        "U5",
        "U5",
        "U5",  # U5: A, B, C
    ]
    articulos = ["A", "B", "B", "C", "A", "C", "B", "A", "B", "C"]
    frecuencias = [1, 1, 1, 1, 1, 1, 2, 1, 1, 1]

    # Agregar usuarios U6 a U15 que compran "X" (esto aumenta total_users a 15)
    for i in range(6, 16):
        monederos.append(f"U{i}")
        articulos.append("X")
        frecuencias.append(1)

    data = {
        "Monedero_Id": monederos,
        "ArticuloPadre_Id": articulos,
        "Frecuencia": frecuencias,
    }
    df = pl.DataFrame(data)

    # Llamamos a la función con valores específicos para probar todas las funcionalidades
    recs = generate_customer_recommendations(
        df,
        max_n_recommendations=2,
        min_cooccur_threshold=1,
        min_lift_threshold=0.5,
        min_confidence_level=60.0,  # Umbral bajo para asegurar que pasen las pruebas
    )

    # Verificar estructura del resultado
    expected_columns = [
        "Monedero_Id",
        "Articulo_Id_Recomendado",
        "Lift_Score",
        "Clientes_Compraron",
        "Significance_Score",
        "Combined_Score",
        "Articulos_Id_Relacionados",
        "Fecha_Generacion",
        "Rank",
    ]

    for col in expected_columns:
        assert col in recs.columns, f"Falta la columna '{col}' en la salida"

    # Verificar que se generaron recomendaciones para U1, U2 y U3
    monederos_con_recomendaciones = set(recs["Monedero_Id"].unique())
    assert {"U1", "U2", "U3"}.issubset(monederos_con_recomendaciones), (
        f"No se generaron recomendaciones para todos los usuarios esperados: {monederos_con_recomendaciones}"
    )

    # Verificar que los valores de Rank estén correctos (empiezan en 1 para cada usuario)
    for monedero in monederos_con_recomendaciones:
        monedero_recs = recs.filter(pl.col("Monedero_Id") == monedero)
        assert min(monedero_recs["Rank"]) == 1, f"Rank no inicia en 1 para {monedero}"

    # Verificar que la fecha de generación sea correcta (hoy)
    today = datetime.now().date()
    assert all(recs["Fecha_Generacion"] == today), (
        "La fecha de generación no es correcta"
    )

    # Verificar Combined_Score y Significance_Score
    assert all(recs["Combined_Score"] > 0), (
        "Hay recomendaciones con Combined_Score <= 0"
    )
    assert all(recs["Significance_Score"] >= 0), (
        "Hay recomendaciones con Significance_Score < 0"
    )

    # Verificar que las recomendaciones esperadas estén presentes
    expected_pairs = {
        ("U1", "C"),
        ("U2", "A"),
        ("U3", "B"),
    }
    actual_pairs = set(recs.select(["Monedero_Id", "Articulo_Id_Recomendado"]).rows())
    assert expected_pairs.issubset(actual_pairs), (
        f"Faltan recomendaciones esperadas.\nEsperado: {expected_pairs}\nObtenido: {actual_pairs}"
    )


def test_generate_recommendations_empty_raises_value_error():
    """
    Verificar que se lance ValueError si no hay recomendaciones.
    """
    # Definimos las compras de los usuarios "clave":
    # U1 compra A y B
    # U2 compra B y C
    # U3 compra A y C
    # U4 compra B (únicamente, se descarta por tener <2 compras)
    # U5 compra A, B y C (se descarta por tener todos los ítems)
    monederos = [
        "U1",
        "U1",  # U1: A, B
        "U2",
        "U2",  # U2: B, C
        "U3",
        "U3",  # U3: A, C
        "U4",  # U4: B
        "U5",
        "U5",
        "U5",  # U5: A, B, C
    ]
    articulos = ["A", "B", "B", "C", "A", "C", "B", "A", "B", "C"]
    frecuencias = [1, 1, 1, 1, 1, 1, 2, 1, 1, 1]

    # Agregar usuarios U6 a U15 que compran "X" (esto aumenta total_users a 15)
    for i in range(6, 16):
        monederos.append(f"U{i}")
        articulos.append("X")
        frecuencias.append(1)

    data = {
        "Monedero_Id": monederos,
        "ArticuloPadre_Id": articulos,
        "Frecuencia": frecuencias,
    }
    df = pl.DataFrame(data)

    # Llamar a la función con un umbral alto para asegurar que no haya recomendaciones
    with pytest.raises(ValueError):
        generate_customer_recommendations(
            df,
            max_n_recommendations=1,
            min_cooccur_threshold=100,
            min_lift_threshold=100.0,
            min_confidence_level=99.99,
        )
