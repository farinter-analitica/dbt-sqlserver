{%- set unique_key_list = ["Emp_Id","Cliente_Id","Articulo_Id"] -%}

{{ 
    config(
		tags=["periodo/diario"],
        materialized="view",
    )
}}

-- Solo editable en DBT DAGSTER
WITH Recomendaciones AS (
    SELECT --TOP (1000) 
        1 as Emp_Id,
        [Monedero_Id] AS Cliente_Id,
        [Articulo_Id_Recomendado] AS Articulo_Id,
        [Lift_Score],
        [Clientes_Compraron],
        [Significance_Score],
        [Combined_Score],
        [Articulos_Id_Relacionados],
        [Fecha_Generacion],
        [Rank]
    FROM [DL_FARINTER].[dbo].[DL_Kielsa_Cliente_ArticuloRecomendado] -- {{ source('DL_FARINTER', 'DL_Kielsa_Cliente_ArticuloRecomendado') }}
)
SELECT A.Emp_Id,
    A.Cliente_Id,
    A.Articulo_Id AS Codigo_Articulo,
    B.Articulo_Nombre AS Articulo_Nombre,
    ROUND(A.Combined_Score*100,2) AS Puntuacion,
    A.Rank,
    'CSC' AS Tipo_Recomendacion_Id,
    'Clientes Similiares Compran' AS Tipo_Recomendacion_Nombre,
    --A.Articulos_Id_Relacionados AS Soporte,
    A.Clientes_Compraron AS Clientes_Compraron
FROM Recomendaciones AS A
    INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo AS B -- {{ ref('BI_Kielsa_Dim_Articulo') }}
    ON A.Emp_Id = B.Emp_Id
    AND A.Articulo_Id = B.Articulo_Id
WHERE A.Rank <= 5