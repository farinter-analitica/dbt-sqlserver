{%- set unique_key_list = ["Cliente_Id","Emp_Id","Codigo_Articulo","Tipo_Recomendacion_Id","Contactar_El"]  -%}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario","periodo_unico/si"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
	) 
}}

WITH 
TopSignificantes AS
(
    SELECT Emp_Id,
        Monedero_Id,
        Articulo_Id,
        Frecuencia,
        Valor_Neto,
        Cantidad_Total,
        Combined_Score,
        Rank
    FROM {{ ref("BI_Kielsa_Agr_Monedero_TopArticulos")}} MTA
	WHERE Rank <= 5
),
ItemsSignificativos AS
(
    SELECT  
        CFC.*,
        MTA.Frecuencia,
        MTA.Valor_Neto,
        MTA.Cantidad_Total,
        MTA.Combined_Score,
        MTA.Articulo_Id,
        MTA.Rank
    FROM {{ ref("CRM_Kielsa_ClienteFrecuenteSinRecetaContactar") }} CFC
    INNER JOIN TopSignificantes MTA 
        ON CFC.Cliente_Id = MTA.Monedero_Id 
        AND CFC.Emp_Id = MTA.Emp_Id
    -- Uncomment to filter for today only
    WHERE CFC.Contactar_El = CAST(GETDATE() AS DATE)
),
Articulos_Relacionados AS (
    SELECT --TOP (1000) 
        1 AS [Emp_Id],
        AR.[Articulo_Id],
        AR.[Articulo_Id_Relacionado],
        A.Articulo_Nombre,
        AR.[Lift_Score],
        AR.[Facturas_Conjuntas],
        AR.[Significance_Score],
        AR.[Combined_Score],
        AR.[Fecha_Generacion],
        AR.[Rank]
    FROM [DL_FARINTER].[dbo].[DL_Kielsa_Articulo_ArticuloRelacionado] AR -- {{ source('DL_FARINTER', 'DL_Kielsa_Articulo_ArticuloRelacionado') }}
    INNER JOIN {{ ref('CRM_Kielsa_ArticuloRecomendable') }} A
        ON AR.Articulo_Id_Relacionado = A.Articulo_Id
        AND 1 = A.Emp_Id
),
TopComprasExisteCiudad AS
(
    SELECT MTA.Emp_Id,
        MTA.Cliente_Id,
        MTA.Articulo_Id,
        A.Articulo_Nombre,
        MTA.Vendedor_Id AS Vendedor_Id,
        MTA.Vendedor_Nombre AS Vendedor_Nombre,
        MTA.Frecuencia,
        MTA.Valor_Neto,
        MTA.Cantidad_Total,
        MTA.Combined_Score,
        MTA.Rank,
        MTA.Contactar_El,
        MTA.Ciclo,
        MTA.Criterio_Seleccion,
        MTA.Sucursal_Id AS Sucursal_Id
    FROM ItemsSignificativos MTA
    INNER JOIN {{ ref('CRM_Kielsa_ArticuloRecomendable') }} AS A
    ON A.Emp_Id = MTA.Emp_Id
    AND A.Articulo_Id = MTA.Articulo_Id
    INNER JOIN {{ ref('BI_Kielsa_Agr_ExistenciaVentas_Ciudad') }}  EC 
    ON MTA.Emp_Id = EC.Emp_Id
    AND MTA.Departamento_Id = EC.Departamento_Id
    AND MTA.Municipio_Id = EC.Municipio_Id
    AND MTA.Ciudad_Id = EC.Ciudad_Id
    AND MTA.Articulo_Id = EC.Articulo_Id
),
ComplementarioTopCompras AS
(
    SELECT AR.Emp_Id,
        MTA.Cliente_Id AS Cliente_Id,
        AR.Articulo_Id_Relacionado AS Codigo_Articulo,
        MAX(AR.Articulo_Nombre) AS Articulo_Nombre,
        MAX(MTA.Vendedor_Id) AS Vendedor_Id,
        MAX(MTA.Vendedor_Nombre) AS Vendedor_Nombre,
        ROUND(MAX(AR.Combined_Score) * 100, 2) AS Puntuacion,
        ROW_NUMBER() OVER(PARTITION BY AR.Emp_Id, MTA.Cliente_Id, MTA.Contactar_El ORDER BY MAX(AR.Combined_Score) DESC) AS Orden,
        'CETC' AS Tipo_Recomendacion_Id,
        'Complementarios con Existencias a Top Compras' AS Tipo_Recomendacion_Nombre,
        MTA.Contactar_El,
        MAX(MTA.Ciclo) AS Ciclo,
        MAX(MTA.Criterio_Seleccion) AS Criterio_Seleccion,
        MAX(MTA.Sucursal_Id) AS Sucursal_Id
    FROM Articulos_Relacionados AS AR
    INNER JOIN ItemsSignificativos MTA
        ON AR.Emp_Id = MTA.Emp_Id
        AND AR.Articulo_Id = MTA.Articulo_Id
    INNER JOIN {{ ref('BI_Kielsa_Agr_ExistenciaVentas_Ciudad') }}     EC 
        ON MTA.Emp_Id = EC.Emp_Id
        AND MTA.Departamento_Id = EC.Departamento_Id
        AND MTA.Municipio_Id = EC.Municipio_Id
        AND MTA.Ciudad_Id = EC.Ciudad_Id
        AND AR.Articulo_Id_Relacionado = EC.Articulo_Id
    GROUP BY 
        AR.Emp_Id,
        MTA.Cliente_Id,
        AR.Articulo_Id_Relacionado,
        MTA.Contactar_El
)
SELECT X.*,
    M.Monedero_Nombre AS Cliente_Nombre,
    S.Sucursal_Nombre,
    GETDATE() AS Fecha_Actualizado
FROM (
    SELECT ISNULL(Emp_Id,0) AS Emp_Id,
        ISNULL(Sucursal_Id,0) AS Sucursal_Id,
        ISNULL(Cliente_Id,0) AS Cliente_Id,
        ISNULL(Vendedor_Id,0) AS Vendedor_Id,
        ISNULL(Vendedor_Nombre,'') AS Vendedor_Nombre,
        ISNULL(Articulo_Id,0) AS Codigo_Articulo,
        Articulo_Nombre,
        ROUND(Combined_Score * 100, 2) AS Puntuacion,
        Rank AS Orden,
        'MCE' AS Tipo_Recomendacion_Id,
        'Mas Comprados por el Cliente con Existencias' AS Tipo_Recomendacion_Nombre,
        Contactar_El,
        Criterio_Seleccion
    FROM TopComprasExisteCiudad
    WHERE Rank <= 2
    
    UNION ALL
    
    -- Complementarios a TopCompras
    SELECT ISNULL(Emp_Id,0) AS Emp_Id,
        ISNULL(Sucursal_Id,0) AS Sucursal_Id,
        ISNULL(Cliente_Id,0) AS Cliente_Id,
        ISNULL(Vendedor_Id,0) AS Vendedor_Id,
        ISNULL(Vendedor_Nombre,'') AS Vendedor_Nombre,
        ISNULL(Codigo_Articulo,0) AS Codigo_Articulo,
        Articulo_Nombre,
        Puntuacion,
        2+Orden AS Orden,
        Tipo_Recomendacion_Id,
        Tipo_Recomendacion_Nombre,
        Contactar_El,
        Criterio_Seleccion
    FROM ComplementarioTopCompras
    WHERE Orden <= 3
) X
INNER JOIN {{ ref("BI_Kielsa_Dim_Monedero")}} M
    ON X.Emp_Id = M.Emp_Id
    AND X.Cliente_Id = M.Monedero_Id
INNER JOIN {{ ref("BI_Kielsa_Dim_Sucursal")}} S
    ON X.Emp_Id = S.Emp_Id
    AND X.Sucursal_Id = S.Sucursal_Id
