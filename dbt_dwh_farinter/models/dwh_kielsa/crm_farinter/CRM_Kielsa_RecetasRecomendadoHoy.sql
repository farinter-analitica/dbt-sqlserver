{%- set unique_key_list = ["Cliente_Id","Emp_Id","Codigo_Articulo","Tipo_Recomendacion_Id"] -%}
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

WITH RecetasHoy AS (
    SELECT --TOP (1000) 
        [Pais_Id],
        [Emp_Id],
        [Sucursal_Id],
        [Sucursal_Nombre],
        [Sucursal_Id_Original],
        [Sucursal_Nombre_Original],
        [Fecha_Compra],
        [Identidad],
        [Cliente_Nombre],
        [Articulo_Id],
        [Articulo_Nombre],
        [Cantidad_Recetada],
        [Comprado_Presentacion],
        [Indicacion_Receta],
        [Vendedor_Id],
        [Receta_Id],
        [Linea_Id],
        [Empleado_Nombre],
        [Contactar_El],
        [Ciclo],
        [Indicador_A_Tiempo],
        [Fecha_Receta],
        [Consumo_Diario],
        [Presentacion],
        [Vendedores_Sucursal],
        [Ultimo_Vendedor_Sucursal],
        [Clientes_Sucursal],
        [Identidad_Sucursal_Orden],
        [Vendedor_Sucursal_Orden]
    FROM [CRM_FARINTER].[dbo].[CRM_Kielsa_RecetasContactarHoy] -- {{ ref('CRM_Kielsa_RecetasContactarHoy') }}
),
CiudadSelecta as
(
    SELECT Emp_Id, Identidad AS Cliente_Id, MAX(Sucursal_Id) AS Sucursal_Id
    FROM RecetasHoy
    GROUP BY Emp_Id, Identidad
),
ExistenciasVentas AS
(
SELECT EC.Emp_Id,
    EC.ArticuloPadre_Id AS Articulo_Id,
    EC.Departamento_Id,
    EC.Municipio_Id,
    EC.Ciudad_Id,
    EC.Cantidad_Existencia
FROM BI_FARINTER.dbo.BI_Kielsa_Agr_Existencia_Ciudad EC -- {{ ref('BI_Kielsa_Agr_Existencia_Ciudad') }}
INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_TipoBodega TB -- {{ ref('BI_Kielsa_Dim_TipoBodega') }}
    ON EC.Emp_Id = TB.Emp_Id
    AND EC.TipoBodega_Id = TB.TipoBodega_Id
WHERE TipoBodega_Nombre = 'VENTAS'
),
RecetasHoyClienteArticulo AS
(
    SELECT 
        RH.Emp_Id,
        RH.Identidad AS Cliente_Id,
        RH.Articulo_Id,
        MAX(RH.Articulo_Nombre) AS Articulo_Nombre,
        MAX(S.Departamento_Id) AS Departamento_Id,
        MAX(S.Municipio_Id) AS Municipio_Id,
        MAX(S.Ciudad_Id) AS Ciudad_Id
    FROM RecetasHoy AS RH
    INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal S -- {{ ref('BI_Kielsa_Dim_Sucursal') }}
    ON RH.Sucursal_Id = S.Sucursal_Id
    AND RH.Emp_Id = S.Emp_Id
    GROUP BY RH.Emp_Id, RH.Identidad, RH.Articulo_Id
),
RecetasHoyClienteArticuloExisteCiudad AS
(
    SELECT 
        RH.Emp_Id,
        RH.Cliente_Id,
        RH.Articulo_Id,
        RH.Articulo_Nombre,
        ROW_NUMBER() OVER(PARTITION BY RH.Emp_Id, RH.Cliente_Id ORDER BY EC.Cantidad_Existencia DESC) AS Orden
    FROM RecetasHoyClienteArticulo AS RH
    INNER JOIN ExistenciasVentas EC 
    ON RH.Emp_Id = EC.Emp_Id
    AND RH.Departamento_Id = EC.Departamento_Id
    AND RH.Municipio_Id = EC.Municipio_Id
    AND RH.Ciudad_Id = EC.Ciudad_Id
    AND RH.Articulo_Id = EC.Articulo_Id
    WHERE EC.Cantidad_Existencia > 0
),
Articulos_Recomendables as
(
    SELECT Emp_Id,
        Articulo_Id,
        Articulo_Nombre
    FROM BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo A
    WHERE A.SubCategoria2Art_Nombre NOT IN (
            'ALIMENTOS',
            'BATERIAS Y ENCENDEDORES',
            'BEBE ACCES',
            'BEBIDAS',
            'BELLEZA ACCES',
            'COSMETICOS ACCES',
            'DECORACION/UTIL HOG',
            'DULCES/SNACKS',
            'EQUIPO/MATERIAL MED',
            'HIGIENE DEL HOG',
            'LIBRERIA',
            'MAMA ACCES',
            'OTROS DONACIONES',
            'PAQUETES CLARO',
            'PAQUETES TIGO',
            'PRESENCIAL',
            'PRIMEROS AUXILIOS',
            'PROMOCIONALES',
            'RECARGAS CLARO',
            'RECARGAS TIGO',
            'SIM CARD TIGO',
            'TARJETA TENGO',
            'TECNOLOGÍA'
        )
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
    FROM [DL_FARINTER].[dbo].[DL_Kielsa_Articulo_ArticuloRelacionado] AR
    INNER JOIN Articulos_Recomendables A
        ON AR.Articulo_Id_Relacionado = A.Articulo_Id
        AND 1 = A.Emp_Id
),
ComplementarioAReceta AS
(
SELECT AR.Emp_Id,
    RH.Cliente_Id AS Cliente_Id,
    AR.Articulo_Id_Relacionado AS Codigo_Articulo,
    AR.Articulo_Nombre AS Articulo_Nombre,
    ROUND(AR.Combined_Score * 100, 2) AS Puntuacion,
    ROW_NUMBER() OVER(PARTITION BY AR.Emp_Id, RH.Cliente_Id ORDER BY AR.Combined_Score DESC) AS Orden,
    'CR' AS Tipo_Recomendacion_Id,
    'Complementarios de Recetados' AS Tipo_Recomendacion_Nombre
    --A.Articulos_Id_Relacionados AS Soporte,
    --A.Clientes_Compraron AS Clientes_Compraron
FROM Articulos_Relacionados AS AR
INNER JOIN RecetasHoyClienteArticulo AS RH
    ON RH.Emp_Id = AR.Emp_Id
    AND RH.Articulo_Id = AR.Articulo_Id
INNER JOIN ExistenciasVentas EC 
    ON RH.Emp_Id = EC.Emp_Id
    AND RH.Departamento_Id = EC.Departamento_Id
    AND RH.Municipio_Id = EC.Municipio_Id
    AND RH.Ciudad_Id = EC.Ciudad_Id
    AND AR.Articulo_Id_Relacionado = EC.Articulo_Id
WHERE EC.Cantidad_Existencia > 0
),
TopComprasExisteCiudad AS
(
    SELECT MTA.Emp_Id,
        MTA.Monedero_Id,
        MTA.Articulo_Id,
        A.Articulo_Nombre,
        MTA.Frecuencia,
        MTA.Valor_Neto,
        MTA.Cantidad_Total,
        MTA.Combined_Score,
        MTA.Rank
    FROM BI_FARINTER.dbo.BI_Kielsa_Agr_Monedero_TopArticulos MTA -- {{ ref('BI_Kielsa_Agr_Monedero_TopArticulos') }}
    INNER JOIN Articulos_Recomendables AS A -- {{ ref('BI_Kielsa_Dim_Articulo') }}
    ON A.Emp_Id = MTA.Emp_Id
    AND A.Articulo_Id = MTA.Articulo_Id
    INNER JOIN CiudadSelecta SS
    ON MTA.Emp_Id = SS.Emp_Id
    AND MTA.Monedero_Id = SS.Cliente_Id
    INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal S -- {{ ref('BI_Kielsa_Dim_Sucursal') }}
    ON SS.Sucursal_Id = S.Sucursal_Id
    AND MTA.Emp_Id = S.Emp_Id
    INNER JOIN ExistenciasVentas EC 
    ON S.Emp_Id = EC.Emp_Id
    AND S.Departamento_Id = EC.Departamento_Id
    AND S.Municipio_Id = EC.Municipio_Id
    AND S.Ciudad_Id = EC.Ciudad_Id
    AND MTA.Articulo_Id = EC.Articulo_Id
    WHERE EC.Cantidad_Existencia > 0
),
ComplementarioTopCompras AS
(
SELECT AR.Emp_Id,
    MTA.Monedero_Id AS Cliente_Id,
    AR.Articulo_Id_Relacionado AS Codigo_Articulo,
    AR.Articulo_Nombre AS Articulo_Nombre,
    ROUND(AR.Combined_Score * 100, 2) AS Puntuacion,
    ROW_NUMBER() OVER(PARTITION BY AR.Emp_Id, MTA.Monedero_Id ORDER BY AR.Combined_Score DESC) AS Orden,
    'RTC' AS Tipo_Recomendacion_Id,
    'Relacionado a Top Compras' AS Tipo_Recomendacion_Nombre
    --A.Articulos_Id_Relacionados AS Soporte,
    --A.Clientes_Compraron AS Clientes_Compraron
FROM Articulos_Relacionados AS AR
INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Agr_Monedero_TopArticulos MTA -- {{ ref('BI_Kielsa_Agr_Monedero_TopArticulos') }}
    ON AR.Emp_Id = MTA.Emp_Id
    AND AR.Articulo_Id = MTA.Articulo_Id
INNER JOIN CiudadSelecta SS
    ON MTA.Emp_Id = SS.Emp_Id
    AND MTA.Monedero_Id = SS.Cliente_Id
INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal S -- {{ ref('BI_Kielsa_Dim_Sucursal') }}
    ON SS.Sucursal_Id = S.Sucursal_Id
    AND MTA.Emp_Id = S.Emp_Id
INNER JOIN ExistenciasVentas EC 
    ON S.Emp_Id = EC.Emp_Id
    AND S.Departamento_Id = EC.Departamento_Id
    AND S.Municipio_Id = EC.Municipio_Id
    AND S.Ciudad_Id = EC.Ciudad_Id
    AND AR.Articulo_Id_Relacionado = EC.Articulo_Id
    WHERE EC.Cantidad_Existencia > 0
)
SELECT ISNULL(Emp_Id,0) AS Emp_Id,
    ISNULL(Cliente_Id,0) AS Cliente_Id,
    ISNULL(Articulo_Id,0) AS Codigo_Articulo,
    Articulo_Nombre AS Articulo_Nombre,
    -1 AS Puntuacion,
    Orden,
    'RHE' AS Tipo_Recomendacion_Id,
    'Recetas para Hoy con Existencias' AS Tipo_Recomendacion_Nombre
FROM RecetasHoyClienteArticuloExisteCiudad RH
WHERE Orden <=5
UNION ALL
-- TopCompras
SELECT ISNULL(Emp_Id,0) AS Emp_Id,
    ISNULL(Monedero_Id,0) AS Cliente_Id,
    ISNULL(Articulo_Id,0) AS Codigo_Articulo,
    Articulo_Nombre,
    ROUND(Combined_Score * 100, 2) AS Puntuacion,
    5+Rank AS Orden,
    'MC' AS Tipo_Recomendacion_Id,
    'Mas Comprados por el Cliente' AS Tipo_Recomendacion_Nombre
FROM TopComprasExisteCiudad
WHERE Rank <= 2
-- Complementario a Receta
UNION ALL
SELECT ISNULL(Emp_Id,0) AS Emp_Id,
    ISNULL(Cliente_Id,0) AS Cliente_Id,
    ISNULL(Codigo_Articulo,0) AS Codigo_Articulo,
    Articulo_Nombre,
    Puntuacion,
    7+Orden,
    Tipo_Recomendacion_Id,
    Tipo_Recomendacion_Nombre 
FROM ComplementarioAReceta
WHERE Orden <= 2
UNION ALL
-- Complementarios a TopCompras
SELECT ISNULL(Emp_Id,0) AS Emp_Id,
    ISNULL(Cliente_Id,0) AS Cliente_Id,
    ISNULL(Codigo_Articulo,0) AS Codigo_Articulo,
    Articulo_Nombre,
    Puntuacion,
    9+Orden,
    Tipo_Recomendacion_Id,
    Tipo_Recomendacion_Nombre 
FROM ComplementarioTopCompras
WHERE Orden <= 2


