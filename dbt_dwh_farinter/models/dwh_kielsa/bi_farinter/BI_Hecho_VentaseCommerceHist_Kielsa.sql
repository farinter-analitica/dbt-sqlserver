{% set unique_key_list = [] %}

{{ 
    config(
		tags=["periodo/diario"],
        materialized="view",
    )
}}
-- Solo editable en DBT DAGSTER
SELECT A.Pais_Id,
    A.Sucursal_Id,
    A.CanalVenta_Id,
    A.conFactura_Id,
    A.Origen_Id,
    A.FormaPago_Id,
    A.TipoPago_Id,
    A.Tipo_Id,
    A.Articulo_Id,
    A.Fecha_Id,
    A.Anio_Id,
    A.Mes_Id,
    A.Dia_Id,
    A.Dias_Id,
    A.Cantidad,
    A.Descuento,
    A.Venta_Neta,
    A.Costo,
    A.Utilidad
FROM DL_FARINTER.[dbo].[DL_Hecho_VentaseCommerceHist_Kielsa] AS A -- {{ ref('DL_Hecho_VentaseCommerceHist_Kielsa') }}
UNION ALL
SELECT A.Pais_Id,
    A.Principal_Id AS Sucursal_Id,
    CONVERT(int, A.Categoria1_Id) AS CanalVenta_Id,
    1 AS conFactura_Id,
    1 AS Origen_Id,
    0 AS FormaPago_Id,
    0 AS TipoPago_Id,
    2 AS Tipo_Id,
    A.Categoria2_Id AS Articulo_Id,
    A.Fecha_Id,
    year(A.Fecha_Id) AS Anio_Id,
    MONTH(A.Fecha_Id) AS Mes_Id,
    DAY(A.Fecha_Id) AS Dia_Id,
    datepart(weekday, Fecha_Id) AS Dias_Id,
    sum(A.Cantidad) AS Cantidad,
    sum(A.Descuento) AS Descuento,
    sum(A.Venta_Neta) AS Venta_Neta,
    sum(A.Costo) AS Costo,
    sum(A.Utilidad) AS Utilidad
FROM BI_FARINTER.dbo.BI_Hecho_ProyeccionVentaseCommerce_Kielsa AS A -- {{ source('BI_FARINTER', 'BI_Hecho_ProyeccionVentaseCommerce_Kielsa') }}
WHERE A.Tipo_Id = 2
GROUP BY A.Pais_Id,
    A.Principal_Id,
    A.Categoria1_Id,
    A.Categoria2_Id,
    A.Fecha_Id