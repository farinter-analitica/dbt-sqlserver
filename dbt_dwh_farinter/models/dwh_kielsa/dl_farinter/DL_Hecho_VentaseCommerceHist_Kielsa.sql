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
    0 AS Tipo_Id,
    A.Articulo_Id,
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
FROM DL_FARINTER.[dbo].[DL_Kielsa_VentaseCommerceHist] AS A -- {{ ref('DL_Kielsa_VentaseCommerceHist') }}
GROUP BY A.Pais_Id,
    A.Sucursal_Id,
    A.CanalVenta_Id,
    A.conFactura_Id,
    A.Origen_Id,
    A.FormaPago_Id,
    A.TipoPago_Id,
    A.Articulo_Id,
    A.Fecha_Id
