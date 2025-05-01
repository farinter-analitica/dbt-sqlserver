{% set unique_key_list = ["almacen_id","casa_id","material","periodo"] %}
{{ 
    config(
		tags=["automation/periodo_mensual_inicio"],
		materialized="view",
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ]
	) 
}}

/*
-- Solo editable en dbt
-- Solo se debe usar esta vista para el EXCEL de la herramienta de analisis de pedidos.

*/
SELECT 
    ALM.Sociedad_Id AS sociedad_id,
    ST.Centro_Almacen_Id AS almacen_id,
    ISNULL(MAT.Casa_Id, '') AS casa_id,
    ST.Material_Id AS material,
    ISNULL(ST.Material_Nombre, 'X') AS material_nombre,
    FORMAT(ST.Fecha_Id, 'yyyyMM') AS periodo,
    ISNULL(ST.Stock_Cierre, 0) AS stock
FROM {{ ref('BI_SAP_Agr_SocAlmArt_Stock_Plan') }} ST
INNER JOIN {{ ref('BI_SAP_Dim_Almacen') }} ALM
    ON ST.Centro_Almacen_Id = ALM.CenAlm_Id
LEFT JOIN {{ source('BI_FARINTER', 'BI_Dim_Articulo_SAP') }} MAT
    ON ST.Material_Id = MAT.Material_Id
WHERE ST.Fecha_Id >= DATEADD(MONTH, -13, GETDATE()) -- Last 12 complete months + current
