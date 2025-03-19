{% set unique_key_list = ["almacen_id","casa_id","material","periodo"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/mensual"],
		materialized="view",
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ]
	) 
}}

{#
-- Solo se debe usar esta vista para el EXCEL de la herramienta de analisis de pedidos.

let
    Source = Excel.Workbook(File.Contents("\\10.0.5.157\ftpfarinter\Planning\BaseDatos\Stock_Cubo.xlsx"), null, true),
    TbStockMensual_Table = Source{[Item="TbStockMensual",Kind="Table"]}[Data],
    #"Changed Type" = Table.TransformColumnTypes(TbStockMensual_Table,{{"sociedad_id", type text}, {"almacen_id", type text}, {"casa_id", type text}, {"material", type text}, {"material_nombre", type text}, {"periodo", type text}, {"stock", type number}}),
    #"Removed Other Columns" = Table.SelectColumns(#"Changed Type",{"sociedad_id", "almacen_id", "casa_id", "material", "material_nombre", "periodo", "stock"})
in
    #"Removed Other Columns"



#}

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
