{% set unique_key_list = ["almacen_id","casa_id","material","periodo","gpo_cliente"] %}
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
    #"Removed Other Columns" = Table.SelectColumns(UnPivot,{"sociedad_id", "Material", "Centro_Almacen", "Gpo_Cliente", "casa_id", "sub_gpo_id", "Sociedad", "yyyymm_texto", "Pronostico"})

#}
{% set current_date = modules.datetime.datetime.now() %}
{% set first_day_current_month = current_date.replace(day=1) %}
{% set one_year_ago = first_day_current_month.replace(year=first_day_current_month.year-1) %}
{% set one_month_ago = (first_day_current_month - modules.datetime.timedelta(days=1)).replace(day=1) %}
{% set v_fecha_desde = one_month_ago.strftime('%Y%m%d') %}
SELECT 
    ALM.Sociedad_Id AS sociedad_id,
    DL.Centro_Almacen_Id AS Centro_Almacen,
    ISNULL(MAT.Casa_Id, '') AS casa_id,
    DL.Gpo_Plan AS gpo_plan,
    DL.Material_Id AS Material,
    ISNULL(DL.Material_Nombre, 'X') AS material_nombre,
    ISNULL(GC.GrupoClientes_Nombre, 'X') AS grupo_cliente,
    ISNULL(GC.GrupoClientes_Id, 'X') AS Gpo_Cliente,
    YEAR(DL.Fecha_Id)*100 + MONTH(DL.Fecha_Id) AS periodo,
    ISNULL(DL.forecast, 0) AS Pronostico,
    ISNULL(DL.error,0) AS Error
FROM {{ source('BI_FARINTER', 'BI_SAP_Hecho_SocAlmArtGpoCli_Forecast') }} DL
INNER JOIN {{ ref("BI_SAP_Dim_Almacen")}} ALM
    ON DL.Centro_Almacen_Id = ALM.CenAlm_Id
LEFT JOIN {{ source('BI_FARINTER', 'BI_Dim_Articulo_SAP') }} MAT
    ON DL.Material_Id = MAT.Material_Id
LEFT JOIN {{ref('BI_SAP_Dim_GrupoCliente_Plan')}} GC
    ON DL.Gpo_Cliente = GC.GrupoClientes_Nombre
WHERE DL.Fecha_Id >= '{{v_fecha_desde}}' --12 MESES COMPLETOS
--AND DL.Fecha_Id < DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) --ULTIMO MES COMPLETO
