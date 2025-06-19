{% set unique_key_list = ["almacen_id","casa_id","material","periodo","gpo_cliente"] %}
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

sociedad_id	almacen_id	casa_id	material	material_nombre	grupo_cliente	gpo_cliente	periodo	valor_neto_vta	cantidad_vta	valor_costo_vta	valor_util_vta	valor_vta
1200	1210-1005	C00004	12000281	CLOROX ORIGINAL GALON (3.79 L )	DRORISA	1301	202412	156.3	2	120.37	35.93	156.3

*/

{% set current_date = modules.datetime.datetime.now() %}
{% set first_day_current_month = current_date.replace(day=1) %}
{% set one_year_ago = first_day_current_month.replace(year=first_day_current_month.year-1) %}
{% set v_fecha_desde = one_year_ago.strftime('%Y%m%d') %}
SELECT 
    ALM.Sociedad_Id AS sociedad_id,
    DL.Centro_Almacen_Id AS almacen_id,
    ISNULL(MAT.Casa_Id, '') AS casa_id,
    DL.Gpo_Plan AS gpo_plan,
    DL.Material_Id AS material,
    ISNULL(DL.Material_Nombre, 'X') AS material_nombre,
    ISNULL(GC.GrupoClientes_Nombre, 'X') AS grupo_cliente,
    ISNULL(GC.GrupoClientes_Id, 'X') AS gpo_cliente,
    FORMAT(DL.Fecha_Id, 'yyyyMM') AS periodo,
    ISNULL(DP.Valor_Neto, 0) AS valor_neto_vta,
    ISNULL(DL.Demanda_Positiva + DL.Demanda_Negativa, 0) AS cantidad_vta,
    ISNULL(DL.Limpios_Para_Promedio, 0) AS cantidad_vta_limpia,
    ISNULL(DL.Demanda_Positiva, 0) AS demanda_positiva,
	ISNULL(DL.Demanda_Negativa, 0) AS demanda_negativa,
	--ISNULL(DL.Vencidos_Entrada, 0) AS vencidos_entrada,
    ISNULL(DP.Valor_Costo, 0) AS valor_costo_vta,
    ISNULL(DP.Valor_Utilidad, 0) AS valor_util_vta,
    ISNULL(DP.Valor_Bruto, 0) AS valor_vta
FROM {{ source('BI_FARINTER', 'BI_SAP_Hecho_SocAlmArtGpoCli_Demanda_Limpia') }} DL
INNER JOIN {{ ref("BI_SAP_Dim_Almacen")}} ALM
    ON DL.Centro_Almacen_Id = ALM.CenAlm_Id
LEFT JOIN {{ ref("BI_SAP_Agr_SocAlmArtGpoCli_Demanda_Plan") }} DP
    ON DL.Centro_Almacen_Id = DP.Centro_Almacen_Id
    AND DL.Material_Id = DP.Material_Id
    AND DL.Gpo_Cliente = DP.Gpo_Cliente
    AND DL.Anio_Id = DP.Anio_Id
    AND DL.Mes_Id = DP.Mes_Id
    AND DL.Fecha_Id = EOMONTH(DP.Fecha_Id)
LEFT JOIN {{ source('BI_FARINTER', 'BI_Dim_Articulo_SAP') }} MAT
    ON DL.Material_Id = MAT.Material_Id
LEFT JOIN {{ref('BI_SAP_Dim_GrupoCliente_Plan')}} GC
    ON DL.Gpo_Cliente = GC.GrupoClientes_Nombre
WHERE DL.Fecha_Id >= '{{v_fecha_desde}}' --12 MESES COMPLETOS
--AND DL.Fecha_Id < DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) --ULTIMO MES COMPLETO
