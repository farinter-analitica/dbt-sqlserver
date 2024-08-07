
{%- set unique_key_list = ["Factura_Id","Emp_Id","Orden_Id","Suc_Id","TipoDoc_Id","CC_Id","Caja_Id"] -%}
{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
		"{{ dwh_farinter_remove_incremental_temp_table() }}",
		"{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
		]
		
) }}

{%- set origenes = [
	{"emp_id" : 1, "origen" : var('P_DWHPRD_LS') ~ ".REP_LDCOM_HN",},
	{"emp_id" : 2, "origen" : var('P_DWHPRD_LS') ~ ".REP_LDCOM_GT",},
	{"emp_id" : 3, "origen" : var('P_DWHPRD_LS') ~ ".REP_LDCOM_NI",},
	{"emp_id" : 4, "origen" : var('P_DWHPRD_LS') ~ ".REP_LDCOM_CR",},
	{"emp_id" : 5, "origen" : var('P_DWHPRD_LS') ~ ".REP_LDCOM_SV",},
] -%}

WITH DatosBase
AS
(
    {%- for item in origenes -%}
        {%- if not loop.first %}
		UNION ALL{%- endif %}
		{%- if is_incremental() -%}
			{%- set last_date = run_single_value_query_on_relation_and_return(
				query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this ~ " where Emp_Id = " ~ item.emp_id, relation_not_found_value='19000101'|string
				)|string -%}
		{%- else -%}
			{%- set last_date = '19000101' -%}
		{%- endif %}		
	SELECT ISNULL(FEX.[Consecutivo],0) AS [Consecutivo]
			,ISNULL(FEX.Emp_Id,0) AS [Emp_Id]
			,ISNULL(FEX.TipoDoc_Id,0) AS [TipoDoc_id]
			,ISNULL(FEX.Suc_Id,0) AS [Suc_Id]
			,ISNULL(FEX.Caja_Id,0) AS [Caja_Id]
			,ISNULL(FEX.Factura_Id,0) AS [Factura_Id]
			,ISNULL(FEX.CC_Id,0) AS [CC_Id]
			,ISNULL(OE.Orden_Id,0) AS [Orden_Id]
			,FEX.[Cierre_Id]
			,ISNULL(OE.Orden_Inicio_Registro,'19000101') AS [Orden_Inicio_Registro]
			,ISNULL(OE.Orden_Fec_Terminada,'19000101') AS [Orden_Fec_Terminada]
	FROM {{ item.origen }}.dbo.Exp_Orden_Encabezado OE
	INNER JOIN  {{ item.origen }}.dbo.Exp_Factura_Express FEX
		ON OE.Emp_Id = FEX.Emp_Id AND OE.CC_Id = FEX.CC_Id AND OE.Orden_Id = FEX.Orden_Id
	WHERE OE.Emp_Id = {{ item.emp_id }}  
		AND (OE.Orden_Inicio_Registro >= '{{ last_date }}' OR OE.Orden_Fec_Terminada >= '{{ last_date }}')
    {%- endfor -%}   
)
SELECT *
	, GETDATE() AS [Fecha_Carga]
	, GETDATE() AS [Fecha_Actualizado]
FROM datosBase