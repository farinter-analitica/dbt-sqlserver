
{%- set unique_key_list = ["Orden_Id","Emp_Id","CC_Id"] -%}
{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="ignore",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
		"{{ dwh_farinter_remove_incremental_temp_table() }}",
		"{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
		]
		
) }}

{%- set query_empresas -%}
SELECT Empresa_Id, Empresa_Id_Original, Pais_Id
	, LS_LDCOM_RepLocal AS Servidor_Vinculado, D_LDCOM_RepLocal as Base_Datos
	--,LS_LDCOM, D_LDCOM
	--,LS_LDCOM_Replica AS Servidor_Vinculado, D_LDCOM_Replica as Base_Datos
FROM BI_FARINTER.dbo.BI_Kielsa_Dim_Empresa WITH (NOLOCK)
WHERE LS_LDCOM_RepLocal IS NOT NULL and Es_Empresa_Principal = 1
{%- endset -%}
{%- set empresas = run_query_and_return(query_empresas) -%} {# Returns: [{Empresa_Id,Emp_Id_Original,Pais_Id,LS_LDCOM_Replica,D_LDCOM_Replica}] #}

WITH DatosBase
AS
(
    {%- for item in empresas -%}
        {%- if not loop.first %}
		UNION ALL{%- endif %}
		{%- if is_incremental() -%}
			{%- set last_date = run_single_value_query_on_relation_and_return(
				query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this ~ " where Emp_Id = " ~ item['Empresa_Id'], relation_not_found_value='19000101'|string
				)|string -%}
		{%- else -%}
			{%- set last_date = '19000101' -%}
		{%- endif %}		
	SELECT ISNULL(CAST({{item['Empresa_Id']}} AS SMALLINT),0) AS [Emp_Id]
		,ISNULL(OE.CC_Id,0) AS [CC_Id]
		,ISNULL(OE.Orden_Id,0) AS [Orden_Id]
		,OE.[Ubicacion_Id]
		,OE.[Cliente_Id]
		,OE.[Zona_Id]
		,OE.[Repartidor_Id]
		,OE.[Estatus_Id] COLLATE DATABASE_DEFAULT AS [Estatus_Id]
		,OE.[Vendedor_Id]
		,ISNULL(OE.Orden_Inicio_Registro,'19000101') AS [Orden_Inicio_Registro]
		,OE.[Orden_Fec_Terminada] 
		,OE.[Orden_Usuario_Registro] COLLATE DATABASE_DEFAULT AS [Orden_Usuario_Registro]
		--, FEX.TipoDoc_Id
		--, FEX.Suc_Id
		--, FEX.Caja_Id
		--, FEX.Factura_Id
	FROM {{item['Servidor_Vinculado']}}.{{item['Base_Datos']}}.dbo.Exp_Orden_Encabezado OE
	--LEFT JOIN  {{ item.origen }}.dbo.Exp_Factura_Express FEX
		--ON OE.Emp_Id = FEX.Emp_Id AND OE.CC_Id = FEX.CC_Id AND OE.Orden_Id = FEX.Orden_Id
	WHERE OE.Emp_Id = {{ item['Empresa_Id_Original'] }}  
		AND (OE.Orden_Inicio_Registro >= '{{ last_date }}' OR OE.Orden_Fec_Terminada >= '{{ last_date }}')
    {%- endfor -%}   
)
SELECT *
	, GETDATE() AS [Fecha_Carga]
	, GETDATE() AS [Fecha_Actualizado]
FROM datosBase