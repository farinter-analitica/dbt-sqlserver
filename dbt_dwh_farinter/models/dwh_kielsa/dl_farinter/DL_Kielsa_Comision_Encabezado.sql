
{%- set unique_key_list = ["Comision_Id","Emp_Id"] -%}
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

{%- set query_empresas -%}
SELECT Empresa_Id, Empresa_Id_Original, Pais_Id
	--, LS_LDCOM_RepLocal AS Servidor_Vinculado, D_LDCOM_RepLocal as Base_Datos
	--,LS_LDCOM AS Servidor_Vinculado, D_LDCOM as Base_Datos
	,LS_LDCOM_Replica AS Servidor_Vinculado, D_LDCOM_Replica as Base_Datos
FROM BI_FARINTER.dbo.BI_Kielsa_Dim_Empresa WITH (NOLOCK)
WHERE LS_LDCOM_Replica IS NOT NULL and Es_Empresa_Principal = 1
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
      ,ISNULL([Comision_Id],	0) AS [Comision_Id]
      ,[Comision_Nombre] COLLATE DATABASE_DEFAULT AS [Comision_Nombre]
      ,[Comision_Fecha]
      ,[Comision_Fecha_Inicial]
      ,[Comision_Fecha_Final]
      ,[Comision_Estado] COLLATE DATABASE_DEFAULT AS [Comision_Estado]
      ,[Comision_Fec_Actualizacion]
	FROM {{item['Servidor_Vinculado']}}.{{item['Base_Datos']}}.dbo.Comision_Encabezado
	WHERE Emp_Id = {{ item['Empresa_Id_Original'] }}  
		AND (Comision_Fec_Actualizacion >= '{{ last_date }}' OR Comision_Fecha_Final >= '{{ last_date }}')
    {%- endfor -%}   
)
SELECT *
	, GETDATE() AS [Fecha_Carga]
	, GETDATE() AS [Fecha_Actualizado]
FROM datosBase
