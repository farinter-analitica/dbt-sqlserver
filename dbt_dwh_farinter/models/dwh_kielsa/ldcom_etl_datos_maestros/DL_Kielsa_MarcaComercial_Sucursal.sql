
{%- set unique_key_list = ["Suc_Id","Emp_Id","Marca_Comercial_Id","Marca_Comercial_x_Sucursal_Id"] -%}
{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="fail",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=true, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
		]
		
) }}

{%- set query_empresas -%}
SELECT Empresa_Id, Empresa_Id_Original, Pais_Id
	--, LS_LDCOM_RepLocal, D_LDCOM_RepLocal 
	--,LS_LDCOM, D_LDCOM
	,LS_LDCOM_Replica AS Servidor_Vinculado, D_LDCOM_Replica as Base_Datos
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
		query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Marca_Fec_Actualizacion)), 112), '19000101')  from  """ ~ this ~ " where Emp_Id = " ~ item['Empresa_Id'], relation_not_found_value='19000101'|string
		)|string -%}
{%- else -%}
	{%- set last_date = '19000101' -%}
{%- endif %}		
	SELECT ISNULL({{item['Empresa_Id']}},0) AS [Emp_Id]
		, ISNULL(MCS.Marca_Comercial_x_Sucursal_Id,0) AS [Marca_Comercial_x_Sucursal_Id]
		, ISNULL(MCS.Marca_Comercial_Id,0) AS [Marca_Comercial_Id]
		, ISNULL(MCS.Suc_Id,0) AS [Suc_Id]
		, MCS.Marca_Principal
		, MCS.Marca_Fec_Actualizacion 
	FROM {{item['Servidor_Vinculado']}}.{{item['Base_Datos']}}.dbo.Marca_Comercial_x_Sucursal MCS
	INNER JOIN {{item['Servidor_Vinculado']}}.{{item['Base_Datos']}}.dbo.Marca_Comercial MC
	ON MC.Marca_Comercial_Id = MCS.Marca_Comercial_Id
	WHERE MC.Emp_Id = {{item['Empresa_Id_Original']}} 
	{% if is_incremental() %}
		AND MCS.Marca_Fec_Actualizacion >= '{{last_date}}'
	{% else %}
	  --AND MCS.Marca_Fec_Actualizacion >= '{{last_date}}'
	{% endif %}

{% endfor -%}
)
SELECT *
	, GETDATE() AS [Fecha_Carga]
	, GETDATE() AS [Fecha_Actualizado]
FROM datosBase