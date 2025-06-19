
{%- set unique_key_list = ["Vendedor_Id","Usuario_Id","Emp_Id"] -%}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
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

{%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Ult_Fec_Actualizacion)), 112), '19000101')  from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '19000101' %}
{%- endif %}

{# Verificar cuales estan accesibles #}
{%- set valid_empresas = [] -%}
{%- for item in empresas -%}
    {%- if check_linked_server(item['Servidor_Vinculado']) -%}
        {%- do valid_empresas.append(item) -%}
    {%- endif -%}
{%- endfor -%}

WITH DatosBase
AS
(
{%- for item in valid_empresas -%}
{%- if not loop.first %}
	UNION ALL{%- endif %}
	SELECT ISNULL({{item['Empresa_Id']}},0) AS [Emp_Id]
		, ISNULL(CAST(Usuario_Id AS INT),0) AS [Usuario_Id]
		, ISNULL(CAST(Vendedor_Id AS INT),0) AS [Vendedor_Id]
		,[Ult_Fec_Actualizacion]
	FROM {{item['Servidor_Vinculado']}}.{{item['Base_Datos']}}.dbo.[Vendedor_x_Usuario] 
	WHERE Emp_Id = {{item['Empresa_Id_Original']}} --AND Fecha_Actualizado >= {{last_date}}
	{%- if is_incremental() %}
	AND Ult_Fec_Actualizacion >= {{last_date}}
	{%- endif %}

{% endfor -%}
)
SELECT *
	, ISNULL({{ dwh_farinter_hash_column(unique_key_list) }},'') as Hash_UsuVenEmp
	, GETDATE() AS [Fecha_Carga]
	, GETDATE() AS [Fecha_Actualizado]
FROM datosBase