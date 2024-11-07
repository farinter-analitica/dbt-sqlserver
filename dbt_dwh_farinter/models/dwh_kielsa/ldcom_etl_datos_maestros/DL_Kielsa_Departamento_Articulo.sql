
{%- set unique_key_list = ["DeptoArt_Id","Emp_Id"] -%}
{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
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
	SELECT ISNULL({{item['Empresa_Id']}},0) AS [Emp_Id]
		, ISNULL(CAST(Depto_Id AS INT),0) AS DeptoArt_Id
		, UPPER(Depto_Nombre) COLLATE DATABASE_DEFAULT AS [DeptoArt_Nombre]
	FROM {{item['Servidor_Vinculado']}}.{{item['Base_Datos']}}.dbo.Departamento 
	WHERE Emp_Id = {{item['Empresa_Id_Original']}} --AND Fecha_Actualizado >= {{last_date}}
{% endfor -%}
)
SELECT *
	, ABS(CAST(CAST(HASHBYTES('SHA2_256', CONCAT(DeptoArt_Id, '-', Emp_Id)) AS INT) AS bigint))  AS Hash_DeptoArtEmp 
	,CASE 
			WHEN DeptoArt_Nombre LIKE '%MP&A%' THEN 'MP&A' 
			WHEN DeptoArt_Nombre LIKE '%FARMA%' THEN 'FARMA' 
			WHEN DeptoArt_Nombre LIKE '%CONSUMO%' THEN 'CONSUMO' 
			WHEN DeptoArt_Nombre LIKE '%OTRO%DONACI%N%' THEN 'OTROS/DONACION' 
			WHEN DeptoArt_Nombre LIKE '%SERVICIO%' THEN 'SERVICIOS' 
			WHEN DeptoArt_Nombre LIKE '%EMPAQUETADO%' THEN 'EMPAQUETADO' 
			WHEN DeptoArt_Nombre LIKE '%BOLETO%CUPONE%' THEN 'BOLETOS/CUPONES' 
			ELSE 'No definido' 
		END AS Tipo_Canal
	, GETDATE() AS [Fecha_Carga]
	, GETDATE() AS [Fecha_Actualizado]
FROM datosBase

