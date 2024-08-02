
{% set unique_key_list = ["Casa_Id","Emp_Id"] %}
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
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=true, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
		]
		
) }}
--dbt dagster 
SELECT --TOP (1000) 
    	[Emp_Id]
      ,[Casa_Id]
      ,[Casa_Nombre]
      ,[Hash_CasaEmp]
      ,ISNULL({{ dwh_farinter_hash_column(unique_key_list) }},'') AS [HashStr_CasaEmp]
      ,ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
  FROM {{ source('DL_FARINTER', 'DL_Kielsa_Casa') }} 