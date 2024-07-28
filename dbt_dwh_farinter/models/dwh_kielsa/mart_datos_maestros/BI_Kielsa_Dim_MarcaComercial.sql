
{# Add dwh_farinter_remove_incremental_temp_table to all incremental models #}
{# unique_key is accessible with config.get('unique_key') but it returns a string #}
{# remember that macro here executes before the model is created, so we can't use it here #}
{% set unique_key_list = ["Marca_Comercial_Id","Emp_Id"] %}
{# Post_hook can't access this context variables so we create the string here if needed only if the macros dont depende on query execution (just returns the query text) #}
{#{% set post_hook_dwh_farinter_create_primary_key =  dwh_farinter_create_primary_key(this,columns=unique_key_list, create_clustered=False, is_incremental=0, show_info=True, if_another_exists_drop_it=True)  %}#}
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

SELECT ISNULL([Marca_Comercial_Id],0) AS [Marca_Comercial_Id]
	,ISNULL([Emp_Id],0) AS [Emp_Id]
	,ISNULL({{ dwh_farinter_hash_column(unique_key_list) }},'') AS [HashStr_MarcaEmp]
	,ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Carga]
	,ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Actualizado]
FROM (VALUES 
                (1, 1, 'KIELSA'),
                (1, 2, 'HERDEZ'),
                (2, 2, 'KIELSA'),
                (1, 3, 'KIELSA'),
                (2, 3, 'FARMEX'),
                (1, 4, 'KIELSA'),
                (1, 5, 'BRASIL')) AS [t] ([Marca_Comercial_Id], [Emp_Id], [Marca_Nombre])