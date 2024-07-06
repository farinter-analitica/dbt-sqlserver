
{# Add dwh_farinter_remove_incremental_temp_table to all incremental models #}
{# unique_key is accessible with config.get('unique_key') but it returns a string #}
{# remember that macro here executes before the model is created, so we can't use it here #}
{% set unique_key_list = ["Alerta_Id","Emp_Id"] %}
{# Post_hook can't access this context variables so we create the string here if needed only if the macros dont depende on query execution (just returns the query text) #}
{#{% set post_hook_dwh_farinter_create_primary_key =  dwh_farinter_create_primary_key(this,columns=unique_key_list, create_clustered=False, is_incremental=0, show_info=True, if_another_exists_drop_it=True)  %}#}
{{ 
    config(
		as_columnstore=false,
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


SELECT --TOP (1000) 
    ISNULL([Alerta_Id],0) AS [Alerta_Id]
    ,ISNULL([Emp_Id],0) AS [Emp_Id]
    ,[Nombre]
    ,[Descripcion]
    ,[Activa]
    ,[Fecha_Inicio]
    ,[Fecha_Final]
    ,[Mensaje]
    ,[Bit_Recomendacion]
    ,[Bit_Cronico]
    ,[Bit_MPA]
    ,[Hash_AlertaEmp]
    ,[Cuadro_Basico]
    , CASE WHEN Bit_MPA = 1 THEN 'Marcas Propias y Alianzas'
        WHEN Bit_Recomendacion = 1 THEN 'Recomendacion'
        ELSE 'Otros' END AS [Grupo_Meta]
    , ISNULL({{ dwh_farinter_hash_column( columns = unique_key_list, table_alias="A") }},'') AS [HashStr_AleEmp]   
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Actualizado]
FROM [DL_FARINTER].[dbo].[DL_Kielsa_PV_Alerta] A