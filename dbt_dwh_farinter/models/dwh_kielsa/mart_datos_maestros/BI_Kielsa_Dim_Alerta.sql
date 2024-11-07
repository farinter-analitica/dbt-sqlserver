
{% set unique_key_list = ["Alerta_Id","Emp_Id"] %}
{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario"],
		materialized="table",
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
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
FROM [DL_FARINTER].[dbo].[DL_Kielsa_PV_Alerta] A