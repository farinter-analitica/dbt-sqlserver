
{% set unique_key_list = ["Cliente_Id","Emp_Id"] %}
{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario","periodo/por_hora"],
		materialized="incremental",
        incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="ignore",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=true, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
		]
		
) }}
--dbt dagster 
SELECT [Emp_Id]
        ,[Cliente_Id]
        ,[Cliente_Nombre]
        ,[Tipo_Cliente_Id]
        ,[Cedula]
        ,[Direccion]
        ,[Correo]
        ,[Credito_Limite]
        ,[Credito_Saldo]
        ,[Indicador_Cliente_Activo]
        ,[Indicador_Filtro_Unico]
        ,[Indicador_Borrado]
        ,[Estado]
        ,ISNULL({{ dwh_farinter_hash_column(unique_key_list) }},'') AS [HashStr_CliEmp]
        ,ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
	    , {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Cliente_Id'], input_length=29, table_alias='')}} [EmpCli_Id]
        ,Hash_ClienteEmp
        ,Tipo_Cliente
        ,[Fecha_Actualizado]
  FROM {{ source('DL_FARINTER', 'DL_Kielsa_Cliente') }} 