
{# Add dwh_farinter_remove_incremental_temp_table to all incremental models #}
{# unique_key is accessible with config.get('unique_key') but it returns a string #}
{# remember that macro here executes before the model is created, so we can't use it here #}
{% set unique_key_list = ["Sucursal_Id","Emp_Id"] %}
{# Post_hook can't access this context variables so we create the string here if needed only if the macros dont depende on query execution (just returns the query text) #}
{#{% set post_hook_dwh_farinter_create_primary_key =  dwh_farinter_create_primary_key(this,columns=unique_key_list, create_clustered=False, is_incremental=0, show_info=True, if_another_exists_drop_it=True)  %}#}
{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario", "periodo/por_hora"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="ignore",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_clustered_columnstore_index(is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
		]
		
) }}

/*{{ dwh_farinter_create_dummy_data(unique_key=unique_key_list, is_incremental=0, show_info=False)   }}

{{dwh_farinter_union_all_dummy_data(unique_key=unique_key_list, is_incremental=0, show_info=False) }}

*/

SELECT ISNULL([Sucursal_Id],0) AS [Sucursal_Id]
        ,ISNULL([Emp_Id],0) AS [Emp_Id]
        ,[Version_Id]
        ,[Version_Fecha]
        ,[Sucursal_Nombre]
        ,[Marca]
        ,[Zona_Id]
        ,CAST([Zona_Nombre] AS VARCHAR(100)) AS [Zona_Nombre]
        ,[Departamento_Id]
        ,[Departamento_Nombre]
        ,[Municipio_Id]
        ,[Municipio_Nombre]
        ,[Ciudad_Id]
        ,[Ciudad_Nombre]
        ,[TipoSucursal_Id]
        ,[TipoSucursal_Nombre]
        ,[Direccion]
        ,[Estado]
        ,[JOB] as [JOP]
        ,[Supervisor]
        ,[Longitud]
        ,[Latitud]
        ,[CEDI_Id]
        ,[Indicador_CEDI]
        ,[Sucursal_Sinonimo]
        ,[Hash_SucursalEmp]
        ,[Hash_SucursalEmpVersion]
        , {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Sucursal_Id'], input_length=19, table_alias='S')}} [EmpSuc_Id]
        ,ABS(CAST(HASHBYTES('SHA1', 
                        CONCAT([JOB],'')) AS bigint)) AS [Hash_JOP]
        ,ABS(CAST(HASHBYTES('SHA1', 
                        CONCAT([Supervisor],'')) AS bigint)) AS [Hash_Supervisor]
        ,ISNULL({{ dwh_farinter_hash_column(["JOB"]) }},'') AS [HashStr_JOP]
        ,ISNULL({{ dwh_farinter_hash_column(["Supervisor"]) }},'') AS [HashStr_Supervisor]
        ,ISNULL({{ dwh_farinter_hash_column(unique_key_list) }},'') AS [HashStr_SucEmp]
        ,ISNULL({{ dwh_farinter_hash_column(unique_key_list+["Version_Id"]) }},'') AS [HashStr_SucEmpVersion]
        ,LEFT(CONVERT(VARCHAR(32), HASHBYTES('MD5', CAST(CONCAT(Emp_Id,'-', Sucursal_Id) AS NVARCHAR(50))), 2), 32) as HashMD5_EmpSuc
        ,ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
        ,ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
FROM {{ source('DL_FARINTER', 'DL_Kielsa_Sucursal') }} S
{% if is_incremental() %}
  --WHERE S.Fecha_Actualizado >= coalesce((select max(Fecha_Actualizado) from {{ this }}), '19000101')
{% else %}
  --WHERE S.Fecha_Actualizado >= '19000101'
{% endif %}

