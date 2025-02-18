
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
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_clustered_columnstore_index(is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
		]
		
) -}}


SELECT ISNULL(S.[Sucursal_Id],0) AS [Sucursal_Id]
        ,ISNULL(S.[Emp_Id],0) AS [Emp_Id]
        ,[Version_Id]
        ,[Version_Fecha]
        ,S.[Sucursal_Nombre]
        ,ISNULL(SN.[Sucursal_Numero],0) AS [Sucursal_Numero]
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
        ,ISNULL(RSJ.[Usuario_Id], '0') as [Usuario_JOP_Id]
        -- TODO: Migrar tablero de Jefrey a usar ID en lugar de nombres, implementar nuevamente el control por usuario.
        ,CASE WHEN S.Emp_id = 1 then S.JOB  COLLATE DATABASE_DEFAULT ELSE  ISNULL(RSJ.[Usuario_Nombre], 'No Definido') END as [JOP]
        ,ISNULL(RSS.[Usuario_Id], '0') as [Usuario_Supervisor_Id]
        ,CASE WHEN S.Emp_id = 1 then S.Supervisor COLLATE DATABASE_DEFAULT ELSE  ISNULL(RSS.[Usuario_Nombre], 'No Definido') END as [Supervisor]
        ,[Longitud]
        ,[Latitud]
        ,[CEDI_Id]
        ,[Indicador_CEDI]
        ,[Sucursal_Sinonimo] COLLATE DATABASE_DEFAULT AS [Sucursal_Sinonimo] 
        ,[Hash_SucursalEmp]
        ,[Hash_SucursalEmpVersion]
        , {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Sucursal_Id'], input_length=19, table_alias='S')}} [EmpSuc_Id]
        ,ABS(CAST(HASHBYTES('SHA1', 
                        CONCAT([JOB],'')) AS bigint)) AS [Hash_JOP]
        ,ABS(CAST(HASHBYTES('SHA1', 
                        CONCAT([Supervisor],'')) AS bigint)) AS [Hash_Supervisor]
        ,ISNULL({{ dwh_farinter_hash_column(["JOB"], table_alias='S' ) }},'') AS [HashStr_JOP]
        ,ISNULL({{ dwh_farinter_hash_column(["Supervisor"], table_alias='S') }},'') AS [HashStr_Supervisor]
        ,ISNULL({{ dwh_farinter_hash_column(unique_key_list, table_alias='S') }},'') AS [HashStr_SucEmp]
        ,ISNULL({{ dwh_farinter_hash_column(unique_key_list+["Version_Id"], table_alias='S') }},'') AS [HashStr_SucEmpVersion]
        ,LEFT(CONVERT(VARCHAR(32), HASHBYTES('MD5', CAST(CONCAT(S.Emp_Id,'-', S.Sucursal_Id) AS NVARCHAR(50))), 2), 32) as HashMD5_EmpSuc
        ,ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
        ,ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
FROM DL_FARINTER.dbo.DL_Kielsa_Sucursal S -- {{ source('DL_FARINTER', 'DL_Kielsa_Sucursal') }} S
LEFT JOIN (SELECT
    *, CAST(SUBSTRING(SNP2.Sucursal_Nombre, SNP2.Posicion_Inicial, SNP2.Posicion_Final) AS INT) AS [Sucursal_Numero]
  FROM
    (SELECT
      *
      , (LEN(Sucursal_Nombre) + 2 - Posicion_Inicial_Derecha) - Posicion_Inicial AS Longitud_Numero
      , LEN(Sucursal_Nombre) Longitud_Nombre
    FROM
      (SELECT
        *
        , PATINDEX('%[0-9]%', [Sucursal_Nombre]) AS Posicion_Inicial
        , PATINDEX('%[A-Z]%', SUBSTRING([Sucursal_Nombre], PATINDEX('%[0-9]%', [Sucursal_Nombre]) + 1, 50)) AS Posicion_Final
        , PATINDEX('%[0-9]%', REVERSE([Sucursal_Nombre])) AS Posicion_Inicial_Derecha
        , PATINDEX(
          '%[A-Z]%'
          , SUBSTRING(REVERSE([Sucursal_Nombre]), PATINDEX('%[0-9]%', REVERSE([Sucursal_Nombre])) + 1, 50)) AS Posicion_Final_Derecha
      FROM
        (SELECT
          [Sucursal_Id]
          , [Emp_Id]
          , 'A' + REPLACE(REPLACE(UPPER([Sucursal_Nombre]), ' ', ''), '-', '') + 'Z' AS [Sucursal_Nombre]
        FROM	DL_FARINTER.dbo.DL_Kielsa_Sucursal -- {{ source('DL_FARINTER', 'DL_Kielsa_Sucursal') }}
        ) SN ) SNP
    WHERE Posicion_Inicial > 0) SNP2) SN
  ON S.Sucursal_Id = SN.Sucursal_Id
  AND S.Emp_Id = SN.Emp_Id
LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_Sucursal_Rol_Usuario_Asignado RSJ -- {{ ref('DL_Kielsa_Sucursal_Rol_Usuario_Asignado') }} US
  ON S.Emp_Id = RSJ.Emp_Id
  AND S.Sucursal_Id = RSJ.Suc_Id
  AND RSJ.Rol_Sucursal = 'JOP'
LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_Sucursal_Rol_Usuario_Asignado RSS -- {{ ref('DL_Kielsa_Sucursal_Rol_Usuario_Asignado') }} US
  ON S.Emp_Id = RSS.Emp_Id
  AND S.Sucursal_Id = RSS.Suc_Id
  AND RSS.Rol_Sucursal = 'Supervisor'
{% if is_incremental() %}
  --WHERE S.Fecha_Actualizado >= coalesce((select max(Fecha_Actualizado) from {{ this }}), '19000101')
{% else %}
  --WHERE S.Fecha_Actualizado >= '19000101'
{% endif %}

