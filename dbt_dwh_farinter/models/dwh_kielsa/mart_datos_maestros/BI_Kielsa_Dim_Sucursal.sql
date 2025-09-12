{# Add dwh_farinter_remove_incremental_temp_table to all incremental models #}
{# unique_key is accessible with config.get('unique_key') but it returns a string #}
{# remember that macro here executes before the model is created, so we can't use it here #}
{% set unique_key_list = ["Sucursal_Id","Emp_Id"] %}
{# Post_hook can't access this context variables so we create the string here if
needed only if the macros dont depende on query execution (just returns the query text) #}
{#{% set post_hook_dwh_farinter_create_primary_key =  
dwh_farinter_create_primary_key(this,columns=unique_key_list, create_clustered=False, 
is_incremental=0, show_info=True, if_another_exists_drop_it=True)  %}#}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario", "automation/periodo_por_hora"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		on_schema_change="append_new_columns",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
		]
		
) -}}

WITH --noqa: disable=RF02, RF03, ST06
Sucursal_Numero AS (
    SELECT
        *,
        CAST(SUBSTRING(SNP2.Sucursal_Nombre, SNP2.Posicion_Inicial, SNP2.Posicion_Final) AS INT) AS [Sucursal_Numero],
        CAST(SUBSTRING(SNP2.Sucursal_Nombre, SNP2.Posicion_Inicial, SNP2.Posicion_Final) AS VARCHAR(50)) AS [Sucursal_Numero_Char],
        LEN(CAST(SUBSTRING(SNP2.Sucursal_Nombre, SNP2.Posicion_Inicial, SNP2.Posicion_Final) AS VARCHAR(50))) AS [Sucursal_Numero_Len]
    FROM
        (SELECT
            *,
            (LEN(Sucursal_Nombre) + 2 - Posicion_Inicial_Derecha) - Posicion_Inicial AS Longitud_Numero,
            LEN(Sucursal_Nombre) AS Longitud_Nombre
        FROM
            (
                SELECT
                    *,
                    PATINDEX('%[0-9]%', [Sucursal_Nombre]) AS Posicion_Inicial,
                    PATINDEX('%[A-Z]%', SUBSTRING([Sucursal_Nombre], PATINDEX('%[0-9]%', [Sucursal_Nombre]) + 1, 50)) AS Posicion_Final,
                    PATINDEX('%[0-9]%', REVERSE([Sucursal_Nombre])) AS Posicion_Inicial_Derecha,
                    PATINDEX(
                        '%[A-Z]%',
                        SUBSTRING(REVERSE([Sucursal_Nombre]), PATINDEX('%[0-9]%', REVERSE([Sucursal_Nombre])) + 1, 50)
                    ) AS Posicion_Final_Derecha
                FROM
                    (
                        SELECT
                            [Sucursal_Id],
                            [Emp_Id],
                            'A' + REPLACE(REPLACE(UPPER([Sucursal_Nombre]), ' ', ''), '-', '') + 'Z' AS [Sucursal_Nombre]
                        FROM DL_FARINTER.dbo.DL_Kielsa_Sucursal -- {{ source('DL_FARINTER', 'DL_Kielsa_Sucursal') }}
                    ) AS SN
            ) AS SNP
        WHERE Posicion_Inicial > 0) AS SNP2
),

Sucursal_Numero_Fila AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY [Emp_Id], [Sucursal_Numero] ORDER BY [Sucursal_Id] DESC) AS [Fila]
    FROM Sucursal_Numero
),

Sucursal_Numero_Fila_Unico AS (
    SELECT *
    FROM Sucursal_Numero_Fila
    WHERE [Fila] = 1
)

SELECT
    ISNULL(S.[Sucursal_Id], 0) AS [Sucursal_Id],
    ISNULL(S.[Emp_Id], 0) AS [Emp_Id],
    S.[Version_Id],
    S.[Version_Fecha],
    S.[Sucursal_Nombre],
    ISNULL(SN.[Sucursal_Numero], 0) AS [Sucursal_Numero],
    S.[Marca],
    S.[Zona_Id],
    {{ dwh_farinter_concat_key_columns(
          columns=['Emp_Id', 'Zona_Id'], 
          input_length=49, 
          table_alias='S') }} AS EmpZona_Id,
    CAST(S.[Zona_Nombre] AS VARCHAR(100)) AS [Zona_Nombre],
    S.[Departamento_Id],
    {{ dwh_farinter_concat_key_columns(
          columns=['Emp_Id', 'Departamento_Id'], 
          input_length=49, 
          table_alias='S') }} AS EmpDep_Id,
    S.[Departamento_Nombre],
    S.[Municipio_Id],
    {{ dwh_farinter_concat_key_columns(
          columns=['Emp_Id', 'Departamento_Id', 'Municipio_Id'], 
          input_length=49, 
          table_alias='S') }} AS EmpDepMun_Id,
    S.[Municipio_Nombre],
    S.[Ciudad_Id],
    {{ dwh_farinter_concat_key_columns(
          columns=['Emp_Id', 'Departamento_Id', 'Municipio_Id', 'Ciudad_Id'], 
          input_length=49, 
          table_alias='S') }} AS EmpDepMunCiu_Id,
    S.[Ciudad_Nombre],
    S.[TipoSucursal_Id],
    S.[TipoSucursal_Nombre],
    CASE
        WHEN SN.[Sucursal_Numero_Char] IS NULL OR SN.[Sucursal_Numero_Len] = 0
            THEN 'No Definido'
        WHEN S.Emp_Id = 1 THEN CONCAT('kielsa', REPLICATE('0', 3 - SN.[Sucursal_Numero_Len]) + SN.[Sucursal_Numero_Char], '@kielsa.hn')
        WHEN S.Emp_Id = 2 THEN CONCAT('h', SN.[Sucursal_Numero_Char], '@farmaciasherdez.com')
        WHEN S.Emp_Id = 3 THEN CONCAT('kielsa', REPLICATE('0', 3 - SN.[Sucursal_Numero_Len]) + SN.[Sucursal_Numero_Char], '@kielsa.ni')
        WHEN S.Emp_Id = 4 THEN CONCAT('kielsa', REPLICATE('0', 3 - SN.[Sucursal_Numero_Len]) + SN.[Sucursal_Numero_Char], '@kielsa.cr')
        WHEN S.Emp_Id = 5 THEN CONCAT('b', REPLICATE('0', 3 - SN.[Sucursal_Numero_Len]) + SN.[Sucursal_Numero_Char], '@grupobrasilsv.com')
        ELSE 'No Definido'
    END AS [Correo_e],
    S.[Direccion],
    S.[Estado],
    ISNULL(RSJ.[Usuario_Id], '0') AS [Usuario_JOP_Id],
    -- TODO: Migrar tablero de Jefrey a usar ID en lugar de nombres, implementar nuevamente el control por usuario.
    CASE WHEN S.Emp_id = 1 THEN S.JOB COLLATE DATABASE_DEFAULT ELSE ISNULL(RSJ.[Usuario_Nombre], 'No Definido') END AS JOP,
    ISNULL(RSS.[Usuario_Id], '0') AS [Usuario_Supervisor_Id],
    CASE WHEN S.Emp_id = 1 THEN S.Supervisor COLLATE DATABASE_DEFAULT ELSE ISNULL(RSS.[Usuario_Nombre], 'No Definido') END AS [Supervisor],
    RSS.[Usuario_Nombre] AS Usuario_Supervisor_Nombre,
    S.[Longitud],
    S.[Latitud],
    S.[CEDI_Id],
    S.[Indicador_CEDI],
    S.[Sucursal_Sinonimo] COLLATE DATABASE_DEFAULT AS [Sucursal_Sinonimo],
    S.[Hash_SucursalEmp],
    S.[Hash_SucursalEmpVersion],
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Sucursal_Id'], input_length=19, table_alias='S') }} AS [EmpSuc_Id],
    ABS(CAST(HASHBYTES(
        'SHA1',
        CONCAT(S.JOB, '')
    ) AS BIGINT)) AS [Hash_JOP],
    ABS(CAST(HASHBYTES(
        'SHA1',
        CONCAT(S.[Supervisor], '')
    ) AS BIGINT)) AS [Hash_Supervisor],
    ISNULL({{ dwh_farinter_hash_column(["JOB"], table_alias='S' ) }}, '') AS [HashStr_JOP],
    ISNULL({{ dwh_farinter_hash_column(["Supervisor"], table_alias='S') }}, '') AS [HashStr_Supervisor],
    ISNULL({{ dwh_farinter_hash_column(["Usuario_Id"], table_alias='RSS') }}, '') AS [HashStr_SupervisorId],
    ISNULL({{ dwh_farinter_hash_column(unique_key_list, table_alias='S') }}, '') AS [HashStr_SucEmp],
    ISNULL({{ dwh_farinter_hash_column(unique_key_list+["Version_Id"], table_alias='S') }}, '') AS [HashStr_SucEmpVersion],
    LEFT(CONVERT(VARCHAR(32), HASHBYTES('MD5', CAST(CONCAT(S.Emp_Id, '-', S.Sucursal_Id) AS NVARCHAR(50))), 2), 32) AS HashMD5_EmpSuc,
    ISNULL(CAST(GETDATE() AS DATETIME), '19000101') AS [Fecha_Carga],
    ISNULL(CAST(GETDATE() AS DATETIME), '19000101') AS [Fecha_Actualizado]
FROM DL_FARINTER.dbo.DL_Kielsa_Sucursal AS S -- {{ source('DL_FARINTER', 'DL_Kielsa_Sucursal') }} S
LEFT JOIN Sucursal_Numero_Fila_Unico AS SN
    ON
        S.Sucursal_Id = SN.Sucursal_Id
        AND S.Emp_Id = SN.Emp_Id
LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_Sucursal_Rol_Usuario_Asignado AS RSJ -- {{ ref('DL_Kielsa_Sucursal_Rol_Usuario_Asignado') }} US
    ON
        S.Emp_Id = RSJ.Emp_Id
        AND S.Sucursal_Id = RSJ.Suc_Id
        AND RSJ.Rol_Sucursal = 'JOP'
LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_Sucursal_Rol_Usuario_Asignado AS RSS -- {{ ref('DL_Kielsa_Sucursal_Rol_Usuario_Asignado') }} US
    ON
        S.Emp_Id = RSS.Emp_Id
        AND S.Sucursal_Id = RSS.Suc_Id
        AND RSS.Rol_Sucursal = 'Supervisor'
{% if is_incremental() %}
--WHERE S.Fecha_Actualizado >= coalesce((select max(Fecha_Actualizado) from {{ this }}), '19000101')
{% else %}
  --WHERE S.Fecha_Actualizado >= '19000101'
{% endif %}
