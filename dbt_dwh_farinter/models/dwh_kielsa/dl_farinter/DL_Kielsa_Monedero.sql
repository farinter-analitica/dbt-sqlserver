{% set unique_key_list = ["Monedero_Id", "Emp_Id"] %}
{{
    config(
        as_columnstore=true,
        tags=["periodo/diario", "automation/periodo_por_hora"],
        materialized="incremental",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        on_schema_change="append_new_columns",
        merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
        merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=true, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
            "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=is_incremental()) }}"
        ]
    )
}}
{%- if execute %}
    {%- if flags.FULL_REFRESH and var('allow_full_refresh', False) != True %}
        {{- exceptions.raise_compiler_error(
        "Full refresh no esta permitido a menos se pase \"--vars '{\"allow_full_refresh\": true}'\" en el comando de ejecución de dbt."
        ) }}
    {%- endif %}
{%- endif %}
-- dbt dagster - Reemplazo del SP DL_paCargarKielsa_Monedero (tabla principal)
-- Lógica replicada fielmente del SP original
-- noqa:disable=RF02
WITH monedero_source AS (
    SELECT
        A.Fecha_Actualizado,
        CAST(A.Emp_Id AS SMALLINT) AS Emp_Id,
        CAST(A.Monedero_Formato_Identificacion AS SMALLINT) AS Identificacion_Formato,
        CAST(A.Monedero_Fecha_Nacimiento AS DATETIME) AS Nacimiento,
        CAST(A.MonederoTarj_Activa AS BIT) AS Activo_Indicador,
        CAST(A.Monedero_Acumula AS BIT) AS Acumula_Indicador,
        CAST(A.Monedero_Principal AS BIT) AS Principal_Indicador,
        CAST(A.MonederoTarj_Saldo AS DECIMAL(16, 4)) AS Saldo_Puntos,
        CAST(A.MonederoTarj_Ingreso AS DATETIME) AS Ingreso,
        CAST(A.MonederoTarj_UltCompra AS DATETIME) AS UltimaCompra,
        CAST(A.Monedero_Tarj_Fec_Actualizacion AS DATETIME) AS Fecha_Modificado,
        CAST(A.Monedero_Segundo_Nombre AS NVARCHAR(100)) AS Segundo_Nombre,
        CAST(A.Monedero_Segundo_Apellido AS NVARCHAR(100)) AS Segundo_Apellido,
        CAST(A.Monedero_Nivel1 AS SMALLINT) AS Departamento_Id,
        CAST(A.Monedero_Nivel2 AS SMALLINT) AS Municipio_Id,
        CAST(A.Monedero_Nivel3 AS SMALLINT) AS Ciudad_Id,
        CAST(A.Monedero_Nivel4 AS SMALLINT) AS Barrio_Id,
        CAST(A.Consecutivo AS BIGINT) AS Consecutivo,
        CAST(REPLACE(REPLACE(RTRIM(LTRIM(A.MonederoTarj_Id)), ' ', ''), '-', '') AS NVARCHAR(40)) AS Monedero_Id,
        CAST(
            CONCAT(
                RTRIM(LTRIM(REPLACE(A.Monedero_Primer_Nombre, '  ', ''))), ' ',
                RTRIM(LTRIM(REPLACE(A.Monedero_Segundo_Nombre, '  ', ''))), ' ',
                RTRIM(LTRIM(REPLACE(A.Monedero_Primer_Apellido, '  ', ''))), ' ',
                RTRIM(LTRIM(REPLACE(A.Monedero_Segundo_Apellido, '  ', '')))
            ) COLLATE DATABASE_DEFAULT AS NVARCHAR(200)
        ) AS Monedero_Nombre,
        CAST(B.Monedero_Nombre COLLATE DATABASE_DEFAULT AS NVARCHAR(50)) AS Tipo_Plan,
        CAST(REPLACE(REPLACE(A.Monedero_Identificacion, ' ', ''), '-', '') COLLATE DATABASE_DEFAULT AS NVARCHAR(100)) AS Identificacion,
        CAST(A.Monedero_Telefono COLLATE DATABASE_DEFAULT AS NVARCHAR(20)) AS Telefono,
        CAST(A.Monedero_Celular COLLATE DATABASE_DEFAULT AS NVARCHAR(20)) AS Celular,
        CAST(DATEDIFF(YEAR, A.Monedero_Fecha_Nacimiento, GETDATE()) AS INT) AS Edad,
        CAST(
            CASE
                WHEN A.Monedero_Fecha_Nacimiento IS NULL THEN 'No definido'
                WHEN DATEDIFF(YEAR, A.Monedero_Fecha_Nacimiento, GETDATE()) BETWEEN 1 AND 13 THEN '(0-13) NIÑO'
                WHEN DATEDIFF(YEAR, A.Monedero_Fecha_Nacimiento, GETDATE()) BETWEEN 14 AND 19 THEN '(14-19) ADOLESCENTE'
                WHEN DATEDIFF(YEAR, A.Monedero_Fecha_Nacimiento, GETDATE()) BETWEEN 20 AND 35 THEN '(20-35) JOVEN'
                WHEN DATEDIFF(YEAR, A.Monedero_Fecha_Nacimiento, GETDATE()) BETWEEN 36 AND 65 THEN '(36-65) ADULTO'
                ELSE '(66-Mas) ADULTO MAYOR'
            END AS NVARCHAR(50)
        ) AS RangoEdad,
        CAST(A.Monedero_Email COLLATE DATABASE_DEFAULT AS NVARCHAR(100)) AS Correo,
        CAST(
            CASE A.Monedero_Genero
                WHEN 1 THEN 'M'
                WHEN 0 THEN 'H'
                ELSE 'N'
            END COLLATE DATABASE_DEFAULT AS CHAR(1)
        ) AS Genero,
        CAST(A.MonederoTarj_Id COLLATE DATABASE_DEFAULT AS NVARCHAR(40)) AS MonederoTarj_Id_Original,
        CAST(UPPER(LEFT(A.Monedero_Primer_Nombre, 1)) + LOWER(SUBSTRING(A.Monedero_Primer_Nombre, 2, LEN(A.Monedero_Primer_Nombre))) AS NVARCHAR(100)) AS Nombre,
        CAST(UPPER(LEFT(A.Monedero_Primer_Apellido, 1)) + LOWER(SUBSTRING(A.Monedero_Primer_Apellido, 2, LEN(A.Monedero_Primer_Apellido))) AS NVARCHAR(100)) AS Apellido,
        ROW_NUMBER() OVER (
            PARTITION BY REPLACE(REPLACE(RTRIM(LTRIM(A.MonederoTarj_Id)), ' ', ''), '-', ''), A.Emp_Id
            ORDER BY A.MonederoTarj_UltCompra DESC, A.Monedero_Tarj_Fec_Actualizacion DESC, A.Consecutivo DESC
        ) AS FilaMonedero
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_Monedero_Tarjetas_Replica') }} AS A
    INNER JOIN {{ source('DL_FARINTER', 'DL_Kielsa_Monedero_Plan') }} AS B
        ON A.Monedero_Id = B.Monedero_Id AND A.Emp_Id = B.Emp_Id
    WHERE
        A.MonederoTarj_Id <> 'x'
        {% if is_incremental() %}
        -- Lógica de fecha incremental del SP
            AND A.Fecha_Actualizado >= (
                SELECT DATEADD(DAY, -1, COALESCE(MAX(BRA.Bitacora_Fecha), '1900-01-01'))
                FROM DL_FARINTER.dbo.DL_CnfBitacora AS BRA
                WHERE BRA.Bitacora_Tabla = 'DL_Kielsa_Monedero' AND BRA.Bitacora_Tipo = 'Finalizado'
            )
        {% endif %}
),

final_source AS (
    SELECT *
    FROM monedero_source
    WHERE FilaMonedero = 1
)

{% if is_incremental() %}
    ,
    existing_records AS (
        SELECT er.*
        FROM {{ this }} AS er
        INNER JOIN final_source AS fs
            ON
                er.Monedero_Id = fs.Monedero_Id
                AND er.Emp_Id = fs.Emp_Id
    )
    ,
    merged_records AS (
        SELECT
            fs.Monedero_Id,
            fs.Emp_Id,
            fs.Monedero_Nombre,
            fs.Tipo_Plan,  -- Mantener los IDs únicos
            fs.Identificacion,
            fs.Identificacion_Formato,
            fs.Telefono,
            fs.Celular,
            fs.Nacimiento,
            fs.Edad,
            fs.RangoEdad,
            fs.Correo,
            fs.Activo_Indicador,
            fs.Acumula_Indicador,
            fs.Principal_Indicador,
            fs.Genero,
            fs.Saldo_Puntos,
            fs.Ingreso,
            fs.MonederoTarj_Id_Original,
            fs.Nombre,
            fs.Apellido,
            fs.UltimaCompra,
            fs.Fecha_Modificado,
            fs.Segundo_Nombre,
            fs.Segundo_Apellido,
            fs.Departamento_Id,
            fs.Municipio_Id,
            fs.Ciudad_Id,
            fs.Barrio_Id,
            fs.Consecutivo,
            -- noqa:disable=RF02
            CASE
                WHEN er.Monedero_Id IS NULL THEN 1 -- Nuevo registro, versión inicial 1
                WHEN
                    er.Version_Fecha < CAST(GETDATE() AS DATE)
                    AND (
                        ISNULL(er.Monedero_Nombre, '') COLLATE DATABASE_DEFAULT <> ISNULL(fs.Monedero_Nombre, '') COLLATE DATABASE_DEFAULT
                        OR ISNULL(er.Tipo_Plan, '') COLLATE DATABASE_DEFAULT <> ISNULL(fs.Tipo_Plan, '') COLLATE DATABASE_DEFAULT
                        OR ISNULL(er.Identificacion, '') COLLATE DATABASE_DEFAULT <> ISNULL(fs.Identificacion, '') COLLATE DATABASE_DEFAULT
                        OR ISNULL(er.Identificacion_Formato, -1) <> ISNULL(fs.Identificacion_Formato, -1)
                        OR ISNULL(er.Telefono, '') COLLATE DATABASE_DEFAULT <> ISNULL(fs.Telefono, '') COLLATE DATABASE_DEFAULT
                        OR ISNULL(er.Celular, '') COLLATE DATABASE_DEFAULT <> ISNULL(fs.Celular, '') COLLATE DATABASE_DEFAULT
                        OR ISNULL(er.Nacimiento, '1900-01-01T00:00:00') <> ISNULL(fs.Nacimiento, '1900-01-01T00:00:00')
                        OR ISNULL(er.Correo, '') COLLATE DATABASE_DEFAULT <> ISNULL(fs.Correo, '') COLLATE DATABASE_DEFAULT
                        OR ISNULL(er.Activo_Indicador, 0) <> ISNULL(fs.Activo_Indicador, 0)
                        OR ISNULL(er.Acumula_Indicador, 0) <> ISNULL(fs.Acumula_Indicador, 0)
                        OR ISNULL(er.Principal_Indicador, 0) <> ISNULL(fs.Principal_Indicador, 0)
                        OR ISNULL(er.Genero, '') COLLATE DATABASE_DEFAULT <> ISNULL(fs.Genero, '') COLLATE DATABASE_DEFAULT
                        OR ISNULL(er.Ingreso, '1900-01-01T00:00:00') <> ISNULL(fs.Ingreso, '1900-01-01T00:00:00')
                        OR ISNULL(er.MonederoTarj_Id_Original, '') COLLATE DATABASE_DEFAULT <> ISNULL(fs.MonederoTarj_Id_Original, '') COLLATE DATABASE_DEFAULT
                        OR ISNULL(er.Departamento_Id, -1) <> ISNULL(fs.Departamento_Id, -1)
                    )
                    THEN er.Version_Id + 1
                ELSE er.Version_Id
            END AS Version_Id,
            CASE
                WHEN er.Monedero_Id IS NULL THEN GETDATE() -- Nuevo registro, fecha actual
                WHEN
                    er.Version_Fecha < CAST(GETDATE() AS DATE)
                    AND (
                        ISNULL(er.Monedero_Nombre, '') COLLATE DATABASE_DEFAULT <> ISNULL(fs.Monedero_Nombre, '') COLLATE DATABASE_DEFAULT
                        OR ISNULL(er.Tipo_Plan, '') COLLATE DATABASE_DEFAULT <> ISNULL(fs.Tipo_Plan, '') COLLATE DATABASE_DEFAULT
                        OR ISNULL(er.Identificacion, '') COLLATE DATABASE_DEFAULT <> ISNULL(fs.Identificacion, '') COLLATE DATABASE_DEFAULT
                        OR ISNULL(er.Identificacion_Formato, -1) <> ISNULL(fs.Identificacion_Formato, -1)
                        OR ISNULL(er.Telefono, '') COLLATE DATABASE_DEFAULT <> ISNULL(fs.Telefono, '') COLLATE DATABASE_DEFAULT
                        OR ISNULL(er.Celular, '') COLLATE DATABASE_DEFAULT <> ISNULL(fs.Celular, '') COLLATE DATABASE_DEFAULT
                        OR ISNULL(er.Nacimiento, '1900-01-01T00:00:00') <> ISNULL(fs.Nacimiento, '1900-01-01T00:00:00')
                        OR ISNULL(er.Correo, '') COLLATE DATABASE_DEFAULT <> ISNULL(fs.Correo, '') COLLATE DATABASE_DEFAULT
                        OR ISNULL(er.Activo_Indicador, 0) <> ISNULL(fs.Activo_Indicador, 0)
                        OR ISNULL(er.Acumula_Indicador, 0) <> ISNULL(fs.Acumula_Indicador, 0)
                        OR ISNULL(er.Principal_Indicador, 0) <> ISNULL(fs.Principal_Indicador, 0)
                        OR ISNULL(er.Genero, '') COLLATE DATABASE_DEFAULT <> ISNULL(fs.Genero, '') COLLATE DATABASE_DEFAULT
                        OR ISNULL(er.Ingreso, '1900-01-01T00:00:00') <> ISNULL(fs.Ingreso, '1900-01-01T00:00:00')
                        OR ISNULL(er.MonederoTarj_Id_Original, '') COLLATE DATABASE_DEFAULT <> ISNULL(fs.MonederoTarj_Id_Original, '') COLLATE DATABASE_DEFAULT
                        OR ISNULL(er.Departamento_Id, -1) <> ISNULL(fs.Departamento_Id, -1)
                    )
                    THEN GETDATE()
                ELSE er.Version_Fecha
            END AS Version_Fecha,
            -- noqa:enable=RF02
            CAST(GETDATE() AS DATETIME) AS Fecha_Carga
        FROM final_source AS fs
        LEFT JOIN existing_records AS er ON fs.Monedero_Id = er.Monedero_Id AND fs.Emp_Id = er.Emp_Id
    )

    SELECT
        m.Version_Id,
        m.Version_Fecha,
        m.Monedero_Nombre,
        m.Tipo_Plan,
        m.Identificacion,
        m.Identificacion_Formato,
        m.Telefono,
        m.Celular,
        m.Nacimiento,
        m.Edad,
        m.RangoEdad,
        m.Correo,
        m.Activo_Indicador,
        m.Acumula_Indicador,
        m.Principal_Indicador,
        m.Genero,
        m.Saldo_Puntos,
        m.Ingreso,
        m.MonederoTarj_Id_Original,
        m.Nombre,
        m.Apellido,
        m.UltimaCompra,
        m.Fecha_Modificado,
        m.Segundo_Nombre,
        m.Segundo_Apellido,
        m.Departamento_Id,
        m.Municipio_Id,
        m.Ciudad_Id,
        m.Barrio_Id,
        m.Consecutivo,
        m.Fecha_Carga,
        ISNULL(m.Monedero_Id, '') AS Monedero_Id,
        ISNULL(m.Emp_Id, 0) AS Emp_Id,
        ABS(CAST(HASHBYTES('SHA1', CONCAT(m.Monedero_Id, m.Emp_Id)) AS BIGINT)) AS Hash_MonederoEmp,
        ABS(CAST(HASHBYTES('SHA1', CONCAT(m.Monedero_Id, m.Emp_Id, m.Version_Id)) AS BIGINT)) AS Hash_MonederoEmpVersion,
        ISNULL(CONVERT(VARCHAR(32), HASHBYTES('SHA2_256', CAST(CONCAT(m.Monedero_Id, '-', m.Emp_Id) AS VARCHAR(8000))), 2), '') AS HashStr_MonEmp,
        ISNULL(CONVERT(VARCHAR(32), HASHBYTES('SHA2_256', CAST(CONCAT(m.Monedero_Id, '-', m.Emp_Id, '-', m.Version_Id) AS VARCHAR(8000))), 2), '') AS HashStr_MonEmpVer
    FROM merged_records AS m

{% else %}
-- Lógica para carga completa (full refresh)
SELECT --noqa: ST06
    ISNULL(fs.Monedero_Id, '') AS Monedero_Id,
    ISNULL(fs.Emp_Id, 0) AS Emp_Id,
    1 AS Version_Id,
    GETDATE() AS Version_Fecha,
    fs.Monedero_Nombre, fs.Tipo_Plan, fs.Identificacion, fs.Identificacion_Formato, fs.Telefono,
    fs.Celular, fs.Nacimiento, fs.Edad, fs.RangoEdad, fs.Correo, fs.Activo_Indicador,
    fs.Acumula_Indicador, fs.Principal_Indicador, fs.Genero, fs.Saldo_Puntos, fs.Ingreso,
    fs.MonederoTarj_Id_Original, fs.Nombre, fs.Apellido, fs.UltimaCompra, fs.Fecha_Modificado,
    ABS(CAST(HASHBYTES('SHA1', CONCAT(fs.Monedero_Id, fs.Emp_Id)) AS BIGINT)) AS Hash_MonederoEmp,
    ABS(CAST(HASHBYTES('SHA1', CONCAT(fs.Monedero_Id, fs.Emp_Id, 1)) AS BIGINT)) AS Hash_MonederoEmpVersion,
    fs.Segundo_Nombre, fs.Segundo_Apellido, fs.Departamento_Id, fs.Municipio_Id, fs.Ciudad_Id,
    fs.Barrio_Id, fs.Consecutivo,
    ISNULL(CONVERT(VARCHAR(32), HASHBYTES('SHA2_256', CAST(CONCAT(fs.Monedero_Id, '-', fs.Emp_Id) AS VARCHAR(8000))), 2), '') AS HashStr_MonEmp,
    ISNULL(CONVERT(VARCHAR(32), HASHBYTES('SHA2_256', CAST(CONCAT(fs.Monedero_Id, '-', fs.Emp_Id, '-', 1) AS VARCHAR(8000))), 2), '') AS HashStr_MonEmpVer,
    CAST(GETDATE() AS DATETIME) AS Fecha_Carga
FROM final_source fs
{% endif %}
