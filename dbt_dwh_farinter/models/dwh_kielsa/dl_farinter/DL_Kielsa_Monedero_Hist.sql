{% set unique_key_list = ["Monedero_Id", "Emp_Id", "Version_Id"] %}
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

-- dbt dagster - Reemplazo del SP DL_paCargarKielsa_Monedero (tabla histórica)
-- Lógica replicada fielmente del SP original
-- dbt dagster - Reemplazo del SP DL_paCargarKielsa_Monedero (tabla histórica)

{% if is_incremental() %}
    -- Para cargas incrementales, necesitamos manejar los cambios de versión
    WITH current_records AS (
        SELECT M.*
        FROM {{ ref('DL_Kielsa_Monedero') }} AS M
        WHERE M.Fecha_Actualizado >= DATEADD(DAY, -1, (
            SELECT COALESCE(MAX(T.Fecha_Modificado), '19000101')
            FROM {{ this }} AS T
        ))
    ),

    existing_hist AS (
        SELECT eh.*
        FROM {{ this }} AS eh
        INNER JOIN ( --noqa: ST05
            SELECT
                Monedero_Id,
                Emp_Id
            FROM current_records
        ) AS cr
            ON
                eh.Monedero_Id = cr.Monedero_Id
                AND eh.Emp_Id = cr.Emp_Id
    ),

    version_changes AS (
        -- Identificar registros que necesitan nueva versión histórica
        SELECT
            cr.*,
            eh.Version_Id AS existing_version_id,
            CASE
                WHEN eh.Version_Id IS NULL THEN 'NEW'  -- Registro completamente nuevo
                WHEN cr.Version_Id > eh.Version_Id THEN 'VERSION_CHANGE'  -- Cambio de versión
                ELSE 'UPDATE'  -- Actualización sin cambio de versión
            END AS change_type
        FROM current_records AS cr
        LEFT JOIN existing_hist AS eh
            ON
                cr.Monedero_Id = eh.Monedero_Id
                AND cr.Emp_Id = eh.Emp_Id
                AND eh.Version_Hasta = CAST('99991231' AS DATE)  -- Solo la versión activa
    ),

    records_to_close AS (
        -- Cerrar versiones anteriores cuando hay cambio de versión
        SELECT
            eh.*,
            CAST(GETDATE() AS DATE) AS new_version_hasta
        FROM existing_hist AS eh
        INNER JOIN version_changes AS vc
            ON
                eh.Monedero_Id = vc.Monedero_Id
                AND eh.Emp_Id = vc.Emp_Id
                AND eh.Version_Id = vc.existing_version_id
        WHERE
            vc.change_type = 'VERSION_CHANGE'
            AND eh.Version_Hasta = CAST('99991231' AS DATE)
    ),

    new_historical_records AS (
        -- Nuevos registros históricos (nuevos monederos o nuevas versiones)
        SELECT  --noqa: ST06
            CAST(ISNULL(Monedero_Id, '') AS NVARCHAR(40)) AS Monedero_Id,
            CAST(ISNULL(Emp_Id, 0) AS SMALLINT) AS Emp_Id,
            Version_Id,
            Version_Fecha AS Version_Desde,
            CAST('99991231' AS DATE) AS Version_Hasta,
            Monedero_Nombre,
            Tipo_Plan,
            Identificacion,
            Identificacion_Formato,
            Telefono,
            Celular,
            Nacimiento,
            Edad,
            RangoEdad,
            Correo,
            Activo_Indicador,
            Acumula_Indicador,
            Principal_Indicador,
            Genero,
            Saldo_Puntos,
            Ingreso,
            MonederoTarj_Id_Original,
            Nombre,
            Apellido,
            UltimaCompra,
            Fecha_Modificado,
            HashStr_MonEmp,
            HashStr_MonEmpVer,
            Segundo_Nombre,
            Segundo_Apellido,
            Departamento_Id,
            Municipio_Id,
            Ciudad_Id,
            Barrio_Id,
            Consecutivo
        FROM version_changes
        WHERE change_type IN ('NEW', 'VERSION_CHANGE')
    ),

    updated_existing_records AS (
        -- Actualizar registros existentes sin cambio de versión
        SELECT  --noqa: ST06
            CAST(ISNULL(vc.Monedero_Id, '') AS NVARCHAR(40)) AS Monedero_Id,
            CAST(ISNULL(vc.Emp_Id, 0) AS SMALLINT) AS Emp_Id,
            vc.Version_Id,
            vc.Version_Fecha AS Version_Desde,
            eh.Version_Hasta,
            vc.Monedero_Nombre,
            vc.Tipo_Plan,
            vc.Identificacion,
            vc.Identificacion_Formato,
            vc.Telefono,
            vc.Celular,
            vc.Nacimiento,
            vc.Edad,
            vc.RangoEdad,
            vc.Correo,
            vc.Activo_Indicador,
            vc.Acumula_Indicador,
            vc.Principal_Indicador,
            vc.Genero,
            vc.Saldo_Puntos,
            vc.Ingreso,
            vc.MonederoTarj_Id_Original,
            vc.Nombre,
            vc.Apellido,
            vc.UltimaCompra,
            vc.Fecha_Modificado,
            vc.HashStr_MonEmp,
            vc.HashStr_MonEmpVer,
            vc.Segundo_Nombre,
            vc.Segundo_Apellido,
            vc.Departamento_Id,
            vc.Municipio_Id,
            vc.Ciudad_Id,
            vc.Barrio_Id,
            vc.Consecutivo
        FROM version_changes AS vc
        INNER JOIN existing_hist AS eh
            ON
                vc.Monedero_Id = eh.Monedero_Id
                AND vc.Emp_Id = eh.Emp_Id
                AND vc.Version_Id = eh.Version_Id
        WHERE vc.change_type = 'UPDATE'
    ),

    closed_versions AS (
        -- Versiones cerradas por cambio de versión
        SELECT  --noqa: ST06
            CAST(ISNULL(Monedero_Id, '') AS NVARCHAR(40)) AS Monedero_Id,
            CAST(ISNULL(Emp_Id, 0) AS SMALLINT) AS Emp_Id,
            Version_Id,
            Version_Desde,
            new_version_hasta AS Version_Hasta,
            Monedero_Nombre,
            Tipo_Plan,
            Identificacion,
            Identificacion_Formato,
            Telefono,
            Celular,
            Nacimiento,
            Edad,
            RangoEdad,
            Correo,
            Activo_Indicador,
            Acumula_Indicador,
            Principal_Indicador,
            Genero,
            Saldo_Puntos,
            Ingreso,
            MonederoTarj_Id_Original,
            Nombre,
            Apellido,
            UltimaCompra,
            Fecha_Modificado,
            HashStr_MonEmp,
            HashStr_MonEmpVer,
            Segundo_Nombre,
            Segundo_Apellido,
            Departamento_Id,
            Municipio_Id,
            Ciudad_Id,
            Barrio_Id,
            Consecutivo
        FROM records_to_close
    ),

    Combinados AS (
        -- Combinar todos los registros para la carga incremental
        SELECT * FROM new_historical_records
        UNION ALL
        SELECT * FROM updated_existing_records
        UNION ALL
        SELECT * FROM closed_versions
    )

    SELECT *
    FROM Combinados
{% else %}
    -- Para carga completa inicial, tomar todos los registros de la tabla principal
    SELECT  --noqa: ST06
        CAST(ISNULL(Monedero_Id, '') AS NVARCHAR(40)) AS Monedero_Id,
        CAST(ISNULL(Emp_Id, 0) AS SMALLINT) AS Emp_Id,
        Version_Id,
        Version_Fecha AS Version_Desde,
        CAST('99991231' AS DATE) AS Version_Hasta,
        Monedero_Nombre,
        Tipo_Plan,
        Identificacion,
        Identificacion_Formato,
        Telefono,
        Celular,
        Nacimiento,
        Edad,
        RangoEdad,
        Correo,
        Activo_Indicador,
        Acumula_Indicador,
        Principal_Indicador,
        Genero,
        Saldo_Puntos,
        Ingreso,
        MonederoTarj_Id_Original,
        Nombre,
        Apellido,
        UltimaCompra,
        Fecha_Modificado,
        HashStr_MonEmp,
        HashStr_MonEmpVer,
        Segundo_Nombre,
        Segundo_Apellido,
        Departamento_Id,
        Municipio_Id,
        Ciudad_Id,
        Barrio_Id,
        Consecutivo
    FROM {{ ref('DL_Kielsa_Monedero') }}
    
{% endif %}
