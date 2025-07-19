{%- set unique_key_list = ["Comision_Id","Articulo_Id","Suc_Id","Emp_Id"] -%}

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
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
            "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Emp_Id', 'Suc_Id', 'Articulo_Id', 'Es_Ultimo_Rango']) }}",
            "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
        ]
    ) 
}}

{%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -1, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, 
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '19000101' %}
{%- endif %}

WITH Comision_Nuevos_Actualizados AS (
    -- 1. Obtener registros nuevos o modificados desde el origen
    SELECT
        CD.Emp_Id,
        CD.Comision_Id,
        CD.Suc_Id,
        CD.Articulo_Id,
        CD.Comision_Monto AS Detalle_Comision_Monto,
        CD.Consecutivo AS Detalle_Consecutivo,
        CE.Comision_Fecha_Inicial,
        CE.Comision_Fecha_Final,
        CE.Comision_Nombre,
        CE.Comision_Estado,
        (SELECT MAX(fecha) FROM (
            VALUES
            (ISNULL(CE.Fecha_Actualizado, '19000101')),
            (ISNULL(CD.Fecha_Actualizado, '19000101'))
        ) AS MaxFecha (fecha)) AS Fecha_Actualizado
    FROM {{ ref('DL_Kielsa_Comision_Encabezado') }} AS CE
    INNER JOIN {{ ref('DL_Kielsa_Comision_Detalle') }} AS CD
        ON
            CE.Emp_Id = CD.Emp_Id
            AND CE.Comision_Id = CD.Comision_Id
    {% if is_incremental() %}
        WHERE (SELECT MAX(fecha) FROM (
            VALUES
            (ISNULL(CE.Fecha_Actualizado, '19000101')),
            (ISNULL(CD.Fecha_Actualizado, '19000101'))
        ) AS MaxFecha (fecha)) >= '{{ last_date }}'
    {% endif %}
)

{% if is_incremental() %}
    , Claves_Afectadas_Con_Fechas AS (
    -- 2. Identificar las particiones que tienen cambios y sus rangos de fechas
        SELECT
            Emp_Id,
            Suc_Id,
            Articulo_Id,
            MIN(Comision_Fecha_Inicial) AS Min_Fecha_Inicial,
            MAX(Comision_Fecha_Final) AS Max_Fecha_Final
        FROM Comision_Nuevos_Actualizados
        GROUP BY Emp_Id, Suc_Id, Articulo_Id
    ),

    Datos_Para_Procesar AS (
    -- 3. Traer datos nuevos + registros existentes de las particiones afectadas
        SELECT *
        FROM Comision_Nuevos_Actualizados

        UNION ALL

        SELECT
            T.Emp_Id,
            T.Comision_Id,
            T.Suc_Id,
            T.Articulo_Id,
            T.Detalle_Comision_Monto,
            T.Detalle_Consecutivo,
            T.Comision_Fecha_Inicial,
            T.Comision_Fecha_Final_Original AS Comision_Fecha_Final,
            T.Comision_Nombre,
            T.Comision_Estado,
            T.Fecha_Actualizado
        FROM {{ this }} AS T
        INNER JOIN Claves_Afectadas_Con_Fechas AS CA
            ON
                T.Emp_Id = CA.Emp_Id
                AND T.Suc_Id = CA.Suc_Id
                AND T.Articulo_Id = CA.Articulo_Id
        WHERE
        -- Solo traer registros existentes que puedan solapar con los nuevos
            (
                T.Comision_Fecha_Inicial <= CA.Max_Fecha_Final
                AND T.Comision_Fecha_Final_Original >= CA.Min_Fecha_Inicial
            )
            -- Evitar duplicados con los registros nuevos
            AND NOT EXISTS (
                SELECT 1
                FROM Comision_Nuevos_Actualizados AS CNA
                WHERE
                    T.Emp_Id = CNA.Emp_Id
                    AND T.Suc_Id = CNA.Suc_Id
                    AND T.Articulo_Id = CNA.Articulo_Id
                    AND T.Comision_Id = CNA.Comision_Id
            )
    )
{% endif %}

, Comision_Con_Rangos_Corregidos AS (
    -- 4. Corregir rangos para evitar solapamientos de forma sistemática
    SELECT
        *,
        -- Paso 1: Ordenar por fecha inicial y luego por ID para tener secuencia determinista
        ROW_NUMBER() OVER (
            PARTITION BY Emp_Id, Suc_Id, Articulo_Id
            ORDER BY Comision_Fecha_Inicial ASC, Comision_Id ASC
        ) AS Orden_Secuencial,

        -- Paso 2: Determinar la fecha final corregida considerando solapamientos múltiples
        CASE
            -- Si hay un siguiente rango, la fecha final será el mínimo entre:
            -- a) La fecha final original
            -- b) Un día antes del siguiente inicio
            WHEN
                LEAD(Comision_Fecha_Inicial, 1) OVER (
                    PARTITION BY Emp_Id, Suc_Id, Articulo_Id
                    ORDER BY Comision_Fecha_Inicial ASC, Comision_Id ASC
                ) IS NOT NULL
                THEN
                    CASE
                        WHEN
                            DATEADD(DAY, -1, LEAD(Comision_Fecha_Inicial, 1) OVER (
                                PARTITION BY Emp_Id, Suc_Id, Articulo_Id
                                ORDER BY Comision_Fecha_Inicial ASC, Comision_Id ASC
                            )) < Comision_Fecha_Final
                            THEN DATEADD(DAY, -1, LEAD(Comision_Fecha_Inicial, 1) OVER (
                                PARTITION BY Emp_Id, Suc_Id, Articulo_Id
                                ORDER BY Comision_Fecha_Inicial ASC, Comision_Id ASC
                            ))
                        ELSE Comision_Fecha_Final
                    END
            ELSE Comision_Fecha_Final  -- Es el último, mantener fecha final original
        END AS Comision_Fecha_Final_Corregida_Temp,

        GETDATE() AS Fecha_Carga
    FROM {% if is_incremental() %} Datos_Para_Procesar {% else %} Comision_Nuevos_Actualizados {% endif %}
),

Comision_Rangos_Validos AS (
    -- 5. Validar rangos y eliminar los que quedan inválidos después de correcciones
    SELECT
        *,
        -- Marcar rangos válidos (fecha final >= fecha inicial)
        Comision_Fecha_Final_Corregida_Temp AS Comision_Fecha_Final_Corregida,

        -- Renombrar para claridad
        CASE
            WHEN Comision_Fecha_Final_Corregida_Temp >= Comision_Fecha_Inicial THEN 1
            ELSE 0
        END AS Es_Rango_Valido
    FROM Comision_Con_Rangos_Corregidos
),

Comision_Sin_Duplicados AS (
    -- 6. Procesar TODOS los registros para duplicados (válidos e inválidos)
    SELECT
        *,
        -- Identificar duplicados exactos (misma fecha inicial y final corregida)
        COUNT(*) OVER (
            PARTITION BY Emp_Id, Suc_Id, Articulo_Id,
            Comision_Fecha_Inicial, Comision_Fecha_Final_Corregida
        ) AS Cant_Duplicados,

        -- Marcar cuál registro mantener en caso de duplicados
        ROW_NUMBER() OVER (
            PARTITION BY Emp_Id, Suc_Id, Articulo_Id,
            Comision_Fecha_Inicial, Comision_Fecha_Final_Corregida
            ORDER BY Comision_Id DESC  -- Mantener el de mayor ID
        ) AS Orden_Duplicado
    FROM Comision_Rangos_Validos
    -- NO filtrar aquí - procesamos todo para el merge incremental
),

Comision_Con_Indicadores_Finales AS (
    -- 7. Calcular indicadores finales sobre datos limpios
    SELECT
        *,
        -- Marcar duplicados
        CASE WHEN Cant_Duplicados > 1 THEN 1 ELSE 0 END AS Es_Duplicado,

        -- Marcar cuáles mantener: debe ser válido Y ser el elegido en caso de duplicados
        CASE
            WHEN Es_Rango_Valido = 1 AND Orden_Duplicado = 1 THEN 1
            ELSE 0
        END AS Mantener_Registro,

        -- Recalcular Es_Ultimo_Rango solo sobre los datos finales válidos y únicos
        CASE
            WHEN
                Es_Rango_Valido = 1
                AND Orden_Duplicado = 1
                AND ROW_NUMBER() OVER (
                    PARTITION BY Emp_Id, Suc_Id, Articulo_Id
                    ORDER BY Comision_Fecha_Final_Corregida DESC, Comision_Id DESC
                ) = 1 THEN 1
            ELSE 0
        END AS Es_Ultimo_Rango
    FROM Comision_Sin_Duplicados
)

SELECT
    C.Emp_Id,
    C.Comision_Id,
    C.Suc_Id,
    C.Articulo_Id,
    C.Detalle_Comision_Monto,
    C.Detalle_Consecutivo,
    C.Comision_Fecha_Inicial,
    C.Comision_Fecha_Final AS Comision_Fecha_Final_Original,
    C.Comision_Fecha_Final_Corregida AS Comision_Fecha_Final,
    C.Comision_Nombre,
    C.Comision_Estado,
    C.Fecha_Actualizado,
    C.Es_Ultimo_Rango,
    C.Es_Duplicado,
    C.Mantener_Registro,
    C.Fecha_Carga,

    -- Campos para concatenación de IDs
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id', 'Articulo_Id', 'Comision_Id'], input_length=49, table_alias='C') }} AS EmpSucArtCom_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id', 'Comision_Id'], input_length=49, table_alias='C') }} AS EmpSucCom_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Comision_Id'], input_length=49, table_alias='C') }} AS EmpCom_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id'], input_length=49, table_alias='C') }} AS EmpSuc_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Articulo_Id'], input_length=49, table_alias='C') }} AS EmpArt_Id

FROM Comision_Con_Indicadores_Finales AS C
