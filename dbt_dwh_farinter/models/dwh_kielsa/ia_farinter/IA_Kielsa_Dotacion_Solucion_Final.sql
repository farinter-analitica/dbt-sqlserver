{% set unique_key_list = ['emp_id','suc_id','anio','fecha_ejecucion_id','semana','operational_mode', 'turno_id'] -%}

{{ 
    config(
        tags=["automation/periodo_mensual_inicio", "automation_only"],
        materialized="view",
    )
}}

-- Solo dbt
-- CTE que obtiene la última versión del encabezado por (emp_id, suc_id, anio, semana, operational_mode)
WITH LatestEncabezado AS (
    SELECT *
    FROM (
        SELECT
            e.*,
            ROW_NUMBER() OVER (
                PARTITION BY e.emp_id, e.suc_id, e.anio, e.semana, e.operational_mode
                ORDER BY e.fecha_ejecucion_id DESC
            ) AS rn
        FROM {{ source('IA_FARINTER', 'IA_Kielsa_Dotacion_Solucion_Encabezado') }} AS e
    ) AS x
    WHERE rn = 1
)

-- Reconstruye el resultado final uniendo la última versión del encabezado con los turnos correspondientes
-- Resultado final: unir la última versión del encabezado (CTE) con los turnos y devolver todos los campos
SELECT
    le.*,
    -- columnas de turnos que no duplican las del encabezado
    t.dia_id,
    t.rol_id,
    t.rol_nombre,
    t.empleado_numero,
    t.fecha_hora_entrada,
    t.fecha_hora_salida,
    t.codigo_turno,
    t.turno_id,
    t.hora_central,
    t.desviacion,
    t.tipo_var_used
FROM LatestEncabezado AS le
INNER JOIN {{ source('IA_FARINTER', 'IA_Kielsa_Dotacion_Solucion_Turnos') }} AS t
    ON
        le.emp_id = t.emp_id
        AND le.suc_id = t.suc_id
        AND le.anio = t.anio
        AND le.semana = t.semana
        AND le.fecha_ejecucion_id = t.fecha_ejecucion_id
        AND le.operational_mode = t.operational_mode;
