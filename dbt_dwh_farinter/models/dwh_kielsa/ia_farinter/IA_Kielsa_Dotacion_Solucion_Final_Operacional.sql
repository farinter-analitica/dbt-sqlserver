{% set unique_key_list = ['emp_id','suc_id','anio','fecha_ejecucion_id','semana','turno_id'] -%}

{{ 
    config(
        tags=["automation/periodo_mensual_inicio", "automation_only"],
        materialized="view",
    )
}}

-- Solo dbt
-- Versión derivada que prefiere el modo 'operational' por (emp_id, suc_id, anio, semana).
-- Si no existe ninguna fila con operational_mode = 'operational' para la combinación,
-- se devuelve la fila no-operational más reciente.

WITH LatestEncabezado AS (
    SELECT *
    FROM (
        SELECT
            e.*,
            ROW_NUMBER() OVER (
                PARTITION BY e.emp_id, e.suc_id, e.anio, e.semana
                ORDER BY
                    -- Dentro de la misma prioridad, toma la más reciente
                    e.fecha_ejecucion_id DESC,
                    -- Prioriza filas con operational_mode = 'operational'
                    CASE WHEN e.operational_mode = 1 THEN 0 ELSE 1 END
            ) AS rn
        FROM {{ source('IA_FARINTER', 'IA_Kielsa_Dotacion_Solucion_Encabezado') }} AS e
    ) AS x
    WHERE rn = 1
)

SELECT
    le.*,
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
