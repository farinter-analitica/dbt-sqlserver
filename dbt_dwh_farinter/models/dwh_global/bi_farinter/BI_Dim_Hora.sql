{% set unique_key_list = ["Hora_Id"] %}
{{ 
    config(
        as_columnstore=true,
        tags=["periodo/diario","periodo_unico/si"],
        materialized="table",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        on_schema_change = "append_new_columns",
        merge_exclude_columns=unique_key_list + ["Fecha_Procesado"],
        merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Procesado","Es_Hora_Actual"],
        post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
    ) 
}}
-- Solo editable en DBT DAGSTER

-- 20240320: Creado a partir del procedimiento BI_paCargarDim_Hora

WITH Horas AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS Hora_Id
    FROM 
        (VALUES(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),
               (1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1)) AS nums(n)
)
SELECT
    Hora_Id,
    FORMAT(DATEADD(HOUR, Hora_Id, '00:00:00'), 'HH:mm') AS Hora_Formato,
    CASE 
        WHEN Hora_Id BETWEEN 0 AND 5 THEN 'MADRUGADA'
        WHEN Hora_Id BETWEEN 6 AND 11 THEN 'MAÑANA'
        WHEN Hora_Id BETWEEN 12 AND 17 THEN 'TARDE'
        WHEN Hora_Id BETWEEN 18 AND 23 THEN 'NOCHE'
    END AS Horario_Nombre,
    CASE 
        WHEN Hora_Id BETWEEN 0 AND 5 THEN 1
        WHEN Hora_Id BETWEEN 6 AND 11 THEN 2
        WHEN Hora_Id BETWEEN 12 AND 17 THEN 3
        WHEN Hora_Id BETWEEN 18 AND 23 THEN 4
    END AS Horario_Orden,
    CASE 
        WHEN Hora_Id BETWEEN 8 AND 17 THEN 1
        ELSE 0
    END AS Es_Horario_Laboral,
    CASE
        WHEN Hora_Id < 12 THEN 'AM'
        ELSE 'PM'
    END AS Periodo_Dia,
    CASE
        WHEN Hora_Id = 0 THEN 'Medianoche'
        WHEN Hora_Id = 12 THEN 'Mediodía'
        ELSE 'No definido'
    END AS Periodo_Hora,
    CASE
        WHEN Hora_Id = 0 THEN 2
        WHEN Hora_Id = 12 THEN 3
        ELSE 99
    END AS Periodo_Hora_Orden,
    GETDATE() AS Fecha_Procesado
FROM 
    Horas
WHERE 
    Hora_Id BETWEEN 0 AND 23
