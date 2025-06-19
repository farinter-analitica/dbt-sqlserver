{% set unique_key_list = ["Dia_Semana_Iso_Id"] %}
{{ 
    config(
        as_columnstore=true,
        tags=["periodo/diario","periodo_unico/si"],
        materialized="incremental",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        on_schema_change = "append_new_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
        post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
    ) 
}}
-- Solo editable en DBT DAGSTER

WITH DiasSemana AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Dia_Semana_Iso_Id
    FROM 
        (VALUES(1),(1),(1),(1),(1),(1),(1)) AS nums(n)
)
SELECT
    ISNULL(Dia_Semana_Iso_Id,0) AS Dia_Semana_Iso_Id,
    CASE 
        WHEN Dia_Semana_Iso_Id = 1 THEN 'Lunes'
        WHEN Dia_Semana_Iso_Id = 2 THEN 'Martes'
        WHEN Dia_Semana_Iso_Id = 3 THEN 'Miércoles'
        WHEN Dia_Semana_Iso_Id = 4 THEN 'Jueves'
        WHEN Dia_Semana_Iso_Id = 5 THEN 'Viernes'
        WHEN Dia_Semana_Iso_Id = 6 THEN 'Sábado'
        WHEN Dia_Semana_Iso_Id = 7 THEN 'Domingo'
    END AS Dia_Semana_Nombre,
    CASE 
        WHEN Dia_Semana_Iso_Id = 1 THEN 'Lun'
        WHEN Dia_Semana_Iso_Id = 2 THEN 'Mar'
        WHEN Dia_Semana_Iso_Id = 3 THEN 'Mié'
        WHEN Dia_Semana_Iso_Id = 4 THEN 'Jue'
        WHEN Dia_Semana_Iso_Id = 5 THEN 'Vie'
        WHEN Dia_Semana_Iso_Id = 6 THEN 'Sáb'
        WHEN Dia_Semana_Iso_Id = 7 THEN 'Dom'
    END AS Dia_Semana_Nombre_Corto,
    CASE 
        WHEN Dia_Semana_Iso_Id BETWEEN 1 AND 5 THEN 1
        ELSE 0
    END AS Es_Dia_Laboral,
    CASE 
        WHEN Dia_Semana_Iso_Id BETWEEN 1 AND 5 THEN 'Día Laboral'
        ELSE 'Fin de Semana'
    END AS Tipo_Dia,
    CASE 
        WHEN Dia_Semana_Iso_Id = 7 THEN 1  -- Domingo es 1 en formato US
        ELSE Dia_Semana_Iso_Id + 1         -- Resto de días se desplazan
    END AS Dia_Semana_DomingoPrimero_Id,
    GETDATE() AS Fecha_Actualizado
FROM 
    DiasSemana
WHERE 
    Dia_Semana_Iso_Id BETWEEN 1 AND 7
