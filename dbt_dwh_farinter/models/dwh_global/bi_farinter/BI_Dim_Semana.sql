{% set unique_key_list = ["Semana_del_Anio_ISO"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario","periodo_unico/si"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
	) 
}}

--DBT DAGSTER

WITH WeekStart AS (
    SELECT 
        [Semana_del_Anio_ISO],
        MIN(Fecha_Calendario) AS Primera_Fecha_Semana
    FROM {{ ref ('BI_Dim_Calendario_Dinamico') }}
    GROUP BY [Semana_del_Anio_ISO]
)
SELECT DISTINCT
    C.[Semana_del_Anio_ISO],
    DATEPART(MONTH, W.Primera_Fecha_Semana) AS Semana_Mes_ISO,
    DATEPART(YEAR, W.Primera_Fecha_Semana) AS Semana_Anio_ISO,
    CASE 
        WHEN DATEPART(MONTH, W.Primera_Fecha_Semana) <= 3 THEN 1
        WHEN DATEPART(MONTH, W.Primera_Fecha_Semana) <= 6 THEN 2
        WHEN DATEPART(MONTH, W.Primera_Fecha_Semana) <= 9 THEN 3
        WHEN DATEPART(MONTH, W.Primera_Fecha_Semana) <= 12 THEN 4
    END AS Semana_Trimestre_ISO
FROM {{ ref ('BI_Dim_Calendario_Dinamico') }} C
JOIN WeekStart W ON C.[Semana_del_Anio_ISO] = W.[Semana_del_Anio_ISO]


/*
SELECT TOP 100 * FROM BI_Dim_Calendario_Dinamico WHERE Mes_Relativo BETWEEN -12 AND  12 
	and abs(Mes_Relativo) <> abs(SUBSTRING(Mes_NN_Relativo,5,5)) ORDER BY Fecha_Calendario 
*/