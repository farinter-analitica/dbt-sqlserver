{% set unique_key_list = ["Suc_Id", "Dia_Semana_Iso_Id", "Emp_Id"] -%}

{{ 
    config(
		as_columnstore=true,
		tags=["automation/eager"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
        ]
	) 
}}


{%- if is_incremental() %}
	{%- set last_date = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -2, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this,
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
	{%- set last_date = '19000101' %}
{%- endif %}

WITH
nocodb_horarios AS (
    SELECT h.[id],
            h.[emp_id],
            h.[suc_id],
            h.[dia_id],
            cast(cast('1900-01-01T'+h.h_apertura+':00' as datetimeoffset) AT TIME ZONE 'Central America Standard Time' AS time
                ) AS [h_apertura],
            cast(cast('1900-01-01T'+h.h_cierre+':00' as datetimeoffset) AT TIME ZONE 'Central America Standard Time' AS time
                ) AS [h_cierre],
            h.[fecha_carga],
            h.[fecha_actualizado],
            ns.es_activa,
            ns.es_24_horas,
            ns.jop_nombre,
            ns.supervisor_nombre
    FROM [DL_FARINTER].[nocodb_data_gf].[kielsa_sucursal_horario_dia] h -- {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_sucursal_horario_dia') }}
    INNER JOIN [DL_FARINTER].[nocodb_data_gf].[kielsa_sucursal] ns -- {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_sucursal') }}
        ON ns.emp_id=h.emp_id
        AND ns.suc_id=h.suc_id
    {%- if is_incremental() %}
    WHERE h.fecha_actualizado >= '{{last_date}}'
    {%- endif %}       
),

Horario_Bruto AS 
(

    SELECT 
        CAST(h.emp_id AS INT) AS Emp_Id,
        CAST(h.Suc_Id AS INT) AS Suc_Id,
        h.dia_id as Dia_Semana_Iso_Id, 
        H.h_apertura AS H_Apertura,
        H.h_cierre AS H_Cierre,
        CAST(h.H_Apertura AS datetime) AS FH_Apertura,
        CASE WHEN h.H_Apertura >= h.H_Cierre 
			THEN DATEADD(DAY,1,CAST(h.H_Cierre  AS datetime))
			ELSE CAST(h.H_Cierre  AS datetime) END AS FH_Cierre
		, 
		CASE WHEN h.H_Cierre > h.H_Apertura THEN 0 ELSE 1 END AS Es_Cierre_Dia_Siguiente,
        h.es_activa,
        s.Usuario_Supervisor_Id as Supervisor_Id,
        u.Usuario_Nombre as Supervisor_Nombre
        -- CASE 
		-- 	WHEN H_Apertura > H_Cierre THEN 0
        --     WHEN DATEDIFF(SECOND, CAST(H_Apertura AS TIME), CAST(H_Cierre AS TIME)) / 3600.0 > 23.5 
        --     THEN 1 ELSE 0 
        -- END AS Es_24_Horas
		--select top 100 *
    FROM nocodb_horarios h 
    LEFT JOIN [BI_FARINTER].[dbo].[BI_Kielsa_Dim_Sucursal] s -- {{ ref('BI_Kielsa_Dim_Sucursal') }}
        ON h.Suc_Id = s.Sucursal_Id
        AND s.Emp_Id = h.Emp_Id
    LEFT JOIN [BI_FARINTER].[dbo].[BI_Kielsa_Dim_Usuario] u -- {{ ref('BI_Kielsa_Dim_Usuario') }}
        ON s.Usuario_Supervisor_Id = u.Usuario_Id
        AND u.Emp_Id = s.Emp_Id

	--where H_Apertura > H_Cierre
    --WHERE activa = 1
),
Horarios AS 
(
    SELECT 
        ISNULL(Emp_Id, 0) AS Emp_Id,
        ISNULL(Suc_Id, 0)AS Suc_Id,
        ISNULL(Dia_Semana_Iso_Id, 0) AS Dia_Semana_Iso_Id, 
		H_Apertura,
		H_Cierre,
        FH_Apertura,
        FH_Cierre, 
		ROUND(DATEDIFF(SECOND, FH_Apertura , FH_Cierre) / 3600.0,2) AS Horas_Abierto,
		Es_Cierre_Dia_Siguiente,
        CASE 
            WHEN DATEDIFF(SECOND, FH_Apertura , FH_Cierre) / 3600.0 > 23.5 
            THEN 1 ELSE 0 
        END AS Es_24_Horas,
		ROUND(DATEDIFF(SECOND, CAST('19000101' AS DATETIME) , FH_Cierre) / 3600.0,2) AS Horas_Cero_Hasta_Cierre,
        es_activa AS Es_Activa,
        Supervisor_Id,
        Supervisor_Nombre
		--select top 100 *
    FROM [Horario_Bruto]
	--where H_Apertura > H_Cierre
    --WHERE H_Apertura  IS NOT NULL
)
SELECT * ,
        GETDATE() AS Fecha_Actualizado
FROM Horarios