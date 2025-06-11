{% set unique_key_list = ["Fecha_Id", "Hora_Id", "Emp_Id", "Suc_Id"] %}
{{- 
    config(
		as_columnstore=true,
		tags=["periodo/diario","periodo_unico/si"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", 
            create_clustered=false, 
            is_incremental=is_incremental(), 
            if_another_exists_drop_it=true) }}",
        ]
	) 
}}
{%- if is_incremental() -%}
	{%- set v_fecha_inicio = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -15, max(Fecha_Actualizado)), 112), '19000101')  
            from  """ ~ this,
            relation_not_found_value='19000101'|string)|string %}
	{%- set v_fecha_fin = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,max(Mov_Fecha), 112), '19000101')  
            from  """ ~ source('DL_FARINTER','DL_Kielsa_Mov_Inventario_Encabezado'),
            relation_not_found_value='19000101'|string)|string %}
    {%- set v_anio_mes_inicio =  v_fecha_inicio[:6]  %}
{%- else -%}
	{%- set v_fecha_inicio = (modules.datetime.datetime.now()-
        modules.datetime.timedelta(days=365*3)).replace(month=1, day=1).strftime('%Y%m%d') %}
    {%- set v_fecha_fin = modules.datetime.datetime.now().strftime('%Y%m%d') %}
    {%- set v_anio_mes_inicio =  v_fecha_inicio[:6]  %}
{%- endif -%}

WITH 
Movimientos_Considerados AS (
    SELECT
        ME.Emp_Id,
        ME.Suc_Id,
        ME.Mov_Fecha_Aplicado,
        ME.Mov_Fecha_Recibido,
        ME.Mov_Id
    FROM {{ source('DL_FARINTER','DL_Kielsa_Mov_Inventario_Encabezado') }} ME
    INNER JOIN {{ ref('BI_Kielsa_Dim_Usuario') }} U
    ON ME.Emp_Id = U.Emp_Id
    AND ME.Usuario_Id = U.Usuario_Id
    WHERE ME.Mov_Estado IN ('RE', 'AP')
    AND U.Usuario_Nombre NOT IN ('ENCARGADO WEB', 'Administrador', 'Logical Data')
    AND ME.Indicador_Borrado <> 1
    {% if is_incremental() %}
    AND ME.Mov_Fecha >= '{{v_fecha_inicio}}' AND ME.Mov_Fecha <= '{{v_fecha_fin}}'
    {% endif %}
),

Aplicados AS (
    SELECT
        ME.Emp_Id,
        ME.Suc_Id,
        CAST(ME.Mov_Fecha_Aplicado AS DATE) AS Fecha_Id,
        DATEPART(HOUR, ME.Mov_Fecha_Aplicado) AS Hora_Id,
        ME.Mov_Id
    FROM Movimientos_Considerados ME
    ),
Recibidos AS (
    SELECT
        ME.Emp_Id,
        ME.Suc_Id,
        CAST(ME.Mov_Fecha_Recibido AS DATE) AS Fecha_Id,
        DATEPART(HOUR, ME.Mov_Fecha_Recibido) AS Hora_Id,
        ME.Mov_Id
    FROM Movimientos_Considerados ME
    ),
Actividades_Movimientos AS (
    SELECT R.Emp_Id, R.Suc_Id, R.Fecha_Id, R.Hora_Id, 'Mov_Aplicado' AS Tipo_Actividad, COUNT(R.Mov_Id) AS Conteo_Movimientos
    FROM Recibidos R
    GROUP BY R.Emp_Id, R.Suc_Id, R.Fecha_Id, R.Hora_Id
    UNION ALL
    SELECT A.Emp_Id, A.Suc_Id, A.Fecha_Id, A.Hora_Id, 'Mov_Recibido' AS Tipo_Actividad, COUNT(A.Mov_Id) AS Conteo_Movimientos
    FROM Aplicados A
    GROUP BY A.Emp_Id, A.Suc_Id, A.Fecha_Id, A.Hora_Id
    )
SELECT  ISNULL(AM.Emp_Id, 0) AS Emp_Id,
        ISNULL(AM.Suc_Id, 0) AS Suc_Id,
        ISNULL(AM.Fecha_Id, '19000101') AS Fecha_Id,
        ISNULL(AM.Hora_Id, 0) AS Hora_Id,
        SUM(CASE WHEN AM.Tipo_Actividad = 'Mov_Aplicado' THEN AM.Conteo_Movimientos ELSE 0 END) AS Conteo_Movimientos_Aplicados,
        SUM(CASE WHEN AM.Tipo_Actividad = 'Mov_Recibido' THEN AM.Conteo_Movimientos ELSE 0 END) AS Conteo_Movimientos_Recibidos,
        GETDATE() AS Fecha_Actualizado
FROM Actividades_Movimientos AM
GROUP BY AM.Emp_Id, AM.Suc_Id, AM.Fecha_Id, AM.Hora_Id