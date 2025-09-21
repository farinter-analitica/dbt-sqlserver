{% set unique_key_list = ["Sucursal_Id", "FechaHora_Id", "Pais_Id"] -%}

{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
	) 
}}

{%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -30, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{%- else %}
	{%- set last_date = '19000101' %}
{%- endif %}

WITH Marcador_Base AS (
    SELECT
        Empleado_Marcador_Id,
        Pais_Id,
        Sucursal_Id,
        FH_Entrada_Corregida AS FH_Entrada,
        FH_Salida_Corregida AS FH_Salida,
        CAST(FH_Entrada_Corregida AS date) AS Fecha_Entrada,
        CAST(FH_Salida_Corregida AS date) AS Fecha_Salida,
        FH_Apertura_Sucursal,
        FH_Cierre_Sucursal
    FROM BI_FARINTER.dbo.BI_Kielsa_Hecho_RelojMarcador -- {{ ref('BI_Kielsa_Hecho_RelojMarcador') }}
    WHERE
        Pais_Id = 1 AND Sucursal_Id IS NOT NULL AND Es_Valido = 1
        {% if is_incremental() %}
            AND FH_Entrada_Corregida >= '{{ last_date }}'
        {% endif %}

),

Calendario AS (
    SELECT
        Fecha_Calendario,
        Semana_del_Anio_ISO,
        Dia_de_la_Semana -- este calendario ya trae semana y dia iso
    FROM BI_FARINTER.dbo.BI_Dim_Calendario_Dinamico -- {{ ref('BI_Dim_Calendario_Dinamico') }}
),

InnerCross_Fecha AS (
    SELECT
        Marcador_Base.*,
        Calendario.*
    FROM Marcador_Base
    CROSS JOIN Calendario
    WHERE
        Marcador_Base.Fecha_Entrada <= Calendario.Fecha_Calendario
        AND Marcador_Base.Fecha_Salida >= Calendario.Fecha_Calendario
),

InnerCross_Horas AS (
    SELECT
        ICF.*,
        CAST(H.hora_id AS int) AS hora_id,
        DATEADD(HOUR, H.hora_id, CAST(ICF.Fecha_Calendario AS datetime)) AS FechaHora_Id
    FROM InnerCross_Fecha [ICF]
    CROSS JOIN BI_FARINTER.dbo.BI_Dim_Hora H -- {{ ref('BI_Dim_Hora') }}
    WHERE
        ICF.FH_Entrada <= DATEADD(HOUR, H.hora_id, CAST(ICF.Fecha_Calendario AS datetime))
        AND ICF.FH_Salida >= DATEADD(HOUR, H.hora_id, CAST(ICF.Fecha_Calendario AS datetime))
),

Empleados_Por_Hora AS (
    SELECT
        ISNULL(ICF.Pais_Id, 0) AS Pais_Id,
        ISNULL(ICF.Sucursal_Id, 0) AS Sucursal_Id,
        ICF.Fecha_Calendario,
        MAX(ICF.Dia_de_la_Semana) AS Dia_de_la_Semana_ISO,
        MAX(ICF.Semana_del_Anio_ISO) AS Semana_del_Anio_ISO,
        ICF.hora_id AS Hora_Id,
        ISNULL(MAX(ICF.FechaHora_Id), '19000101') AS FechaHora_Id,
        COUNT(DISTINCT ICF.Empleado_Marcador_Id) AS Cantidad_Empleados
    FROM InnerCross_Horas ICF
    GROUP BY ICF.Pais_Id, ICF.Sucursal_Id, ICF.Fecha_Calendario, ICF.hora_id
)

SELECT --TOP 100
    *,
    GETDATE() AS Fecha_Actualizado
FROM Empleados_Por_Hora
