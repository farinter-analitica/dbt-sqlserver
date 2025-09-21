{% set unique_key_list = ["Articulo_Id", "Fecha_Desde", "Emp_Id"] %}

{{-
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		on_schema_change="append_new_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		unique_key=unique_key_list,
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
		])
}}

{%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Hasta)), 112), '19000101') as fecha_a from  """ ~ this,
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '19000101' %}
{%- endif %}


-- SCD de Cuadro Básico por Emp y Artículo usando el histórico de posiciones
WITH
-- Identificar las combinaciones que tienen datos nuevos en este run incremental
combinaciones_con_datos_nuevos AS (
    {%- if is_incremental() %}
        SELECT DISTINCT --noqa: ST06
            CAST(H.Emp_Id AS INT) AS Emp_Id,
            H.Articulo_Id
        FROM {{ ref('BI_Kielsa_Hecho_FacturaPosicion_DimHist') }} AS H
        WHERE H.Factura_Fecha > '{{ last_date }}'
    {%- else %}
    -- En carga completa, procesar todas las combinaciones
    SELECT DISTINCT
        H.Articulo_Id,
        CAST(H.Emp_Id AS INT) AS Emp_Id
    FROM {{ ref('BI_Kielsa_Hecho_FacturaPosicion_DimHist') }} AS H
    WHERE H.Factura_Fecha >= '20230101'
    {% endif %}
),

base_raw AS (
    SELECT --noqa: ST06
        H.Factura_Fecha, -- conservar datetime para ordenar dentro del día
        CAST(H.Emp_Id AS INT) AS Emp_Id,
        H.Articulo_Id,
        CAST(ISNULL(H.Cuadro_Id, 0) AS INT) AS Cuadro_Id
    FROM {{ ref('BI_Kielsa_Hecho_FacturaPosicion_DimHist') }} AS H
    INNER JOIN combinaciones_con_datos_nuevos AS ccdn
        ON
            CAST(H.Emp_Id AS INT) = ccdn.Emp_Id
            AND H.Articulo_Id = ccdn.Articulo_Id
    WHERE H.Factura_Fecha >= '20230101'
),

-- Una sola fila por día (Emp, Artículo) eligiendo la última ocurrencia del día
dedup_dia AS (
    SELECT --noqa: ST06
        CAST(br.Factura_Fecha AS DATE) AS Fecha,
        br.Emp_Id,
        br.Articulo_Id,
        br.Cuadro_Id,
        ROW_NUMBER() OVER (
            PARTITION BY br.Emp_Id, br.Articulo_Id, CAST(br.Factura_Fecha AS DATE)
            ORDER BY br.Factura_Fecha DESC
        ) AS rn
    FROM base_raw AS br
),

base AS (
    SELECT
        Fecha,
        Emp_Id,
        Articulo_Id,
        Cuadro_Id
    FROM dedup_dia
    WHERE rn = 1
),

marcado AS (
    SELECT
        *,
        CASE
            WHEN LAG(Cuadro_Id) OVER (PARTITION BY Emp_Id, Articulo_Id ORDER BY Fecha) IS NULL THEN 1
            WHEN Cuadro_Id <> LAG(Cuadro_Id) OVER (PARTITION BY Emp_Id, Articulo_Id ORDER BY Fecha) THEN 1
            ELSE 0
        END AS es_cambio
    FROM base
),

agrupado AS (
    SELECT
        *,
        SUM(es_cambio) OVER (
            PARTITION BY Emp_Id, Articulo_Id
            ORDER BY Fecha
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS grp
    FROM marcado
),

rangos AS (
    SELECT
        Emp_Id,
        Articulo_Id,
        Cuadro_Id,
        MIN(Fecha) AS Fecha_Desde
    FROM agrupado
    GROUP BY Emp_Id, Articulo_Id, Cuadro_Id, grp
),

rangos_con_fin AS (
    SELECT
        r.Emp_Id,
        r.Articulo_Id,
        r.Cuadro_Id,
        r.Fecha_Desde,
        DATEADD(
            DAY, -1,
            LEAD(r.Fecha_Desde)
                OVER (
                    PARTITION BY r.Emp_Id, r.Articulo_Id
                    ORDER BY r.Fecha_Desde
                )
        ) AS Fecha_Hasta,
        CONVERT(VARCHAR(8), r.Fecha_Desde, 112) AS Fecha_Id -- reemplaza FORMAT por CONVERT (112) por rendimiento
    FROM rangos AS r
),

-- En modo incremental, solo necesitamos recalcular los rangos para las combinaciones con datos nuevos
{% if is_incremental() %}
    rangos_finales AS (
    -- Todos los rangos recalculados para las combinaciones con datos nuevos
    -- La estrategia farinter_merge se encarga de actualizar/insertar según corresponda
        SELECT rcf.*
        FROM rangos_con_fin AS rcf
    )
{% else %}
rangos_finales AS (
    SELECT
        *
    FROM rangos_con_fin
)
{% endif %}

SELECT --noqa: ST06
    ISNULL(Emp_Id, 0) AS Emp_Id,
    ISNULL(Articulo_Id, 'X') AS Articulo_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Articulo_Id'], input_length=29, table_alias='') }} AS [EmpArt_Id],
    Cuadro_Id,
    ISNULL(CAST(Fecha_Desde AS DATE), '1900-01-01') AS [Fecha_Desde],
    ISNULL(CAST(Fecha_Hasta AS DATE), '9999-12-31') AS [Fecha_Hasta],
    -- Surrogate key estable por versión SCD (Emp_Id, Articulo_Id, Fecha_Desde),
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Articulo_Id', 'Fecha_Id'], input_length=64, table_alias='') }} AS [EmpArtFec_Id]
FROM rangos_finales;
