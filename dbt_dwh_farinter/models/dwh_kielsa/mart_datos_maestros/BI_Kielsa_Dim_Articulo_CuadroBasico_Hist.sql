{% set unique_key_list = ["Articulo_Id", "Fecha_Desde", "Emp_Id"] %}

{{-
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="table",
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

-- SCD de Cuadro Básico por Emp y Artículo usando el histórico de posiciones
WITH base AS (
    SELECT --noqa: ST06
        CAST(H.Factura_Fecha AS DATE) AS Fecha,
        CAST(H.Emp_Id AS INT) AS Emp_Id,
        H.Articulo_Id,
        CAST(ISNULL(H.Cuadro_Id, 0) AS INT) AS Cuadro_Id
    FROM {{ ref('BI_Kielsa_Hecho_FacturaPosicion_DimHist') }} AS H
    WHERE H.Factura_Fecha >= '20230101'
    GROUP BY CAST(H.Factura_Fecha AS DATE), CAST(H.Emp_Id AS INT), H.Articulo_Id, CAST(H.Cuadro_Id AS INT)
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
        FORMAT(r.Fecha_Desde, 'yyyyMMdd') AS Fecha_Id
    FROM rangos AS r
)

SELECT --noqa: ST06
    ISNULL(Emp_Id, 0) AS Emp_Id,
    ISNULL(Articulo_Id, 'X') AS Articulo_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Articulo_Id'], input_length=29, table_alias='') }} AS [EmpArt_Id],
    Cuadro_Id,
    ISNULL(CAST(Fecha_Desde AS DATE), '1900-01-01') AS [Fecha_Desde],
    ISNULL(CAST(Fecha_Hasta AS DATE), '9999-12-31') AS [Fecha_Hasta],
    -- Surrogate key estable por versión SCD (Emp_Id, Articulo_Id, Fecha_Desde),
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Articulo_Id', 'Fecha_Id'], input_length=64, table_alias='') }} AS [EmpArtFec_Id]
FROM rangos_con_fin;
