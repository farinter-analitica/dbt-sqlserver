{% set unique_key_list = ["Monedero_Id","Emp_Id", "Articulo_Id", "Sucursal_Id"] %}

{% if is_incremental() %}
    {% set v_last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, 0, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
    {% set v_merge_exclude_columns = unique_key_list + ["Fecha_Carga","Fecha_Primer_Factura"] %}
    {% set v_merge_check_diff_exclude_columns = v_merge_exclude_columns + ["Fecha_Actualizado"] %}
    {% set v_last_date_control = (modules.datetime.datetime.fromisoformat(v_last_date) - modules.datetime.timedelta(days=30)).strftime('%Y%m%d') %}
{% else %}
	{% set v_last_date = '19000101' %}
	{% set v_last_date_control = '19000101' %}
	{% set v_merge_exclude_columns = unique_key_list + ["Fecha_Carga"] %}
	{% set v_merge_check_diff_exclude_columns = v_merge_exclude_columns + ["Fecha_Actualizado"] %}
{% endif %}

{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario", "detener_carga/si"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="fail",
		merge_exclude_columns= v_merge_exclude_columns,
		merge_check_diff_exclude_columns= v_merge_check_diff_exclude_columns,
		post_hook=[
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
		]
		
) }}

-- depends_on: {{ this }}
{% if is_incremental() %}
    WITH Fecha_Desde AS (
        --Se hace asi ya que cada empresa puede estar actualizando por separado
        SELECT
            Emp_Id,
            Monedero_Id,
            Articulo_Id,
            Sucursal_Id AS Sucursal_Id,
            Fecha_Actualizado AS Fecha_Desde_Real,
            CASE WHEN Fecha_Actualizado > DATEADD(DAY, -180, GETDATE()) THEN DATEADD(DAY, -180, GETDATE()) ELSE Fecha_Actualizado END AS Fecha_Desde,
            YEAR((Fecha_Actualizado)) * 100 + MONTH((Fecha_Actualizado)) AS AnioMes_Id_Desde
        FROM {{ this }}
    --GROUP BY Emp_Id
    ),
{% else %}
WITH
{% endif %}
Fecha_Max AS (
    --Excluir las facturas del ultimo día (puede estar incompleto, ej. hoy)
    SELECT
        CAST(MAX(Factura_Fecha) AS DATE) AS Fecha_Max,
        Emp_Id,
        MAX(AnioMes_Id) AS AnioMes_Id_Max

    /*
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_FacturaEncabezado') }} */

    FROM [BI_FARINTER].[dbo].[BI_Kielsa_Hecho_FacturaPosicion] FE -- {{ ref ('BI_Kielsa_Hecho_FacturaPosicion') }} FE
    WHERE AnioMes_Id >= '{{ v_last_date_control[0:6] }}'
    GROUP BY Emp_Id
),

Resumen_FacturaEncabezado AS (
    SELECT
        FP.MonederoTarj_Id_Limpio AS Monedero_Id,
        FP.Emp_Id,
        FP.Suc_Id AS Sucursal_Id,
        FP.Articulo_Id AS Articulo_Id,
        MIN(FP.Factura_Fecha) AS Fecha_Primer_Factura,
        MAX(FP.Factura_Fecha) AS Fecha_Ultima_Factura

        {% if is_incremental() %}
            , SUM(CASE WHEN FP.Factura_Fecha > ISNULL(FD.Fecha_Desde_Real, '19000101') THEN 1 ELSE 0 END) AS Cantidad_Facturas
        {% else %}
		, COUNT(DISTINCT FP.Factura_Id) AS Cantidad_Facturas
		{% endif %}
        --, COUNT(DISTINCT FE.Factura_Fecha) AS Veces
        , SUM(CASE
            WHEN FP.Factura_Fecha BETWEEN DATEADD(DAY, -30, GETDATE()) AND GETDATE()
                THEN FP.Cantidad_Padre
            ELSE 0
        END) AS Cantidad_30,
        SUM(CASE
            WHEN FP.Factura_Fecha BETWEEN DATEADD(DAY, -60, GETDATE()) AND GETDATE()
                THEN FP.Cantidad_Padre
            ELSE 0
        END) AS Cantidad_60,
        SUM(CASE
            WHEN FP.Factura_Fecha BETWEEN DATEADD(DAY, -90, GETDATE()) AND GETDATE()
                THEN FP.Cantidad_Padre
            ELSE 0
        END) AS Cantidad_90,
        SUM(CASE
            WHEN FP.Factura_Fecha BETWEEN DATEADD(DAY, -120, GETDATE()) AND GETDATE()
                THEN FP.Cantidad_Padre
            ELSE 0
        END) AS Cantidad_120,
        SUM(CASE
            WHEN FP.Factura_Fecha BETWEEN DATEADD(DAY, -180, GETDATE()) AND GETDATE()
                THEN FP.Cantidad_Padre
            ELSE 0
        END) AS Cantidad_180

    {% if is_incremental() %}
        FROM [BI_FARINTER].[dbo].[BI_Kielsa_Hecho_FacturaPosicion] FP -- {{ ref ('BI_Kielsa_Hecho_FacturaPosicion') }} FE
        INNER JOIN Fecha_Max FM
            ON
                FM.Emp_Id = FP.Emp_Id
                AND FP.Factura_Fecha < FM.Fecha_Max
                AND FP.AnioMes_Id <= FM.AnioMes_Id_Max
        LEFT JOIN Fecha_Desde FD
            ON
                FD.Emp_Id = FP.Emp_Id
                AND FD.Articulo_Id = FP.Articulo_Id
                AND FD.Suc_Id = FP.Suc_Id
                AND FD.Monedero_Id = MonederoTarj_Id_Limpio
                AND FP.Factura_Fecha > FD.Fecha_Desde
                AND FP.AnioMes_Id >= FD.AnioMes_Id_Desde
        WHERE FD.Emp_Id IS NOT NULL OR (FD.Emp_Id IS NULL AND FP.Fecha_Actualizado > '{{ v_last_date }}' AND FP.AnioMes_Id >= '{{ v_last_date[0:6] }}')
    {% else %}
    FROM  {{ source('DL_FARINTER', 'DL_Kielsa_FacturasPosiciones') }}  FP

{% endif %}
    GROUP BY FP.MonederoTarj_Id_Limpio, FP.Emp_Id, FP.Suc_Id, FP.Articulo_Id, FP.Cantidad_Padre
)

SELECT
    ISNULL(RFE.Monedero_Id, '') Monedero_Id,
    ISNULL(RFE.Emp_Id, 0) Emp_Id,
    RFE.Fecha_Primer_Factura Fecha_Primer_Factura,
    RFE.Fecha_Ultima_Factura Fecha_Ultima_Factura,
    ISNULL(RFE.Sucursal_Id, 0) Sucursal_Id,
    ISNULL(RFE.Articulo_Id, 0) Articulo_Id
    --	, RFE.Veces Veces

    {% if is_incremental() %}
        , RFE.Cantidad_Facturas + ISNULL(MAGR.Cantidad_Facturas, 0) Cantidad_Facturas
    {% else %}
		, RFE.Cantidad_Facturas Cantidad_Facturas
{% endif %}
    , RFE.Cantidad_30 Cantidad_30,
    RFE.Cantidad_60 Cantidad_60,
    RFE.Cantidad_90 Cantidad_90,
    RFE.Cantidad_120 Cantidad_120,
    RFE.Cantidad_180 Cantidad_180,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Monedero_Id', 'Sucursal_Id', 'Articulo_Id'], input_length=49, table_alias='RFE') }} [EmpMonSucArt_Id],
    GETDATE() AS Fecha_Actualizado
FROM Resumen_FacturaEncabezado RFE
{% if is_incremental() %}
    LEFT JOIN {{ this }} MAGR
        ON
            MAGR.Monedero_Id = RFE.Monedero_Id
            AND MAGR.Emp_Id = RFE.Emp_Id
            AND MAGR.Sucursal_Id = RFE.Sucursal_Id
            AND MAGR.Articulo_Id = RFE.Articulo_Id
{% endif %}
