{% set unique_key_list = ["Monedero_Id","Emp_Id"] %}

{% if is_incremental() %}
    {% set v_last_date = run_single_value_query_on_relation_and_return(
		query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, 0, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this,
		relation_not_found_value='19000101'|string)|string %}
    {% set v_merge_exclude_columns = unique_key_list + ["Fecha_Carga","Fecha_Primer_Factura"] %}
    {% set v_merge_check_diff_exclude_columns = v_merge_exclude_columns + ["Fecha_Actualizado"] %}
    {% set v_last_date_control = (modules.datetime.datetime.fromisoformat(v_last_date) -
		modules.datetime.timedelta(days=30)).strftime('%Y%m%d') %}
{% else %}
	{% set v_last_date = '19000101' %}
	{% set v_last_date_control = '19000101' %}
	{% set v_merge_exclude_columns = unique_key_list + ["Fecha_Carga"] %}
	{% set v_merge_check_diff_exclude_columns = v_merge_exclude_columns + ["Fecha_Actualizado"] %}
{% endif %}
	{% set v_180_days = (modules.datetime.datetime.fromisoformat(v_last_date) -
		modules.datetime.timedelta(days=180)).strftime('%Y%m%d') %}

{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
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
            (Fecha_Actualizado) AS Fecha_Desde,
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
        Emp_Id,
        CAST(MAX(Factura_Fecha) AS DATE) AS Fecha_Max,
        MAX(AnioMes_Id) AS AnioMes_Id_Max
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_FacturaEncabezado') }}
    WHERE AnioMes_Id >= '{{ v_last_date_control[0:6] }}'
    GROUP BY Emp_ID
),

Resumen_FacturaEncabezado AS (
    SELECT
        FE.MonederoTarj_Id_Limpio AS Monedero_Id,
        FE.Emp_Id,
        MIN(FE.Factura_Fecha) AS Fecha_Primer_Factura,
        MAX(FE.Factura_Fecha) AS Fecha_Ultima_Factura,
        COUNT(FE.Factura_Id) AS Cantidad_Facturas,
        COUNT(DISTINCT FE.Factura_Fecha) AS Cantidad_Dias

    FROM {{ source('DL_FARINTER', 'DL_Kielsa_FacturaEncabezado') }} AS FE
    {% if is_incremental() %}
        INNER JOIN Fecha_Max AS FM
            ON
                FE.Emp_Id = FM.Emp_Id
                AND FE.Factura_Fecha < FM.Fecha_Max
                AND FE.AnioMes_Id <= FM.AnioMes_Id_Max
        LEFT JOIN Fecha_Desde AS FD
            ON
                FE.Emp_Id = FD.Emp_Id
                AND FE.MonederoTarj_Id_Limpio = FD.Monedero_Id
                AND FE.Factura_Fecha > FD.Fecha_Desde
                AND FE.AnioMes_Id >= FD.AnioMes_Id_Desde
        WHERE
            FD.Emp_Id IS NOT NULL
            OR (
                FD.Emp_Id IS NULL
                AND FE.Fecha_Actualizado > '{{ v_last_date }}'
                AND FE.AnioMes_Id >= '{{ v_last_date[0:6] }}'
            )
    {% endif %}
    GROUP BY FE.MonederoTarj_Id_Limpio, FE.Emp_Id
),

MetricasRecientes AS (
    SELECT
        FE.Monedero_Id,
        FE.Emp_Id,
        CAST(
            SUM(FE.Valor_Total - FE.Valor_Impuesto)
            / NULLIF(COUNT(DISTINCT FE.EmpSucDocCajFac_Id), 0) AS NUMERIC(18, 4)
        ) AS Ticket_Promedio
    FROM {{ ref('BI_Kielsa_Hecho_FacturaEncabezado') }} AS FE
    WHERE
        FE.AnioMes_Id >= ('{{ v_180_days[0:6] }}')
        AND FE.Factura_Fecha > CAST('{{ v_180_days }}' AS DATE)
    GROUP BY
        FE.Monedero_Id,
        FE.Emp_Id
)

SELECT --noqa: ST06
    ISNULL(RFE.Monedero_Id, '') AS Monedero_Id,
    ISNULL(RFE.Emp_Id, 0) AS Emp_Id,
    RFE.Fecha_Primer_Factura,
    RFE.Fecha_Ultima_Factura,
    {% if is_incremental() -%}
        RFE.Cantidad_Facturas + ISNULL(MAGR.Cantidad_Facturas, 0) AS Cantidad_Facturas,
        RFE.Cantidad_Dias + ISNULL(MAGR.Cantidad_Dias, 0) AS Cantidad_Dias,
    {% else -%}
		RFE.Cantidad_Facturas AS Cantidad_Facturas,
		RFE.Cantidad_Dias AS Cantidad_Dias,
{% endif %}
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Monedero_Id'], input_length=49, table_alias='RFE') }} AS [EmpMon_Id],
    ISNULL(CAST(ROUND(MR.Ticket_Promedio, 4) AS DECIMAL(18, 6)), 0.0) AS Ticket_Promedio,
    GETDATE() AS Fecha_Actualizado
FROM Resumen_FacturaEncabezado AS RFE
LEFT JOIN MetricasRecientes AS MR
    ON
        RFE.Monedero_Id = MR.Monedero_Id
        AND RFE.Emp_Id = MR.Emp_Id
{% if is_incremental() %}
    LEFT JOIN {{ this }} AS MAGR
        ON
            RFE.Monedero_Id = MAGR.Monedero_Id
            AND RFE.Emp_Id = MAGR.Emp_Id
{% endif %}
