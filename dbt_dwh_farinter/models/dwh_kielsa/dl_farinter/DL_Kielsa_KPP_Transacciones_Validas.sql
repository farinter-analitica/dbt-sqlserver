
{%- set unique_key_list = ["Transaccion_Id"] -%}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario", "automation/periodo_por_hora"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="fail",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
      "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['TarjetaKC_Id']) }}",
		]
		
) }}

{% if is_incremental() %}
    {% set last_date = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this,
        relation_not_found_value='00000000'|string)|string %}
{% else %}
    {% set last_date = '00000000'|string %}
{% endif %}

WITH TransactionSequence AS (
    SELECT
        t.Transaccion_Id,
        t.TarjetaKC_Id,
        t.Cliente_Nombre,
        t.Monto_Rebajado,
        t.Fecha,
        t.NumeroTarjeta,
        t.TipoTarjeta,
        t.ResponseMessage,
        t.transaction_id,
        t.transaction_reference,
        t.payment_uuid,
        t.payment_hash,
        t.response_reason,
        t.ResponseSuccess,
        t.Fecha_Actualizado,
        -- Contamos transacciones positivas anteriores en el mismo día
        COUNT(
            CASE
                WHEN t.Monto_Rebajado > 0 THEN 1
            END
        ) OVER (
            PARTITION BY t.TarjetaKC_Id,
            CAST(t.Fecha AS DATE)
            ORDER BY t.Transaccion_Id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS Transacciones_Positivas_Previas,
        LEAD(t.Monto_Rebajado) OVER (
            PARTITION BY t.TarjetaKC_Id
            ORDER BY t.Transaccion_Id
        ) AS Siguiente_Monto
    FROM {{ ref('DL_Kielsa_KPP_Transacciones') }} AS t
    {% if is_incremental() %}
        WHERE t.Fecha_Actualizado > '{{ last_date }}'
    {% endif %}
),

LogValidation AS (
    SELECT
        TarjetaKC_Id,
        CAST(FRegistro AS DATE) AS Fecha_Log,
        COUNT(*) AS Total_Errores,
        STRING_AGG(ErrorMessage, ' | ') WITHIN GROUP (
            ORDER BY FRegistro
        ) AS Errores_Concatenados
    FROM {{ ref('DL_Kielsa_KPP_LogSuscripcion') }}
    WHERE
        (ErrorMessage LIKE '%error%' AND ErrorMessage NOT LIKE '%usuario no encontrado%')
        OR ErrorMessage LIKE '%unauth%'
        OR ErrorMessage LIKE '%time%out%'
    GROUP BY
        TarjetaKC_Id,
        CAST(FRegistro AS DATE)
),

ValidTransactions AS (
    SELECT
        t.*,
        l.Errores_Concatenados AS Mensajes_Error,
        l.Total_Errores,
        CASE
            WHEN
                t.Monto_Rebajado > 0
                AND (
                    t.Siguiente_Monto IS NULL
                    OR t.Siguiente_Monto >= 0
                ) -- Si hay errores, verificamos que sean menos que las transacciones positivas previas
                AND (
                    l.Total_Errores IS NULL
                    OR ISNULL(t.ResponseSuccess, '') LIKE '%true%'
                    OR l.Total_Errores <= t.Transacciones_Positivas_Previas
                )
                AND ISNULL(t.ResponseSuccess, '') NOT LIKE '%false%' THEN 1
            WHEN
                t.Monto_Rebajado > 0
                AND t.Siguiente_Monto = -t.Monto_Rebajado THEN 0
            ELSE 0
        END AS Es_Transaccion_Valida
    FROM TransactionSequence AS t
    LEFT JOIN LogValidation
        AS l ON t.TarjetaKC_Id = l.TarjetaKC_Id
    AND CAST(t.Fecha AS DATE) = l.Fecha_Log
)

SELECT
    *,
    CASE WHEN Es_Transaccion_Valida = 1 AND Total_Errores > 0 THEN 1 ELSE 0 END AS Es_Transaccion_Sospechosa
FROM ValidTransactions
--WHERE TarjetaKC_Id = '0603-1989-00447' --ORDER BY Fecha;
