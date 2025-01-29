{% set unique_key_list = ["Suscripcion_Id", "Transaccion_Id"] %}

{{ 
    config(
        as_columnstore=true,
        tags=["periodo/diario"],
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
            "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
        ]
    ) 
}}

{%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '19000101' %}
{%- endif %}

WITH Transacciones_Iniciales AS (
    -- Identificar primera transacción para cada suscripción nueva
    SELECT 
        t.Transaccion_Id,
        t.TarjetaKC_Id,
        t.Fecha,
        ROW_NUMBER() OVER (PARTITION BY t.TarjetaKC_Id, CAST(t.Fecha AS DATE) ORDER BY t.Transaccion_Id) as rn
    FROM {{ ref('DL_Kielsa_KPP_Transacciones_Validas') }} t
    INNER JOIN  {{ source('DL_FARINTER', 'DL_Kielsa_KPP_Suscripcion') }} s
        ON s.TarjetaKC_Id = t.TarjetaKC_Id  COLLATE DATABASE_DEFAULT
        AND CAST(s.FRegistro AS DATE) = CAST(t.Fecha AS DATE)
    WHERE t.Es_Transaccion_Valida = 1
        {% if is_incremental() %}
        AND s.Fecha_Actualizado >= '{{ last_date }}'
        {% endif %}

),
Suscripciones_Nuevas AS (
    -- Captura suscripciones iniciales con su transacción correspondiente
    SELECT 
        ISNULL(ti.Transaccion_Id,0) as Transaccion_Id,  -- Será 0 si no hay transacción en la misma fecha
        s.Suscripcion_Id,
        s.TarjetaKC_Id COLLATE DATABASE_DEFAULT AS TarjetaKC_Id,
        s.Identidad_Limpia,
        COALESCE(t.Monto_Rebajado, p.Costo) AS Monto_Rebajado, 
        CONVERT(DATE, s.FRegistro) AS Fecha_Transaccion,
        s.Usuario_Registro COLLATE DATABASE_DEFAULT AS Usuario_Id,
        s.Sucursal_Registro  AS Sucursal_Id,
        s.CodPlanKielsaClinica COLLATE DATABASE_DEFAULT AS Articulo_Id,
        s.TipoPlan AS Plan_Id,
        1 AS Numero_Transaccion,
        s.Fecha_Actualizado
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_KPP_Suscripcion') }} s
    INNER JOIN {{ ref('DL_Kielsa_KPP_Plan_Suscripcion') }} p 
        ON s.TipoPlan = p.Plan_Id
    LEFT JOIN Transacciones_Iniciales ti
        ON s.TarjetaKC_Id = ti.TarjetaKC_Id  COLLATE DATABASE_DEFAULT
        AND CAST(s.FRegistro AS DATE) = CAST(ti.Fecha AS DATE)
        AND ti.rn = 1
    LEFT JOIN {{ ref('DL_Kielsa_KPP_Transacciones_Validas') }} t
        ON ti.Transaccion_Id = t.Transaccion_Id
    {% if is_incremental() %}
    WHERE s.Fecha_Actualizado >= '{{ last_date }}'
    {% endif %}

    UNION ALL

    -- Captura renovaciones excluyendo transacciones ya usadas
    SELECT 
        t.Transaccion_Id,
        s.Suscripcion_Id,
        t.TarjetaKC_Id COLLATE DATABASE_DEFAULT AS TarjetaKC_Id,
        s.Identidad_Limpia,
        t.Monto_Rebajado,
        t.Fecha AS Fecha_Transaccion,
        s.Usuario_Registro COLLATE DATABASE_DEFAULT AS  Usuario_Id,
        s.Sucursal_Registro AS Sucursal_Id,
        s.CodPlanKielsaClinica COLLATE DATABASE_DEFAULT AS Articulo_Id,
        s.TipoPlan AS Plan_Id,
        ROW_NUMBER() OVER (PARTITION BY t.TarjetaKC_Id ORDER BY t.Fecha) + 1 AS Numero_Transaccion,
        s.Fecha_Actualizado
    FROM {{ ref('DL_Kielsa_KPP_Transacciones_Validas') }} t
    INNER JOIN {{ source('DL_FARINTER', 'DL_Kielsa_KPP_Suscripcion') }} s
        ON t.TarjetaKC_Id = s.TarjetaKC_Id COLLATE DATABASE_DEFAULT
    LEFT JOIN Transacciones_Iniciales ti
        ON t.Transaccion_Id = ti.Transaccion_Id
        AND ti.rn = 1
    WHERE t.Es_Transaccion_Valida = 1
    AND ti.Transaccion_Id IS NULL  -- Excluye transacciones ya usadas en suscripciones nuevas
)
SELECT 
    s.Transaccion_Id,
    s.Suscripcion_Id,
    s.TarjetaKC_Id,
    s.Identidad_Limpia,
    s.Monto_Rebajado,
    s.Fecha_Transaccion,
    s.Numero_Transaccion,
    s.Articulo_Id,
    s.Plan_Id,
    s.Usuario_Id,
    s.Sucursal_Id,
    LEAD(s.Fecha_Transaccion) OVER (PARTITION BY s.TarjetaKC_Id ORDER BY s.Numero_Transaccion) AS Siguiente_Fecha_Transaccion,
    LEAD(s.Articulo_Id) OVER (PARTITION BY s.TarjetaKC_Id ORDER BY s.Numero_Transaccion) AS Siguiente_Articulo_Id,
    LEAD(s.Sucursal_Id) OVER (PARTITION BY s.TarjetaKC_Id ORDER BY s.Numero_Transaccion) AS Siguiente_Sucursal_Id,
    LEAD(s.Monto_Rebajado) OVER (PARTITION BY s.TarjetaKC_Id ORDER BY s.Numero_Transaccion) AS Siguiente_Monto_Rebajado,
    LEAD(s.Usuario_Id) OVER (PARTITION BY s.TarjetaKC_Id ORDER BY s.Numero_Transaccion) AS Siguiente_Usuario_Id,
    MIN(s.Fecha_Transaccion) OVER (PARTITION BY s.TarjetaKC_Id) AS Fecha_Primera_Suscripcion,
    CASE WHEN s.Numero_Transaccion <= 1 THEN 'Nuevo' ELSE 'Renovacion' END AS Tipo_Suscripcion,
    s.Fecha_Actualizado
FROM Suscripciones_Nuevas s
--where s.Identidad_Limpia='0801198704679'