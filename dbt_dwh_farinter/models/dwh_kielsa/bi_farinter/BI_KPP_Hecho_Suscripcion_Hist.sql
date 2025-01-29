{% set unique_key_list = ["Suscripcion_Id", "Transaccion_Id", "Log_Id"] %}

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
    SELECT 
        ISNULL(t.Transaccion_Id, 0) as Transaccion_Id,
        ISNULL(l.Id, 0) as Log_Id,
        ISNULL(t.Transaccion_Id, l.Id) as Evento_Id,
        s.Suscripcion_Id,
        COALESCE(t.TarjetaKC_Id, l.TarjetaKC_Id) COLLATE DATABASE_DEFAULT AS TarjetaKC_Id,
        s.Identidad_Limpia,
        t.Monto_Rebajado AS Monto_Rebajado,
        cast(COALESCE(t.Fecha, l.Fecha) as date) AS Fecha_Transaccion,
        COALESCE(l.Usuario_Registro, s.Usuario_Registro) COLLATE DATABASE_DEFAULT AS Usuario_Id,
        COALESCE(l.Sucursal_Registro, s.Sucursal_Registro) AS Sucursal_Id,
        COALESCE(l.CodPlanKielsaClinica, s.CodPlanKielsaClinica) COLLATE DATABASE_DEFAULT AS Articulo_Id,
        COALESCE(l.TipoPlan, s.TipoPlan) AS Plan_Id,
        l.Tipo_Ingreso,
        l.Origen,
        ROW_NUMBER() OVER (PARTITION BY COALESCE(t.TarjetaKC_Id COLLATE DATABASE_DEFAULT, l.TarjetaKC_Id COLLATE DATABASE_DEFAULT), CAST(COALESCE(t.Fecha, l.Fecha) AS DATE) 
                          ORDER BY t.Transaccion_Id desc, l.Id desc) as rn,
        CASE WHEN CAST(s.FRegistro AS DATE) = cast(COALESCE(t.Fecha, l.Fecha) as date) THEN 1 ELSE 0 END Es_Evento_Inicial 
    FROM {{ ref('DL_Kielsa_KPP_Transacciones_Validas') }} t
    FULL OUTER JOIN {{ ref('DL_Kielsa_KPP_LogMovimientoSuscripcion') }} l
        ON l.TarjetaKC_Id = t.TarjetaKC_Id COLLATE DATABASE_DEFAULT
        AND CAST(l.Fecha AS DATE) = CAST(t.Fecha AS DATE)
        AND l.Tipo_Documento = 'suscripcion'
        AND l.id>0
    INNER JOIN {{ source('DL_FARINTER', 'DL_Kielsa_KPP_Suscripcion') }} s
        ON s.TarjetaKC_Id = COALESCE(t.TarjetaKC_Id, l.TarjetaKC_Id) COLLATE DATABASE_DEFAULT
    WHERE ((t.Es_Transaccion_Valida = 1 and t.Es_Transaccion_Sospechosa =0) OR l.Id IS NOT NULL)
        {% if is_incremental() %}
        AND s.Fecha_Actualizado >= '{{ last_date }}'
        {% endif %}
),

Suscripciones_Nuevas AS (
    SELECT 
        ISNULL(ti.Transaccion_Id, 0) as Transaccion_Id,
        ISNULL(ti.Log_Id, 0) as Log_Id,
        s.Suscripcion_Id,
        s.TarjetaKC_Id COLLATE DATABASE_DEFAULT AS TarjetaKC_Id,
        s.Identidad_Limpia,
        COALESCE(t.Monto_Rebajado, p.Costo) AS Monto_Rebajado,
        CONVERT(DATE, s.FRegistro) AS Fecha_Transaccion,
        COALESCE(l.Usuario_Registro, s.Usuario_Registro) COLLATE DATABASE_DEFAULT AS Usuario_Id,
        COALESCE(l.Sucursal_Registro, s.Sucursal_Registro) AS Sucursal_Id,
        COALESCE(l.CodPlanKielsaClinica, s.CodPlanKielsaClinica) COLLATE DATABASE_DEFAULT AS Articulo_Id,
        COALESCE(l.TipoPlan, s.TipoPlan) AS Plan_Id,
        l.Tipo_Ingreso,
        l.Origen,
        1 AS Numero_Transaccion,
        s.Fecha_Actualizado
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_KPP_Suscripcion') }} s
    INNER JOIN {{ ref('DL_Kielsa_KPP_Plan_Suscripcion') }} p 
        ON s.TipoPlan = p.Plan_Id
    LEFT JOIN Transacciones_Iniciales ti
        ON s.TarjetaKC_Id = ti.TarjetaKC_Id COLLATE DATABASE_DEFAULT
        AND ti.Es_Evento_Inicial = 1
        AND ti.rn = 1
    LEFT JOIN {{ ref('DL_Kielsa_KPP_Transacciones_Validas') }} t
        ON ti.Transaccion_Id = t.Transaccion_Id
    LEFT JOIN {{ ref('DL_Kielsa_KPP_LogMovimientoSuscripcion') }} l
        ON ti.Log_Id = l.Id
    {% if is_incremental() %}
    WHERE s.Fecha_Actualizado >= '{{ last_date }}'
    {% endif %}

    UNION ALL

    -- Captura renovaciones excluyendo transacciones ya usadas
    SELECT 
        ISNULL(l.Transaccion_Id, 0) as Transaccion_Id,
        ISNULL(l.Log_Id, 0) as Log_Id,
        s.Suscripcion_Id,
        l.TarjetaKC_Id COLLATE DATABASE_DEFAULT AS TarjetaKC_Id,
        s.Identidad_Limpia,
        COALESCE(l.Monto_Rebajado, p.Costo) AS Monto_Rebajado,
        l.Fecha_Transaccion,
        l.Usuario_Id,
        l.Sucursal_Id,
        l.Articulo_Id,
        l.Plan_Id,
        l.Tipo_Ingreso,
        l.Origen,
        ROW_NUMBER() OVER (PARTITION BY l.TarjetaKC_Id 
                          ORDER BY l.Fecha_Transaccion, l.Evento_Id) + 1 AS Numero_Transaccion,
        s.Fecha_Actualizado
    FROM Transacciones_Iniciales l
    INNER JOIN {{ source('DL_FARINTER', 'DL_Kielsa_KPP_Suscripcion') }} s
        ON l.TarjetaKC_Id = s.TarjetaKC_Id COLLATE DATABASE_DEFAULT
    LEFT JOIN {{ ref('DL_Kielsa_KPP_Plan_Suscripcion') }} p 
        ON l.Plan_Id = p.Plan_Id
    WHERE l.Es_Evento_Inicial = 0  -- Excluye transacciones ya usadas en suscripciones nuevas
        AND l.rn = 1  -- Excluye duplicados
        {% if is_incremental() %}
        AND s.Fecha_Actualizado >= '{{ last_date }}'
        {% endif %}

)
SELECT 
    s.Transaccion_Id,
    s.Log_Id,
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
    s.Tipo_Ingreso,
    s.Origen,
    LEAD(s.Fecha_Transaccion) OVER (PARTITION BY s.TarjetaKC_Id ORDER BY s.Numero_Transaccion) AS Siguiente_Fecha_Transaccion,
    LEAD(s.Articulo_Id) OVER (PARTITION BY s.TarjetaKC_Id ORDER BY s.Numero_Transaccion) AS Siguiente_Articulo_Id,
    LEAD(s.Sucursal_Id) OVER (PARTITION BY s.TarjetaKC_Id ORDER BY s.Numero_Transaccion) AS Siguiente_Sucursal_Id,
    LEAD(s.Monto_Rebajado) OVER (PARTITION BY s.TarjetaKC_Id ORDER BY s.Numero_Transaccion) AS Siguiente_Monto_Rebajado,
    LEAD(s.Usuario_Id) OVER (PARTITION BY s.TarjetaKC_Id ORDER BY s.Numero_Transaccion) AS Siguiente_Usuario_Id,
    MIN(s.Fecha_Transaccion) OVER (PARTITION BY s.TarjetaKC_Id) AS Fecha_Primera_Suscripcion,
    CASE WHEN s.Numero_Transaccion <= 1 THEN 'Nuevo' ELSE 'Renovacion' END AS Tipo_Suscripcion,
    s.Fecha_Actualizado
FROM Suscripciones_Nuevas s
--where s.Suscripcion_Id=76293
