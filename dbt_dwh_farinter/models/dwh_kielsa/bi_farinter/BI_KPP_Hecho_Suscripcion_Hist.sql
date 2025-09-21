{% set unique_key_list = ["Fecha_Transaccion", "TarjetaKC_Id", "Log_Id"] %}

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
            "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
        ]
    ) 
}}

{%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this,
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '19000101' %}
{%- endif %}

WITH Suscripcion AS (
    SELECT
        s.Suscripcion_Id,
        s.Identidad_Limpia,
        s.Fecha_Actualizado,
        s.TipoPlan,
        s.FRegistro,
        s.Sucursal_Registro AS Sucursal_Registro,
        s.TarjetaKC_Id COLLATE DATABASE_DEFAULT AS TarjetaKC_Id, -- noqa: RF03
        s.Usuario_Registro COLLATE DATABASE_DEFAULT AS Usuario_Registro, -- noqa: RF03
        s.CodPlanKielsaClinica COLLATE DATABASE_DEFAULT AS CodPlanKielsaClinica -- noqa: RF03
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_KPP_Suscripcion') }} AS s
    {% if is_incremental() %}
        WHERE s.Fecha_Actualizado >= '{{ last_date }}'
    {% endif %}
),

Transacciones_Base AS (
    SELECT t.* FROM {{ ref('DL_Kielsa_KPP_Transacciones_Validas') }} AS t
    WHERE (t.Es_Transaccion_Valida = 1 AND t.Es_Transaccion_Sospechosa = 0)
),

Transacciones_Iniciales AS (
    SELECT
        s.Suscripcion_Id,
        s.Identidad_Limpia,
        t.Monto_Rebajado AS Monto_Rebajado,
        l.Tipo_Ingreso,
        l.Origen,
        ISNULL(t.Transaccion_Id, 0) AS Transaccion_Id,
        ISNULL(l.Id, 0) AS Log_Id,
        ISNULL(t.Transaccion_Id, l.Id) AS Evento_Id,
        COALESCE(
            t.TarjetaKC_Id COLLATE DATABASE_DEFAULT, -- noqa: RF02
            l.TarjetaKC_Id COLLATE DATABASE_DEFAULT -- noqa: RF02
        ) AS TarjetaKC_Id,
        CAST(COALESCE(t.Fecha, l.Fecha) AS date) AS Fecha_Transaccion,
        COALESCE(
            l.Usuario_Registro COLLATE DATABASE_DEFAULT, -- noqa: RF02
            s.Usuario_Registro COLLATE DATABASE_DEFAULT -- noqa: RF02
        ) AS Usuario_Id,
        COALESCE(l.Sucursal_Registro, s.Sucursal_Registro) AS Sucursal_Id,
        COALESCE(
            l.CodPlanKielsaClinica COLLATE DATABASE_DEFAULT, -- noqa: RF02
            s.CodPlanKielsaClinica COLLATE DATABASE_DEFAULT -- noqa: RF02
        ) AS Articulo_Id,
        COALESCE(l.TipoPlan, s.TipoPlan) AS Plan_Id,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(
                t.TarjetaKC_Id COLLATE DATABASE_DEFAULT, -- noqa: RF02
                l.TarjetaKC_Id COLLATE DATABASE_DEFAULT -- noqa: RF02
            ), CAST(COALESCE(t.Fecha, l.Fecha) AS date)
            ORDER BY t.Transaccion_Id DESC, l.Id DESC
        ) AS rn,
        CASE WHEN CAST(s.FRegistro AS date) = CAST(COALESCE(t.Fecha, l.Fecha) AS date) THEN 1 ELSE 0 END AS Es_Evento_Inicial
    FROM Transacciones_Base AS t
    FULL OUTER JOIN {{ ref('DL_Kielsa_KPP_LogMovimientoSuscripcion') }} AS l
        ON
            l.TarjetaKC_Id = t.TarjetaKC_Id COLLATE DATABASE_DEFAULT -- noqa: RF02
            AND CAST(l.Fecha AS date) = CAST(t.Fecha AS date)
            AND l.Tipo_Documento = 'suscripcion'
            AND l.id > 0
    INNER JOIN Suscripcion AS s
        ON s.TarjetaKC_Id = COALESCE(
            t.TarjetaKC_Id COLLATE DATABASE_DEFAULT, -- noqa: RF02
            l.TarjetaKC_Id COLLATE DATABASE_DEFAULT -- noqa: RF02
        )
    WHERE (t.Transaccion_Id IS NOT NULL OR l.Id IS NOT NULL)
),

Suscripciones_Nuevas AS (
    SELECT -- noqa: ST06
        ISNULL(ti.Transaccion_Id, 0) AS Transaccion_Id,
        ISNULL(ti.Log_Id, 0) AS Log_Id,
        ISNULL(ti.Evento_Id, 0) AS Evento_Id,
        s.Suscripcion_Id,
        s.TarjetaKC_Id COLLATE DATABASE_DEFAULT AS TarjetaKC_Id, -- noqa: RF02
        s.Identidad_Limpia,
        COALESCE(t.Monto_Rebajado, p.Costo) AS Monto_Rebajado,
        CAST(s.FRegistro AS date) AS Fecha_Transaccion,
        COALESCE(
            l.Usuario_Registro COLLATE DATABASE_DEFAULT, -- noqa: RF02
            s.Usuario_Registro COLLATE DATABASE_DEFAULT -- noqa: RF02
        ) AS Usuario_Id,
        COALESCE(l.Sucursal_Registro, s.Sucursal_Registro) AS Sucursal_Id,
        COALESCE(
            l.CodPlanKielsaClinica COLLATE DATABASE_DEFAULT, -- noqa: RF02
            s.CodPlanKielsaClinica COLLATE DATABASE_DEFAULT -- noqa: RF02
        ) AS Articulo_Id,
        COALESCE(l.TipoPlan, s.TipoPlan) AS Plan_Id,
        l.Tipo_Ingreso,
        l.Origen,
        1 AS Numero_Transaccion,
        s.Fecha_Actualizado
    FROM Suscripcion AS s
    INNER JOIN {{ ref('DL_Kielsa_KPP_Plan_Suscripcion') }} AS p
        ON s.TipoPlan = p.Plan_Id
    LEFT JOIN Transacciones_Iniciales AS ti
        ON
            s.TarjetaKC_Id = ti.TarjetaKC_Id COLLATE DATABASE_DEFAULT -- noqa: RF02
            AND ti.Es_Evento_Inicial = 1
            AND ti.rn = 1
    LEFT JOIN {{ ref('DL_Kielsa_KPP_Transacciones_Validas') }} AS t
        ON ti.Transaccion_Id = t.Transaccion_Id
    LEFT JOIN {{ ref('DL_Kielsa_KPP_LogMovimientoSuscripcion') }} AS l
        ON ti.Log_Id = l.Id
),

Renovaciones AS (
    -- Captura renovaciones excluyendo transacciones ya usadas
    SELECT -- noqa: ST06
        ISNULL(l.Transaccion_Id, 0) AS Transaccion_Id,
        ISNULL(l.Log_Id, 0) AS Log_Id,
        ISNULL(l.Evento_Id, 0) AS Evento_Id,
        s.Suscripcion_Id,
        l.TarjetaKC_Id,
        s.Identidad_Limpia,
        COALESCE(l.Monto_Rebajado, p.Costo) AS Monto_Rebajado,
        l.Fecha_Transaccion,
        l.Usuario_Id,
        l.Sucursal_Id,
        l.Articulo_Id,
        l.Plan_Id,
        l.Tipo_Ingreso,
        l.Origen,
        ROW_NUMBER() OVER (
            PARTITION BY l.TarjetaKC_Id
            ORDER BY l.Fecha_Transaccion, l.Evento_Id
        ) + 1 AS Numero_Transaccion,
        s.Fecha_Actualizado
    FROM Transacciones_Iniciales AS l
    INNER JOIN Suscripcion AS s
        ON l.TarjetaKC_Id = s.TarjetaKC_Id
    LEFT JOIN {{ ref('DL_Kielsa_KPP_Plan_Suscripcion') }} AS p
        ON l.Plan_Id = p.Plan_Id
    WHERE
        l.Es_Evento_Inicial = 0  -- Excluye transacciones ya usadas en suscripciones nuevas
        AND l.rn = 1  -- Excluye duplicados
),

Todas AS (
    SELECT * FROM Suscripciones_Nuevas
    UNION ALL
    SELECT R.* FROM Renovaciones AS R
)

SELECT -- noqa: ST06
    1 AS Emp_Id,
    s.Transaccion_Id,
    s.Log_Id,
    s.Evento_Id,
    s.Suscripcion_Id,
    ISNULL(s.TarjetaKC_Id, '') AS TarjetaKC_Id,
    s.Identidad_Limpia,
    CONCAT(1, '-', s.Identidad_Limpia) AS EmpMon_Id,
    s.Monto_Rebajado,
    ISNULL(s.Fecha_Transaccion, '19000101') AS Fecha_Transaccion,
    s.Numero_Transaccion,
    s.Articulo_Id,
    CONCAT(1, '-', s.Articulo_Id) AS EmpArt_Id,
    s.Plan_Id,
    s.Usuario_Id,
    ISNULL(U.Ultimo_Vendedor_Id_Asignado, 0) AS Vendedor_Id,
    CONCAT(1, '-', ISNULL(U.Ultimo_Vendedor_Id_Asignado, 0)) AS EmpVen_Id,
    s.Sucursal_Id,
    CONCAT(1, '-', s.Sucursal_Id) AS EmpSuc_Id,
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
FROM Todas AS s
LEFT JOIN {{ ref("BI_Kielsa_Dim_Usuario") }} AS U
    ON
        s.Usuario_Id = U.Usuario_Login
        AND U.Emp_Id = 1

--where s.Suscripcion_Id=76293
