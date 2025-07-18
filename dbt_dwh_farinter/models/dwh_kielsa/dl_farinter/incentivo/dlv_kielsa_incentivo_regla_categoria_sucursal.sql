{{ 
    config(
		tags=["automation/periodo_mensual_inicio", "automation_only"],
        materialized="view",
    )
}}

with base as (
    select
        rcs.id,
        rcs.categoria,
        rcs.valor_min,
        rcs.valor_max,
        rcs.regla_id,
        r.emp_id,
        r.fecha_desde,
        r.fecha_hasta
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla_categoria_sucursal') }} as rcs
    left join {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla') }} as r
        on rcs.regla_id = r.id
)

select * from base
