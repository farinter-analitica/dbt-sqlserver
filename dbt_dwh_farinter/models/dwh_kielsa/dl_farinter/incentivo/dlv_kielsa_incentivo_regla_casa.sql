{{ 
    config(
		tags=["automation/periodo_mensual_inicio", "automation_only"],
        materialized="view",
    )
}}

with base as (
    select
        rc.id,
        rc.regla_id,
        rc.casa_id_ld,
        rc.valor_regalia,
        r.emp_id,
        r.fecha_desde,
        r.fecha_hasta
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla_casa') }} as rc
    left join {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla') }} as r
        on rc.regla_id = r.id
)

select * from base
