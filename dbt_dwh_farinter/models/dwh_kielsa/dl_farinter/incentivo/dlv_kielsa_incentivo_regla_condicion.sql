{{ config(materialized="view") }}

with base as (
    select
        rc.id,
        rc.regla_id,
        rc.condicion,
        rc.valor,
        r.emp_id,
        r.fecha_desde,
        r.fecha_hasta
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla_condicion') }} as rc
    left join {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla') }} as r
        on rc.regla_id = r.id
)

select * from base
