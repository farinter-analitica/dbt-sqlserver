{{ config(materialized="view") }}

with base as (
    select
        rr.id,
        rr.regla_id,
        rr.valor_predeterminado,
        rr.excluir_marca_propia,
        rr.aplica_por_part,
        r.emp_id,
        r.fecha_desde,
        r.fecha_hasta
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla_regalia') }} as rr
    left join {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla') }} as r
        on rr.regla_id = r.id
)

select * from base
