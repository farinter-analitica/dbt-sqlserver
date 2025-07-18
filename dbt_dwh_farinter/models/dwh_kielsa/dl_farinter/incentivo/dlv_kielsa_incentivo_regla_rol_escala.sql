{{ 
    config(
		tags=["automation/periodo_mensual_inicio", "automation_only"],
        materialized="view",
    )
}}

with base as (
    select
        rre.id,
        rre.regla_id,
        rre.rol_id,
        rre.condicion,
        rre.valor_min,
        rre.valor_max,
        rre.valor_incentivo,
        rre.porc_venta_general,
        r.emp_id,
        r.fecha_desde,
        r.fecha_hasta,
        rol.nombre as rol_nombre
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla_rol_escala') }} as rre
    left join {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla') }} as r
        on rre.regla_id = r.id
    left join {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_rol') }} as rol
        on rre.rol_id = rol.id
)

select * from base
