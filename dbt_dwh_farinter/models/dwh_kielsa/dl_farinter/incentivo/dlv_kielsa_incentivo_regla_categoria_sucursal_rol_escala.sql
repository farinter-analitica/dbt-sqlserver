{{ 
    config(
		tags=["automation/periodo_mensual_inicio", "automation_only"],
        materialized="view",
    )
}}

with base as (
    select
        rcsre.id,
        rcsre.regla_categoria_sucursal_id,
        rcsre.rol_id,
        rcsre.condicion,
        rcsre.valor_min,
        rcsre.valor_max,
        rcsre.valor_incentivo,
        rcs.regla_id,
        rcs.categoria,
        r.emp_id,
        r.fecha_desde,
        r.fecha_hasta,
        rol.nombre as rol_nombre
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla_categoria_sucursal_rol_escala') }} as rcsre
    left join {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla_categoria_sucursal') }} as rcs
        on rcsre.regla_categoria_sucursal_id = rcs.id
    left join {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla') }} as r
        on rcs.regla_id = r.id
    left join {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_rol') }} as rol
        on rcsre.rol_id = rol.id
)

select * from base
