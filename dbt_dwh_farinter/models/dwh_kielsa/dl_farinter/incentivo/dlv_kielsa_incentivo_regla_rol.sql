{{ 
    config(
		tags=["automation/periodo_mensual_inicio", "automation_only"],
        materialized="view",
    )
}}

with base as (
    select
        rr.id,
        rr.regla_id,
        rr.rol_id,
        rol.rol_id_ld,
        rr.part_regalia,
        rr.codigo_tipo,
        rr.tipo_aplicacion,
        rr.part_comision,
        rr.valor_por_receta_seguro,
        r.emp_id,
        r.fecha_desde,
        r.fecha_hasta,
        rol.nombre as rol_nombre
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla_rol') }} as rr
    left join {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla') }} as r
        on rr.regla_id = r.id
    left join {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_rol') }} as rol
        on rr.rol_id = rol.id
)

select * from base
