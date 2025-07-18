{% set unique_key_list = ["regla_id","rol_id","casa_id","categoria","condicion"] -%}

{{ 
    config(
        tags=["automation/periodo_mensual_inicio"],
        materialized="view",
    )
}}


with regla as (
    select
        id as regla_id,
        emp_id,
        fecha_desde,
        fecha_hasta
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla') }}
),

regla_regalia as (
    select
        id,
        regla_id,
        valor_predeterminado,
        excluir_marca_propia,
        aplica_por_part
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla_regalia') }}
),

regla_casa as (
    select
        id,
        regla_id,
        casa_id,
        valor_regalia
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla_casa') }}
),

regla_rol as (
    select
        rr.id,
        rr.regla_id,
        rr.rol_id,
        rr.part_regalia,
        rr.aplicar_por_sucursal,
        rr.part_comision,
        rr.valor_por_receta_seguro,
        r.nombre as rol_nombre
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla_rol') }} as rr
    left join {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_rol') }} as r
        on rr.rol_id = r.id
),

regla_categoria_sucursal as (
    select
        id,
        categoria,
        valor_min,
        valor_max,
        regla_id
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla_categoria_sucursal') }}
),

regla_categoria_sucursal_rol_escala as (
    select
        id,
        regla_categoria_sucursal_id,
        rol_id,
        condicion,
        valor_min,
        valor_max,
        valor_incentivo
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla_categoria_sucursal_rol_escala') }}
),

regla_condicion as (
    select
        id,
        regla_id,
        condicion,
        valor
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla_condicion') }}
),

regla_rol_escala as (
    select
        id,
        regla_id,
        rol_id,
        condicion,
        valor_min,
        valor_max,
        valor_incentivo,
        porc_venta_general
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla_rol_escala') }}
),

configuracion_completa as (
    select
        r.regla_id,
        r.emp_id,
        r.fecha_desde,
        r.fecha_hasta,
        -- Configuración básica de regalía
        rreg.valor_predeterminado,
        rreg.excluir_marca_propia,
        rreg.aplica_por_part,
        -- Configuración por casa
        rcasa.casa_id,
        rcasa.valor_regalia as valor_regalia_casa,
        -- Configuración por rol
        rrol.rol_id,
        rrol.rol_nombre,
        rrol.part_regalia,
        rrol.part_comision,
        rrol.valor_por_receta_seguro,
        rrol.aplicar_por_sucursal,
        -- Condiciones generales
        rcond.condicion,
        rcond.valor as valor_condicion,
        -- Escalas por rol (JOIN CORREGIDO: rol_id -> kielsa_incentivo_rol.id directamente)
        rescala.condicion as escala_condicion,
        rescala.valor_min as escala_valor_min,
        rescala.valor_max as escala_valor_max,
        rescala.valor_incentivo as escala_valor_incentivo,
        rescala.porc_venta_general,
        -- Categorías de sucursal
        rcat.categoria,
        rcat.valor_min as categoria_valor_min,
        rcat.valor_max as categoria_valor_max,
        -- Escalas por categoría (JOIN CORREGIDO: rol_id -> kielsa_incentivo_rol.id directamente)
        rcatesc.condicion as categoria_escala_condicion,
        rcatesc.valor_min as categoria_escala_valor_min,
        rcatesc.valor_max as categoria_escala_valor_max,
        rcatesc.valor_incentivo as categoria_escala_valor_incentivo
    from regla as r
    left join regla_regalia as rreg
        on r.regla_id = rreg.regla_id
    left join regla_casa as rcasa
        on r.regla_id = rcasa.regla_id
    left join regla_rol as rrol
        on r.regla_id = rrol.regla_id
    left join regla_condicion as rcond
        on r.regla_id = rcond.regla_id
    -- CORREGIDO: rescala.rol_id es FK a kielsa_incentivo_rol.id, no a través de rrol
    left join regla_rol_escala as rescala
        on r.regla_id = rescala.regla_id
    -- CORREGIDO: rcatesc.rol_id es FK a kielsa_incentivo_rol.id, no a través de rrol
    left join regla_categoria_sucursal as rcat
        on r.regla_id = rcat.regla_id
    left join regla_categoria_sucursal_rol_escala as rcatesc
        on rcat.id = rcatesc.regla_categoria_sucursal_id
)

select * from configuracion_completa
