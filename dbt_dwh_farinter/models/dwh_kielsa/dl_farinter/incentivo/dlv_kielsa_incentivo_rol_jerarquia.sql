{% set unique_key_list = ["id"] -%}
{{ 
    config(
		tags=["automation/periodo_mensual_inicio", "automation_only"],
        materialized="view",
    )
-}}

with rol_con_profundidad as (
    -- Miembro ancla: roles en la cima de la jerarquía (sin responsable)
    select
        id,
        rol_id_ld,
        emp_id,
        nombre,
        id_responsable,
        codigo_tipo,
        tipo_aplicacion,
        mapear_a_id,
        fecha_creado,
        fecha_actualizado,
        1 as profundidad
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_rol') }}
    where id_responsable is null

    union all

    -- Miembro recursivo: une con los roles que tienen un responsable
    select
        hijo.id,
        hijo.rol_id_ld,
        hijo.emp_id,
        hijo.nombre,
        hijo.id_responsable,
        hijo.codigo_tipo,
        hijo.tipo_aplicacion,
        hijo.mapear_a_id,
        hijo.fecha_creado,
        hijo.fecha_actualizado,
        padre.profundidad + 1 as profundidad
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_rol') }} as hijo
    inner join rol_con_profundidad as padre on hijo.id_responsable = padre.id
)

select
    id,
    rol_id_ld,
    emp_id,
    nombre,
    id_responsable,
    codigo_tipo,
    tipo_aplicacion,
    mapear_a_id,
    fecha_creado,
    fecha_actualizado,
    profundidad
from rol_con_profundidad
