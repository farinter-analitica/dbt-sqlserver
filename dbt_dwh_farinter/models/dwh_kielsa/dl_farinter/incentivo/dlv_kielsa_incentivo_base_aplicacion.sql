{{ 
    config(
		tags=["automation/periodo_mensual_inicio", "automation_only"],
        materialized="view",
    )
}}
-- Vista base para aplicación de incentivos
-- Genera los registros base que permiten unir las métricas de incentivos
-- con las reglas configuradas según rol, usuario/vendedor y sucursal(es)

with reglas_rol as (
    select
        r.id as regla_id,
        r.emp_id,
        r.fecha_desde,
        r.fecha_hasta,
        coalesce(rr.part_regalia, 1.0) as part_regalia,
        coalesce(rr.part_comision, 1.0) as part_comision,
        coalesce(rr.valor_por_receta_seguro, 0.0) as valor_por_receta_seguro,
        coalesce(rr.rol_id_ld, kr.rol_id_ld) as rol_id,
        coalesce(rr.rol_nombre, kr.nombre) as rol_nombre,
        coalesce(rr.codigo_tipo, 'vendedor_id') as codigo_tipo,
        coalesce(rr.tipo_aplicacion, 'individual_por_codigo') as tipo_aplicacion
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla') }} as r
    left join {{ ref('dlv_kielsa_incentivo_regla_rol') }} as rr
        on r.id = rr.regla_id
    left join {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_rol') }} as kr
        on rr.rol_id_ld is null and r.emp_id = kr.emp_id
    where
        r.fecha_desde <= getdate()
        and (r.fecha_hasta is null or r.fecha_hasta >= getdate())
),

aplicacion_base as (
    select
        rr.regla_id,
        rr.emp_id,
        rr.rol_id,
        rr.rol_nombre,
        rr.codigo_tipo,
        rr.fecha_desde,
        rr.fecha_hasta,
        rr.part_regalia,
        rr.part_comision,
        rr.valor_por_receta_seguro,
        rr.tipo_aplicacion,
        -- Usuario
        coalesce(usuc.Usuario_Nombre, u.Usuario_Nombre, vsuc.Vendedor_Nombre, vi.Vendedor_Nombre) as Usuario_Nombre,
        -- Sucursal
        coalesce(usuc.Suc_Id, u.Sucursal_Id_Asignado, vsuc.Suc_Id, vi.Sucursal_Id_Asignado) as Suc_Id,
        -- Usuario_Id
        coalesce(usuc.Usuario_Id, u.Usuario_Id) as Usuario_Id,
        -- Vendedor_Id
        coalesce(vsuc.Vendedor_Id, vi.Vendedor_Id) as Vendedor_Id
    from reglas_rol as rr
    left join {{ ref('BI_Kielsa_Dim_UsuarioSucursal') }} as usuc
        on
            rr.emp_id = usuc.Emp_Id
            and rr.rol_id = usuc.Rol_Id
            and usuc.Bit_Activo = 1
            and rr.codigo_tipo = 'usuario_id'
            and rr.tipo_aplicacion = 'multiple_sucursal'
    left join {{ ref('BI_Kielsa_Dim_Usuario') }} as u
        on
            rr.emp_id = u.Emp_Id
            and u.Bit_Activo = 1
            and rr.codigo_tipo = 'usuario_id'
            and rr.rol_id = u.Rol_Id
            and rr.tipo_aplicacion in ('unica_sucursal', 'individual_por_codigo')
    left join {{ ref('BI_Kielsa_Dim_Vendedor') }} as vi
        on
            rr.emp_id = vi.Emp_Id and rr.rol_id = vi.Rol_Id
            and vi.Bit_Activo = 1
            and rr.codigo_tipo = 'vendedor_id'
            and rr.tipo_aplicacion in ('unica_sucursal', 'individual_por_codigo')
    left join {{ ref('BI_Kielsa_Dim_VendedorSucursal') }} as vsuc
        on
            rr.emp_id = vsuc.Emp_Id
            and rr.codigo_tipo = 'vendedor_id'
            and vsuc.Bit_Activo = 1 and rr.rol_id = vsuc.rol_id and rr.tipo_aplicacion = 'multiple_sucursal'
)

select
    regla_id,
    emp_id,
    rol_id,
    rol_nombre,
    codigo_tipo,
    fecha_desde,
    fecha_hasta,
    part_regalia,
    part_comision,
    valor_por_receta_seguro,
    tipo_aplicacion,
    Suc_Id,
    Usuario_Id,
    Vendedor_Id,
    Usuario_Nombre,
    -- Campos adicionales útiles para joins
    {{ dwh_farinter_concat_key_columns(columns=["emp_id","regla_id"], input_length=99) }} as EmpRegla_Id,
    {{ dwh_farinter_concat_key_columns(columns=["emp_id","rol_id"], input_length=99) }} as EmpRol_Id,
    {{ dwh_farinter_concat_key_columns(columns=["emp_id","Usuario_Id"], input_length=99) }} as EmpUsuario_Id,
    {{ dwh_farinter_concat_key_columns(columns=["emp_id","Vendedor_Id"], input_length=99) }} as EmpVendedor_Id,
    case
        when Suc_Id is not null then {{ dwh_farinter_concat_key_columns(columns=["emp_id","Suc_Id"], input_length=99) }}
    end as EmpSuc_Id,
    case
        when Usuario_Id is not null and Suc_Id is not null then {{ dwh_farinter_concat_key_columns(columns=["emp_id","Suc_Id","Usuario_Id"], input_length=99) }}
    end as EmpSucUsuario_Id,
    case
        when Vendedor_Id is not null and Suc_Id is not null then {{ dwh_farinter_concat_key_columns(columns=["emp_id","Suc_Id","Vendedor_Id"], input_length=99) }}
    end as EmpSucVendedor_Id
from aplicacion_base
where (Usuario_Id is not null or Vendedor_Id is not null)
