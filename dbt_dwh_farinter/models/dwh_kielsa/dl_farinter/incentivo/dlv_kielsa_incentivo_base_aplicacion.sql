{% set unique_key_list = ["Usuario_Id", "Vendedor_Id", "Suc_Id", "Emp_Id", "regla_id"] %}

{{ 
    config(
		as_columnstore=true,
		tags=["automation/periodo_diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
        full_refresh = false,
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['fecha_hasta', 'fecha_desde', 'emp_id', 'suc_id', 'vendedor_id', 'usuario_id']) }}"
        ]
	) 
}}

/*
-- Base para aplicación de incentivos
NO realizar full refresh, fecha_valido indica la validez actual de registros por asignación de sucursal.
En el caso hipotetico de que cambie la asignación, se sabrá por la fecha de validez cual es la actual,
sin embargo, si se realiza un full refresh, se perderán las asignaciones pasadas y su fecha final,
claro, esto solo funciona para una asignación y se sobreescribirá la fecha anterior si se vuelve a asignar la misma,
esto significa que es importante tampoco hacer full refresh en el modelo siguiente que controla los incentivos.
-- Genera los registros base que permiten unir las métricas de incentivos
-- con las reglas configuradas según rol, usuario/vendedor y sucursal(es)
*/
with reglas_rol as (
    select
        r.fecha_desde,
        r.fecha_hasta,
        isnull(r.id, 0) as regla_id,
        isnull(r.emp_id, 0) as emp_id,
        coalesce(rr.part_regalia, 1.0) as part_regalia,
        coalesce(rr.part_comision, 1.0) as part_comision,
        coalesce(rr.valor_por_receta_seguro, 0.0) as valor_por_receta_seguro,
        coalesce(rr.rol_id_ld, kr.rol_id_ld) as rol_id,
        coalesce(rr.rol_nombre, kr.nombre) as rol_nombre,
        coalesce(rr.codigo_tipo, kr.codigo_tipo) as codigo_tipo,
        coalesce(rr.tipo_aplicacion, kr.tipo_aplicacion) as tipo_aplicacion
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla') }} as r
    left join {{ ref('dlv_kielsa_incentivo_regla_rol') }} as rr
        on r.id = rr.regla_id
    left join {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_rol') }} as kr
        on rr.rol_id_ld is null and r.emp_id = kr.emp_id
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
        coalesce(usuc.Suc_Id, u.Sucursal_Id_Asignado, vsuc.Suc_Id, vi.Sucursal_Id_Asignado, 0) as Suc_Id,
        -- Usuario_Id para el caso
        coalesce(usuc.Usuario_Id, u.Usuario_Id, 0) as Usuario_Id,
        -- Vendedor_Id para el caso
        coalesce(vsuc.Vendedor_Id, vi.Vendedor_Id, 0) as Vendedor_Id
    from reglas_rol as rr
    left join {{ ref('BI_Kielsa_Dim_UsuarioSucursal') }} as usuc
        on
            rr.emp_id = usuc.Emp_Id
            and rr.rol_id = usuc.Rol_Id_Mapeado
            and usuc.Bit_Activo = 1
            and rr.codigo_tipo = 'usuario_id'
            and rr.tipo_aplicacion = 'multiple_sucursal'
            --and usuc.Usuario_Id > 0
    left join {{ ref('BI_Kielsa_Dim_Usuario') }} as u
        on
            rr.emp_id = u.Emp_Id
            and u.Bit_Activo = 1
            and rr.codigo_tipo = 'usuario_id'
            and rr.rol_id = u.Rol_Id_Mapeado
            and rr.tipo_aplicacion in ('unica_sucursal', 'individual_por_codigo')
            --and u.Usuario_Id > 0
    left join {{ ref('BI_Kielsa_Dim_Vendedor') }} as vi
        on
            rr.emp_id = vi.Emp_Id
            and rr.rol_id = vi.Rol_Id_Mapeado
            and vi.Bit_Activo = 1
            and rr.codigo_tipo = 'vendedor_id'
            and rr.tipo_aplicacion in ('unica_sucursal', 'individual_por_codigo')
            --and vi.Vendedor_Id > 0
    left join {{ ref('BI_Kielsa_Dim_VendedorSucursal') }} as vsuc
        on
            rr.emp_id = vsuc.Emp_Id
            and rr.codigo_tipo = 'vendedor_id'
            and vsuc.Bit_Activo = 1
            and rr.rol_id = vsuc.Rol_Id_Mapeado
            and rr.tipo_aplicacion = 'multiple_sucursal'
            --and vsuc.Vendedor_Id > 0
    where
        coalesce(vsuc.Vendedor_Id, vi.Vendedor_Id) is not null
        or coalesce(usuc.Usuario_Id, u.Usuario_Id) is not null
)

select
    isnull(regla_id, 0) as regla_id,
    isnull(emp_id, 0) as emp_id,
    rol_id,
    rol_nombre,
    codigo_tipo,
    fecha_desde,
    fecha_hasta,
    part_regalia,
    part_comision,
    valor_por_receta_seguro,
    tipo_aplicacion,
    isnull(Suc_Id, 0) as Suc_Id,
    isnull(Usuario_Id, 0) as Usuario_Id,
    isnull(Vendedor_Id, 0) as Vendedor_Id,
    Usuario_Nombre,
    cast(getdate() as date) as Fecha_Validado, --Esta fecha indica el registro es válido hasta esta fecha
    -- Campos adicionales útiles para modelos
    {{ dwh_farinter_concat_key_columns(columns=["emp_id","regla_id"], input_length=40) }} as EmpRegla_Id,
    {{ dwh_farinter_concat_key_columns(columns=["emp_id","rol_id"], input_length=40) }} as EmpRol_Id,
    {{ dwh_farinter_concat_key_columns(columns=["emp_id","Usuario_Id"], input_length=40) }} as EmpUsu_Id,
    {{ dwh_farinter_concat_key_columns(columns=["emp_id","Vendedor_Id"], input_length=40) }} as EmpVen_Id,
    case
        when Suc_Id is not null then {{ dwh_farinter_concat_key_columns(columns=["emp_id","Suc_Id"], input_length=40) }}
    end as EmpSuc_Id,
    case
        when Usuario_Id is not null and Suc_Id is not null then {{ dwh_farinter_concat_key_columns(columns=["emp_id","Suc_Id","Usuario_Id"], input_length=99) }}
    end as EmpSucUsu_Id,
    case
        when Vendedor_Id is not null and Suc_Id is not null then {{ dwh_farinter_concat_key_columns(columns=["emp_id","Suc_Id","Vendedor_Id"], input_length=99) }}
    end as EmpSucVen_Id
from aplicacion_base
