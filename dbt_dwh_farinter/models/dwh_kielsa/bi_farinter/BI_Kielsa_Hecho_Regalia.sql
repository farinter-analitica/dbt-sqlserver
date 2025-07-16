{% set unique_key_list = ["Regalia_Id",'Suc_Id','Caja_Id','Emp_Id','Detalle_Id'] -%}

{{ 
    config(
        tags=["automation/periodo_mensual_inicio"],
        materialized="view",
    )
}}


with
regla as (
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
        r.rol_id,
        rr.part_regalia,
        rr.aplicar_por_sucursal
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla_rol') }} as rr
    inner join {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_rol') }} as r
        on rr.rol_id = r.id
),

articulos as (
    select
        Emp_Id,
        Articulo_Id,
        Bit_Marca_Propia,
        Casa_Id
    from {{ ref('BI_Kielsa_Dim_Articulo') }}
),

vendedores as (
    select
        Emp_Id,
        Vendedor_Id,
        Rol_Id
    from {{ ref('BI_Kielsa_Dim_Empleado') }}
),

regalia_detalle as (
    select *
    from {{ ref('BI_Kielsa_Hecho_Regalia_Detalle') }}
    --where Detalle_Fecha >= '20250701'
),

regalia_con_regla as (
    select
        rd.*,
        a.Bit_Marca_Propia,
        a.Casa_Id,
        v.Rol_Id,
        r.regla_id,
        r.emp_id as regla_emp_id,
        r.fecha_desde,
        r.fecha_hasta,
        rreg.valor_predeterminado,
        rreg.aplica_por_part,
        rreg.excluir_marca_propia
    from regalia_detalle as rd
    left join articulos as a
        on rd.Emp_Id = a.Emp_Id and rd.Articulo_Padre_Id = a.Articulo_Id
    left join vendedores as v
        on rd.Emp_Id = v.Emp_Id and rd.Vendedor_Id = v.Vendedor_Id
    left join regla as r
        on rd.Emp_Id = r.emp_id and (rd.Detalle_Fecha) between (r.fecha_desde) and (r.fecha_hasta)
    left join regla_regalia as rreg
        on r.regla_id = rreg.regla_id
),

regalia_con_casa as (
    select
        rcr.*,
        rcasa.valor_regalia
    from regalia_con_regla as rcr
    left join regla_casa as rcasa
        on rcr.regla_id = rcasa.regla_id and rcr.Casa_Id = rcasa.casa_id
),

regalia_con_rol as (
    select
        rcc.*,
        rrol.part_regalia
    from regalia_con_casa as rcc
    left join regla_rol as rrol
        on rcc.regla_id = rrol.regla_id and rcc.Rol_Id = rrol.rol_id
    --where regalia_id = 118 AND Detalle_Fecha >='20250701' and emp_id = 5
)

select
    -- Primary Keys and Foreign Keys
    RD.Regalia_Id,
    RD.Emp_Id,
    RD.Suc_Id,
    RD.Caja_Id,
    RD.Detalle_Id,
    RD.Articulo_Id,
    RD.Articulo_Padre_Id,
    RD.Bodega_Id,
    RD.Consecutivo,

    -- Encabezado
    RD.Cliente_Id,
    RD.Vendedor_Id,
    RD.Identidad_Limpia,
    RD.EmpSucCajReg_Id,
    RD.EmpSuc_Id,
    RD.EmpCli_Id,
    RD.EmpVen_Id,
    RD.EmpMon_Id,
    RD.Tipo_Origen,
    RD.Operacion_Id,

    -- Date and Time
    RD.Detalle_Momento,
    RD.Detalle_Fecha,
    RD.Detalle_Hora,

    -- Transaction Details
    RD.Cantidad_Original,
    RD.Cantidad_Padre,
    RD.Valor_Costo_Unitario,
    RD.Precio_Unitario,
    RD.Valor_Costo_Total,
    RD.Valor_Total,

    -- Product Information
    RD.Porcentaje_Impuesto,
    RD.Valor_Impuesto,

    -- Audit Fields
    RD.Fecha_Carga,
    RD.Fecha_Actualizado,
    RD.EmpSucCajRegDet_Id,
    RD.EmpArt_Id,

    -- Lógica de incentivo
    case
        when RD.excluir_marca_propia = 1 and coalesce(RD.Bit_Marca_Propia, 0) = 1 then 0
        else 1
    end as Regalia_Aplica_Incentivo,
    cast(case
        -- Si no aplica incentivo, es 0
        when (RD.excluir_marca_propia = 1 and coalesce(RD.Bit_Marca_Propia, 0) = 1) then 0.0
        -- Si aplica por rol, multiplica por Part_Rol
        when RD.aplica_por_part = 1
            then coalesce(RD.valor_regalia, RD.valor_predeterminado) * coalesce(RD.part_regalia, 0.0)
        -- Si NO aplica por rol, solo el incentivo por casa o default
        else coalesce(RD.valor_regalia, RD.valor_predeterminado)
    end as decimal(18, 6)) as Regalia_Valor_Incentivo_Unitario,
    cast(case
        when (RD.excluir_marca_propia = 1 and coalesce(RD.Bit_Marca_Propia, 0) = 1) then 0.0
        when RD.aplica_por_part = 1
            then RD.Cantidad_Padre * (coalesce(RD.valor_regalia, RD.valor_predeterminado) * coalesce(RD.part_regalia, 0.0))
        else RD.Cantidad_Padre * coalesce(RD.valor_regalia, RD.valor_predeterminado)
    end as decimal(18, 6)) as Regalia_Valor_Incentivo_Total

from regalia_con_rol as RD
--where regalia_id = 118 AND RD.Detalle_Fecha >='20250701'
