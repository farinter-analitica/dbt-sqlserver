{% set unique_key_list = ["Regalia_Id",'Suc_Id','Caja_Id','Emp_Id','Detalle_Id'] -%}

{{ 
    config(
        tags=["automation/periodo_mensual_inicio"],
        materialized="view",
    )
}}
with
-- Tabla principal de reglas
regla_incentivo as (
    select * from (values
        (1, 1, '2020-01-01', '9999-12-31', 15, 0, 1),   -- Honduras
        (2, 5, '2020-01-01', '9999-12-31', 0.75, 1, 0)  -- El Salvador
    ) as t(Regla_Id, Emp_Id, Fecha_Desde, Fecha_Hasta, Valor_Predeterminado, Aplica_Por_Part, Excluir_Marca_Propia)
),
-- Incentivo por casa
regla_incentivo_casa as (
    select * from (values
        (1, 266, 8.0),
        (1, 869, 8.0)
        -- Puedes agregar más casas para otras reglas
    ) as t(Regla_Id, Casa_Id, Incentivo_Casa)
),
-- Incentivo por rol
regla_incentivo_rol as (
    select * from (values
        (2, 'Jefe de Farmacia', 0.2050),
        (2, 'Sub Jefe de Farmacia', 0.10),
        (2, 'Dependiente-Pre venta', 0.64),
        (2, 'Cajero - Vendedor', 0.64),
        (2, 'Cajero', 0.64),
        (2, 'Cajero - Lider', 0.64),
        (2, 'Supervisor de Zona', 0.045),
        (2, 'Gerente de Ventas', 0.01)
    ) as t(Regla_Id, Rol, Part_Rol)
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
        Rol
    from {{ ref('BI_Kielsa_Dim_Empleado') }}
),
regalia_detalle as (
    select *
    from {{ ref('BI_Kielsa_Hecho_Regalia_Detalle') }}
),
-- Determinar la regla aplicable por empresa y fecha
regalia_con_regla as (
    select
        rd.*,
        a.Bit_Marca_Propia,
        a.Casa_Id,
        v.Rol,
        ri.Regla_Id,
        ri.Valor_Predeterminado,
        ri.Aplica_Por_Part,
        ri.Excluir_Marca_Propia
    from regalia_detalle rd
    left join articulos a
        on rd.Emp_Id = a.Emp_Id and rd.Articulo_Padre_Id = a.Articulo_Id
    left join vendedores v
        on rd.Emp_Id = v.Emp_Id and rd.Vendedor_Id = v.Vendedor_Id
    left join regla_incentivo ri
        on rd.Emp_Id = ri.Emp_Id
        and cast(rd.Detalle_Fecha as date) between cast(ri.Fecha_Desde as date) and cast(ri.Fecha_Hasta as date)
),
-- Join con incentivos por casa
regalia_con_casa as (
    select
        rcr.*,
        ric.Incentivo_Casa
    from regalia_con_regla rcr
    left join regla_incentivo_casa ric
        on rcr.Regla_Id = ric.Regla_Id and rcr.Casa_Id = ric.Casa_Id
),
-- Join con incentivos por rol
regalia_con_rol as (
    select
        rcc.*,
        rir.Part_Rol
    from regalia_con_casa rcc
    left join regla_incentivo_rol rir
        on rcc.Regla_Id = rir.Regla_Id and rcc.Rol = rir.Rol
),
roles as (
    select distinct
        Rol_iD,
        Emp_Id,
        Rol_Nombre
    from {{ source('DL_FARINTER', 'DL_Kielsa_Seg_Rol') }} rcr
    inner join regla_incentivo_rol rir
        on rcr.Rol_Nombre = rir.Rol
)

SELECT
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
    
    -- Lógica de incentivo
    case
        when Excluir_Marca_Propia = 1 and coalesce(Bit_Marca_Propia, 0) = 1 then 0
        else 1
    end as Regalia_Aplica_Incentivo,
    case
        -- Si no aplica incentivo, es 0
        when (Excluir_Marca_Propia = 1 and coalesce(Bit_Marca_Propia, 0) = 1) then 0.0
        -- Si aplica por rol, multiplica por Part_Rol
        when Aplica_Por_Part = 1
            then coalesce(Incentivo_Casa, Valor_Predeterminado) * coalesce(Part_Rol, 0.0)
        -- Si NO aplica por rol, solo el incentivo por casa o default
        else coalesce(Incentivo_Casa, Valor_Predeterminado)
    end as Regalia_Valor_Incentivo_Unitario,
    case
        when (Excluir_Marca_Propia = 1 and coalesce(Bit_Marca_Propia, 0) = 1) then 0.0
        when Aplica_Por_Part = 1
            then Cantidad_Padre * (coalesce(Incentivo_Casa, Valor_Predeterminado) * coalesce(Part_Rol, 0.0))
        else Cantidad_Padre * coalesce(Incentivo_Casa, Valor_Predeterminado)
    end as Regalia_Valor_Incentivo_Total,

    -- Audit Fields
    RD.Fecha_Carga,
    RD.Fecha_Actualizado,
    
    -- Concatenated Keys
    RD.EmpSucCajRegDet_Id,
    RD.EmpArt_Id

from regalia_con_rol RD

