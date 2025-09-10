{% set unique_key_list = ["Factura_Id","Suc_Id","Emp_Id","TipoDoc_Id","Caja_Id","Factura_Fecha"] %}

{{ 
    config(
        tags=["automation/periodo_mensual_inicio", "periodo_unico/si"],
        materialized="view",
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ]
    ) 
}}

select
    fe.[Factura_Fecha],
    fe.[Emp_Id],
    fe.[Suc_Id],
    fe.[Bodega_Id],
    fe.[Caja_Id],
    fe.[TipoDoc_Id],
    fe.[Factura_Id],
    fe.[SubDoc_Id],
    fe.[Consecutivo_Factura],
    fe.[Monedero_Id],
    fe.[Cliente_Id],
    fe.[Vendedor_Id],
    fe.[Factura_Clave_Tributaria],
    fe.[Factura_Estado],
    fe.[Factura_Origen],
    fe.[Same_Id],
    fe.[Fecha_Actualizado],
    fe.[EmpSucDocCajFac_Id],
    fe.[EmpMon_Id],
    dh.EmpZona_Id as [EmpZona_Id_Hist],
    dh.EmpDep_Id as [EmpDep_Id_Hist],
    dh.EmpDepMun_Id as [EmpDepMun_Id_Hist],
    dh.EmpDepMunCiu_Id as [EmpDepMunCiu_Id_Hist],
    dh.EmpTipoSucursal_Id as [EmpTipoSucursal_Id_Hist],
    dh.EmpPlan_Id as [EmpPlan_Id_Hist],
    dh.EmpTipoCliente_Id as [EmpTipoCliente_Id_Hist],
    dh.CanalVenta_Id as [CanalVenta_Id_Hist]
from [dbo].[BI_Kielsa_Hecho_FacturaEncabezado] as fe
left join [dbo].[BI_Kielsa_Hecho_FacturaEncabezado_DimHist] as dh
    on
        fe.Emp_Id = dh.Emp_Id
        and fe.Suc_Id = dh.Suc_Id
        and fe.TipoDoc_Id = dh.TipoDoc_Id
        and fe.Caja_Id = dh.Caja_Id
        and fe.Factura_Fecha = dh.Factura_Fecha
        and fe.Factura_Id = dh.Factura_Id
