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
    fe.[AnioMes_Id],
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
    isnull(dh.EmpZona_Id, 'X') as [EmpZona_Id_Hist],
    isnull(dh.EmpDep_Id, 'X') as [EmpDep_Id_Hist],
    isnull(dh.EmpDepMun_Id, 'X') as [EmpDepMun_Id_Hist],
    isnull(dh.EmpDepMunCiu_Id, 'X') as [EmpDepMunCiu_Id_Hist],
    isnull(dh.EmpTipoSucursal_Id, 'X') as [EmpTipoSucursal_Id_Hist],
    isnull(dh.EmpPlan_Id, 'X') as [EmpPlan_Id_Hist],
    isnull(dh.EmpTipoCliente_Id, 'X') as [EmpTipoCliente_Id_Hist],
    isnull(dh.CanalVenta_Id, 0) as [CanalVenta_Id_Hist]
from [dbo].[BI_Kielsa_Hecho_FacturaEncabezado] as fe
left join [dbo].[BI_Kielsa_Hecho_FacturaEncabezado_DimHist] as dh
    on
        fe.Emp_Id = dh.Emp_Id
        and fe.Suc_Id = dh.Suc_Id
        and fe.TipoDoc_Id = dh.TipoDoc_Id
        and fe.Caja_Id = dh.Caja_Id
        and fe.Factura_Fecha = dh.Factura_Fecha
        and fe.Factura_Id = dh.Factura_Id
