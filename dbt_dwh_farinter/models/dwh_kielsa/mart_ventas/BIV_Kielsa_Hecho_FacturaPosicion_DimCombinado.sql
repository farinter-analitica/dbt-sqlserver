{% set unique_key_list = ["Factura_Id","Suc_Id","Emp_Id","TipoDoc_Id","Caja_Id","Factura_Fecha","Articulo_Id"] %}

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
    fp.[Factura_Fecha],
    fp.[Hora_Id],
    fp.[Emp_Id],
    fp.[Suc_Id],
    fp.[Bodega_Id],
    fp.[TipoDoc_Id],
    fp.[Articulo_Id],
    fp.[Detalle_Cantidad],
    fp.[Cantidad_Padre],
    fp.[Valor_Bruto],
    fp.[Valor_Neto],
    fp.[Valor_Utilidad],
    fp.[Valor_Costo],
    fp.[Valor_Descuento],
    fp.[Valor_Impuesto],
    fp.[Valor_Descuento_Financiero],
    fp.[Valor_Acum_Monedero],
    fp.[Valor_Descuento_Cupon],
    fp.[Valor_Descuento_Monedero],
    fp.[SubDoc_Id],
    fp.[Cliente_Id],
    fp.[Vendedor_Id],
    fp.[Factura_Estado],
    fp.[Factura_Origen],
    fp.[CanalVenta_Id],
    fp.[EmpMon_Id],
    fp.[Same_Id],
    fp.[TipoUtilidad_Id],
    fp.[Descuento_Proveedor],
    fp.[EmpSucDocCajFac_Id],
    dh.EmpCasa_Id as [EmpCasa_Id_Hist],
    dh.EmpMarca1_Id as [EmpMarca1_Id_Hist],
    dh.EmpCategoriaArt_Id as [EmpCategoriaArt_Id_Hist],
    dh.EmpDeptoArt_Id as [EmpDeptoArt_Id_Hist],
    dh.EmpCatSubCategoria1Art_Id as [EmpCatSubCategoria1Art_Id_Hist],
    dh.EmpCatSubCategoria1_2Art_Id as [EmpCatSubCategoria1_2Art_Id_Hist],
    dh.EmpCatSubCategoria1_2_3Art_Id as [EmpCatSubCategoria1_2_3Art_Id_Hist],
    dh.EmpCatSubCategoria1_2_3_4Art_Id as [EmpCatSubCategoria1_2_3_4Art_Id_Hist],
    dh.EmpProv_Id as [EmpProv_Id_Hist],
    deh.EmpZona_Id as [EmpZona_Id_Hist],
    deh.EmpDep_Id as [EmpDep_Id_Hist],
    deh.EmpDepMun_Id as [EmpDepMun_Id_Hist],
    deh.EmpDepMunCiu_Id as [EmpDepMunCiu_Id_Hist],
    deh.EmpTipoSucursal_Id as [EmpTipoSucursal_Id_Hist],
    deh.EmpPlan_Id as [EmpPlan_Id_Hist],
    deh.EmpTipoCliente_Id as [EmpTipoCliente_Id_Hist],
    deh.CanalVenta_Id as [CanalVenta_Id_Hist]
from [dbo].[BI_Kielsa_Hecho_FacturaPosicion] as fp
left join [dbo].[BI_Kielsa_Hecho_FacturaPosicion_DimHist] as dh
    on
        fp.Emp_Id = dh.Emp_Id
        and fp.Suc_Id = dh.Suc_Id
        and fp.TipoDoc_Id = dh.TipoDoc_Id
        and fp.Caja_Id = dh.Caja_Id
        and fp.Factura_Fecha = dh.Factura_Fecha
        and fp.Factura_Id = dh.Factura_Id
        and fp.Articulo_Id = dh.Articulo_Id
left join [dbo].[BI_Kielsa_Hecho_FacturaEncabezado_DimHist] as deh
    on
        fp.Emp_Id = deh.Emp_Id
        and fp.Suc_Id = deh.Suc_Id
        and fp.TipoDoc_Id = deh.TipoDoc_Id
        and fp.Caja_Id = deh.Caja_Id
        and fp.Factura_Fecha = deh.Factura_Fecha
        and fp.Factura_Id = deh.Factura_Id
