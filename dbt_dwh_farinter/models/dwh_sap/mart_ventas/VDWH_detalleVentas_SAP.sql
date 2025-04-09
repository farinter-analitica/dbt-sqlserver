{{ 
    config(
		materialized="view",
		tags=["periodo/diario"],
	) 
}}
-- Solo editable en DBT DAGSTER

SELECT -- TOP (1000) 
    [Organizacion_Id],
    [Organizacion_Nombre],
    [Centro_Id],
    [Centro_Nombre],
    [Zona_Id],
    [Zona_Nombre],
    [Vendedor_Id],
    [Vendedor_Nombre],
    [TipoVendedor],
    [TipoCliente_Id],
    [TipoCliente_Nombre],
    [Cliente_Id],
    [Cliente_Nombre],
    [Casa_Id],
    [Casa_Nombre],
    [Articulo_Id_Original],
    [Articulo_Id],
    [Articulo_Nombre],
    [Marca],
    [Marca_Nombre],
    [ClasificacionArt],
    [TipoFactura_Id],
    [TipoFactura_Nombre],
    [Factura_Id],
    [TipoArt2_Id],
    [TipoArt2_Nombre],
    [CanalDistribucion_Id],
    [CanalDist_Nombre],
    [Fecha_Id],
    [Dia_Id],
    [Anio_Calendario],
    [AnioMesCreado_Id],
    [Cantidad],
    [Venta_Bruta],
    [Descuento],
    [Costo]
  FROM {{ ref('BI_SAP_Agr_DetalleVentas_Clasico') }}
--where [Fecha_Id] < '20240101'
