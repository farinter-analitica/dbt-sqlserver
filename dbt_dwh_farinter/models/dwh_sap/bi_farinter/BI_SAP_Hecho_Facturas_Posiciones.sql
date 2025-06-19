{% set unique_key_list = ["Almacen_Id"] %}
{{ 
    config(
		tags=["periodo/diario"],
		materialized="view",
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ]
	) 
}}

--Solo editable en dbt

SELECT AnioMesCreado_Id, FechaCreado_Id, HoraCreado_Id, Sociedad_Id, Centro_Id, Almacen_Id, Expedicion_Id, Pedido_Id, Motivo_Id, Factura_Id, Posicion_Id, Posicion_Tipo, Entrega_Id, Vendedor_Id, [OficinaVenta_Id], CentroCoste_Id, GrupoComisiones_Id, Articulo_Id, Lote_Id, Casa_Id, Tipo_Material_Id, Grupo_Materiales_Id, UM_Venta, UM_Base, Sector_Id, Cantidad, Cantidad_SKU, Venta, Descuento, Precio, Costo, Impuesto, Peso_Bruto, UM_Peso, Volumen, UM_Volumen 
FROM BI_SAP_Hecho_Facturas_Posiciones_Actual -- {{ source('BI_FARINTER','BI_SAP_Hecho_Facturas_Posiciones_Actual') }}
	WHERE AnioMesCreado_Id>=YEAR(GETDATE())*100+01
UNION ALL
SELECT AnioMesCreado_Id, FechaCreado_Id, HoraCreado_Id, Sociedad_Id, Centro_Id, Almacen_Id, Expedicion_Id, Pedido_Id, Motivo_Id, Factura_Id, Posicion_Id, Posicion_Tipo, Entrega_Id, Vendedor_Id, [OficinaVenta_Id], CentroCoste_Id, GrupoComisiones_Id, Articulo_Id, Lote_Id, Casa_Id, Tipo_Material_Id, Grupo_Materiales_Id, UM_Venta, UM_Base, Sector_Id, Cantidad, Cantidad_SKU, Venta, Descuento, Precio, Costo, Impuesto, Peso_Bruto, UM_Peso, Volumen, UM_Volumen 
FROM BI_SAP_Hecho_Facturas_Posiciones_Reciente -- {{ source('BI_FARINTER','BI_SAP_Hecho_Facturas_Posiciones_Reciente') }}
	WHERE AnioMesCreado_Id BETWEEN (YEAR(GETDATE())-5)*100+01	AND (YEAR(GETDATE())-1)*100+12

--SELECT TOP 1000 * FROM [BI_SAP_Hecho_Facturas_Posiciones]
--SELECT DISTINCT AnioMesCreado_Id FROM [BI_SAP_Hecho_Facturas_Posiciones]