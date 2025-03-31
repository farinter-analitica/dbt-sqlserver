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

SELECT AnioMes_Id,AnioMesCreado_Id ,Pais_Id ,[Pais_ISO3] ,Region_Id 
					,Sociedad_Id ,Zona_Id ,Factura_Id ,TipoFactura_Id ,[ClaseFactura_Id] 
					,[TipoDocumento_Id] ,[DocumentoContable_Id] ,Referencia_Id ,Cliente_Id ,Solicitante_Id 
					,GrupoPrecioCliente_Id ,GrupoCliente_Id 
					,CanalDistribucion_Id ,MonedaDocumento_Id 
					,TasaCambioDocumento ,[EsquemaPrecios_Id] ,[CondicionDocumento_Id] ,[TipoListaPrecio_Id] 
					,[GrupoPrecioClientes_Id] ,[CondicionesPago_Id] 
					,[AreaControlCredito_Id] ,Moneda_Id ,SectorDocumento_Id ,FechaCreado_Id 
					,HoraCreado_Id ,[CreadoPor] ,Fecha_Id ,Cantidad ,Cantidad_SKU 
					,Venta ,Descuento ,Precio_Promedio ,Costo ,Impuesto ,Posiciones, Indicador_Anulado

FROM BI_SAP_Dim_Facturas_Actual -- {{ source('BI_FARINTER','BI_SAP_Dim_Facturas_Actual') }}
	WHERE AnioMesCreado_Id>=(YEAR(GETDATE())-1)*100+01 AND
		Fecha_Id>=DATEFROMPARTS(YEAR(GETDATE()),01,01)
UNION ALL
SELECT AnioMes_Id,AnioMesCreado_Id ,Pais_Id ,[Pais_ISO3] ,Region_Id 
					,Sociedad_Id ,Zona_Id ,Factura_Id ,TipoFactura_Id ,[ClaseFactura_Id] 
					,[TipoDocumento_Id] ,[DocumentoContable_Id] ,Referencia_Id ,Cliente_Id ,Solicitante_Id 
					,GrupoPrecioCliente_Id ,GrupoCliente_Id 
					,CanalDistribucion_Id ,MonedaDocumento_Id 
					,TasaCambioDocumento ,[EsquemaPrecios_Id] ,[CondicionDocumento_Id] ,[TipoListaPrecio_Id] 
					,[GrupoPrecioClientes_Id] ,[CondicionesPago_Id] 
					,[AreaControlCredito_Id] ,Moneda_Id ,SectorDocumento_Id ,FechaCreado_Id 
					,HoraCreado_Id ,[CreadoPor] ,Fecha_Id ,Cantidad ,Cantidad_SKU 
					,Venta ,Descuento ,Precio_Promedio ,Costo ,Impuesto ,Posiciones, Indicador_Anulado
FROM BI_SAP_Dim_Facturas_Reciente -- {{ source('BI_FARINTER','BI_SAP_Dim_Facturas_Reciente') }}
	WHERE AnioMesCreado_Id BETWEEN (YEAR(GETDATE())-6)*100+01	AND (YEAR(GETDATE())-0)*100+12	AND  
		Fecha_Id BETWEEN DATEFROMPARTS(YEAR(GETDATE())-5,01,01) AND DATEFROMPARTS(YEAR(GETDATE())-1,12,31)

--SELECT TOP 1000 * FROM [BI_SAP_Dim_Facturas]
--SELECT DISTINCT AnioMes_Id FROM [BI_SAP_Dim_Facturas]