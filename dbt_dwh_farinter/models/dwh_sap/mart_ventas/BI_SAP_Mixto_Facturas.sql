{{ 
    config(
		materialized="view",
		tags=["periodo/diario"],
	) 
}}
--DBT DAGSTER 
SELECT 'Actual' AS [Periodo]
	, CAL.[AnioMes_Id]
	, C.[AnioMesCreado_Id]
	, C.[FechaCreado_Id]
	, C.[Pais_Id]
	, C.[Region_Id]
	, C.[Sociedad_Id]
	, P.[Centro_Id]
	, P.[Almacen_Id]
	, P.[Expedicion_Id]
	, C.[Zona_Id]
	, P.[Pedido_Id]
	, P.[Motivo_Id]
	, C.[CreadoPor]
	, C.[Factura_Id]
	, P.Posicion_Id
	, C.Indicador_Anulado
	, P.[Entrega_Id]
	, C.ClaseFactura_Id
	, C.TipoDocumento_Id
	, C.[Referencia_Id]
	, P.[Vendedor_Id]
	, P.OficinaVenta_Id
	, C.[Cliente_Id]
	, C.[GrupoPrecioCliente_Id]
	, C.[GrupoCliente_Id]
	, P.[Articulo_Id]
	, P.[Casa_Id]
	, P.Tipo_Material_Id
	, C.CanalDistribucion_Id
	, P.Grupo_Materiales_Id
	, P.UM_Base
	, C.[Moneda_Id]
	, C.[MonedaDocumento_Id]
	, P.[Sector_Id]
	, C.[Fecha_Id]
	, CAL.Anio_Calendario
	, CAL.Trimestre_Calendario
	, CAL.Mes_Calendario
	, CAL.Dia_de_la_Semana AS Dia_de_la_Semana_ISO
	, CASE --De ISO a Domingo es 1
		WHEN CAL.Dia_de_la_Semana = 7
			THEN 1
		ELSE CAL.Dia_de_la_Semana + 1
		END AS [Dias_Id]
	, CAL.Dia_Calendario
	, DATEPART(HOUR, C.HoraCreado_Id) AS Hora_Id
	, C.HoraCreado_Id
	, P.[Cantidad]
	, P.[Cantidad_SKU]
	, P.[Venta]
	, P.[Descuento]
	, P.[Precio]
	, P.[Costo]
	, P.[Impuesto]
	--select top 1 * from BI_SAP_Dim_Facturas_Actual
FROM {{ source('BI_FARINTER', 'BI_SAP_Hecho_Facturas_Posiciones_Actual') }} P
INNER JOIN {{ source('BI_FARINTER', 'BI_SAP_Dim_Facturas_Actual') }} C
	ON P.Factura_Id = C.Factura_Id --AND P.Sociedad_Id = C.Sociedad_Id
INNER JOIN {{ source('BI_FARINTER', 'BI_Dim_Calendario') }} CAL
	ON CAL.Fecha_Id = C.Fecha_Id
	AND CAL.AnioMes_Id = C.AnioMes_Id
INNER JOIN {{ source('BI_FARINTER', 'BI_SAP_Dim_Cliente') }} CLI
	ON CLI.Cliente_Id = C.Cliente_Id
WHERE C.Fecha_Id >= DATEFROMPARTS(YEAR(GETDATE()), 01, 01)

UNION ALL

SELECT 'Reciente' AS [Periodo]
	, CAL.[AnioMes_Id]
	, C.[AnioMesCreado_Id]
	, C.[FechaCreado_Id]
	, C.[Pais_Id]
	, C.[Region_Id]
	, C.[Sociedad_Id]
	, P.[Centro_Id]
	, P.[Almacen_Id]
	, P.[Expedicion_Id]
	, C.[Zona_Id]
	, P.[Pedido_Id]
	, P.[Motivo_Id]
	, C.[CreadoPor]
	, C.[Factura_Id]
	, P.Posicion_Id
	, C.Indicador_Anulado
	, P.[Entrega_Id]
	, C.ClaseFactura_Id
	, C.TipoDocumento_Id
	, C.[Referencia_Id]
	, P.[Vendedor_Id]
	, P.OficinaVenta_Id
	, C.[Cliente_Id]
	, C.[GrupoPrecioCliente_Id]
	, C.[GrupoCliente_Id]
	, P.[Articulo_Id]
	, P.[Casa_Id]
	, P.Tipo_Material_Id
	, C.CanalDistribucion_Id
	, P.Grupo_Materiales_Id
	, P.UM_Base
	, C.[Moneda_Id]
	, C.[MonedaDocumento_Id]
	, P.[Sector_Id]
	, C.[Fecha_Id]
	, CAL.Anio_Calendario
	, CAL.Trimestre_Calendario
	, CAL.Mes_Calendario
	, CAL.Dia_de_la_Semana AS Dia_de_la_Semana_ISO
	, CASE 
		WHEN CAL.Dia_de_la_Semana = 7
			THEN 1
		ELSE CAL.Dia_de_la_Semana + 1
		END AS [Dias_Id]
	, CAL.Dia_Calendario
	, DATEPART(HOUR, C.HoraCreado_Id) AS Hora_Id
	, C.HoraCreado_Id
	, P.[Cantidad]
	, P.[Cantidad_SKU]
	, P.[Venta]
	, P.[Descuento]
	, P.[Precio]
	, P.[Costo]
	, P.[Impuesto]
FROM {{ source('BI_FARINTER', 'BI_SAP_Hecho_Facturas_Posiciones_Reciente') }} P
INNER JOIN {{ source('BI_FARINTER', 'BI_SAP_Dim_Facturas_Reciente') }} C
	ON P.Factura_Id = C.Factura_Id --AND P.Sociedad_Id = C.Sociedad_Id
INNER JOIN {{ source('BI_FARINTER', 'BI_Dim_Calendario') }} CAL
	ON CAL.Fecha_Id = C.Fecha_Id
	AND CAL.AnioMes_Id = C.AnioMes_Id
INNER JOIN {{ source('BI_FARINTER', 'BI_SAP_Dim_Cliente') }} CLI
	ON CLI.Cliente_Id = C.Cliente_Id
WHERE C.Fecha_Id >= DATEFROMPARTS(YEAR(GETDATE()) - 5, 01, 01)
	AND C.Fecha_Id < DATEFROMPARTS(YEAR(GETDATE()), 01, 01)