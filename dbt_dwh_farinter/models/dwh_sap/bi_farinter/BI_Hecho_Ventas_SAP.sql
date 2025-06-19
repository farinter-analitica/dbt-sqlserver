{% set unique_key_list = ["Factura_Id", "Posicion_Id"] %}
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
SELECT  'Actual' as [Periodo]
	,1 AS [Consecutivo]
	, CAL.[AnioMes_Id]
	, C.[AnioMesCreado_Id]
      ,C.[Pais_Id]
      ,C.[Region_Id]
      ,C.[Sociedad_Id]
      ,P.[Centro_Id]
      ,P.[Almacen_Id]
      ,P.[Expedicion_Id]
      ,C.[Zona_Id]
      ,P.[Pedido_Id]
      ,P.[Motivo_Id]
      ,C.[Factura_Id]
	  ,P.Posicion_Id
      ,P.[Entrega_Id]
      ,C.ClaseFactura_Id [TipoFactura_Id]
	  ,C.Indicador_Anulado
	  ,C.TipoDocumento_Id
      ,C.[Referencia_Id]
      ,P.[Vendedor_Id]
	  ,P.OficinaVenta_Id
      ,C.[Cliente_Id]
      ,C.[GrupoPrecioCliente_Id] AS GrupoCliente_Id
      ,C.[GrupoCliente_Id] AS TipoCliente_Id
      ,P.[Articulo_Id]
      ,P.[Casa_Id]
      ,P.Tipo_Material_Id AS [TipoArt1_Id]
      ,C.CanalDistribucion_Id AS [TipoArt2_Id]
      ,P.Grupo_Materiales_Id AS [TipoArt3_Id]
      ,P.UM_Base AS [Unidad_Id]
      ,C.[Moneda_Id]
      ,P.[Sector_Id]
      ,C.[Fecha_Id]
      ,CAL.Anio_Calendario AS [Anio_Id]
      ,CAL.Trimestre_Calendario AS [Trimestre_Id]
      ,CAL.Mes_Calendario AS [Mes_Id]
      ,CAL.Dia_de_la_Semana AS [Dias_Id_ISO]
      ,CASE WHEN CAL.Dia_de_la_Semana =7 THEN 1 ELSE CAL.Dia_de_la_Semana+1 END AS [Dias_Id]
      ,CAL.Dia_Calendario [Dia_Id]
      ,datepart(HOUR,C.HoraCreado_Id) as [Hora_Id]
      ,C.HoraCreado_Id
      ,P.[Cantidad]
      ,P.[Cantidad_SKU]
      ,P.[Venta]
      ,P.[Descuento]
      ,P.[Precio]
      ,P.[Costo]
      ,P.[Impuesto]
  FROM [dbo].[BI_SAP_Hecho_Facturas_Posiciones_Actual] P --{{ source('BI_FARINTER','BI_SAP_Hecho_Facturas_Posiciones_Actual') }}
	INNER JOIN [dbo].[BI_SAP_Dim_Facturas_Actual] C --{{ source('BI_FARINTER','BI_SAP_Dim_Facturas_Actual') }}
	on P.Factura_Id = C.Factura_Id --AND P.Sociedad_Id = C.Sociedad_Id
	INNER JOIN [dbo].[BI_Dim_Calendario] CAL --{{ source('BI_FARINTER','BI_Dim_Calendario') }}
	ON CAL.Fecha_Id = C.Fecha_Id 
	WHERE C.Fecha_Id>=DATEFROMPARTS(YEAR(GETDATE()),01,01)
UNION ALL
SELECT  'Reciente' as [Periodo]
	,1 AS [Consecutivo]
	, CAL.[AnioMes_Id]
	, C.[AnioMesCreado_Id]
      ,C.[Pais_Id]
      ,C.[Region_Id]
      ,C.[Sociedad_Id]
      ,P.[Centro_Id]
      ,P.[Almacen_Id]
      ,P.[Expedicion_Id]
      ,C.[Zona_Id]
      ,P.[Pedido_Id]
      ,P.[Motivo_Id]
      ,C.[Factura_Id]
	  ,P.Posicion_Id
      ,P.[Entrega_Id]
      ,C.ClaseFactura_Id [TipoFactura_Id]
	  ,C.Indicador_Anulado
	  ,C.TipoDocumento_Id
      ,C.[Referencia_Id]
      ,P.[Vendedor_Id]
	  ,P.OficinaVenta_Id
      ,C.[Cliente_Id]
      ,C.[GrupoPrecioCliente_Id] AS GrupoCliente_Id
      ,C.[GrupoCliente_Id]  AS TipoCliente_Id
      ,P.[Articulo_Id]
      ,P.[Casa_Id]
      ,P.Tipo_Material_Id AS [TipoArt1_Id]
      ,C.CanalDistribucion_Id AS [TipoArt2_Id]
      ,P.Grupo_Materiales_Id AS [TipoArt3_Id]
      ,P.UM_Base AS [Unidad_Id]
      ,C.[Moneda_Id]
      ,P.[Sector_Id]
      ,C.[Fecha_Id]
      ,CAL.Anio_Calendario AS [Anio_Id]
      ,CAL.Trimestre_Calendario AS [Trimestre_Id]
      ,CAL.Mes_Calendario AS [Mes_Id]
      ,CAL.Dia_de_la_Semana AS [Dias_Id_ISO]
      ,CASE WHEN CAL.Dia_de_la_Semana =7 THEN 1 ELSE CAL.Dia_de_la_Semana+1 END AS [Dias_Id]
      ,CAL.Dia_Calendario [Dia_Id]
      ,datepart(HOUR,C.HoraCreado_Id) as [Hora_Id]
      ,C.HoraCreado_Id
      ,P.[Cantidad]
      ,P.[Cantidad_SKU]
      ,P.[Venta]
      ,P.[Descuento]
      ,P.[Precio]
      ,P.[Costo]
      ,P.[Impuesto]
  FROM [dbo].[BI_SAP_Hecho_Facturas_Posiciones_Reciente] P --{{ source('BI_FARINTER','BI_SAP_Hecho_Facturas_Posiciones_Reciente') }}
	INNER JOIN [dbo].[BI_SAP_Dim_Facturas_Reciente] C --{{ source('BI_FARINTER','BI_SAP_Dim_Facturas_Reciente') }}
	on P.Factura_Id = C.Factura_Id --AND P.Sociedad_Id = C.Sociedad_Id
	INNER JOIN [dbo].[BI_Dim_Calendario] CAL --{{ source('BI_FARINTER','BI_Dim_Calendario') }}
	ON CAL.Fecha_Id = C.Fecha_Id
	WHERE C.Fecha_Id BETWEEN DATEFROMPARTS(YEAR(GETDATE())-5,01,01) AND DATEFROMPARTS(YEAR(GETDATE())-1,12,31)

