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

SELECT	
	'Actual' as Periodo -- Mes actual
	, E.[Sociedad_Id]
      ,E.[Centro_Id]
      ,E.[Almacen_Id]
      ,E.[Articulo_Id]
      ,E.[Casa_Id]
      ,E.[FechaCrea_Id]
      ,E.[Stock_Id]
      ,C.[Fecha_Id]
      ,C.[AnioMes_Id]
      ,E.[DiasSin_Stock]
      ,E.[Libre_Cantidad]
      ,E.[TransitoAlm_Cantidad]
      ,E.[TransitoCentro_Cantidad]
      ,E.[Calidad_Cantidad]
      ,E.[Bloqueado_Cantidad]
      ,E.[EntregCliente_Cantidad]
      ,E.[EntregTraslado_Cantidad]
      ,E.[Precio_Costo]
      ,E.[StockTotal_Cantidad]
      ,E.[StockTotal_Valor]
  FROM [DL_FARINTER].[dbo].[DL_SAP_Hecho_ExistenciasHist_Actual] E -- {{ source('DL_FARINTER', 'DL_SAP_Hecho_ExistenciasHist_Actual') }}
	INNER JOIN [DL_FARINTER].[dbo].DL_CalendarioBase C ON -- {{ source('DL_FARINTER', 'DL_CalendarioBase') }}
		C.AnioMes_Id=E.AnioMes_Id AND C.Fecha_Id = E.Fecha_Id
  UNION ALL
  SELECT
	'Reciente' as Periodo -- 12 meses
	, E.[Sociedad_Id]
      ,E.[Centro_Id]
      ,E.[Almacen_Id]
      ,E.[Articulo_Id]
      ,E.[Casa_Id]
      ,E.[FechaCrea_Id]
      ,E.[Stock_Id]
      ,C.[Fecha_Id]
      ,C.[AnioMes_Id]
      ,E.[DiasSin_Stock]
      ,E.[Libre_Cantidad]
      ,E.[TransitoAlm_Cantidad]
      ,E.[TransitoCentro_Cantidad]
      ,E.[Calidad_Cantidad]
      ,E.[Bloqueado_Cantidad]
      ,E.[EntregCliente_Cantidad]
      ,E.[EntregTraslado_Cantidad]
      ,E.[Precio_Costo]
      ,E.[StockTotal_Cantidad]
      ,E.[StockTotal_Valor]
  FROM [DL_FARINTER].[dbo].[DL_SAP_Hecho_ExistenciasHist_Reciente] E -- {{ source('DL_FARINTER', 'DL_SAP_Hecho_ExistenciasHist_Reciente') }}
	INNER JOIN [DL_FARINTER].[dbo].DL_CalendarioBase C ON -- {{ source('DL_FARINTER', 'DL_CalendarioBase') }}
		C.AnioMes_Id=E.AnioMes_Id AND C.Fecha_Id = E.Fecha_Id
  UNION ALL
  SELECT
	'Archivo' as Periodo -- > 12 meses
	, E.[Sociedad_Id]
      ,E.[Centro_Id]
      ,E.[Almacen_Id]
      ,E.[Articulo_Id]
      ,E.[Casa_Id]
      ,E.[FechaCrea_Id]
      ,E.[Stock_Id]
      ,C.[Fecha_Id]
      ,C.[AnioMes_Id]
      ,E.[DiasSin_Stock]
      ,E.[Libre_Cantidad]
      ,E.[TransitoAlm_Cantidad]
      ,E.[TransitoCentro_Cantidad]
      ,E.[Calidad_Cantidad]
      ,E.[Bloqueado_Cantidad]
      ,E.[EntregCliente_Cantidad]
      ,E.[EntregTraslado_Cantidad]
      ,E.[Precio_Costo]
      ,E.[StockTotal_Cantidad]
      ,E.[StockTotal_Valor]
  FROM [DL_FARINTER].[dbo].[DL_SAP_Hecho_ExistenciasHist_Archivo] E -- {{ source('DL_FARINTER', 'DL_SAP_Hecho_ExistenciasHist_Archivo') }}
	INNER JOIN [DL_FARINTER].[dbo].DL_CalendarioBase C ON -- {{ source('DL_FARINTER', 'DL_CalendarioBase') }}
		C.AnioMes_Id=E.AnioMes_Id AND C.Fecha_Id = E.Fecha_Id

--SELECT TOP 1000 * FROM DL_SAP_ExistenciasLoteHist