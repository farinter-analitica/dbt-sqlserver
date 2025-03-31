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

	SELECT --TOP 100
	  E.[Sociedad_Id]
      ,E.[Centro_Id]
      ,E.[Almacen_Id]
      ,A.[Articulo_Id]
	  ,A.Tipo_Material_Id AS [TipoArt1_Id]
	  ,A.Casa_Id AS [Casa_Id_Actual]
      ,E.[Casa_Id]
      ,E.[FechaCrea_Id]
	  ,EL.Lote_Id
	  ,CASE WHEN CARTA.Sociedad_Id IS NULL THEN 0 ELSE 1 END as Carta_Id --Recuperar
	  ,Coalesce(L.LoteProveedor_Id,'X') AS LoteProv_Id
	  ,Coalesce(L.Fecha_Caducidad,'9999-12-31') AS [FechaCad_Id]
	  ,Coalesce(L.Fecha_Caducidad_Valida,'') Fecha_Caducidad_Valida
      ,E.[Stock_Id]
      ,C.[Fecha_Id]
      ,C.[AnioMes_Id]
	  ,C.Anio_Calendario AS Anio_Id
	  ,C.Mes_Calendario AS Mes_Id
	  ,C.Trimestre_Calendario AS Trimestre_Id
      ,E.[DiasSin_Stock]
      ,EL.[Libre_Cantidad]
	  ,EL.Precio_Costo*EL.Libre_Cantidad Libre_Valor
      ,EL.[TransitoAlm_Cantidad]
	  ,EL.Precio_Costo*EL.[TransitoAlm_Cantidad] TransitoAlm_Valor
      ,EL.[TransitoCentro_Cantidad]
	  ,EL.Precio_Costo*EL.[TransitoCentro_Cantidad] TransitoCentro_Valor
      ,EL.[Calidad_Cantidad]
 	  ,EL.Precio_Costo*EL.[Calidad_Cantidad] Calidad_Valor
      ,EL.[Bloqueado_Cantidad]
 	  ,EL.Precio_Costo*EL.[Bloqueado_Cantidad] Bloqueado_Valor
      ,EL.[EntregCliente_Cantidad]
 	  ,EL.Precio_Costo*EL.[EntregCliente_Cantidad] EntregCliente_Valor
      ,EL.[EntregTraslado_Cantidad]
 	  ,EL.Precio_Costo*EL.[EntregTraslado_Cantidad] EntregTraslado_Valor
      ,EL.[Precio_Costo] as [Precio]
      ,EL.[StockTotal_Cantidad]
      ,EL.[StockTotal_Valor]
	  ,E.Precio_Costo [Precio_Costo_Articulo]
  FROM [DL_FARINTER].[dbo].[DL_SAP_Hecho_ExistenciasHist_Actual] E  -- {{ source('DL_FARINTER','DL_SAP_Hecho_ExistenciasHist_Actual') }}
	INNER JOIN BI_Dim_Calendario C 
		ON C.AnioMes_Id=E.AnioMes_Id AND C.Fecha_Id = E.Fecha_Id
	INNER JOIN [DL_FARINTER].[dbo].[DL_SAP_Hecho_ExistenciasLoteHist_Actual] EL  -- {{ source('DL_FARINTER','DL_SAP_Hecho_ExistenciasLoteHist_Actual') }}
		ON E.AnioMes_Id=EL.AnioMes_Id 
		AND E.Sociedad_Id=EL.Sociedad_Id 
		AND E.Centro_Id=EL.Centro_Id
		AND E.Almacen_Id=EL.Almacen_Id
		AND E.Articulo_Id=EL.Articulo_Id
	LEFT JOIN [BI_FARINTER].dbo.BI_SAP_Dim_Lote L  -- {{ source('BI_FARINTER','BI_SAP_Dim_Lote') }}
		ON L.Articulo_Id = EL.Articulo_Id
		AND L.Lote_Id = EL.Lote_Id
	INNER JOIN [BI_FARINTER].dbo.BI_Dim_Articulo_SAP A  -- {{ source('BI_FARINTER','BI_Dim_Articulo_SAP') }}
		ON A.Articulo_Id = E.Articulo_Id
	LEFT JOIN [DL_FARINTER].dbo.DL_SAP_Resumen_CartaCompras CARTA -- {{ source('DL_FARINTER','DL_SAP_Resumen_CartaCompras') }}
		ON CARTA.Sociedad_Id = E.Sociedad_Id
		AND CARTA.Articulo_Id = E.Articulo_Id
		AND CARTA.LoteProveedor_Id = L.LoteProveedor_Id
  UNION ALL
	SELECT --TOP 100
	  E.[Sociedad_Id]
      ,E.[Centro_Id]
      ,E.[Almacen_Id]
      ,A.[Articulo_Id]
	  ,A.Tipo_Material_Id AS [TipoArt1_Id]
	  ,A.Casa_Id AS [Casa_Id_Actual]
      ,E.[Casa_Id]
      ,E.[FechaCrea_Id]
	  ,EL.Lote_Id
	  ,CASE WHEN CARTA.Sociedad_Id IS NULL THEN 0 ELSE 1 END as Carta_Id --Recuperar
	  ,Coalesce(L.LoteProveedor_Id,'X') AS LoteProv_Id
	  ,Coalesce(L.Fecha_Caducidad,'9999-12-31') AS [FechaCad_Id]
	  ,Coalesce(L.Fecha_Caducidad_Valida,'') Fecha_Caducidad_Valida
      ,E.[Stock_Id]
      ,C.[Fecha_Id]
      ,C.[AnioMes_Id]
	  ,C.Anio_Calendario AS Anio_Id
	  ,C.Mes_Calendario AS Mes_Id
	  ,C.Trimestre_Calendario AS Trimestre_Id
      ,E.[DiasSin_Stock]
      ,EL.[Libre_Cantidad]
	  ,EL.Precio_Costo*EL.Libre_Cantidad Libre_Valor
      ,EL.[TransitoAlm_Cantidad]
	  ,EL.Precio_Costo*EL.[TransitoAlm_Cantidad] TransitoAlm_Valor
      ,EL.[TransitoCentro_Cantidad]
	  ,EL.Precio_Costo*EL.[TransitoCentro_Cantidad] TransitoCentro_Valor
      ,EL.[Calidad_Cantidad]
 	  ,EL.Precio_Costo*EL.[Calidad_Cantidad] Calidad_Valor
      ,EL.[Bloqueado_Cantidad]
 	  ,EL.Precio_Costo*EL.[Bloqueado_Cantidad] Bloqueado_Valor
      ,EL.[EntregCliente_Cantidad]
 	  ,EL.Precio_Costo*EL.[EntregCliente_Cantidad] EntregCliente_Valor
      ,EL.[EntregTraslado_Cantidad]
 	  ,EL.Precio_Costo*EL.[EntregTraslado_Cantidad] EntregTraslado_Valor
      ,EL.[Precio_Costo] as [Precio]
      ,EL.[StockTotal_Cantidad]
      ,EL.[StockTotal_Valor]
	  ,E.Precio_Costo [Precio_Costo_Articulo]
  FROM [DL_FARINTER].[dbo].[DL_SAP_Hecho_ExistenciasHist_Reciente] E -- {{ source('DL_FARINTER','DL_SAP_Hecho_ExistenciasHist_Reciente') }}
	INNER JOIN BI_Dim_Calendario C 
		ON C.AnioMes_Id=E.AnioMes_Id AND C.Fecha_Id = E.Fecha_Id
	INNER JOIN [DL_FARINTER].[dbo].[DL_SAP_Hecho_ExistenciasLoteHist_Reciente] EL -- {{ source('DL_FARINTER','DL_SAP_Hecho_ExistenciasLoteHist_Reciente') }}
		ON E.AnioMes_Id=EL.AnioMes_Id 
		AND E.Sociedad_Id=EL.Sociedad_Id 
		AND E.Centro_Id=EL.Centro_Id
		AND E.Almacen_Id=EL.Almacen_Id
		AND E.Articulo_Id=EL.Articulo_Id
	LEFT JOIN [BI_FARINTER].dbo.BI_SAP_Dim_Lote L
		ON L.Articulo_Id = EL.Articulo_Id
		AND L.Lote_Id = EL.Lote_Id
	INNER JOIN [BI_FARINTER].dbo.BI_Dim_Articulo_SAP A
		ON A.Articulo_Id = E.Articulo_Id
	LEFT JOIN [DL_FARINTER].dbo.DL_SAP_Resumen_CartaCompras CARTA
		ON CARTA.Sociedad_Id = E.Sociedad_Id
		AND CARTA.Articulo_Id = E.Articulo_Id
		AND CARTA.LoteProveedor_Id = L.LoteProveedor_Id
  UNION ALL
	SELECT --TOP 100
	  E.[Sociedad_Id]
      ,E.[Centro_Id]
      ,E.[Almacen_Id]
      ,A.[Articulo_Id]
	  ,A.Tipo_Material_Id AS [TipoArt1_Id]
	  ,A.Casa_Id AS [Casa_Id_Actual]
      ,E.[Casa_Id]
      ,E.[FechaCrea_Id]
	  ,EL.Lote_Id
	  ,CASE WHEN CARTA.Sociedad_Id IS NULL THEN 0 ELSE 1 END as Carta_Id --Recuperar
	  ,Coalesce(L.LoteProveedor_Id,'X') AS LoteProv_Id
	  ,Coalesce(L.Fecha_Caducidad,'9999-12-31') AS [FechaCad_Id]
	  ,Coalesce(L.Fecha_Caducidad_Valida,'') Fecha_Caducidad_Valida
      ,E.[Stock_Id]
      ,C.[Fecha_Id]
      ,C.[AnioMes_Id]
	  ,C.Anio_Calendario AS Anio_Id
	  ,C.Mes_Calendario AS Mes_Id
	  ,C.Trimestre_Calendario AS Trimestre_Id
      ,E.[DiasSin_Stock]
      ,EL.[Libre_Cantidad]
	  ,EL.Precio_Costo*EL.Libre_Cantidad Libre_Valor
      ,EL.[TransitoAlm_Cantidad]
	  ,EL.Precio_Costo*EL.[TransitoAlm_Cantidad] TransitoAlm_Valor
      ,EL.[TransitoCentro_Cantidad]
	  ,EL.Precio_Costo*EL.[TransitoCentro_Cantidad] TransitoCentro_Valor
      ,EL.[Calidad_Cantidad]
 	  ,EL.Precio_Costo*EL.[Calidad_Cantidad] Calidad_Valor
      ,EL.[Bloqueado_Cantidad]
 	  ,EL.Precio_Costo*EL.[Bloqueado_Cantidad] Bloqueado_Valor
      ,EL.[EntregCliente_Cantidad]
 	  ,EL.Precio_Costo*EL.[EntregCliente_Cantidad] EntregCliente_Valor
      ,EL.[EntregTraslado_Cantidad]
 	  ,EL.Precio_Costo*EL.[EntregTraslado_Cantidad] EntregTraslado_Valor
      ,EL.[Precio_Costo] as [Precio]
      ,EL.[StockTotal_Cantidad]
      ,EL.[StockTotal_Valor]
	  ,E.Precio_Costo [Precio_Costo_Articulo]
  FROM [DL_FARINTER].[dbo].[DL_SAP_Hecho_ExistenciasHist_Archivo] E  -- {{ source('DL_FARINTER','DL_SAP_Hecho_ExistenciasHist_Archivo') }}
	INNER JOIN BI_Dim_Calendario C 
		ON C.AnioMes_Id=E.AnioMes_Id AND C.Fecha_Id = E.Fecha_Id
	INNER JOIN [DL_FARINTER].[dbo].[DL_SAP_Hecho_ExistenciasLoteHist_Archivo] EL  -- {{ source('DL_FARINTER','DL_SAP_Hecho_ExistenciasLoteHist_Archivo') }}
		ON E.AnioMes_Id=EL.AnioMes_Id 
		AND E.Sociedad_Id=EL.Sociedad_Id 
		AND E.Centro_Id=EL.Centro_Id
		AND E.Almacen_Id=EL.Almacen_Id
		AND E.Articulo_Id=EL.Articulo_Id
	LEFT JOIN [BI_FARINTER].dbo.BI_SAP_Dim_Lote L
		ON L.Articulo_Id = EL.Articulo_Id
		AND L.Lote_Id = EL.Lote_Id
	INNER JOIN [BI_FARINTER].dbo.BI_Dim_Articulo_SAP A
		ON A.Articulo_Id = E.Articulo_Id
	LEFT JOIN [DL_FARINTER].dbo.DL_SAP_Resumen_CartaCompras CARTA
		ON CARTA.Sociedad_Id = E.Sociedad_Id
		AND CARTA.Articulo_Id = E.Articulo_Id
		AND CARTA.LoteProveedor_Id = L.LoteProveedor_Id