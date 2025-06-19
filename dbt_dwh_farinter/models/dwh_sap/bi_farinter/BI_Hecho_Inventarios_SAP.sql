{% set unique_key_list = ["Articulo_Id", "Lote_Id", "Almacen_Id", "Sociedad_Id"] %}
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
	  [Sociedad_Id]
      ,[Centro_Id]
      ,[Almacen_Id]
      ,[Articulo_Id]
      ,[TipoArt1_Id]
      ,[Casa_Id_Actual]
      ,[Casa_Id]
      ,[FechaCrea_Id]
      ,[Lote_Id]
      ,[Carta_Id]
      ,[LoteProv_Id]
      ,[FechaCad_Id]
      ,[Fecha_Caducidad_Valida]
      ,[Stock_Id]
      ,[Fecha_Id]
      ,[AnioMes_Id]
      ,[Anio_Id]
      ,[Mes_Id]
      ,[Trimestre_Id]
      ,[DiasSin_Stock]
      ,[Libre_Cantidad]
      ,[Libre_Valor]
      ,[TransitoAlm_Cantidad]
      ,[TransitoAlm_Valor]
      ,[TransitoCentro_Cantidad]
      ,[TransitoCentro_Valor]
      ,[Calidad_Cantidad]
      ,[Calidad_Valor]
      ,[Bloqueado_Cantidad]
      ,[Bloqueado_Valor]
      ,[EntregCliente_Cantidad]
      ,[EntregCliente_Valor]
      ,[EntregTraslado_Cantidad]
      ,[EntregTraslado_Valor]
      ,[Precio]
      ,[StockTotal_Cantidad]
      ,[StockTotal_Valor]
  FROM {{ref('BI_Hecho_InventariosHist_SAP')}}
  WHERE AnioMes_Id = YEAR(GETDATE())*100+MONTH(GETDATE())