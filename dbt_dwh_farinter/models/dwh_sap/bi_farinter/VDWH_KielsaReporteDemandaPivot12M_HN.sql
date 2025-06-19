{% set unique_key_list = ["ArticuloPadre_Id"] %}
{{ 
    config(
		tags=["automation/periodo_mensual_inicio"],
		materialized="view",
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ],
		grants={'select': ['db_planning']}
	) 
}}

-- Solo se debe usar esta vista para el EXCEL de la herramienta de info kielsa.
--Incluido todo lo que consume inventario que necesite incluirse en estadistica de demanda
SELECT [ArticuloPadre_Id]
      ,[Casa_Nombre]
      ,MAX([Ultimo_Cierre]) [Ultimo_Cierre]
      ,SUM([Mes-12]) [Mes-12]
      ,SUM([Mes-11]) [Mes-11]
      ,SUM([Mes-10]) [Mes-10]
      ,SUM([Mes-09]) [Mes-09]
      ,SUM([Mes-08]) [Mes-08]
      ,SUM([Mes-07]) [Mes-07]
      ,SUM([Mes-06]) [Mes-06]
      ,SUM([Mes-05]) [Mes-05]
      ,SUM([Mes-04]) [Mes-04]
      ,SUM([Mes-03]) [Mes-03]
      ,SUM([Mes-02]) [Mes-02]
      ,SUM([Mes-01]) [Mes-01]
FROM (SELECT [ArticuloPadre_Id]
		  ,[Casa_Nombre]
		  ,[Ultimo_Cierre]
		  ,[Mes-12]
		  ,[Mes-11]
		  ,[Mes-10]
		  ,[Mes-09]
		  ,[Mes-08]
		  ,[Mes-07]
		  ,[Mes-06]
		  ,[Mes-05]
		  ,[Mes-04]
		  ,[Mes-03]
		  ,[Mes-02]
		  ,[Mes-01]
	  FROM [BI_FARINTER].[dbo].[VDWH_KielsaReporteRegaliasPivot12M_HN] -- {{ref('VDWH_KielsaReporteRegaliasPivot12M_HN')}})
	  UNION ALL
	  SELECT [ArticuloPadre_Id]
		  ,[Casa_Nombre]
		  ,[Ultimo_Cierre]
		  ,[Mes-12]
		  ,[Mes-11]
		  ,[Mes-10]
		  ,[Mes-09]
		  ,[Mes-08]
		  ,[Mes-07]
		  ,[Mes-06]
		  ,[Mes-05]
		  ,[Mes-04]
		  ,[Mes-03]
		  ,[Mes-02]
		  ,[Mes-01]
	  FROM [BI_FARINTER].[dbo].[VDWH_KielsaReporteVentasPivot12M_HN] -- {{ref('VDWH_KielsaReporteVentasPivot12M_HN')}})
	  ) Consumos
GROUP BY [ArticuloPadre_Id]
		  ,[Casa_Nombre]