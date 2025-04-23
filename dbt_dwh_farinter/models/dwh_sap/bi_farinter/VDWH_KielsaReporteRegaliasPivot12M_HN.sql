{% set unique_key_list = ["ArticuloPadre_Id"] %}
{{ 
    config(
		tags=["automation/periodo_mensual_inicio"],
		materialized="view",
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ]
	) 
}}

-- Solo se debe usar esta vista para el EXCEL de la herramienta de info kielsa.
SELECT RIGHT([ArticuloPadre_Id],10) [ArticuloPadre_Id]
	  ,Casa_Nombre
	  ,Ultimo_Cierre,
		[Mes-12],  
		[Mes-11],  
		[Mes-10],  
		[Mes-09],  
		[Mes-08],  
		[Mes-07],  
		[Mes-06],  
		[Mes-05],  
		[Mes-04],  
		[Mes-03],  
		[Mes-02],  
		[Mes-01]  
FROM  
    (SELECT A.[ArticuloPadre_Id]
	  ,A.Casa_Nombre
	  ,EOMONTH(GETDATE(),-1) as Ultimo_Cierre
	  ,C.Mes_NN_Relativo
      ,[Regalia_CantidadPadre]
  FROM [BI_FARINTER].[dbo].[BI_Hecho_RegaliasHist_Kielsa] R -- {{source('BI_FARINTER', 'BI_Hecho_RegaliasHist_Kielsa')}}
  inner join 
	  (SELECT Anio_Calendario, Mes_Calendario,AnioMes_Id, Mes_NN_Relativo,Fecha_Id
		FROM BI_Dim_Calendario_Dinamico C -- {{ref('BI_Dim_Calendario_Dinamico')}}
	   WHERE C.AnioMes_Id>=((YEAR(GETDATE())-1)*100+MONTH(GETDATE())) 
		AND C.AnioMes_Id<((YEAR(GETDATE()))*100+MONTH(GETDATE()))
	  ) C ON C.Fecha_Id=R.Fecha_Id
	INNER JOIN BI_Dim_ArticuloPadre_Kielsa A -- {{source('BI_FARINTER', 'BI_Dim_ArticuloPadre_Kielsa')}}
    ON A.ArticuloPadre_Id = R.ArticuloPadre_Id AND A.Pais_Id = R.Pais_Id
	WHERE R.Pais_Id=1
 )   
    AS Regalias12Meses
PIVOT  
(  
   SUM([Regalia_CantidadPadre])  
FOR   
[Mes_NN_Relativo]   
    IN ( 		[Mes-12],  
		[Mes-11],  
		[Mes-10],  
		[Mes-09],  
		[Mes-08],  
		[Mes-07],  
		[Mes-06],  
		[Mes-05],  
		[Mes-04],  
		[Mes-03],  
		[Mes-02],  
		[Mes-01]  
	)  
) AS RegaliaPivot
WHERE RIGHT([ArticuloPadre_Id],10)='10000581'