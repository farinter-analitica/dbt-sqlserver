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

-- Solo se debe usar esta vista para el EXCEL de planning.
SELECT ([ArticuloPadre_Id]) [ArticuloPadre_Id]
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
    (SELECT A.Articulo_Codigo_Padre [ArticuloPadre_Id]
	  ,CASA.Casa_Nombre Casa_Nombre
	  ,EOMONTH(GETDATE(),-1) as Ultimo_Cierre
	  ,C.Mes_NN_Relativo
      ,CAST(V.[Cantidad_Padre] AS decimal(18,4)) [Cantidad_Padre]
	  --,V.Detalle_Cantidad
  FROM DL_FARINTER.[dbo].[DL_Kielsa_FacturasPosiciones] V -- {{source('DL_FARINTER', 'DL_Kielsa_FacturasPosiciones')}}
  inner join 
	  (SELECT DISTINCT Anio_Calendario, Mes_Calendario,AnioMes_Id, Mes_NN_Relativo 
		FROM BI_Dim_Calendario_Dinamico C -- {{ref('BI_Dim_Calendario_Dinamico')}}
	   WHERE C.AnioMes_Id>=((YEAR(GETDATE())-1)*100+MONTH(GETDATE())) 
		AND C.AnioMes_Id<((YEAR(GETDATE()))*100+MONTH(GETDATE()))
	  ) C ON C.AnioMes_Id = V.AnioMes_Id
	INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Articulo A -- {{source('DL_FARINTER', 'DL_Kielsa_Articulo')}}
	ON A.Articulo_Id = V.ArticuloPadre_Id AND A.Emp_Id = V.Emp_Id
	INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Casa CASA -- {{source('DL_FARINTER', 'DL_Kielsa_Casa')}}
	ON CASA.Casa_Id = A.Casa_Id AND CASA.Emp_Id = V.Emp_Id
	WHERE V.Emp_Id=1
 )   
    AS Ventas12Meses
PIVOT  
(  
   SUM([Cantidad_Padre]  ) 
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
) AS VentasPivot
--WHERE RIGHT([ArticuloPadre_Id],10)='1110000581'

/*
SELECT top 100 ''
--A.Articulo_Codigo_Padre [ArticuloPadre_Id]
	  --,CASA.Casa_Nombre Casa_Nombre
	  ,EOMONTH(GETDATE(),-1) as Ultimo_Cierre
	  --,C.Mes_NN_Relativo
      ,V.[Cantidad_Padre]
	  ,V.Detalle_Cantidad
  FROM DL_FARINTER.[dbo].[DL_Kielsa_FacturasPosiciones] V
  inner join 
	  (SELECT DISTINCT Anio_Calendario, Mes_Calendario,AnioMes_Id, Mes_NN_Relativo 
		FROM BI_Dim_Calendario_Dinamico C
	   WHERE C.AnioMes_Id>=((YEAR(GETDATE())-1)*100+MONTH(GETDATE())) 
		AND C.AnioMes_Id<((YEAR(GETDATE()))*100+MONTH(GETDATE()))
	  ) C ON C.AnioMes_Id = V.AnioMes_Id
	INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Articulo A ON A.Articulo_Id = V.ArticuloPadre_Id AND A.Emp_Id = V.Emp_Id
	--INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Casa CASA ON CASA.Casa_Id = V.Articulo_Casa_Id AND CASA.Emp_Id = V.Emp_Id
	WHERE V.Emp_Id=1
*/