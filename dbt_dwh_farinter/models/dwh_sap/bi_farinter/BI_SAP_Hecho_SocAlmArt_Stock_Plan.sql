{{ 
    config(
		materialized="view",
		tags=["periodo/diario"],
	) 
}}
--DBT DAGSTER 
{% set v_semanas_muestra = 12 %}
{% set v_dias_muestra = v_semanas_muestra * 7 %}
{% set v_fecha_inicio = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=v_dias_muestra)).strftime('%Y%m%d') %}
{% set v_fecha_fin = modules.datetime.datetime.now().strftime('%Y%m%d')  %}
{% set v_anio_mes_inicio =  v_fecha_inicio[:6]  %}

SELECT --top 100
		S.Sociedad_Id as Sociedad_Id
		, E1.Almacen_Id AS Centro_Almacen_Id
		, C.Material_Id AS Material_Id
		, CAL.Anio_Calendario AS Anio_Id
		, CAL.Mes_Calendario Mes_Id
        , CAL.Mes_Inicio AS Fecha_Id
        , (CONVERT(DECIMAL, V.Libre_Cantidad + V.Calidad_Cantidad + V.TransitoAlm_Cantidad+ V.TransitoCentro_Cantidad)) AS Stock_Cierre
		, V.DiasSin_Stock
		, (B.Gpo_Obs_Nombre_Corto) AS Gpo_Obs_Nombre_Corto
		, B.Gpo_Obs_Id
        , B.Gpo_Plan_Id COLLATE DATABASE_DEFAULT AS Gpo_Plan
		, (C.Sector_Id) AS Sector
		, (C.Articulo_Nombre) AS Material_Nombre
		, C.Articulo_Id
	FROM 	dbo.BI_SAP_Hecho_ExistenciasHist V
		INNER JOIN dbo.BI_Dim_Articulo_SAP C
			ON V.Articulo_Id = C.Articulo_Id
		INNER JOIN dbo.BI_Dim_Sociedad_SAP S
			ON V.Sociedad_Id = S.Sociedad_Id
		INNER JOIN DL_FARINTER.dbo.DL_Planning_ParamSocMat B
			ON S.Sociedad_Id = B.Sociedad_Id AND C.Articulo_Id = B.Articulo_Id
		INNER JOIN dbo.BI_Dim_Centro_SAP D
			ON V.Centro_Id = D.Centro_Id
		INNER JOIN DL_FARINTER.[dbo].[DL_Edit_AlmacenFP_SAP] E1
			ON V.Almacen_Id = E1.Almacen_Id 
		INNER JOIN dbo.BI_Dim_Calendario CAL
			ON V.AnioMes_Id = CAL.AnioMes_Id
			AND V.Fecha_Id = CAL.Fecha_Id
		WHERE CAL.Anio_Calendario >= YEAR(DATEADD(year, -5, DATEADD(MONTH, -1, EOMONTH(GETDATE()))))
			AND CAL.Fecha_Id > DATEADD(year, -5, DATEADD(MONTH, -1, EOMONTH(GETDATE())))
			AND CAL.Fecha_Id <= DATEADD(MONTH, -1, EOMONTH(GETDATE()))
			AND V.Libre_Cantidad + V.Calidad_Cantidad + V.TransitoAlm_Cantidad+ V.TransitoCentro_Cantidad > 0 
			AND V.Stock_id = 1 AND 
			E1.Planificado = 'S'	--and B.Gpo_Obs_Id = 'COINS' --and A.Sociedad_Id = '1200' 
			AND S.Sociedad_Id IN ( '1200', '1300', '1301', '1700', '2500' )