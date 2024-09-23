{{ 
    config(
		materialized="view",
		tags=["periodo/por_hora"],
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
		, CONVERT(INT, A.Anio_Id) AS Anio_Id
		, CONVERT(INT, A.Mes_Id) AS Mes_Id
        , DATEFROMPARTS(A.Anio_Id, A.Mes_Id, 1) AS Fecha_Id
        , A.Cantidad_SKU AS Stock_Cierre
		, (B.Gpo_Obs_Nombre_Corto) AS Gpo_Obs_Nombre_Corto
		, B.Gpo_Obs_Id
        , B.Gpo_Plan_Id COLLATE DATABASE_DEFAULT AS Gpo_Plan
		, (C.Sector_Id) AS Sector
		, (C.Articulo_Nombre) AS Material_Nombre
		, C.Articulo_Id
	FROM
		(SELECT
			C.Anio_Calendario AS Anio_Id
			, C.Mes_Calendario Mes_Id
			, V.Sociedad_Id
			, V.Articulo_Id
			, V.Almacen_Id
			, V.Centro_Id
			, (CONVERT(DECIMAL, V.Libre_Cantidad + V.Calidad_Cantidad + V.TransitoAlm_Cantidad+ V.TransitoCentro_Cantidad)) AS Cantidad_SKU
		FROM	dbo.BI_SAP_Hecho_ExistenciasHist V
		INNER JOIN dbo.BI_Dim_Calendario C
			ON V.AnioMes_Id = C.AnioMes_Id
			AND V.Fecha_Id = C.Fecha_Id
		WHERE C.Anio_Calendario >= YEAR(DATEADD(year, -5, DATEADD(MONTH, -1, EOMONTH(GETDATE()))))
			AND C.Fecha_Id > DATEADD(year, -5, DATEADD(MONTH, -1, EOMONTH(GETDATE())))
			AND C.Fecha_Id <= DATEADD(MONTH, -1, EOMONTH(GETDATE()))
		) A
	INNER JOIN dbo.BI_Dim_Articulo_SAP C
		ON A.Articulo_Id = C.Articulo_Id
	INNER JOIN dbo.BI_Dim_Sociedad_SAP S
		ON A.Sociedad_Id = S.Sociedad_Id
	INNER JOIN DL_FARINTER.dbo.DL_Planning_ParamSocMat B
		ON S.Sociedad_Id = B.Sociedad_Id AND C.Articulo_Id = B.Articulo_Id
	INNER JOIN dbo.BI_Dim_Centro_SAP D
		ON A.Centro_Id = D.Centro_Id
	INNER JOIN DL_FARINTER.[dbo].[DL_Edit_AlmacenFP_SAP] E1
		ON A.Almacen_Id = E1.Almacen_Id
	WHERE E1.Planificado = 'S'	--and B.Gpo_Obs_Id = 'COINS' --and A.Sociedad_Id = '1200' 
        AND A.Sociedad_Id IN ( '1200', '1300', '1301', '1700', '2500' )
    --AND  A.Sociedad_Id = '1200' and B.Gpo_Obs_Id = 'FIC' --and B.Gpo_Obs_Id = 'COINS'  --AND A.Sociedad_Id = '1200'   --
