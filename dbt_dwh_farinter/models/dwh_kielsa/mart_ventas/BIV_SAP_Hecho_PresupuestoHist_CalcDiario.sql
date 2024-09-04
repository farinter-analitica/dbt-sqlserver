
{{ 
    config(
		materialized="view",
		tags=["periodo/diario"],
	) 
}}
--DBT DAGSTER

SELECT --top 1000
	PH	.[Sociedad_Id]
	, PH.[Zona_Id]
	, PH.[Division_Id]
	, PH.[Articulo_Id]
	, PH.[Cliente_Id]
	, PH.[Casa_Id]
	, PH.[Vendedor_Id]
	, CAL.[Fecha_Calendario] AS Fecha_Id
	, PH.Monto AS Monto_Mensual
	, CAST(ROUND(CASE WHEN CAL.Es_dia_Habil = 1 THEN PH.[Monto] * 1.0 
		/ SUM(CAL.Es_dia_Habil) OVER (PARTITION BY CAL.AnioMes_Id, S.Pais_Id, S.Sociedad_Id)
		ELSE 0.0 END, 2) AS DECIMAL(16, 4)) AS [Monto_Diario]
	, SUM(CAL.Es_dia_Habil) OVER (PARTITION BY CAL.AnioMes_Id, S.Pais_Id, S.Sociedad_Id) AS Dias_Laborales
	, CAL.[AnioMes_Id]
	, S.Pais_Id
	, CAL.Es_dia_Habil
FROM	{{ source ('BI_FARINTER', 'BI_SAP_Hecho_PresupuestoHist') }} PH
INNER JOIN {{ ref ('BI_SAP_Dim_Sociedad')}} S
	ON S.Sociedad_Id = PH.Sociedad_Id
INNER JOIN {{ ref ('BI_Dim_Calendario_LaboralPais')}} CAL
	ON	CAL.[AnioMes_Id] = PH.[AnioMes_Id] 
	AND CAL.Pais_ISO2 = S.Pais_Id --AND CAL.Sociedad_Id  = S.Sociedad_Id
--     WHERE PH.[Cliente_Id] = '0000100758'
-- 	--AND PH.[Casa_Id] = 'F00070'
-- 	AND PH.[Vendedor_Id] = '121'
-- 	AND [Sociedad_Id] = '1200'
-- 	AND [Zona_Id] = '000002'
--     and cal.Es_dia_Habil = 0
-- ORDER BY Fecha_Id