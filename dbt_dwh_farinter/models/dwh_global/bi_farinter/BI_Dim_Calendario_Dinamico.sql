
{{ 
    config(
		materialized="view",
		tags=["periodo/diario","periodo_unico/si"],
	) 
}}
--DBT DAGSTER

--20240903: Creado

SELECT
	C.[Fecha_Calendario]
	, C.[Anio_Calendario]
	, C.[Mes_Calendario]
	, C.[Dia_Calendario]
	, C.Trimestre_Calendario
	, C.Semana_del_Trimestre
	, C.Dia_del_Trimestre
	, C.AnioMes_Id AS AnioMes
	, C.Mes_Inicio
	, C.Mes_Fin
	, C.Es_Fin_Mes
	, C.Dias_en_Mes
	, C.Semana_del_Mes
	, C.Semana_Inicio
	, C.Semana_Fin
	, C.Mes_Nombre
	, LEFT(C.Mes_Nombre,3) Mes_Nombre_Corto
	, C.Dia_de_la_Semana
	, C.Dia_Nombre
	, C.Dia_del_Anio
	, C.Fecha_Id
	, C.Anio_ISO
	, C.Semana_del_Anio_ISO
	, C.Mes_ISO
	, C.Dia_del_Anio_ISO
	, C.Es_Fin_Anio
	, C.Es_dia_Habil
	, CNL.Json_Paises_Id AS [NoLaboral_Paises]
	, CNL.Json_Sociedades_Id AS [NoLaboral_Sociedades]
	, [Es_Inicio_Anio]
	, [Es_Bisiesto]
	, [Es_Inicio_Mes]
	, [Semana_del_Anio]
	, [YYYYMMDDext]
	, [YYYYMMDD]
	, [YYYYMM]
	, [AnioMes_Id]
	, CASE
		WHEN Fecha_Calendario = Fecha_Hoy
			THEN 'Hoy'
		WHEN Fecha_Calendario = EOMONTH(Fecha_Hoy, -1)
			THEN 'Cierre del mes anterior'
		WHEN Dia_Calendario = Dias_en_Mes AND	Fecha_Hoy BETWEEN Mes_Inicio AND Mes_Fin
			THEN 'Cierre del mes actual'
		WHEN Dia_Calendario = 15 AND Fecha_Hoy BETWEEN Mes_Inicio AND Mes_Fin
			THEN '15 del mes actual'
		WHEN Dia_de_la_Semana = 1 AND	Fecha_Calendario BETWEEN Fecha_Hoy AND DATEADD(DAY, 7, Fecha_Hoy)
			THEN 'Lunes próximo'
		WHEN Dia_de_la_Semana = 1 AND	Fecha_Calendario BETWEEN DATEADD(DAY, -7, Fecha_Hoy) AND Fecha_Hoy
			THEN 'Lunes anterior'
		WHEN Fecha_Calendario = DATEADD(DAY, 7, Fecha_Hoy)
			THEN 'En 7 días'
		WHEN Fecha_Calendario = DATEADD(DAY, -7, Fecha_Hoy)
			THEN 'Hace 7 días'
		WHEN Fecha_Calendario = EOMONTH(Fecha_Hoy, 1)
			THEN 'Fin del mes siguiente'
		WHEN Fecha_Calendario = EOMONTH(Fecha_Hoy, -2)
			THEN 'Cierre hace dos meses'
		WHEN Fecha_Calendario = DATEFROMPARTS(YEAR(Fecha_Hoy), 1, 1)
			THEN 'Inicio del año'
		WHEN Fecha_Calendario = DATEFROMPARTS(YEAR(Fecha_Hoy), 12, 31)
			THEN 'Fin del año'
		WHEN Fecha_Calendario = DATEFROMPARTS(YEAR(Fecha_Hoy) - 1, 1, 1)
			THEN 'Inicio del año anterior'
		WHEN Fecha_Calendario = DATEFROMPARTS(YEAR(Fecha_Hoy) - 1, 12, 31)
			THEN 'Fin del año anterior'
		WHEN Fecha_Calendario = DATEADD(YEAR, -1, Fecha_Hoy)
			THEN 'Hace un año'
		WHEN Fecha_Calendario = DATEADD(YEAR, 1, Fecha_Hoy)
			THEN 'En un año'
		ELSE 'No Usar'
	END AS Periodo_Diario
	, CASE
		WHEN Fecha_Calendario = Fecha_Hoy
			THEN 1
		WHEN Fecha_Calendario = EOMONTH(Fecha_Hoy, -1)
			THEN 2
		WHEN Dia_Calendario = Dias_en_Mes AND	Fecha_Hoy BETWEEN Mes_Inicio AND Mes_Fin
			THEN 3
		WHEN Dia_Calendario = 15 AND Fecha_Hoy BETWEEN Mes_Inicio AND Mes_Fin
			THEN 4
		WHEN Dia_de_la_Semana = 1 AND	Fecha_Calendario BETWEEN Fecha_Hoy AND DATEADD(DAY, 7, Fecha_Hoy)
			THEN 5
		WHEN Dia_de_la_Semana = 1 AND	Fecha_Calendario BETWEEN DATEADD(DAY, -7, Fecha_Hoy) AND Fecha_Hoy
			THEN 6
		WHEN Fecha_Calendario = DATEADD(DAY, 7, Fecha_Hoy)
			THEN 7
		WHEN Fecha_Calendario = DATEADD(DAY, -7, Fecha_Hoy)
			THEN 8
		WHEN Fecha_Calendario = EOMONTH(Fecha_Hoy, 1)
			THEN 9
		WHEN Fecha_Calendario = EOMONTH(Fecha_Hoy, -2)
			THEN 10
		WHEN Fecha_Calendario = DATEFROMPARTS(YEAR(Fecha_Hoy), 1, 1)
			THEN 11
		WHEN Fecha_Calendario = DATEFROMPARTS(YEAR(Fecha_Hoy), 12, 31)
			THEN 12
		WHEN Fecha_Calendario = DATEFROMPARTS(YEAR(Fecha_Hoy) - 1, 1, 1)
			THEN 13
		WHEN Fecha_Calendario = DATEFROMPARTS(YEAR(Fecha_Hoy) - 1, 12, 31)
			THEN 14
		WHEN Fecha_Calendario = DATEADD(YEAR, -1, Fecha_Hoy)
			THEN 15
		WHEN Fecha_Calendario = DATEADD(YEAR, 1, Fecha_Hoy)
			THEN 16
		ELSE 99
	END AS Periodo_Diario_Orden
	, CASE
		WHEN Anio_Calendario >= YEAR(Fecha_Hoy) + 2
			THEN 'En 2 años o más'
		WHEN Anio_Calendario = YEAR(Fecha_Hoy) + 1
			THEN 'Año siguiente'
		WHEN Anio_Calendario = YEAR(Fecha_Hoy)
			THEN 'Año actual'
		WHEN Anio_Calendario = YEAR(Fecha_Hoy) - 1
			THEN 'Año anterior'
		WHEN Anio_Calendario = YEAR(Fecha_Hoy) - 2
			THEN 'Hace 2 años'
		WHEN Anio_Calendario <= YEAR(Fecha_Hoy) - 3
			THEN 'Hace 3 años o más'
		ELSE 'No usar'
	END AS Periodo_Anual
	, CASE
		WHEN Anio_Calendario >= YEAR(Fecha_Hoy) + 2
			THEN 0
		WHEN Anio_Calendario = YEAR(Fecha_Hoy) + 1
			THEN 1
		WHEN Anio_Calendario = YEAR(Fecha_Hoy)
			THEN 2
		WHEN Anio_Calendario = YEAR(Fecha_Hoy) - 1
			THEN 3
		WHEN Anio_Calendario = YEAR(Fecha_Hoy) - 2
			THEN 4
		WHEN Anio_Calendario <= YEAR(Fecha_Hoy) - 3
			THEN 5
		ELSE 99
	END AS Periodo_Anual_Orden
	, CASE
		WHEN Mes_Fin >= EOMONTH(Fecha_Hoy, 2)
			THEN 'En 2 meses o más'
		WHEN Mes_Fin = EOMONTH(Fecha_Hoy, 1)
			THEN 'Mes siguiente'
		WHEN Mes_Fin = EOMONTH(Fecha_Hoy, 0)
			THEN 'Mes actual'
		WHEN Mes_Fin = EOMONTH(Fecha_Hoy, -1)
			THEN 'Mes anterior'
		WHEN Mes_Fin = EOMONTH(Fecha_Hoy, -2)
			THEN 'Hace 2 meses'
		WHEN Mes_Fin <= EOMONTH(Fecha_Hoy, -3)
			THEN 'Hace 3 meses o más'
		ELSE 'No usar'
	END AS Periodo_Mensual
	, CASE
		WHEN Mes_Fin >= EOMONTH(Fecha_Hoy, 2)
			THEN 1
		WHEN Mes_Fin = EOMONTH(Fecha_Hoy, 1)
			THEN 2
		WHEN Mes_Fin = EOMONTH(Fecha_Hoy, 0)
			THEN 3
		WHEN Mes_Fin = EOMONTH(Fecha_Hoy, -1)
			THEN 4
		WHEN Mes_Fin = EOMONTH(Fecha_Hoy, -2)
			THEN 5
		WHEN Mes_Fin <= EOMONTH(Fecha_Hoy, -3)
			THEN 6
		ELSE 99
	END AS Periodo_Mensual_Orden
	, (Anio_Calendario - YEAR(Fecha_Hoy)) * 12 + Mes_Calendario - MONTH(Fecha_Hoy) AS Meses_Desde_Fecha_Procesado
	, Anio_Calendario - YEAR(Fecha_Hoy) AS Años_Desde_Fecha_Procesado
	, GETDATE() AS Fecha_Procesado
	, DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) AS [Mes_Relativo]
	, DATEDIFF(YEAR, (Fecha_Hoy), [mes_inicio]) AS [Anio_Relativo]
	, DATEDIFF(DAY, (Fecha_Hoy), [Fecha_Calendario]) AS [Dia_Relativo]
    , CONCAT('Mes'
		, CASE
			WHEN [Mes_Fin] < EOMONTH((Fecha_Hoy), 0)
				THEN '-' 
			WHEN [Mes_Fin] = EOMONTH((Fecha_Hoy), 0)
				THEN '_'
			ELSE
				'+' 
			END
		, CASE LEN(ABS(DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]))) WHEN 1 THEN '0' ELSE '' END
		, ABS(DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]))) AS [Mes_NN_Relativo]
	, CASE
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) BETWEEN 1 AND 6
			THEN '1 - 6 Meses Proximos'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) BETWEEN 7 AND 12
			THEN '7 - 12 Meses Proximos'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) BETWEEN -6 AND -1
			THEN '1 - 6 Meses Anteriores'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) BETWEEN -12 AND -7
			THEN '7 - 12 Meses Anteriores'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) BETWEEN 13 AND 24
			THEN '13 - 24 Meses Proximos'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) BETWEEN -24 AND -13
			THEN '13 - 24 Meses Anteriores'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) < -24
			THEN 'Hace más de 24 meses'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) > 24
			THEN 'Después de 24 meses'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) = 0
			THEN 'Mes en Curso'
		ELSE NULL
	END AS [Mes_Relativo_Tipo_Semestral]
	, CASE
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) BETWEEN 1 AND 3
			THEN '1 - 3 Meses Proximos'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) BETWEEN 4 AND 6
			THEN '4 - 6 Meses Proximos'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) BETWEEN -3 AND -1
			THEN '1 - 3 Meses Anteriores'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) BETWEEN -6 AND -4
			THEN '4 - 6 Meses Anteriores'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) BETWEEN 7 AND 9
			THEN '7 - 9 Meses Proximos'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) BETWEEN 10 AND 12
			THEN '10 - 12 Meses Proximos'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) BETWEEN -9 AND -7
			THEN '7 - 9 Meses Anteriores'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) BETWEEN -12 AND -10
			THEN '10 - 12 Meses Anteriores'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) BETWEEN 13 AND 24
			THEN '13 - 24 Meses Proximos'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) BETWEEN -24 AND -13
			THEN '13 - 24 Meses Anteriores'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) < -24
			THEN 'Hace más de 24 meses'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) > 24
			THEN 'Después de 24 meses'
		WHEN DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]) = 0
			THEN 'Mes en Curso'
		ELSE NULL
	END AS [Mes_Relativo_Tipo_Trimestral]
	,CASE WHEN DATEDIFF(DAY, (Fecha_Hoy), [Fecha_Calendario])=0 THEN '[0] Días'
		WHEN DATEDIFF(DAY, (Fecha_Hoy), [Fecha_Calendario]) > 360 THEN N'(360,∞) Días'
		WHEN DATEDIFF(DAY, (Fecha_Hoy), [Fecha_Calendario]) > 0 THEN '(' + FORMAT(ROUND((DATEDIFF(DAY, (Fecha_Hoy), [Fecha_Calendario]) - 1) / 30,0,1) * 30, '0') + ', ' + FORMAT(ROUND((DATEDIFF(DAY, (Fecha_Hoy), [Fecha_Calendario]) - 1) / 30,0,1) * 30 + 30, '0') + '] Días'
		WHEN DATEDIFF(DAY, (Fecha_Hoy), [Fecha_Calendario]) < -360 THEN  N'(-∞,-360) Días' 
		ELSE '[' + FORMAT(CEILING((DATEDIFF(DAY, (Fecha_Hoy), [Fecha_Calendario]) - 1) / 30) * 30, '0') + ', ' + FORMAT(CEILING((DATEDIFF(DAY, (Fecha_Hoy), [Fecha_Calendario]) - 1) / 30) * 30 + 30, '0') + ') Días' END AS Dia_Relativo_Rango_30Dias
	,	CASE WHEN DATEDIFF(DAY, (Fecha_Hoy), [Fecha_Calendario])=0 THEN 0
		WHEN DATEDIFF(DAY, (Fecha_Hoy), [Fecha_Calendario]) > 360 THEN 900
		WHEN DATEDIFF(DAY, (Fecha_Hoy), [Fecha_Calendario]) > 0 THEN ROUND((DATEDIFF(DAY, (Fecha_Hoy), [Fecha_Calendario]) - 1) / 30,0,1) * 30 + 30
		WHEN DATEDIFF(DAY, (Fecha_Hoy), [Fecha_Calendario]) < -360 THEN  -900
		ELSE CEILING((DATEDIFF(DAY, (Fecha_Hoy), [Fecha_Calendario]) - 1) / 30) * 30 + 30 END AS Dia_Relativo_Rango_30Dias_Orden
    , FLOOR( DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio])/12.0) Ciclo12_N
    , (( DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio]))-FLOOR( DATEDIFF(MONTH, (Fecha_Hoy), [mes_inicio])/12.0)*12+1)*1 as Ciclo12_Periodo
FROM	{{ source ('BI_FARINTER', 'BI_Dim_Calendario') }} C
LEFT JOIN {{ source ('DL_FARINTER', 'DL_Edit_CalendarioNoLaboral') }} CNL
	ON CNL.Fecha_Id = C.Fecha_Calendario --DATE=DATE
OUTER APPLY
	(SELECT CAST(GETDATE() AS DATE) AS Fecha_Hoy) F
WHERE Fecha_Calendario
BETWEEN DATEFROMPARTS(YEAR(GETDATE()) - 120, 1, 1) AND DATEFROMPARTS(YEAR(GETDATE()) + 5, 12, 31)

/*
SELECT TOP 100 * FROM BI_Dim_Calendario_Dinamico WHERE Mes_Relativo BETWEEN -12 AND  12 
	and abs(Mes_Relativo) <> abs(SUBSTRING(Mes_NN_Relativo,5,5)) ORDER BY Fecha_Calendario 
*/