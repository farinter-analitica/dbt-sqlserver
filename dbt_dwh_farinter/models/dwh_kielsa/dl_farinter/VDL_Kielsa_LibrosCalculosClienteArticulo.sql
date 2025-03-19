{{ 
    config(
		tags=["periodo/diario", "periodo_unico/si"],
		materialized="view",
	) 
}}
--Vista 
--AXELL PADILLA -- 20230727 
SELECT --top 1000
	CA.Emp_Id
	, LHR.Ultima_Sucursal_Id
	, CA.Monedero_Id
	, CA.Articulo_Id
	, CA.Cantidad_Padre_Promedio
	, UFACTURA.Ultima_Cantidad_Padre
	, CA.Fecha_Ultima_Compra
	, CA.Veces_Comprado
	, AP.Presentación AS Presentacion
	, CAST(UFACTURA.Ultima_Cantidad_Padre * AP.Presentación AS DECIMAL(16, 4)) AS UltimaCompra_Presentacion
	, RA.Cantidad_Recetas AS Cantidad_Recetas_Articulo
	, RC.Cantidad_Recetas_Cliente
	, LHR.Patologias_Nombre
	, LHR.Patologia_1_Nombre
	, LHR.Vendedor_Id
	, RA.Consumo_Diario_Promedio
	, RA.Duracion_Tratamiento_DiasPromedio
	, (RA.Consumo_Diario_Promedio * RA.Duracion_Tratamiento_DiasPromedio) AS Necesidad_Vida_Tratamiento_Promedio
	, CASE
		WHEN (RA.Consumo_Diario_Promedio * RA.Duracion_Tratamiento_DiasPromedio) - UFACTURA.Ultima_Cantidad_Padre
			* AP.Presentación > 0
			THEN 'NO'
		ELSE 'SI'
	END AS Tratamiento_Completo
	, CASE
		WHEN (RA.Consumo_Diario_Promedio * RA.Duracion_Tratamiento_DiasPromedio) - UFACTURA.Ultima_Cantidad_Padre
			* AP.Presentación > 0
			THEN 0
		ELSE 1
	END AS Indicador_Tratamiento_Completo
	, CAST(ROUND((1.0 * UFACTURA.Ultima_Cantidad_Padre * AP.Presentación) / NULLIF(RA.Consumo_Diario_Promedio, 0), 2) AS DECIMAL(16, 4)) AS Dias_Stock_Comprados_Estimado
	, CAST(ROUND((1.0 * UFACTURA.Ultima_Cantidad_Padre * AP.Presentación) / NULLIF(RA.Consumo_Diario_Promedio, 0), 2) AS DECIMAL(16, 4))
	- DATEDIFF(DAY, CA.Fecha_Ultima_Compra, GETDATE()) AS Dias_Stock_Actual_Estimado
	, DATEADD(
		DAY
		, CASE 
          WHEN ((1.0 * UFACTURA.Ultima_Cantidad_Padre * AP.Presentación) / NULLIF(RA.Consumo_Diario_Promedio, 0)) > 3650 THEN 3650 -- Cap at 10 years
          ELSE ((1.0 * UFACTURA.Ultima_Cantidad_Padre * AP.Presentación) / NULLIF(RA.Consumo_Diario_Promedio, 0))
        END
		, CA.Fecha_Ultima_Compra) AS Contactar_Estimado_El
	, CASE
		WHEN CAST(GETDATE() AS DATE) < DATEADD(
											DAY, 
                      CASE 
                        WHEN ((1.0 * UFACTURA.Ultima_Cantidad_Padre * AP.Presentación) / NULLIF(RA.Consumo_Diario_Promedio, 0)) > 3650 THEN 3650 -- Cap at 10 years
                        ELSE ((1.0 * UFACTURA.Ultima_Cantidad_Padre * AP.Presentación) / NULLIF(RA.Consumo_Diario_Promedio, 0))
                      END                      
											, CA.Fecha_Ultima_Compra)
			THEN 'Si'
		ELSE 'No'
	END AS A_Tiempo_Estimado
	, CASE
		WHEN CAST(GETDATE() AS DATE) < DATEADD(
											DAY, 
                      CASE 
                        WHEN ((1.0 * UFACTURA.Ultima_Cantidad_Padre * AP.Presentación) / NULLIF(RA.Consumo_Diario_Promedio, 0)) > 3650 THEN 3650 -- Cap at 10 years
                        ELSE ((1.0 * UFACTURA.Ultima_Cantidad_Padre * AP.Presentación) / NULLIF(RA.Consumo_Diario_Promedio, 0))
                      END
											, CA.Fecha_Ultima_Compra)
			THEN 1
		ELSE 0
	END AS Indicador_A_Tiempo
	, ATR.Dia_Semana DiaSemana_Preferido
	, ATR.Hora Hora_Preferida
FROM
	(SELECT
		MonederoTarj_Id_Limpio AS Monedero_Id
		, Articulo_Id
		, Emp_Id
		, CAST(AVG(Cantidad_Padre) AS DECIMAL(16, 4)) AS Cantidad_Padre_Promedio
		, MAX(Consecutivo_Factura) AS Consecutivo_Ultima_Compra
		, MAX(Factura_Fecha) AS Fecha_Ultima_Compra
		, MAX(AnioMes_Id) AS AnioMes_Id_Ultima_Compra
		, COUNT(DISTINCT Factura_Id) AS Veces_Comprado
	FROM	BI_FARINTER.dbo.BI_Kielsa_Hecho_FacturaPosicion FP -- {{ ref('BI_Kielsa_Hecho_FacturaPosicion') }}
	INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Libros_Cliente LK
		ON LK.Identidad_Limpia = FP.MonederoTarj_Id_Limpio AND LK.Pais_Id = FP.Emp_Id	---OJOOOOOOOOO

	WHERE FP.Factura_Fecha BETWEEN DATEADD(Month, -6, GETDATE()) AND GETDATE()
		AND FP.AnioMes_Id = YEAR(FP.Factura_Fecha) * 100 + MONTH(FP.Factura_Fecha)
		AND FP.AnioMes_Id >= YEAR(DATEADD(Month, -6, GETDATE())) * 100 + MONTH(DATEADD(Month, -6, GETDATE()))
		AND EXISTS
		(SELECT TOP 100 * --LH.Identidad_Limpia
		FROM	DL_Kielsa_Libros_Historico LH
		WHERE LH.Identidad_Limpia = LK.Identidad_Limpia
			AND LH.Pais_Id = LK.Pais_Id
			AND LH.Fecha_Creacion BETWEEN DATEADD(Month, -6, GETDATE()) AND GETDATE()
			AND LH.AnioMes_Id = YEAR(LH.Fecha_Creacion) * 100 + MONTH(LH.Fecha_Creacion))
	GROUP BY FP.MonederoTarj_Id_Limpio, FP.Articulo_Id, FP.Emp_Id) CA
INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_ArticuloPresentacion AP
	ON AP.Articulo_Id = CA.Articulo_Id
INNER JOIN DL_FARINTER.dbo.VDL_Kielsa_RecetasCalculosArticulo RA
	ON RA.Articulo_Id = CA.Articulo_Id and RA.Emp_Id = CA.Emp_Id
LEFT JOIN
	(SELECT
		Consecutivo_Factura, Articulo_Id, AnioMes_Id, SUM(Cantidad_Padre) Ultima_Cantidad_Padre
	FROM	{{ ref('BI_Kielsa_Hecho_FacturaPosicion') }} FP
	GROUP BY AnioMes_Id, Consecutivo_Factura, Articulo_Id) UFACTURA
	ON UFACTURA.Consecutivo_Factura = CA.Consecutivo_Ultima_Compra
	AND UFACTURA.Articulo_Id = CA.Articulo_Id
	AND UFACTURA.AnioMes_Id = CA.AnioMes_Id_Ultima_Compra
LEFT JOIN AN_FARINTER.dbo.VAN_Cal_AtributosCliente_Kielsa ATR
	ON ATR.Cliente_Id = CA.Monedero_Id AND ATR.Pais_Id = CA.Emp_Id
LEFT JOIN
	(SELECT
		idpais, identidad AS Monedero_Id, COUNT(1) AS Cantidad_Recetas_Cliente
	FROM	DL_FARINTER.dbo.DL_Kielsa_RecetasCabecera
	WHERE fecha_Receta BETWEEN DATEADD(Month, -12, GETDATE()) AND GETDATE()
		AND AnioMes_Id = YEAR(fecha_Receta) * 100 + MONTH(fecha_Receta)
	GROUP BY idpais, identidad) RC
	ON RC.Monedero_Id = CA.Monedero_Id AND RC.idpais = CA.Emp_Id
INNER JOIN DL_FARINTER.dbo.VDL_Kielsa_Libros_Historico_Resumido LHR
	ON LHR.Monedero_Id = CA.Monedero_Id AND LHR.Pais_Id = CA.Emp_Id --OJJOOOOOOOOOOOOOOOOOO
																	--OFFSET 0 ROWS FETCH NEXT 1000 ROWS ONLY
