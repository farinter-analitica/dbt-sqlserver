
{{ 
    config(
		materialized="view",
		tags=["periodo/diario","periodo/por_hora"],
	) 
}}
--DBT DAGSTER

--20230927: Axell Padilla > Vista de las posiciones de facturas para crear el hecho.
SELECT
	FE.[Factura_Fecha] Factura_Fecha
	, FP.[Detalle_Fecha]
	, FE.[Emp_Id]
	, FE.[Suc_Id]
	, FE.[Bodega_Id]
	, FE.[Caja_Id]
	, FE.[TipoDoc_Id]
	, FE.[Factura_Id]
	, FP.[Detalle_Id]
	, FP.[Articulo_Id]
	, FP.[Detalle_Cantidad]
	, FP.[Cantidad_Padre]
	, FP.[Valor_Bruto]
	, FP.[Valor_Neto]
	, FP.[Valor_Utilidad]
	, FP.[Valor_Costo]
	, FP.[Valor_Descuento]
	, FP.[Valor_Impuesto]
	, FP.[Valor_Descuento_Financiero]
	, FP.[Valor_Acum_Monedero]
	, FP.[Valor_Descuento_Cupon]
	, FP.[Valor_Descuento_Monedero]
	, FP.[Detalle_Precio_Unitario]
	, FP.[Detalle_Descuento_Monto]
	, FP.[Detalle_Costo_Unitario]
	, FP.[Detalle_Impuesto_Monto]
	, FP.[Detalle_AcumMonedero]
	, FP.[Detalle_Saldo]
	, FP.[Detalle_Costo_Unitario_Dolar]
	, FP.[Detalle_Descuento_Porc]
	, FP.[Detalle_Impuesto_Porc]
	, FP.[Detalle_Total]
	, FP.[Detalle_PorcMonedero]
	, FP.[Detalle_Regalo]
	, FP.[Detalle_Gasto_Monto]
	, FP.[Detalle_Manejo_Costo]
	, FP.[Detalle_Costo_Fact]
	, FP.[Detalle_Unidad_Simbolo]
	, FP.[Detalle_Precio_Original]
	, FP.[Detalle_Desc_Financiero_Porc]
	, FE.[SubDoc_Id]
	, FE.[Consecutivo_Factura]
	, FE.[MonederoTarj_Id]
	, FE.Monedero_Id [MonederoTarj_Id_Limpio] --borrar cuando sea posible
	, FE.[Monedero_Id]
	, FE.[Cliente_Id]
	, FE.[Vendedor_Id]
	, FE.[Factura_Estado]
	, FE.[Factura_Origen]
	, FE.[AnioMes_Id]
	, CAL.Dia_de_la_Semana
	, E.Pais_Id
    , ART.Hash_ArticuloEmp --borrar cuando sea posible
	, ART.HashStr_ArtEmp
    , C.Hash_CasaEmp --borrar cuando sea posible
	, CASE
			WHEN FEXP.Factura_Id IS NOT NULL
				THEN 5 --eCommerce
			ELSE CASE FE.Factura_Origen
						WHEN 'FA'
							THEN 1	--Venta de sucursal
						WHEN 'EX'
							THEN 2	--Domicilio
						WHEN 'PU'
							THEN 3	--Recoger en sucursal
						WHEN 'PR'
							THEN 4	--Preventa
						ELSE 0
					END --Origen desconocido
		END 
		AS CanalVenta_Id
	, FE.HashStr_CliEmp
	, FE.HashStr_MonEmp
	, FE.HashStr_SubDDocEmp
	, FE.HashStr_SucEmp
	, FE.HashStr_SucBodEmp
	, FE.HashStr_EmplEmp
	, C.HashStr_CasaEmp 
    , SUC.Hash_SucursalEmp --borrar cuando sea posible
	, DOC.Hash_SubDocEmp --borrar cuando sea posible
	, CAST(CAST(CLI.HashStr_CliEmp AS varbinary) AS BIGINT)  Hash_ClienteEmp --borrar cuando sea posible
	, MON.Hash_MonederoEmp --borrar cuando sea posible
	, ISNULL(SAM.Tipo_Id, 0) AS Same_Id --borrar cuando sea posible
	, CASE
		WHEN FP.Valor_Utilidad >= 0
			THEN 1
		ELSE 0
	END AS TipoUtilidad_Id
	, (FP.Detalle_Cantidad * FP.Detalle_Precio_Unitario * (ISNULL(FPD.Descuento_Porcentaje, 0) / 100.0)) AS [Descuento_Proveedor]
	, FE.[HashStr_FacSucEmpDocCaj]
FROM {{ref ('BI_Kielsa_Hecho_FacturaEncabezado')}} FE
INNER JOIN (
		SELECT  [Emp_Id], [Suc_Id], [TipoDoc_id], [Caja_Id], [Factura_Id], [AnioMes_Id], [Articulo_Id]
		, MAX([Detalle_Fecha] ) AS [Detalle_Fecha]
		, MAX(FP.[Detalle_Id]) AS [Detalle_Id]
		, SUM(FP.[Detalle_Cantidad]) AS [Detalle_Cantidad]
		, SUM(FP.[Cantidad_Padre]) AS [Cantidad_Padre]
		, SUM(FP.[Valor_Bruto]) 	AS [Valor_Bruto]
		, SUM(FP.[Valor_Neto]) 	AS [Valor_Neto]
		, SUM(FP.[Valor_Utilidad]) AS [Valor_Utilidad]
		, SUM(FP.[Valor_Costo]) 	AS [Valor_Costo]
		, SUM(FP.[Valor_Descuento]) AS [Valor_Descuento]
		, SUM(FP.[Valor_Impuesto]) AS [Valor_Impuesto]
		, SUM(FP.[Valor_Descuento_Financiero]) AS [Valor_Descuento_Financiero]
		, SUM(FP.[Valor_Acum_Monedero]) AS [Valor_Acum_Monedero]
		, SUM(FP.[Valor_Descuento_Cupon]) AS [Valor_Descuento_Cupon]
		, SUM(FP.[Valor_Descuento_Monedero]) AS [Valor_Descuento_Monedero]
		, MAX(FP.[Detalle_Precio_Unitario]) AS [Detalle_Precio_Unitario]
		, MAX(FP.[Detalle_Descuento_Monto]) AS [Detalle_Descuento_Monto]
		, MAX(FP.[Detalle_Costo_Unitario]) AS [Detalle_Costo_Unitario]
		, MAX(FP.[Detalle_Impuesto_Monto]) AS [Detalle_Impuesto_Monto]
		, MAX(FP.[Detalle_AcumMonedero]) AS [Detalle_AcumMonedero]
		, MAX(FP.[Detalle_Saldo]) AS [Detalle_Saldo]
		, MAX(FP.[Detalle_Costo_Unitario_Dolar]) AS [Detalle_Costo_Unitario_Dolar]
		, MAX(FP.[Detalle_Descuento_Porc]) AS [Detalle_Descuento_Porc]
		, MAX(FP.[Detalle_Impuesto_Porc]) AS [Detalle_Impuesto_Porc]
		, MAX(FP.[Detalle_Total]) AS [Detalle_Total]
		, MAX(FP.[Detalle_PorcMonedero]) AS [Detalle_PorcMonedero]
		, MAX(CAST(FP.[Detalle_Regalo] AS SMALLINT)) AS [Detalle_Regalo]
		, MAX(FP.[Detalle_Gasto_Monto]) AS [Detalle_Gasto_Monto]
		, MAX(FP.[Detalle_Manejo_Costo]) AS [Detalle_Manejo_Costo]
		, MAX(FP.[Detalle_Costo_Fact]) AS [Detalle_Costo_Fact]
		, MAX(FP.[Detalle_Unidad_Simbolo]) AS [Detalle_Unidad_Simbolo]
		, MAX(FP.[Detalle_Precio_Original]) AS [Detalle_Precio_Original]
		, MAX(FP.[Detalle_Desc_Financiero_Porc]) 	AS [Detalle_Desc_Financiero_Porc]

		FROM {{source ('DL_FARINTER', 'DL_Kielsa_FacturasPosiciones')}} FP
		GROUP BY [AnioMes_Id], [Emp_Id], [Suc_Id], [TipoDoc_id], [Caja_Id], [Factura_Id],  [Articulo_Id]
	) FP
	ON FE.Emp_Id = FP.Emp_Id
	AND FE.Suc_Id = FP.Suc_Id
	AND FE.TipoDoc_id = FP.TipoDoc_id
	AND FE.Caja_Id = FP.Caja_Id
	AND FE.Factura_Id = FP.Factura_Id
	AND FE.AnioMes_Id = FP.AnioMes_Id
INNER JOIN {{source ('BI_FARINTER', 'BI_Dim_Calendario')}} CAL --Optimizacion por tema de particiones
	ON CAL.Fecha_Id = FE.Factura_Fecha AND CAL.AnioMes_Id = FE.AnioMes_Id
LEFT JOIN {{ref ('BI_Kielsa_Dim_Monedero')}} MON
	ON MON.Monedero_Id = FE.Monedero_Id
	AND MON.Emp_Id = FP.Emp_Id
LEFT JOIN (SELECT Emp_Id, TipoDoc_Id, Suc_Id, Caja_Id, Factura_Id, AnioMes_Id, Articulo_Id, SUM(Descuento_Porcentaje) AS Descuento_Porcentaje 
		FROM {{source ('DL_FARINTER', 'DL_Kielsa_FacturaPosicionDescuento')}} 
		WHERE Descuento_Origen = 6
		GROUP BY AnioMes_Id,Emp_Id, TipoDoc_Id, Suc_Id, Caja_Id, Factura_Id,  Articulo_Id) FPD
	ON FPD.Emp_Id = FP.Emp_Id
	AND FPD.TipoDoc_Id = FP.TipoDoc_Id
	AND FPD.Suc_Id = FP.Suc_Id
	AND FPD.Caja_Id = FP.Caja_Id
	AND FPD.Factura_Id = FP.Factura_Id
	AND FPD.Articulo_Id = FP.Articulo_Id
	AND FPD.AnioMes_Id = FP.AnioMes_Id	
LEFT JOIN {{ref ('BI_Kielsa_Dim_Articulo')}} ART
	ON ART.Articulo_Id = FP.Articulo_Id
	AND ART.Emp_Id = FP.Emp_Id
LEFT JOIN {{source ('BI_FARINTER', 'BI_Kielsa_Dim_Empresa')}} E
	ON E.Empresa_Id = FE.Emp_Id
LEFT JOIN {{ref ('BI_Kielsa_Dim_Casa')}} C
	ON C.Casa_Id = ART.Casa_Id
	AND C.Emp_Id = ART.Emp_Id
LEFT JOIN {{ref ('BI_Kielsa_Dim_Sucursal')}} SUC
    ON SUC.Sucursal_Id = FE.Suc_Id
	AND SUC.Emp_Id = FE.Emp_Id
LEFT JOIN {{source ('BI_FARINTER', 'BI_Hecho_SameSucursales_Kielsa')}} SAM
	ON SAM.Sucursal_Id_Solo = FE.Suc_Id
	AND SAM.Pais_Id = FE.Emp_Id
	AND SAM.Anio_Id = CAL.Anio_Calendario
	AND SAM.Mes_Id = CAL.Mes_Calendario
LEFT JOIN {{ref('BI_Kielsa_Dim_TipoDocumentoSub')}} DOC
    ON DOC.Documento_Id = FE.TipoDoc_Id
	AND DOC.SubDocumento_Id = FE.SubDoc_Id
	AND DOC.Emp_Id = FE.Emp_Id
LEFT JOIN {{ref('BI_Kielsa_Dim_Cliente')}} CLI
	ON CLI.Cliente_Id = FE.Cliente_Id
	AND CLI.Emp_Id = FE.Emp_Id
LEFT JOIN (SELECT DISTINCT A.Emp_Id, A.TipoDoc_Id, A.Suc_Id, A.Caja_Id, A.Factura_Id 
		FROM DL_FARINTER.dbo.DL_Kielsa_Exp_Factura_Express A
		)	FEXP
	ON FE.Emp_Id = FEXP.Emp_Id
	AND FE.TipoDoc_Id = FEXP.TipoDoc_Id
	AND FE.Suc_Id = FEXP.Suc_Id
	AND FE.Caja_Id = FEXP.Caja_Id
	AND FE.Factura_Id = FEXP.Factura_Id 
--OPTION (FORCE ORDER);
	    
--SELECT TOP 1000 * FROM BI_Kielsa_Hecho_FacturaPosicion