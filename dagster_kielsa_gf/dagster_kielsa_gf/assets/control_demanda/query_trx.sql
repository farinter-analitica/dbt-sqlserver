
SELECT TOP 100
    FE.EmpSucDocCajFac_Id,
    FE.Emp_Id,
    FE.Suc_Id,
    FE.TipoDoc_id,
    FE.Caja_Id,
    FE.Factura_Id,
    FE.Factura_Fecha,
    FP.cantidad_unidades,
    FP.cantidad_productos,
    FP.valor_neto,
    FP.contiene_servicios,
    FP.contiene_tengo,
    TB.TipoBodega_Nombre,
    S.Departamento_Id,
    S.EmpDepMunCiu_Id,
    S.Zona_Nombre,
    S.TipoSucursal_Id,
    FP.acumula_monedero,
    TC.TipoCliente_Nombre,
    CASE WHEN TC.TipoCliente_Nombre LIKE '%TER%EDAD%' 
            OR M.Tipo_Plan LIKE '%TER%EDAD%' 
        THEN 1 ELSE 0 END AS es_tercera_edad,
    M.Tipo_Plan,
    M.Nacimiento
FROM BI_FARINTER.dbo.BI_Kielsa_Hecho_FacturaEncabezado FE
INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Bodega BD
ON FE.Emp_Id = BD.Emp_Id
AND FE.Suc_Id = BD.Sucursal_Id
AND FE.Bodega_Id = BD.Bodega_Id
INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_TipoBodega TB
ON FE.Emp_Id = TB.Emp_Id
AND BD.TipoBodega_Id = TB.TipoBodega_Id
INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal S
ON FE.Emp_Id = S.Emp_Id
AND FE.Suc_Id = S.Sucursal_Id
INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Monedero M
ON FE.Emp_Id = M.Emp_Id
AND FE.Monedero_Id = M.Monedero_Id
INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Cliente C
ON FE.Emp_Id = C.Emp_Id
AND FE.Cliente_Id = C.Cliente_Id
INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_TipoCliente TC
ON FE.Emp_Id = TC.Emp_Id
AND C.Tipo_Cliente_Id = TC.TipoCliente_Id
LEFT JOIN 
    (SELECT 
        FP.Emp_Id, 
        FP.Suc_Id, 
        FP.TipoDoc_id, 
        FP.Caja_Id, 
        FP.Factura_Id,
        SUM(FP.Cantidad_Padre) AS cantidad_unidades,
        COUNT(DISTINCT FP.Articulo_Id) AS cantidad_productos,
        SUM(FP.Valor_Neto) AS valor_neto,
        CASE WHEN SUM(FP.Valor_Acum_Monedero)>0 THEN 1 ELSE 0 END AS acumula_monedero,
        CASE WHEN MAX(CASE WHEN A.DeptoArt_Nombre LIKE '%SERVICIO%' THEN 1 ELSE 0 END) =1
            THEN 1
            ELSE 0
        END AS contiene_servicios,
        CASE WHEN MAX(CASE WHEN A.Articulo_Nombre LIKE '%TENGO%' THEN 1 ELSE 0 END) =1
            THEN 1
            ELSE 0
        END AS contiene_tengo
    FROM BI_FARINTER.dbo.BI_Kielsa_Hecho_FacturaPosicion FP
    INNER JOIN [BI_FARINTER].[dbo].[BI_Kielsa_Dim_Articulo] A
    ON FP.Articulo_Id = A.Articulo_Id
    AND FP.Emp_Id = A.Emp_Id
    GROUP BY FP.Emp_Id, FP.Suc_Id, FP.TipoDoc_id, FP.Caja_Id, FP.Factura_Id
    ) FP
	ON FE.Emp_Id = FP.Emp_Id
	AND FE.Suc_Id = FP.Suc_Id
	AND FE.TipoDoc_id = FP.TipoDoc_id
	AND FE.Caja_Id = FP.Caja_Id
	AND FE.Factura_Id = FP.Factura_Id

WHERE FE.Factura_Fecha>= '20250401' 
--AND TB.TipoBodega_Nombre = 'VENTAS'