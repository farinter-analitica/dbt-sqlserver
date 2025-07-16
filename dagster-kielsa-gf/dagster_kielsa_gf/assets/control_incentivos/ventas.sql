SELECT --TOP 100
    FE.Emp_Id,
    FE.Suc_Id,
    FE.TipoDoc_id,
    FE.Caja_Id,
    FE.Factura_Id,
    FE.Factura_Fecha,
    MAX(FE.EmpSucDocCajFac_Id) AS EmpSucDocCajFac_Id,
    MAX(FE.Monedero_Id) AS Monedero_Id,
    MAX(M.Tipo_Plan) AS Tipo_Plan,
    MAX(TC.TipoCliente_Nombre) AS TipoCliente_Nombre,
    FP.Articulo_Id,
    SUM(FP.Cantidad_Padre) AS Cantidad_Padre,
    SUM(FP.Valor_Acum_Monedero) AS Valor_Acum_Monedero,
    SUM(FP.Valor_Neto) AS Valor_Neto
FROM BI_FARINTER.dbo.BI_Kielsa_Hecho_FacturaEncabezado FE
INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Hecho_FacturaPosicion FP
    ON FE.Emp_Id = FP.Emp_Id
    AND FE.Suc_Id = FP.Suc_Id
    AND FE.TipoDoc_id = FP.TipoDoc_id
    AND FE.Caja_Id = FP.Caja_Id
    AND FE.Factura_Id = FP.Factura_Id
    AND FE.Factura_Fecha = FP.Factura_Fecha
INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Monedero M
    ON FE.Emp_Id = M.Emp_Id
    AND FE.Monedero_Id = M.Monedero_Id
INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Cliente C
    ON FE.Emp_Id = C.Emp_Id
    AND FE.Cliente_Id = C.Cliente_Id
INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_TipoCliente TC
    ON FE.Emp_Id = TC.Emp_Id
    AND C.Tipo_Cliente_Id = TC.TipoCliente_Id
WHERE FE.Factura_Fecha>= '20250601' 
GROUP BY     
    FE.Factura_Fecha,
    FE.Emp_Id,
    FE.Suc_Id,
    FE.TipoDoc_id,
    FE.Caja_Id,
    FE.Factura_Id,
    FP.Articulo_Id
