SELECT top 100
    RE.Regalia_Id,
    RE.Regalia_Fecha,
    RE.Regalia_Momento AS Regalia_FechaHora,
    RE.Emp_Id,
    RE.Suc_Id,
    RE.Bodega_Id,
    RE.Caja_Id,
    RE.EmpSucCajReg_Id,
    RD.Detalle_Id,
    RD.Articulo_Id,
    RD.Articulo_Padre_Id,
    RE.Cliente_Id,
    RE.Mov_Id,
    RE.Vendedor_Id,
    RE.Operacion_Id,
    RE.Preventa_Id,
    RE.Tipo_Origen,
    RD.Vendedor_Id,
    RE.Identidad_Limpia,
    RD.Detalle_Momento,
    RD.Cantidad_Original,
    RD.Cantidad_Padre,
    RD.Valor_Costo_Unitario,
    RD.Valor_Costo_Total,
    RD.Precio_Unitario,
    RD.Valor_Impuesto
FROM [BI_FARINTER].[dbo].[BI_Kielsa_Hecho_Regalia_Detalle] RD
INNER JOIN  [BI_FARINTER].[dbo].[BI_Kielsa_Hecho_Regalia_Encabezado] RE
ON RD.Regalia_Id = RE.Regalia_Id
AND RD.Emp_Id = RE.Emp_Id
AND RD.Suc_Id = RE.Suc_Id
AND RD.Bodega_Id = RE.Bodega_Id
AND RD.Caja_Id = RE.Caja_Id
WHERE RE.Emp_Id = 1
AND 