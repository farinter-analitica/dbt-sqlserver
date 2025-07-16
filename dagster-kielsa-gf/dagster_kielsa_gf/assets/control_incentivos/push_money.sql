

WITH ComisionBitacora AS (
    SELECT 
        CB.Emp_Id,
        CB.Suc_Id,
        CB.Comision_Fecha,
        CB.Vendedor_Id,
        CB.Articulo_Id,
        CB.Comision_CantArticulo,
        CB.Comision_Monto,
        CB.Fecha_Actualizado
    FROM "DL_FARINTER"."dbo"."DL_Kielsa_Comision_Bitacora" CB
    WHERE CB.Comision_Fecha >= '2025-04-01'
),

ComisionEncabezado AS (
    SELECT 
        CE.Emp_Id,
        CE.Comision_Id,
        CE.Comision_Nombre,
        CE.Comision_Fecha,
        CE.Comision_Fecha_Inicial,
        CE.Comision_Fecha_Final,
        CE.Comision_Estado,
        CE.Comision_Fec_Actualizacion,
        CE.Fecha_Actualizado
    FROM "DL_FARINTER"."dbo"."DL_Kielsa_Comision_Encabezado" CE
),

ComisionDetalle AS (
    SELECT 
        CD.Emp_Id,
        CD.Comision_Id,
        CD.Suc_Id,
        CD.Articulo_Id,
        CD.Comision_Monto AS Detalle_Comision_Monto,
        CD.Comision_Fec_Actualizacion,
        CD.Consecutivo,
        CD.Fecha_Actualizado
    FROM "DL_FARINTER"."dbo"."DL_Kielsa_Comision_Detalle" CD
),

Comision_Atributos AS (
    SELECT 
        CD.Emp_Id,
        CD.Comision_Id,
        CD.Suc_Id,
        CD.Articulo_Id,
        CD.Detalle_Comision_Monto,
        CD.Consecutivo as Detalle_Consecutivo,
        CE.Comision_Fecha_Inicial,
        CE.Comision_Fecha_Final,
        CE.Comision_Nombre,
        CE.Comision_Estado,
        (SELECT MAX(fecha) FROM (VALUES 
            (ISNULL(CE.Fecha_Actualizado, '19000101')),
            (ISNULL(CD.Fecha_Actualizado, '19000101'))
        ) AS MaxFecha(fecha)) AS Fecha_Actualizado
    FROM ComisionEncabezado CE 
    INNER JOIN ComisionDetalle CD 
        ON CE.Emp_Id = CD.Emp_Id 
        AND CE.Comision_Id = CD.Comision_Id
),

Staging AS (
-- Consulta principal que une las tres tablas
SELECT 
    -- Campos de identificación
    CB.Emp_Id,
    CB.Suc_Id,
    CB.Comision_Fecha,
    CB.Vendedor_Id,
    CB.Articulo_Id,
    
    -- Campos de la bitácora de comisiones
    CB.Comision_CantArticulo,
    CB.Comision_Monto,
    
    -- Campos del encabezado de comisiones
    CE.Comision_Id,
    CE.Comision_Nombre,
    CE.Comision_Fecha_Inicial,
    CE.Comision_Fecha_Final,
    CE.Comision_Estado,
    
    -- Campos del detalle de comisiones
    CE.Detalle_Comision_Monto,
    CE.Detalle_Consecutivo,
    ROW_NUMBER() OVER (PARTITION BY CB.Comision_Fecha, CB.Vendedor_Id, CB.Emp_Id, CB.Suc_Id, CB.Articulo_Id 
        ORDER BY CE.Comision_Fecha_Final DESC) AS rn,
    
    -- Campos de auditoría
    GETDATE() AS Fecha_Carga,
    (SELECT MAX(fecha) FROM (VALUES 
        (CB.Fecha_Actualizado),
        (CE.Fecha_Actualizado)
    ) AS MaxFecha(fecha)) AS Fecha_Actualizado
FROM ComisionBitacora CB
LEFT JOIN Comision_Atributos CE 
    ON CB.Emp_Id = CE.Emp_Id 
    AND CB.Comision_Fecha BETWEEN CE.Comision_Fecha_Inicial AND CE.Comision_Fecha_Final
    AND CB.Suc_Id = CE.Suc_Id 
    AND CB.Articulo_Id = CE.Articulo_Id
)
select count(*) from (
SELECT --TOP 1000 
    *,
    
    -- Campos para concatenación de IDs
    CAST(CONCAT("Emp_Id", '-', "Suc_Id", '-', "Comision_Id",'')  COLLATE DATABASE_DEFAULT  AS VARCHAR(51)) AS EmpSucCom_Id,
    CAST(CONCAT("Emp_Id", '-', "Suc_Id",'')  COLLATE DATABASE_DEFAULT  AS VARCHAR(50)) AS EmpSuc_Id,
    CAST(CONCAT("Emp_Id", '-', "Articulo_Id",'')  COLLATE DATABASE_DEFAULT  AS VARCHAR(50)) AS EmpArt_Id,
    CAST(CONCAT("Emp_Id", '-', "Vendedor_Id",'')  COLLATE DATABASE_DEFAULT  AS VARCHAR(50)) AS EmpVen_Id

FROM Staging
where rn=1
) xz