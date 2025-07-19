{{ 
    config(
		tags=["automation/periodo_mensual_inicio", "automation_only"],
        materialized="view",
    )
}}
-- Staging: Facturas de aseguradoras agrupadas por Emp, Suc, Vendedor, Fecha
WITH BaseFacturaAsegurada AS (
    SELECT
        FE.Emp_Id,
        FE.Suc_Id,
        FE.Vendedor_Id,
        COUNT(DISTINCT FE.Cliente_Id) AS Cantidad_Clientes_Asegurados,
        CAST(FE.Factura_Fecha AS DATE) AS Fecha_Id,
        COUNT(DISTINCT FE.EmpSucDocCajFac_Id) AS Cantidad_Facturas_Aseguradas,
        MAX(FE.Fecha_Actualizado) AS Fecha_Actualizado
    FROM {{ ref('BI_Kielsa_Hecho_FacturaEncabezado') }} AS FE
    INNER JOIN {{ ref('BI_Kielsa_Dim_Cliente') }} AS C
        ON
            FE.Emp_Id = C.Emp_Id
            AND FE.Cliente_Id = C.Cliente_Id
    INNER JOIN {{ ref('BI_Kielsa_Dim_TipoCliente') }} AS TC
        ON
            FE.Emp_Id = TC.Emp_Id
            AND C.Tipo_Cliente_Id = TC.TipoCliente_Id
    WHERE
        TC.TipoCliente_Nombre LIKE '%ASEGURADO%'
        AND FE.Vendedor_Id IS NOT NULL
        AND FE.Vendedor_Id != 0
    GROUP BY FE.Emp_Id, FE.Suc_Id, FE.Vendedor_Id, CAST(FE.Factura_Fecha AS DATE)
)

SELECT * FROM BaseFacturaAsegurada
