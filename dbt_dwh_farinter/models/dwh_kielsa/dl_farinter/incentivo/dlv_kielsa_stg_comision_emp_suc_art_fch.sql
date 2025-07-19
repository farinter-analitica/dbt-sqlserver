{{ 
    config(
		tags=["automation/periodo_mensual_inicio", "automation_only"],
        materialized="view",
    )
}}
-- Staging: Comisiones agrupadas por Emp, Suc, Art, Fecha
WITH BaseComision AS (
    SELECT
        CB.Emp_Id,
        CB.Suc_Id,
        CB.Articulo_Id,
        COUNT(DISTINCT CB.Vendedor_Id) AS Cantidad_Vendedores,
        CAST(CB.Comision_Fecha AS DATE) AS Fecha_Id,
        SUM(CB.Comision_Total) AS Comision_Total,
        SUM(CB.Comision_CantArticulo) AS Comision_CantArticulo,
        SUM(CB.Cantidad_Padre) AS Cantidad_Padre,
        MAX(CB.Fecha_Actualizado) AS Fecha_Actualizado
    FROM {{ ref('BI_Kielsa_Hecho_Comision') }} AS CB
    GROUP BY CB.Emp_Id, CB.Suc_Id, CB.Articulo_Id, CAST(CB.Comision_Fecha AS DATE)
)

SELECT * FROM BaseComision
