{% set unique_key_list = ["Monedero_Id","Emp_Id","Hora_Id"] %}

{{ 
    config(
        tags=["periodo/diario", "periodo_unico/si"],
        materialized="view",
        post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ]
    ) 
}}

WITH
Compras_Totales AS (
    SELECT
        ISNULL(FE.Monedero_Id,0) AS Monedero_Id,
        ISNULL(FE.Emp_Id,0) AS Emp_Id,
        COUNT(DISTINCT FE.Factura_Fecha) AS Total_Dias_Compra,
        COUNT(*) AS Total_Compras,
        DATEFROMPARTS(
            YEAR(DATEADD(MONTH, -6, GETDATE())),
            MONTH(DATEADD(MONTH, -6, GETDATE())),
            1
        ) AS Ventana_Desde,
        DATEDIFF(DAY,
            DATEFROMPARTS(
                YEAR(DATEADD(MONTH, -6, GETDATE())),
                MONTH(DATEADD(MONTH, -6, GETDATE())),
                1
            ),
            GETDATE()
        ) AS Total_Dias_Ventana,
        MAX(Fecha_Actualizado) AS Fecha_Actualizado
    FROM {{ref("BI_Kielsa_Hecho_FacturaEncabezado")}} FE
    WHERE FE.Factura_Fecha >= DATEFROMPARTS(
                YEAR(DATEADD(MONTH, -6, GETDATE())),
                MONTH(DATEADD(MONTH, -6, GETDATE())),
                1
            )
    GROUP BY FE.Monedero_Id, FE.Emp_Id
),
Hora_Preferida AS (
    SELECT
        ISNULL(FE.Monedero_Id,0) AS Monedero_Id,
        ISNULL(FE.Emp_Id,0) AS Emp_Id,
        ISNULL(DATEPART(HOUR, FE.Factura_FechaHora),0) AS Hora_Id,
        COUNT(*) AS Conteo_Compras,
        COUNT(DISTINCT FE.Factura_Fecha) AS Conteo_Dias,
        ROW_NUMBER() OVER (PARTITION BY FE.Monedero_Id, FE.Emp_Id
                          ORDER BY COUNT(*) DESC, DATEPART(HOUR, FE.Factura_FechaHora)) AS Ranking,
        DATEFROMPARTS(
            YEAR(DATEADD(MONTH, -6, GETDATE())),
            MONTH(DATEADD(MONTH, -6, GETDATE())),
            1
        ) AS Ventana_Desde
    FROM {{ref("BI_Kielsa_Hecho_FacturaEncabezado")}} FE
    WHERE FE.Factura_Fecha >= DATEFROMPARTS(
                YEAR(DATEADD(MONTH, -6, GETDATE())),
                MONTH(DATEADD(MONTH, -6, GETDATE())),
                1
            )
    GROUP BY FE.Monedero_Id, FE.Emp_Id, DATEPART(HOUR, FE.Factura_FechaHora)
)
SELECT
    HP.Monedero_Id,
    HP.Emp_Id,
    HP.Hora_Id,
    --H.Hora_Formato,
    --H.Horario_Nombre,
    --H.Periodo_Dia,
    HP.Conteo_Compras,
    HP.Conteo_Dias,
    HP.Ranking,
    HP.Ventana_Desde,
    CT.Total_Dias_Compra,
    CT.Total_Compras,
    CT.Total_Dias_Ventana,
    -- Porcentaje de compras en esta hora específica
    CAST(HP.Conteo_Compras AS FLOAT) / NULLIF(CT.Total_Compras, 0) AS Porcentaje_Compras,
    -- Porcentaje de días con compras en esta hora
    CAST(HP.Conteo_Dias AS FLOAT) / NULLIF(CT.Total_Dias_Compra, 0) AS Porcentaje_Dias,
    -- Frecuencia de compra (días de compra / días totales en ventana)
    CAST(CT.Total_Dias_Compra AS FLOAT) / NULLIF(CT.Total_Dias_Ventana, 0) AS Frecuencia_Compra,
    -- Indicador de validez estadística basado en:
    -- 1. Suficientes compras totales (al menos 3)
    -- 2. Porcentaje significativo de compras en la hora preferida (>20%)
    CASE
        WHEN CT.Total_Compras >= 3
             AND (CAST(HP.Conteo_Compras AS FLOAT) / NULLIF(CT.Total_Compras, 0)) > 0.20
        THEN 1
        ELSE 0
    END AS Indicador_Validez_Estadistica,
    CT.Fecha_Actualizado
FROM Hora_Preferida HP
JOIN Compras_Totales CT 
ON HP.Monedero_Id = CT.Monedero_Id 
AND HP.Emp_Id = CT.Emp_Id
--JOIN {{ ref('BI_Dim_Hora') }} H ON HP.Hora_Id = H.Hora_Id
