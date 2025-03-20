{% set unique_key_list = ["Monedero_Id","Emp_Id","Dia_Semana_Iso"] %}

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
Dia_Semana_Preferido AS (
    SELECT 
        ISNULL(FE.Monedero_Id,0) AS Monedero_Id,
        ISNULL(FE.Emp_Id,0) AS Emp_Id,
        ISNULL(CAL.Dia_de_la_Semana,0) AS Dia_Semana_Iso,
        COUNT(DISTINCT FE.Factura_Fecha) AS Conteo_Dias,
        ROW_NUMBER() OVER (PARTITION BY FE.Monedero_Id, FE.Emp_Id 
                          ORDER BY COUNT(DISTINCT FE.Factura_Fecha) DESC, CAL.Dia_de_la_Semana) AS Ranking,
        DATEFROMPARTS(
            YEAR(DATEADD(MONTH, -6, GETDATE())),
            MONTH(DATEADD(MONTH, -6, GETDATE())),
            1
        ) AS Ventana_Desde
    FROM {{ref("BI_Kielsa_Hecho_FacturaEncabezado")}} FE
    INNER JOIN {{ source('BI_FARINTER', 'BI_Dim_Calendario') }} CAL
    ON FE.Factura_Fecha = CAL.Fecha_Calendario
    WHERE FE.Factura_Fecha >= DATEFROMPARTS(
                YEAR(DATEADD(MONTH, -6, GETDATE())),
                MONTH(DATEADD(MONTH, -6, GETDATE())),
                1
            )
    GROUP BY FE.Monedero_Id, FE.Emp_Id, CAL.Dia_de_la_Semana
)
SELECT 
    DSP.*,
    CT.Total_Dias_Compra,
    CT.Total_Dias_Ventana,
    -- Porcentaje de compras en este día específico
    CAST(DSP.Conteo_Dias AS FLOAT) / NULLIF(CT.Total_Dias_Compra, 0) AS Porcentaje_Dia,
    -- Frecuencia de compra (días de compra / días totales en ventana)
    CAST(CT.Total_Dias_Compra AS FLOAT) / NULLIF(CT.Total_Dias_Ventana, 0) AS Frecuencia_Compra,
    -- Indicador de validez estadística basado en:
    -- 1. Suficientes días de compra (al menos 3)
    -- 2. Porcentaje significativo de compras en el día preferido (>25%)
    CASE 
        WHEN CT.Total_Dias_Compra >= 3 
             AND (CAST(DSP.Conteo_Dias AS FLOAT) / NULLIF(CT.Total_Dias_Compra, 0)) > 0.25
        THEN 1
        ELSE 0
    END AS Indicador_Validez_Estadistica,
    Fecha_Actualizado
FROM Dia_Semana_Preferido DSP
JOIN Compras_Totales CT ON DSP.Monedero_Id = CT.Monedero_Id AND DSP.Emp_Id = CT.Emp_Id
