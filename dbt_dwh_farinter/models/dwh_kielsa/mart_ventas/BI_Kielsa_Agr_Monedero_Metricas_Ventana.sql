{% set unique_key_list = ["Monedero_Id","Emp_Id"] %}

{{ 
    config(
		tags=["periodo/diario", "periodo_unico/si"],
		materialized="view",
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ]
	) 
}}

WITH Metricas_Ventana AS (
    SELECT 
        ISNULL(FE.Monedero_Id, '') AS Monedero_Id,
        ISNULL(FE.Emp_Id, 0) AS Emp_Id,
        COUNT(DISTINCT FE.Factura_Fecha) AS Dias_Con_Compra,
        COUNT(DISTINCT FE.AnioMes_Id) AS Meses_Con_Compra,
        MIN(FE.Factura_Fecha) AS Fecha_Primera_Factura,
        MAX(FE.Factura_Fecha) AS Fecha_Ultima_Factura,
        AVG(DATEPART(DAY, FE.Factura_Fecha)) AS Dia_Promedio_Mes,
        STDEV(DATEPART(DAY, FE.Factura_Fecha)) AS Dia_Desviacion_Mes,
        -- Coeficiente de variación (CV) para medir la consistencia del día de compra
        CASE 
            WHEN AVG(DATEPART(DAY, FE.Factura_Fecha)) > 0 
            THEN STDEV(DATEPART(DAY, FE.Factura_Fecha)) / AVG(DATEPART(DAY, FE.Factura_Fecha)) 
            ELSE NULL 
        END AS Coeficiente_Variacion_Dia_Mes,
        DATEFROMPARTS(
            YEAR(DATEADD(MONTH, -6, GETDATE())),
            MONTH(DATEADD(MONTH, -6, GETDATE())),
            1
        ) AS Ventana_Desde
    FROM {{ ref("BI_Kielsa_Hecho_FacturaEncabezado") }} FE
        INNER JOIN {{ source('BI_FARINTER', 'BI_Dim_Calendario') }} CAL 
        ON FE.Factura_Fecha = CAL.Fecha_Calendario
    WHERE FE.Factura_Fecha >= DATEFROMPARTS(
            YEAR(DATEADD(MONTH, -6, GETDATE())),
            MONTH(DATEADD(MONTH, -6, GETDATE())),
            1
        )
        AND FE.Monedero_Id IS NOT NULL
    GROUP BY FE.Monedero_Id, FE.Emp_Id
)

-- Finalmente unimos todo
SELECT M.*,
    CASE
        WHEN M.Dia_Promedio_Mes - M.Dia_Desviacion_Mes < 1 THEN 1
        ELSE M.Dia_Promedio_Mes - M.Dia_Desviacion_Mes
    END AS Dia_Minimo_Mes,
    CASE
        WHEN M.Dia_Promedio_Mes + M.Dia_Desviacion_Mes > 31 THEN 31
        ELSE M.Dia_Promedio_Mes + M.Dia_Desviacion_Mes
    END AS Dia_Maximo_Mes,
    -- Indicador de validez estadística mejorado que considera:
    -- 1. Suficientes días de compra (mínimo 6)
    -- 2. Suficientes meses con compras (mínimo 3)
    -- 3. Consistencia en el día del mes (coeficiente de variación menor a 0.5)
    -- 4. Compras recientes (última compra en los últimos 3 meses)
    CASE
        WHEN M.Dias_Con_Compra >= 6
        AND M.Meses_Con_Compra >= 3
        AND M.Coeficiente_Variacion_Dia_Mes < 0.5
        AND M.Fecha_Ultima_Factura >= DATEADD(MONTH, -3, GETDATE()) THEN 1
        ELSE 0 -- Sin validez estadística suficiente
    END AS Dia_Mes_Validez_Estadistica
FROM Metricas_Ventana M
