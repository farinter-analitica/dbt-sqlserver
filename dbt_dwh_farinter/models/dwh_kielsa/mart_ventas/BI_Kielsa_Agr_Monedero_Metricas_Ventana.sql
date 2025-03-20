
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

WITH
Metricas_Ventana AS
(
    SELECT ISNULL(Monedero_Id,'') AS Monedero_Id
        , ISNULL(FE.Emp_Id,0) AS Emp_Id
		, COUNT(DISTINCT FE.Factura_Fecha) AS Dias_Con_Compra
        , COUNT(DISTINCT FE.AnioMes_Id) AS Meses_Con_Compra
		, MIN(FE.Factura_Fecha) AS Fecha_Primera_Factura
		, MAX(FE.Factura_Fecha) AS Fecha_Ultima_Factura
        , DATEFROMPARTS(
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
    GROUP BY Monedero_Id, FE.Emp_Id
)
SELECT * FROM Metricas_Ventana