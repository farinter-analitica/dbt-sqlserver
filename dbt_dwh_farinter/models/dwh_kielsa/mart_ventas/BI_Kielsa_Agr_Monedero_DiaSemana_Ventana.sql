
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
SELECT * FROM Dia_Semana_Preferido